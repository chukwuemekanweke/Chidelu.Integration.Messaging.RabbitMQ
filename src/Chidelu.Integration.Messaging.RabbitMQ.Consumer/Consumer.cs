using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal sealed class Consumer(
    IServiceProvider serviceProvider,
    ConsumerOptions options,
    ConsumerHandlerMap map,
    ILogger<Consumer>? logger = null) : IConsumer, IAsyncDisposable
{
    private readonly ILogger<Consumer> _logger = logger ?? NullLogger<Consumer>.Instance;
    private readonly ConcurrentDictionary<string, HandlerRegistry> _handlers = new(StringComparer.Ordinal);
    private int _handlersLoaded;
    private MessageDispatcher? _dispatcher;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private IConnection? _conn;
    private IChannel? _ch;
    private string? _consumerTag;
    private CancellationTokenSource? _stopCts;

    public IConsumer AddHandler<TMessage, THandler>()
        where THandler : class, IMessageHandler<TMessage>
    {
        var key = typeof(TMessage).FullName ?? typeof(TMessage).Name;

        var added = _handlers.TryAdd(
            key,
            new HandlerRegistry(
                typeof(TMessage),
                async (sp, obj, ct) =>
                {
#if NET8_0_OR_GREATER
                    await using var scope = sp.CreateAsyncScope();
#else
                    using var scope = sp.CreateScope();
#endif
                    var handler = scope.ServiceProvider.GetRequiredService<THandler>();
                    await handler.HandleAsync((TMessage)obj, ct);
                }));

        if (!added)
        {
            throw new InvalidOperationException($"Handler already registered for message type '{key}'.");
        }

        return this;
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_ch is not null)
        {
            return;
        }

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_ch is not null)
            {
                return;
            }

            var cfg = options.Config;
            var cf = new ConnectionFactory
            {
                HostName = cfg.HostName,
                Port = cfg.Port,
                UserName = cfg.UserName,
                Password = cfg.Password,
                VirtualHost = cfg.VirtualHost,
                ConsumerDispatchConcurrency = cfg.ConcurrentMessageCount
            };

            _conn = await cf.CreateConnectionAsync($"{cfg.ServiceName}-consumer");
            _ch = await _conn.CreateChannelAsync();

            if (cfg.PrefetchCount > 0)
            {
                await _ch.BasicQosAsync(0, cfg.PrefetchCount, false, cancellationToken);
            }

            await EnsureTopologyAsync(cancellationToken);
        }
        finally
        {
            _initLock.Release();
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);

        if (_consumerTag is not null)
        {
            return;
        }

        LoadHandlersFromMap();

        _stopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var channel = _ch!;
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += HandleMessageAsync;

        _consumerTag = await channel.BasicConsumeAsync(
            queue: options.Config.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if(_stopCts != null)
        {
            await _stopCts.CancelAsync();
        }

        if (_ch is null || _consumerTag is null)
        {
            return;
        }

        try
        {
            await _ch.BasicCancelAsync(_consumerTag, cancellationToken: cancellationToken);
        }
        finally
        {
            _consumerTag = null;
        }
    }

    private async Task HandleMessageAsync(object sender, BasicDeliverEventArgs args)
    {
        if (_ch is null)
        {
            return;
        }

        var cancellationToken = _stopCts?.Token ?? CancellationToken.None;
        var envelope = new MessageEnvelope
        {
            Body = args.Body,
            Headers = args.BasicProperties.Headers,
            DeliveryTag = args.DeliveryTag
        };

        var outcome = await GetDispatcher()
            .DispatchAsync(envelope, cancellationToken);

        switch (outcome)
        {
            case DispatchOutcome.Ack:
                await _ch.BasicAckAsync(args.DeliveryTag, multiple: false, cancellationToken: cancellationToken);
                break;
            case DispatchOutcome.NackDrop:
                await _ch.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: false, cancellationToken: cancellationToken);
                break;
            case DispatchOutcome.NackRequeue:
                await _ch.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: true, cancellationToken: cancellationToken);
                break;
        }
    }

    private async Task EnsureTopologyAsync(CancellationToken cancellationToken)
    {
        var cfg = options.Config;
        var deadLetterRoutingKey = cfg.DeadLetterQueue;

        var mainArgs = new Dictionary<string, object?>
        {
            [QueueArgumentKeys.QueueType] = QueueArgumentKeys.QueueTypeQuorum,
            [QueueArgumentKeys.DeliveryLimit] = cfg.MaxRetryCount
        };

        if (!string.IsNullOrWhiteSpace(cfg.DeadLetterQueue))
        {
            mainArgs[QueueArgumentKeys.DeadLetterExchange] = cfg.DeadLetterExchange;
            mainArgs[QueueArgumentKeys.DeadLetterRoutingKey] = deadLetterRoutingKey;
        }

        await _ch!.QueueDeclareAsync(
            queue: cfg.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: mainArgs,
            cancellationToken: cancellationToken);

        if (!string.IsNullOrWhiteSpace(cfg.ExchangeName))
        {
            await _ch.ExchangeDeclareAsync(
                exchange: cfg.ExchangeName,
                type: cfg.ExchangeType,
                durable: true,
                autoDelete: false,
                arguments: null,
                cancellationToken: cancellationToken);

            var routingKeys = ResolveRoutingKeys(cfg.ExchangeType, cfg.ExchangeName);
            foreach (var rk in routingKeys)
            {
                await _ch.QueueBindAsync(
                    queue: cfg.QueueName,
                    exchange: cfg.ExchangeName,
                    routingKey: rk,
                    arguments: null,
                    cancellationToken: cancellationToken);
            }
        }

        if (!string.IsNullOrWhiteSpace(cfg.DeadLetterQueue))
        {
            if (!string.IsNullOrWhiteSpace(cfg.DeadLetterExchange))
            {
                await _ch.ExchangeDeclareAsync(
                    exchange: cfg.DeadLetterExchange,
                    type: ExchangeType.Direct,
                    durable: true,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: cancellationToken);
            }

            var dlqArgs = new Dictionary<string, object?>
            {
                [QueueArgumentKeys.QueueType] = QueueArgumentKeys.QueueTypeQuorum,
                [QueueArgumentKeys.DeadLetterExchange] = string.Empty,
                [QueueArgumentKeys.DeadLetterRoutingKey] = cfg.QueueName
            };

            await _ch.QueueDeclareAsync(
                queue: cfg.DeadLetterQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: dlqArgs,
                cancellationToken: cancellationToken);

            if (!string.IsNullOrWhiteSpace(cfg.DeadLetterExchange))
            {
                await _ch.QueueBindAsync(
                    queue: cfg.DeadLetterQueue,
                    exchange: cfg.DeadLetterExchange,
                    routingKey: deadLetterRoutingKey,
                    arguments: null,
                    cancellationToken: cancellationToken);
            }
        }
    }

    private void LoadHandlersFromMap()
    {
        if (Interlocked.Exchange(ref _handlersLoaded, 1) == 1)
        {
            return;
        }

        foreach (var d in map.Pairs)
        {
            var key = d.MessageType.FullName ?? d.MessageType.Name;
            _handlers.TryAdd(key, new HandlerRegistry(d.MessageType, d.Invoker));
        }
    }

    private IReadOnlyList<string> ResolveRoutingKeys(string exchangeType, string exchangeName)
    {
        if (string.Equals(exchangeType, ExchangeType.Direct, StringComparison.OrdinalIgnoreCase))
        {
            return ResolveMessageTypeRoutingKeys(exchangeName);
        }

        if (string.Equals(exchangeType, ExchangeType.Fanout, StringComparison.OrdinalIgnoreCase))
        {
            return new[] { string.Empty };
        }

        return ResolveMessageTypeRoutingKeys(exchangeName);
    }

    private IReadOnlyList<string> ResolveMessageTypeRoutingKeys(string exchangeName)
    {
        var keys = GetRegisteredMessageTypes()
            .Select(t => t.AssemblyQualifiedName
                ?? t.FullName
                ?? t.Name)
            .Where(k => !string.IsNullOrWhiteSpace(k))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        if (keys.Length == 0)
        {
            throw new InvalidOperationException(
                $"No message handlers registered to infer routing keys for exchange '{exchangeName}'.");
        }

        return keys;
    }

    private IEnumerable<Type> GetRegisteredMessageTypes()
    {
        foreach (var d in map.Pairs)
        {
            yield return d.MessageType;
        }

        foreach (var handler in _handlers.Values)
        {
            yield return handler.MessageType;
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_ch is not null)
            {
                if (_consumerTag is not null)
                {
                    await _ch.BasicCancelAsync(_consumerTag);
                }

                await _ch.CloseAsync();
                await _ch.DisposeAsync();
            }
        }
        finally
        {
            if (_conn is not null)
            {
                await _conn.CloseAsync();
                await _conn.DisposeAsync();
            }
        }
    }

    private MessageDispatcher GetDispatcher()
    {
        return _dispatcher ??= new MessageDispatcher(
            serviceProvider,
            options.Serializer,
            _logger,
            ResolveHandler);
    }

    private HandlerRegistry? ResolveHandler(string key)
        => _handlers.TryGetValue(key, out var registration) ? registration : null;
}
