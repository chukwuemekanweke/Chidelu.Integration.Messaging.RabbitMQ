using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Chidelu.Integration.Messaging.RabbitMQ.Core.Exceptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Diagnostics;
using Headers = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal sealed class Consumer(
    IServiceProvider serviceProvider,
    ConsumerOptions options,
    ConsumerHandlerMap map,
    ILogger<Consumer>? logger = null) : IConsumer, IAsyncDisposable, IAsyncInitializable
{
    private readonly ILogger<Consumer> _logger = logger ?? NullLogger<Consumer>.Instance;
    private readonly ConcurrentDictionary<string, HandlerRegistry> _handlers = new(StringComparer.Ordinal);
    private int _handlersLoaded;

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

    public async Task InitializeAsync(CancellationToken cancellationToken)
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

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_ch is null)
        {
            throw new InvalidOperationException("Consumer not initialized. Call InitializeAsync() before starting.");
        }

        if (_consumerTag is not null)
        {
            return;
        }

        LoadHandlersFromMap();

        _stopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var consumer = new AsyncEventingBasicConsumer(_ch);
        consumer.ReceivedAsync += HandleMessageAsync;

        _consumerTag = await _ch.BasicConsumeAsync(
            queue: options.Config.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopCts?.Cancel();

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

        var ct = _stopCts?.Token ?? CancellationToken.None;
        var headers = args.BasicProperties.Headers;

        using var activity = BeginActivityScopeFromMetadata(headers);

        try
        {
            var assemblyQualifiedType = Headers.GetRequiredString(headers, KnownMetadata.Type);
            var messageType = ResolveMessageType(assemblyQualifiedType);
            var handlerKey = messageType.FullName ?? messageType.Name;

            if (!_handlers.TryGetValue(handlerKey, out var registration))
            {
                throw new CannotProcessMessageNonTransientException($"No handler registered for '{handlerKey}'.");
            }

            var deserialized = options.Serializer.Deserialize(args.Body.Span, messageType)
                ?? throw new DeserializationException(
                    $"Deserialization returned null. MessageType: {messageType}");

            await registration.Invoke(serviceProvider, deserialized, ct).ConfigureAwait(false);

            await _ch.BasicAckAsync(args.DeliveryTag, multiple: false, cancellationToken: ct);
        }
        catch (FailedToProcessMessageException ex)
        {
            await HandleNonRetryableAsync(args, ex, "failed-to-process", ct);
        }
        catch (Exception ex) when (ex is CannotProcessMessageNonTransientException || ex is DeserializationException)
        {
            await HandleNonRetryableAsync(args, ex, "non-transient", ct);
        }
        catch (Exception ex)
        {
            await HandleRetryableAsync(args, ex, ct);
        }
    }

    private async Task HandleRetryableAsync(
        BasicDeliverEventArgs args,
        Exception ex,
        CancellationToken ct)
    {
        _logger.LogError(ex, "Message processing failed; requeueing.");
        await _ch!.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: true, cancellationToken: ct);
    }

    private async Task HandleNonRetryableAsync(
        BasicDeliverEventArgs args,
        Exception ex,
        string reason,
        CancellationToken ct)
    {
        _logger.LogError(ex, "Message rejected without requeue. Reason: {Reason}.", reason);
        await _ch!.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: false, cancellationToken: ct);
    }

    private async Task EnsureTopologyAsync(CancellationToken ct)
    {
        var cfg = options.Config;
        var deadLetterRoutingKey = cfg.DeadLetterQueue;

        var mainArgs = new Dictionary<string, object>
        {
            ["x-queue-type"] = "quorum",
            ["x-delivery-limit"] = cfg.MaxRetryCount
        };

        if (!string.IsNullOrWhiteSpace(cfg.DeadLetterQueue))
        {
            mainArgs["x-dead-letter-exchange"] = cfg.DeadLetterExchange;
            mainArgs["x-dead-letter-routing-key"] = deadLetterRoutingKey;
        }

        await _ch!.QueueDeclareAsync(
            queue: cfg.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: mainArgs,
            cancellationToken: ct);

        if (!string.IsNullOrWhiteSpace(cfg.ExchangeName))
        {
            await _ch.ExchangeDeclareAsync(
                exchange: cfg.ExchangeName,
                type: cfg.ExchangeType,
                durable: true,
                autoDelete: false,
                arguments: null,
                cancellationToken: ct);

            var routingKeys = ResolveRoutingKeys(cfg.ExchangeType, cfg.ExchangeName);
            foreach (var rk in routingKeys)
            {
                await _ch.QueueBindAsync(
                    queue: cfg.QueueName,
                    exchange: cfg.ExchangeName,
                    routingKey: rk,
                    arguments: null,
                    cancellationToken: ct);
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
                    cancellationToken: ct);
            }

            var dlqArgs = new Dictionary<string, object>
            {
                ["x-queue-type"] = "quorum",
                ["x-dead-letter-exchange"] = string.Empty,
                ["x-dead-letter-routing-key"] = cfg.QueueName
            };

            await _ch.QueueDeclareAsync(
                queue: cfg.DeadLetterQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: dlqArgs,
                cancellationToken: ct);

            if (!string.IsNullOrWhiteSpace(cfg.DeadLetterExchange))
            {
                await _ch.QueueBindAsync(
                    queue: cfg.DeadLetterQueue,
                    exchange: cfg.DeadLetterExchange,
                    routingKey: deadLetterRoutingKey,
                    arguments: null,
                    cancellationToken: ct);
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

    private static Type ResolveMessageType(string assemblyQualifiedType)
    {
        var resolved =
            Type.GetType(assemblyQualifiedType, throwOnError: false)
            ?? AppDomain.CurrentDomain.GetAssemblies()
                .Select(a => a.GetType(assemblyQualifiedType, throwOnError: false))
                .FirstOrDefault(t => t is not null);

        return resolved ?? throw new DeserializationException(
            $"Unable to resolve message type '{assemblyQualifiedType}'.");
    }

    private static IDisposable BeginActivityScopeFromMetadata(IDictionary<string, object>? headers)
    {
        var parentId = Headers.GetString(headers, KnownMetadata.OriginatingOperationId);
        if (!string.IsNullOrWhiteSpace(parentId))
        {
            var activity = new Activity("rabbitmq-consumer").SetParentId(parentId);
            activity.Start();
            return activity;
        }

        return new NoopScope();
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
                _ch.Dispose();
            }
        }
        finally
        {
            if (_conn is not null)
            {
                await _conn.CloseAsync();
                _conn.Dispose();
            }
        }
    }

    private sealed class NoopScope : IDisposable { public void Dispose() { } }
}
