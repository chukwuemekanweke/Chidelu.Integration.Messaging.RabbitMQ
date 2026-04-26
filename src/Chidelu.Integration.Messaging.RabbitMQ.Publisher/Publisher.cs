using Chidelu.Integration.Messaging.RabbitMQ.Core;
using RabbitMQ.Client;
using System.Diagnostics;
using Headers = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public sealed class Publisher(PublisherOptions opt) : IPublisher, IAsyncDisposable
{
    private IConnection? _conn;
    private IChannel? _ch;
    private readonly SemaphoreSlim _initLock = new(1, 1);

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

            var cf = new ConnectionFactory
            {
                HostName = opt.Config.HostName,
                Port = opt.Config.Port,
                UserName = opt.Config.UserName,
                Password = opt.Config.Password,
                VirtualHost = opt.Config.VirtualHost
            };

            _conn = await cf.CreateConnectionAsync($"{opt.Config.ServiceName}-publisher", cancellationToken);
            _ch = await _conn.CreateChannelAsync(cancellationToken: cancellationToken);

            await TopologyInstaller.EnsureAsync(_ch, opt.Config, cancellationToken);
        }
        finally
        {
            _initLock.Release();
        }
    }

    public async Task PublishAsync<T>(
        T @event,
        CancellationToken cancellationToken,
        IDictionary<string, string>? extraHeaders = null)
        where T : IEvent
    {
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureInitializedAsync(cancellationToken);

        using var activity = BeginPublishActivity(extraHeaders);

        var channel = _ch!;
        var routingKey = ResolveRoutingKey<T>();
        var body = opt.Serializer.Serialize(@event);

        var properties = BuildProperties(@event, extraHeaders);

        if(string.IsNullOrWhiteSpace(opt.Config.EventsExchange))
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(opt.Config.EventsExchange, nameof(opt.Config.EventsExchange));
        }

        await channel.BasicPublishAsync(
            exchange: opt.Config.EventsExchange!,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: properties,
            body: body,
            cancellationToken: cancellationToken);
    }

    public async Task PublishBatchAsync<T>(
        IEnumerable<T> events,
        CancellationToken cancellationToken,
        IDictionary<string, string>? sharedHeaders = null)
        where T : IEvent
    {
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureInitializedAsync(cancellationToken);

        if (string.IsNullOrWhiteSpace(opt.Config.EventsExchange))
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(opt.Config.EventsExchange, nameof(opt.Config.EventsExchange));
        }

        var channel = _ch!;
        var routingKey = ResolveRoutingKey<T>();
        var exchange = opt.Config.EventsExchange!;

        using (var activity = BeginPublishActivity(sharedHeaders))
        {
            foreach (var e in events)
            {
                var body = opt.Serializer.Serialize(e);
                var properties = BuildProperties(e, sharedHeaders);

                await channel.BasicPublishAsync(
                    exchange: exchange,
                    routingKey: routingKey,
                    mandatory: false,
                    basicProperties: properties,
                    body: body,
                    cancellationToken: cancellationToken);
            }
        }
    }

    private BasicProperties BuildProperties<T>(T @event, IDictionary<string, string>? extraHeaders)
        where T : IEvent
    {
        var basicProperties = CreateBaseProperties(extraHeaders);
        var headers = basicProperties.Headers ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        Headers.SetString(headers, KnownMetadata.Type, typeof(T).AssemblyQualifiedName!);
        Headers.SetGuid(headers, KnownMetadata.MessageId, ResolveMessageId(@event));

        if (extraHeaders is not null)
        {
            foreach (var kv in extraHeaders)
            {
                if (Headers.IsReservedHeader(kv.Key))
                {
                    continue;
                }
                Headers.SetString(headers, kv.Key, kv.Value);
            }
        }

        return basicProperties;
    }

    private BasicProperties CreateBaseProperties(IDictionary<string, string>? extraHeaders)
    {
        var basicProperties = new BasicProperties();
        basicProperties.Persistent = true;
        basicProperties.ContentType = "application/json";
        basicProperties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        var headers = basicProperties.Headers ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var currentContext = opt.MessageContextAccessor.Current;

        if (extraHeaders?.TryGetValue(KnownMetadata.CorrelationId, out var correlationId) == true
            && !string.IsNullOrWhiteSpace(correlationId))
        {
            Headers.SetString(headers, KnownMetadata.CorrelationId, correlationId);
        }
        else if (!string.IsNullOrWhiteSpace(currentContext?.CorrelationId))
        {
            Headers.SetString(headers, KnownMetadata.CorrelationId, currentContext.CorrelationId!);
        }

        if (extraHeaders?.TryGetValue(KnownMetadata.CausationId, out var causationId) == true
            && !string.IsNullOrWhiteSpace(causationId))
        {
            Headers.SetString(headers, KnownMetadata.CausationId, causationId);
        }
        else if (currentContext?.MessageId is Guid incomingMessageId && incomingMessageId != Guid.Empty)
        {
            Headers.SetString(headers, KnownMetadata.CausationId, incomingMessageId.ToString());
        }

        if (Activity.Current is { } act)
        {
            Headers.SetString(headers, KnownMetadata.ParentOperationId, act.Id!);
        }
        else if (extraHeaders?.TryGetValue(KnownMetadata.ParentOperationId, out var parentOperationId) == true
            && !string.IsNullOrWhiteSpace(parentOperationId))
        {
            Headers.SetString(headers, KnownMetadata.ParentOperationId, parentOperationId);
        }

        return basicProperties;
    }

    private static Activity? BeginPublishActivity(IDictionary<string, string>? extraHeaders)
    {
        var parentId = extraHeaders?.TryGetValue(KnownMetadata.ParentOperationId, out var value) == true
            ? value
            : Activity.Current?.Id;

        return RabbitMqDiagnostics.StartActivity("rabbitmq-publish", ActivityKind.Producer, parentId);
    }

    private static Guid ResolveMessageId<T>(T @event)
        where T : IEvent
    {
        if (@event.MessageId != Guid.Empty)
        {
            EnsureGuid7(@event.MessageId);
            return @event.MessageId;
        }

        return Guid.CreateVersion7();
    }

    private static void EnsureGuid7(Guid messageId)
    {
        var text = messageId.ToString("D");
        if (text.Length <= 14 || text[14] != '7')
        {
            throw new ArgumentException("MessageId must be a UUIDv7.", nameof(messageId));
        }
    }

    private static string ResolveRoutingKey<T>()
        => typeof(T).FullName
           ?? typeof(T).Name;

    public async ValueTask DisposeAsync()
    {
        if (_ch is not null)
        {
            await _ch.CloseAsync();
            await _ch.DisposeAsync();
        }

        if (_conn is not null)
        {
            await _conn.CloseAsync();
            await _conn.DisposeAsync();
        }
    }
}
