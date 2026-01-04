using Chidelu.Integration.Messaging.RabbitMQ.Core;
using RabbitMQ.Client;
using System.Diagnostics;
using Headers = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public sealed class Publisher(PublisherOptions opt) : IPublisher, IAsyncDisposable, IAsyncInitializable
{
    private IConnection? _conn;
    private IChannel? _ch;

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var cf = new ConnectionFactory
        {
            HostName = opt.Config.HostName,
            Port = opt.Config.Port,
            UserName = opt.Config.UserName,
            Password = opt.Config.Password,
            VirtualHost = opt.Config.VirtualHost,
            ConsumerDispatchConcurrency = opt.Config.ConcurrentMessageCount
        };

        _conn = await cf.CreateConnectionAsync($"{opt.Config.ServiceName}-publisher");
        _ch = await _conn.CreateChannelAsync();

        await TopologyInstaller.EnsureAsync(_ch, opt.Config);
    }

    public async Task PublishAsync<T>(
        T @event,
        IDictionary<string, string>? extraHeaders = null,
        CancellationToken ct = default)
        where T : IEvent
    {
        ct.ThrowIfCancellationRequested();

        if (_ch is null)
        {
            throw new InvalidOperationException("Publisher not initialized. Call InitializeAsync() before publishing.");
        }

        var rk = ResolveRoutingKey<T>();
        var body = opt.Serializer.Serialize(@event);

        var props = CreateProps(@event, extraHeaders);

        if(string.IsNullOrWhiteSpace(opt.Config.EventsExchange))
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(opt.Config.EventsExchange, nameof(opt.Config.EventsExchange));
        }

        await _ch.BasicPublishAsync(
            exchange: opt.Config.EventsExchange!,
            routingKey: rk,
            mandatory: false,
            basicProperties: props,
            body: body,
            cancellationToken: ct);
    }

    public Task PublishBatchAsync<T>(
        IEnumerable<T> events,
        IDictionary<string, string>? sharedHeaders = null,
        CancellationToken ct = default)
        where T : IEvent
    {
        ct.ThrowIfCancellationRequested();

        if (_ch is null)
        {
            throw new InvalidOperationException("Publisher not initialized. Call InitializeAsync() before publishing.");
        }

        if (string.IsNullOrWhiteSpace(opt.Config.EventsExchange))
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(opt.Config.EventsExchange, nameof(opt.Config.EventsExchange));
        }

        var rk = ResolveRoutingKey<T>();
        var batch = _ch.CreateBasicPublishBatch();

        var exchange = opt.Config.EventsExchange!;

        var count = 0;
        foreach (var e in events)
        {
            var body = opt.Serializer.Serialize(e);
            var props = CreateProps(e, sharedHeaders);
            batch.Add(
                exchange: exchange,
                routingKey: rk,
                mandatory: false,
                properties: props,
                body: body);
            count++;
        }

        if (count > 0)
        {
            batch.Publish();
        }
        return Task.CompletedTask;
    }

    private IBasicProperties CreateProps<T>(T @event, IDictionary<string, string>? extraHeaders)
        where T : IEvent
    {
        var basicProperties = CreateBaseProps(extraHeaders);
        var headers = basicProperties.Headers ??= new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        Headers.SetString(headers, KnownMetadata.Type, typeof(T).AssemblyQualifiedName!);
        Headers.SetGuid(headers, KnownMetadata.MessageId, ResolveMessageId(@event));

        if (extraHeaders is not null)
        {
            foreach (var kv in extraHeaders)
            {
                if (IsReservedHeader(kv.Key))
                {
                    continue;
                }
                Headers.SetString(headers, kv.Key, kv.Value);
            }
        }

        return basicProperties;
    }

    private IBasicProperties CreateBaseProps(IDictionary<string, string>? extraHeaders)
    {
        var basicProperties = _ch!.CreateBasicProperties();
        basicProperties.Persistent = true;
        basicProperties.ContentType = "application/json";
        basicProperties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        basicProperties.AppId = opt.Config.ServiceName;

        var headers = basicProperties.Headers ??= new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        if (extraHeaders?.TryGetValue(KnownMetadata.CorrelationId, out var correlationId) == true
            && !string.IsNullOrWhiteSpace(correlationId))
        {
            Headers.SetString(headers, KnownMetadata.CorrelationId, correlationId);
        }
        if (extraHeaders?.TryGetValue(KnownMetadata.CausationId, out var causationId) == true
            && !string.IsNullOrWhiteSpace(causationId))
        {
            Headers.SetString(headers, KnownMetadata.CausationId, causationId);
        }

        if (Activity.Current is { } act)
        {
            Headers.SetString(headers, KnownMetadata.OriginatingOperationId, act.Id!);
        }

        return basicProperties;
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

    public static bool IsReservedHeader(string key)
        => string.Equals(key, KnownMetadata.Type, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.MessageId, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.CorrelationId, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.CausationId, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.OriginatingOperationId, StringComparison.OrdinalIgnoreCase);

    private static string ResolveRoutingKey<T>()
        => typeof(T).AssemblyQualifiedName
           ?? typeof(T).FullName
           ?? typeof(T).Name;

    public async ValueTask DisposeAsync()
    {
        if (_ch is not null)
        {
            await _ch.CloseAsync();
            _ch.Dispose();

        }

        if (_conn is not null)
        {
            await _conn.CloseAsync();
            _conn.Dispose();
        }
    }
}
