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
    }

    public async Task PublishAsync<T>(
        T @event,
        string? routingKey = null,
        IDictionary<string, string>? extraHeaders = null,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (_ch is null)
        {
            throw new InvalidOperationException("Publisher not initialized. Call InitializeAsync() before publishing.");
        }

        var rk = routingKey ?? typeof(T).Name;
        var body = opt.Serializer.Serialize(@event);

        var props = CreateProps(typeof(T), extraHeaders);

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
        string? routingKey = null,
        IDictionary<string, string>? sharedHeaders = null,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (_ch is null)
        {
            throw new InvalidOperationException("Publisher not initialized. Call InitializeAsync() before publishing.");
        }

        var rk = routingKey ?? typeof(T).Name;
        var batch = _ch.CreateBasicPublishBatch();

        var props = CreateProps(typeof(T), sharedHeaders);

        var count = 0;
        foreach (var e in events)
        {
            var body = opt.Serializer.Serialize(e);
            batch.Add(
                exchange: opt.Config.EventsExchange ?? string.Empty,
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

    private IBasicProperties CreateProps(Type messageType, IDictionary<string, string>? extraHeaders)
    {
        var basicProperties = _ch!.CreateBasicProperties();
        basicProperties.Persistent = true;
        basicProperties.ContentType = "application/json";
        basicProperties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        basicProperties.AppId = opt.Config.AppId ?? opt.Config.ServiceName;

        var headers = basicProperties.Headers ??= new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        Headers.SetString(headers, KnownMetadata.Type, messageType.AssemblyQualifiedName!);

        if (extraHeaders?.TryGetValue(KnownMetadata.MessageId, out var mid) == true 
            && !string.IsNullOrWhiteSpace(mid))
        {
            Headers.SetString(headers, KnownMetadata.MessageId, mid);
        }
        else
        {
            Headers.SetGuid(headers, KnownMetadata.MessageId, Guid.NewGuid());
        }

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

    public static bool IsReservedHeader(string key)
        => string.Equals(key, KnownMetadata.Type, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.MessageId, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.CorrelationId, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.CausationId, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.OriginatingOperationId, StringComparison.OrdinalIgnoreCase);

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
