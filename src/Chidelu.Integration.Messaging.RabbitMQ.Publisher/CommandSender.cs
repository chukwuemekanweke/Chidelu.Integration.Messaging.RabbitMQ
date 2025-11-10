using Chidelu.Integration.Messaging.RabbitMQ.Core;
using RabbitMQ.Client;
using System.Diagnostics;
using Headers = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public sealed class CommandSender(PublisherOptions opt) 
    : ICommandSender, IAsyncDisposable, IAsyncInitializable
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

        _conn = await cf.CreateConnectionAsync($"{opt.Config.ServiceName}-commands");
        _ch = await _conn.CreateChannelAsync();
    }

    public async Task SendAsync<T>(
        T command,
        string queueName,
        IDictionary<string, string>? extraHeaders = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await _ch!.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

        var body = opt.Serializer.Serialize(command);
        var props = CreateProps(typeof(T), extraHeaders);

        await _ch.BasicPublishAsync(
            exchange: opt.Config.CommandsExchange,
            routingKey: queueName,
            mandatory: false,
            basicProperties: props,
            body: body,
            cancellationToken: cancellationToken);
    }

    private IBasicProperties CreateProps(Type messageType, IDictionary<string, string>? extraHeaders)
    {
        var p = _ch!.CreateBasicProperties();
        p.Persistent = true;
        p.ContentType = "application/json";
        p.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        p.AppId = opt.Config.AppId ?? opt.Config.ServiceName;

        var headers = p.Headers ??= new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
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

        if (extraHeaders?.TryGetValue(KnownMetadata.CorrelationId, out var corr) == true
            && !string.IsNullOrWhiteSpace(corr))
        {
            Headers.SetString(headers, KnownMetadata.CorrelationId, corr);
        }
        if (extraHeaders?.TryGetValue(KnownMetadata.CausationId, out var caus) == true
            && !string.IsNullOrWhiteSpace(caus))
        {
            Headers.SetString(headers, KnownMetadata.CausationId, caus);
        }

        if (Activity.Current is { } act)
        {
            Headers.SetString(headers, KnownMetadata.OriginatingOperationId, act.Id!);
        }

        if (extraHeaders is not null)
        {
            foreach (var kv in extraHeaders)
            {
                if (Publisher.IsReservedHeader(kv.Key))
                {
                    continue;
                }
                Headers.SetString(headers, kv.Key, kv.Value);
            }
        }

        return p;
    }

    public async ValueTask DisposeAsync()
    {
        if(_ch is not null) 
        {
            await _ch.CloseAsync();
            _ch.Dispose();

        }

        if(_conn is not null)
        {
            await _conn.CloseAsync();
            _conn.Dispose();
        }                     
    }
}