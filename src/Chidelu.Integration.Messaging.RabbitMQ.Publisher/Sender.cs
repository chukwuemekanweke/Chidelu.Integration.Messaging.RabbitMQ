using Chidelu.Integration.Messaging.RabbitMQ.Core;
using RabbitMQ.Client;
using System.Diagnostics;
using Headers = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public sealed class Sender(SenderOptions opt)
    : ISender, IAsyncDisposable, IAsyncInitializable
{
    private IConnection? _conn;
    private IChannel? _ch;

    public async Task InitializeAsync(CancellationToken cancellationToken)
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

        await TopologyInstaller.EnsureAsync(_ch, opt.Config);
    }

    public async Task SendAsync<T>(
        T command,
        IDictionary<string, string>? extraHeaders = null,
        CancellationToken cancellationToken = default)
        where T : ICommand
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_ch is null)
        {
            throw new InvalidOperationException("Sender not initialized. Call InitializeAsync() before sending.");
        }

        if (string.IsNullOrWhiteSpace(opt.Config.CommandsExchange))
        {
            throw new InvalidOperationException("CommandsExchange is required for command publishing.");
        }

        var exchange = opt.Config.CommandsExchange!;
        var routingKey = ResolveRoutingKey<T>();

        var body = opt.Serializer.Serialize(command);
        var props = CreateProps(command, extraHeaders);

        await _ch.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: props,
            body: body,
            cancellationToken: cancellationToken);
    }

    private IBasicProperties CreateProps<T>(T command, IDictionary<string, string>? extraHeaders)
        where T : ICommand
    {
        var p = _ch!.CreateBasicProperties();
        p.Persistent = true;
        p.ContentType = "application/json";
        p.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        p.AppId = opt.Config.ServiceName;

        var headers = p.Headers ??= new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        Headers.SetString(headers, KnownMetadata.Type, typeof(T).AssemblyQualifiedName!);
        Headers.SetGuid(headers, KnownMetadata.MessageId, ResolveMessageId(command));

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

    private static string ResolveRoutingKey<T>()
        => typeof(T).AssemblyQualifiedName
           ?? typeof(T).FullName
           ?? typeof(T).Name;

    private static Guid ResolveMessageId<T>(T command)
        where T : ICommand
    {
        if (command.MessageId != Guid.Empty)
        {
            EnsureGuid7(command.MessageId);
            return command.MessageId;
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
