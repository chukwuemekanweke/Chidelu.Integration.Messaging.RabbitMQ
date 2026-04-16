using Chidelu.Integration.Messaging.RabbitMQ.Core;
using RabbitMQ.Client;
using System.Diagnostics;
using Headers = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public sealed class Sender(SenderOptions opt)
    : ISender, IAsyncDisposable
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

            _conn = await cf.CreateConnectionAsync($"{opt.Config.ServiceName}-commands", cancellationToken);
            _ch = await _conn.CreateChannelAsync(cancellationToken: cancellationToken);

            await TopologyInstaller.EnsureAsync(_ch, opt.Config, cancellationToken);
        }
        finally
        {
            _initLock.Release();
        }
    }

    public async Task SendAsync<T>(
        T command,
        CancellationToken cancellationToken,
        IDictionary<string, string>? extraHeaders = null)
        where T : ICommand
    {
        cancellationToken.ThrowIfCancellationRequested();
        await EnsureInitializedAsync(cancellationToken);

        if (string.IsNullOrWhiteSpace(opt.Config.CommandsExchange))
        {
            throw new InvalidOperationException("CommandsExchange is required for command publishing.");
        }

        var channel = _ch!;
        var exchange = opt.Config.CommandsExchange!;
        var routingKey = ResolveRoutingKey<T>();

        var body = opt.Serializer.Serialize(command);
        var properties = BuildProperties(command, extraHeaders);

        await channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: properties,
            body: body,
            cancellationToken: cancellationToken);
    }

    private BasicProperties BuildProperties<T>(T command, IDictionary<string, string>? extraHeaders)
        where T : ICommand
    {
        var properties = CreateBaseProperties(extraHeaders);
        var headers = properties.Headers ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        Headers.SetString(headers, KnownMetadata.Type, typeof(T).AssemblyQualifiedName!);
        Headers.SetGuid(headers, KnownMetadata.MessageId, ResolveMessageId(command));

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

        return properties;
    }

    private BasicProperties CreateBaseProperties(IDictionary<string, string>? extraHeaders)
    {
        var properties = new BasicProperties();
        properties.Persistent = true;
        properties.ContentType = "application/json";
        properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        var headers = properties.Headers ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var currentContext = opt.MessageContextAccessor.Current;

        if (extraHeaders?.TryGetValue(KnownMetadata.CorrelationId, out var corr) == true
            && !string.IsNullOrWhiteSpace(corr))
        {
            Headers.SetString(headers, KnownMetadata.CorrelationId, corr);
        }
        else if (!string.IsNullOrWhiteSpace(currentContext?.CorrelationId))
        {
            Headers.SetString(headers, KnownMetadata.CorrelationId, currentContext.CorrelationId!);
        }

        if (extraHeaders?.TryGetValue(KnownMetadata.CausationId, out var caus) == true
            && !string.IsNullOrWhiteSpace(caus))
        {
            Headers.SetString(headers, KnownMetadata.CausationId, caus);
        }
        else if (currentContext?.MessageId is Guid incomingMessageId && incomingMessageId != Guid.Empty)
        {
            Headers.SetString(headers, KnownMetadata.CausationId, incomingMessageId.ToString());
        }

        if (Activity.Current is { } act)
        {
            Headers.SetString(headers, KnownMetadata.ParentOperationId, act.Id!);
        }

        return properties;
    }

    private static string ResolveRoutingKey<T>()
        => typeof(T).FullName
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
            await _ch.DisposeAsync();

        }

        if (_conn is not null)
        {
            await _conn.CloseAsync();
            await _conn.DisposeAsync();
        }
    }
}
