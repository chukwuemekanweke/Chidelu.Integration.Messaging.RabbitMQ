namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public sealed class SenderConfig
{
    public required string ServiceName { get; init; }
    public required string HostName { get; init; }
    public required int Port { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
    public required string VirtualHost { get; init; }

    public required string CommandsExchange { get; init; }

    public string? DependencyInjectionKey { get; init; }

    public string Key => DependencyInjectionKey ?? $"{HostName}:{Port}:{VirtualHost}:{ServiceName}";
}
