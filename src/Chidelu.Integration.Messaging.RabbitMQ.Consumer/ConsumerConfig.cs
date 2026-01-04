namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

public sealed class ConsumerConfig
{
    public required string ServiceName { get; init; }
    public required string HostName { get; init; }
    public required int Port { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
    public required string VirtualHost { get; init; }

    public required string QueueName { get; init; }
    public required string ExchangeName { get; init; }

    public ushort PrefetchCount { get; init; } = 10;
    public int MaxRetryCount { get; init; } = 10;

    public ushort ConcurrentMessageCount { get; init; } = 1;

    public string? DependencyInjectionKey { get; init; }

    public string Key => DependencyInjectionKey ?? $"{HostName}:{Port}:{VirtualHost}:{ServiceName}:{QueueName}";
}
