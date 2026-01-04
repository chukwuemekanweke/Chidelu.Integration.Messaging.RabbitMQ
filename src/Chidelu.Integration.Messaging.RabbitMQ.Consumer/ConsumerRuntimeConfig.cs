namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal sealed class ConsumerRuntimeConfig
{
    public required string ServiceName { get; init; }
    public required string HostName { get; init; }
    public required int Port { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
    public required string VirtualHost { get; init; }

    public required string QueueName { get; init; }
    public required string DeadLetterQueue { get; init; }
    public required string ExchangeName { get; init; }
    public required string ExchangeType { get; init; }

    public ushort PrefetchCount { get; init; } = 10;
    public int MaxRetryCount { get; init; } = 10;

    public required string DeadLetterExchange { get; init; }

    public ushort ConcurrentMessageCount { get; init; } = 1;

    public string Key => $"{HostName}:{Port}:{VirtualHost}:{ServiceName}:{QueueName}";
}
