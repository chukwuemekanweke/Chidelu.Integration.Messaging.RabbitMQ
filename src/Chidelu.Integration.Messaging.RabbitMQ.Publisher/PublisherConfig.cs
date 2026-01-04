namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public sealed class PublisherConfig
{
    public required string ServiceName { get; init; }
    public required string HostName { get; init; }

    public required int Port { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
    public required string VirtualHost { get; init; }

    public required string EventsExchange { get; init; }

    public ushort ConcurrentMessageCount { get; init; }

    public string Key => $"{HostName}:{Port}:{VirtualHost}:{ServiceName}";
}
