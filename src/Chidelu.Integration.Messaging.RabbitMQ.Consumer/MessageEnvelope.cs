namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal sealed class MessageEnvelope
{
    public required ReadOnlyMemory<byte> Body { get; init; }
    public required IDictionary<string, object?>? Headers { get; init; }
    public required ulong DeliveryTag { get; init; }
}
