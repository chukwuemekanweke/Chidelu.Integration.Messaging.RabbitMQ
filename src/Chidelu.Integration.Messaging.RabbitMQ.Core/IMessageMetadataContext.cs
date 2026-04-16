namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public interface IMessageMetadataContext
{
    IReadOnlyDictionary<string, object?> Headers { get; }
    string? MessageType { get; }
    Guid? MessageId { get; }
    string? CorrelationId { get; }
    string? CausationId { get; }
    string? ParentOperationId { get; }
}
