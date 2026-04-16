using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

public interface IMessageContext
{
    IReadOnlyDictionary<string, object?> Headers { get; }
    string? MessageType { get; }
    Guid? MessageId { get; }
    string? CorrelationId { get; }
    string? CausationId { get; }
    string? OriginatingOperationId { get; }
    string? GetHeader(string key);
}
