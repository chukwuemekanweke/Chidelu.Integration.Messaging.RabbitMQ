using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

public interface IMessageContext : IMessageMetadataContext
{
    string? GetHeader(string key);
}
