namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public interface IMessageContextAccessor
{
    IMessageMetadataContext? Current { get; set; }
}
