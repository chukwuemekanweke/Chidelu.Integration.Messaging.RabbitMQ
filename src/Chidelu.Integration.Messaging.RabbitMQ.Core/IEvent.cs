namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public interface IEvent
{
    Guid MessageId { get; }
}
