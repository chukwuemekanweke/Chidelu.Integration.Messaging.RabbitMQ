namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public interface ICommand
{
    Guid MessageId { get; }
}
