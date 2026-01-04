namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

public interface IMessageHandler<in T>
{
    Task HandleAsync(T message, CancellationToken cancellationToken);
}
