namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

public interface ISubscriber
{
    ISubscriber AddHandler<TMessage, THandler>()
        where THandler : class, IMessageHandler<TMessage>;

    Task StartAsync(CancellationToken cancellationToken);
    Task StopAsync(CancellationToken cancellationToken);
}
