namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

public interface IConsumer
{
    IConsumer AddHandler<TMessage, THandler>()
        where THandler : class, IMessageHandler<TMessage>;

    Task StartAsync(CancellationToken cancellationToken);
    Task StopAsync(CancellationToken cancellationToken);
}
