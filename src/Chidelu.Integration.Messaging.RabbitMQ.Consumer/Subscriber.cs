using Microsoft.Extensions.Logging;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal sealed class Subscriber(
    IServiceProvider serviceProvider,
    ConsumerOptions options,
    ConsumerHandlerMap map,
    ILogger<Subscriber> logger)
    : MessageConsumerBase(serviceProvider, options, map, logger), ISubscriber
{
    public ISubscriber AddHandler<TMessage, THandler>()
        where THandler : class, IMessageHandler<TMessage>
    {
        AddHandlerInternal<TMessage, THandler>();
        return this;
    }

    public Task StartAsync(CancellationToken cancellationToken)
        => StartInternalAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken)
        => StopInternalAsync(cancellationToken);
}
