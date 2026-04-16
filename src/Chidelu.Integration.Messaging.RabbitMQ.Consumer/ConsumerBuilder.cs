using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Microsoft.Extensions.DependencyInjection;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

public sealed class ConsumerBuilder
{
    private readonly IServiceCollection _services;
    private readonly ConsumerHandlerMap _map;

    internal ConsumerBuilder(IServiceCollection services, ConsumerHandlerMap map)
    {
        _services = services;
        _map = map;
    }

    public ConsumerBuilder AddHandler<TMessage, THandler>(ServiceLifetime lifetime = ServiceLifetime.Scoped)
        where THandler : class, IMessageHandler<TMessage>
    {
        _services.Add(new ServiceDescriptor(typeof(THandler), typeof(THandler), lifetime));
        _services.Add(new ServiceDescriptor(typeof(IMessageHandler<TMessage>), typeof(THandler), lifetime));

        _map.Add(new ConsumerHandlerMap.HandlerDescriptor(
            typeof(TMessage),
            typeof(THandler),
            async (sp, obj, envelope, ct) =>
            {
#if NET8_0_OR_GREATER
                await using var scope = sp.CreateAsyncScope();
#else
                using var scope = sp.CreateScope();
#endif
                var messageContextAccessor = scope.ServiceProvider.GetRequiredService<IMessageContextAccessor>();
                var previous = messageContextAccessor.Current;
                scope.ServiceProvider.GetRequiredService<MessageContext>().SetHeaders(envelope.Headers);
                var handler = scope.ServiceProvider.GetRequiredService<THandler>();
                try
                {
                    await handler.HandleAsync((TMessage)obj, ct);
                }
                finally
                {
                    messageContextAccessor.Current = previous;
                }
            }));

        return this;
    }
}
