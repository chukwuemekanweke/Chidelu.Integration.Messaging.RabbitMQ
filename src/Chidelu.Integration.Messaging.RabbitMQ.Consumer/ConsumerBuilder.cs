using Microsoft.Extensions.DependencyInjection;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

public sealed class ConsumerBuilder(IServiceCollection services, ConsumerHandlerMap map)
{
    public ConsumerBuilder AddHandler<TMessage, THandler>(ServiceLifetime lifetime = ServiceLifetime.Scoped)
        where THandler : class, IMessageHandler<TMessage>
    {
        services.Add(new ServiceDescriptor(typeof(THandler), typeof(THandler), lifetime));
        services.Add(new ServiceDescriptor(typeof(IMessageHandler<TMessage>), typeof(THandler), lifetime));

        map.Add(new ConsumerHandlerMap.HandlerDescriptor(
            typeof(TMessage),
            typeof(THandler),
            async (sp, obj, ct) =>
            {
#if NET8_0_OR_GREATER
                await using var scope = sp.CreateAsyncScope();
#else
                using var scope = sp.CreateScope();
#endif
                var handler = scope.ServiceProvider.GetRequiredService<THandler>();
                await handler.HandleAsync((TMessage)obj, ct);
            }));

        return this;
    }
}
