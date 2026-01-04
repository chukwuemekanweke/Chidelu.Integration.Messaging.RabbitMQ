using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddPublisher(
        this IServiceCollection services,
        PublisherConfig config,
        string? dependencyInjectionKey = null)
    {
        var key = dependencyInjectionKey ?? config.Key;
        services.TryAddSingleton<IRabbitSerializer, DefaultRabbitSerializer>();

        services.AddKeyedSingleton<PublisherOptions>(key, (sp, _) =>
        {
            var serializer = sp.GetRequiredService<IRabbitSerializer>();
            return new PublisherOptions(config, serializer);
        });

        services.AddKeyedSingleton<IPublisher>(key, (sp, _) =>
        {
            var options = sp.GetRequiredKeyedService<PublisherOptions>(key);
            return new Publisher(options);
        });

        return services;
    }

    public static IServiceCollection AddSender(
        this IServiceCollection services,
        SenderConfig config,
        string? dependencyInjectionKey = null)
    {
        var key = dependencyInjectionKey ?? config.Key;
        services.TryAddSingleton<IRabbitSerializer, DefaultRabbitSerializer>();

        services.AddKeyedSingleton<SenderOptions>(key, (sp, _) =>
        {
            var serializer = sp.GetRequiredService<IRabbitSerializer>();
            return new SenderOptions(config, serializer);
        });

        services.AddKeyedSingleton<ISender>(key, (sp, _) =>
        {
            var options = sp.GetRequiredKeyedService<SenderOptions>(key);
            return new Sender(options);
        });

        return services;
    }
}
