using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSubscriber(
        this IServiceCollection services,
        SubscriberConfig config,
        Action<ConsumerBuilder> configure,
        string? dependencyInjectionKey = null)
    {
        var runtimeConfig = BuildRuntimeConfig(config, ExchangeType.Fanout);
        var dependencyKeyName = dependencyInjectionKey ?? config.Key;
        return AddSubscriberInternal(services, runtimeConfig, configure, dependencyKeyName);
    }

    public static IServiceCollection AddConsumer(
        this IServiceCollection services,
        ConsumerConfig config,
        Action<ConsumerBuilder> configure,
        string? dependencyInjectionKey = null)
    {
        var runtimeConfig = BuildRuntimeConfig(config, ExchangeType.Direct);
        var dependencyKeyName = dependencyInjectionKey ?? config.Key;
        return AddConsumerInternal(services, runtimeConfig, configure, dependencyKeyName);
    }

    private static IServiceCollection AddConsumerInternal(
        IServiceCollection services,
        ConsumerRuntimeConfig config,
        Action<ConsumerBuilder> configure,
        string dependencyInjectionKey)
    {
        services.TryAddSingleton<IRabbitSerializer, DefaultRabbitSerializer>();
        services.TryAddScoped<MessageContext>();
        services.TryAddScoped<IMessageContext>(sp => sp.GetRequiredService<MessageContext>());

        services.AddKeyedSingleton<ConsumerOptions>(dependencyInjectionKey, (sp, _) =>
        {
            var serializer = sp.GetRequiredService<IRabbitSerializer>();
            return new ConsumerOptions(config, serializer);
        });

        var map = new ConsumerHandlerMap();
        configure(new ConsumerBuilder(services, map));
        services.AddKeyedSingleton<ConsumerHandlerMap>(dependencyInjectionKey, map);

        services.AddKeyedSingleton<IConsumer>(dependencyInjectionKey, (sp, _) =>
        {
            var options = sp.GetRequiredKeyedService<ConsumerOptions>(dependencyInjectionKey);
            var handlerMap = sp.GetRequiredKeyedService<ConsumerHandlerMap>(dependencyInjectionKey);

            var logger = sp.GetRequiredService<ILogger<Consumer>>();
            return new Consumer(sp, options, handlerMap, logger);
        });

        return services;
    }

    private static IServiceCollection AddSubscriberInternal(
        IServiceCollection services,
        ConsumerRuntimeConfig config,
        Action<ConsumerBuilder> configure,
        string dependencyInjectionKey)
    {
        services.TryAddSingleton<IRabbitSerializer, DefaultRabbitSerializer>();
        services.TryAddScoped<MessageContext>();
        services.TryAddScoped<IMessageContext>(sp => sp.GetRequiredService<MessageContext>());

        services.AddKeyedSingleton<ConsumerOptions>(dependencyInjectionKey, (sp, _) =>
        {
            var serializer = sp.GetRequiredService<IRabbitSerializer>();
            return new ConsumerOptions(config, serializer);
        });

        var map = new ConsumerHandlerMap();
        configure(new ConsumerBuilder(services, map));
        services.AddKeyedSingleton<ConsumerHandlerMap>(dependencyInjectionKey, map);

        services.AddKeyedSingleton<ISubscriber>(dependencyInjectionKey, (sp, _) =>
        {
            var options = sp.GetRequiredKeyedService<ConsumerOptions>(dependencyInjectionKey);
            var handlerMap = sp.GetRequiredKeyedService<ConsumerHandlerMap>(dependencyInjectionKey);
            var logger = sp.GetRequiredService<ILogger<Subscriber>>();

            return new Subscriber(sp, options, handlerMap, logger);
        });

        return services;
    }

    private static ConsumerRuntimeConfig BuildRuntimeConfig(ConsumerConfig config, string exchangeType)
    {
        if (string.IsNullOrWhiteSpace(config.ExchangeName))
        {
            throw new ArgumentException("ExchangeName is required.", nameof(config));
        }

        if (config.MaxRetryCount <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(config.MaxRetryCount),
                "MaxRetryCount must be greater than zero.");
        }

        var queueName = QueueNameConvention.BuildCommandQueueName(config.QueueName);
        var deadLetterQueueName = QueueNameConvention.BuildDeadLetterQueueName(queueName);
        var deadLetterExchangeName = QueueNameConvention.BuildDeadLetterExchangeName(queueName);

        return new ConsumerRuntimeConfig
        {
            ServiceName = config.ServiceName,
            HostName = config.HostName,
            Port = config.Port,
            UserName = config.UserName,
            Password = config.Password,
            VirtualHost = config.VirtualHost,
            QueueName = queueName,
            DeadLetterQueue = deadLetterQueueName,
            ExchangeName = config.ExchangeName,
            ExchangeType = exchangeType,
            PrefetchCount = config.PrefetchCount,
            MaxRetryCount = config.MaxRetryCount,
            DeadLetterExchange = deadLetterExchangeName,
            ConcurrentMessageCount = config.ConcurrentMessageCount,
        };
    }

    private static ConsumerRuntimeConfig BuildRuntimeConfig(SubscriberConfig config, string exchangeType)
    {
        if (string.IsNullOrWhiteSpace(config.ExchangeName))
        {
            throw new ArgumentException("ExchangeName is required.", nameof(config));
        }

        if (config.MaxRetryCount <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(config.MaxRetryCount),
                "MaxRetryCount must be greater than zero.");
        }

        var queueName = QueueNameConvention.BuildEventQueueName(config.SubscriptionName);
        var deadLetterQueueName = QueueNameConvention.BuildDeadLetterQueueName(queueName);
        var deadLetterExchangeName = QueueNameConvention.BuildDeadLetterExchangeName(queueName);

        return new ConsumerRuntimeConfig
        {
            ServiceName = config.ServiceName,
            HostName = config.HostName,
            Port = config.Port,
            UserName = config.UserName,
            Password = config.Password,
            VirtualHost = config.VirtualHost,
            QueueName = queueName,
            DeadLetterQueue = deadLetterQueueName,
            ExchangeName = config.ExchangeName,
            ExchangeType = exchangeType,
            PrefetchCount = config.PrefetchCount,
            MaxRetryCount = config.MaxRetryCount,
            DeadLetterExchange = deadLetterExchangeName,
            ConcurrentMessageCount = config.ConcurrentMessageCount,
        };
    }
}
