using Chidelu.Integration.Messaging.RabbitMQ.Consumer;
using Chidelu.Integration.Messaging.RabbitMQ.Consumer.DependencyInjection;
using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Chidelu.Integration.Messaging.RabbitMQ.Publisher;
using Chidelu.Integration.Messaging.RabbitMQ.Publisher.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using System.Threading.Channels;

namespace Chidelu.Integration.Messaging.RabbitMQ.IntegrationTests;

[Collection("rabbitmq")]
public sealed class PublisherIntegrationTests(RabbitMqFixture fixture)
{
    private sealed record OrderCreated(Guid MessageId, string OrderId) : IEvent;

    [Fact]
    public async Task PublishAsync_Delivers_To_BoundQueue()
    {
        var id = Guid.NewGuid().ToString("N");
        var exchangeName = $"x.events.orders.{id}";
        var subscriptionName = $"orders-{id}";
        var channel = Channel.CreateUnbounded<OrderCreated>();

        var publisherConfig = new PublisherConfig
        {
            ServiceName = "tests",
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost,
            EventsExchange = exchangeName
        };

        var subscriberConfig = new SubscriberConfig
        {
            ServiceName = "tests",
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost,
            SubscriptionName = subscriptionName,
            ExchangeName = exchangeName,
            PrefetchCount = 1,
            MaxRetryCount = 10,
            ConcurrentMessageCount = 1
        };

        await using var services = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .AddSingleton(channel)
            .AddSubscriber(subscriberConfig, b => b.AddHandler<OrderCreated, CapturingHandler>())
            .AddPublisher(publisherConfig)
            .BuildServiceProvider();

        var subscriber = services.GetRequiredKeyedService<IConsumer>(subscriberConfig.Key);
        await subscriber.StartAsync(CancellationToken.None);

        var publisher = services.GetRequiredKeyedService<IPublisher>(publisherConfig.Key);
        try
        {
            var payload = new OrderCreated(Guid.CreateVersion7(), "ORD-1");
            await publisher.PublishAsync(payload, CancellationToken.None);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var received = await channel.Reader.ReadAsync(cts.Token);

            received.OrderId.ShouldBe("ORD-1");
            received.MessageId.ShouldBe(payload.MessageId);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    private sealed class CapturingHandler(Channel<OrderCreated> channel) : IMessageHandler<OrderCreated>
    {
        public Task HandleAsync(OrderCreated message, CancellationToken cancellationToken)
            => channel.Writer.WriteAsync(message, cancellationToken).AsTask();
    }
}
