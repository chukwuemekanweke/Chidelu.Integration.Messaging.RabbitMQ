using Chidelu.Integration.Messaging.RabbitMQ.Consumer;
using Chidelu.Integration.Messaging.RabbitMQ.Consumer.DependencyInjection;
using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Chidelu.Integration.Messaging.RabbitMQ.Publisher;
using Chidelu.Integration.Messaging.RabbitMQ.Publisher.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using System.Threading.Channels;

namespace Chidelu.Integration.Messaging.RabbitMQ.IntegrationTests;

[Collection("rabbitmq")]
public sealed class SenderIntegrationTests(RabbitMqFixture fixture)
{
    private sealed record ShipOrder(Guid MessageId, string OrderId) : ICommand;
    private sealed record CancelOrder(Guid MessageId, string OrderId) : ICommand;

    [Fact]
    public async Task SendAsync_Routes_To_Only_Matching_Direct_Binding()
    {
        var id = Guid.NewGuid().ToString("N");
        var exchangeName = $"x.commands.orders.{id}";
        var queueKeyOne = $"orders-ship-{id}";
        var queueKeyTwo = $"orders-cancel-{id}";
        var shipChannel = Channel.CreateUnbounded<ShipOrder>();
        var cancelChannel = Channel.CreateUnbounded<CancelOrder>();

        var senderConfig = new SenderConfig
        {
            ServiceName = "tests",
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost,
            CommandsExchange = exchangeName,
            ConcurrentMessageCount = 1
        };

        var consumerConfigOne = new ConsumerConfig
        {
            ServiceName = "tests",
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost,
            QueueName = queueKeyOne,
            ExchangeName = exchangeName,
            PrefetchCount = 1,
            MaxRetryCount = 10,
            ConcurrentMessageCount = 1
        };

        var consumerConfigTwo = new ConsumerConfig
        {
            ServiceName = "tests",
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost,
            QueueName = queueKeyTwo,
            ExchangeName = exchangeName,
            PrefetchCount = 1,
            MaxRetryCount = 10,
            ConcurrentMessageCount = 1
        };

        await using var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(shipChannel)
            .AddSingleton(cancelChannel)
            .AddConsumer(consumerConfigOne, b => b.AddHandler<ShipOrder, CapturingShipHandler>())
            .AddConsumer(consumerConfigTwo, b => b.AddHandler<CancelOrder, CapturingCancelHandler>())
            .AddSender(senderConfig)
            .BuildServiceProvider();

        var consumerOne = services.GetRequiredKeyedService<IConsumer>(consumerConfigOne.Key);
        var consumerTwo = services.GetRequiredKeyedService<IConsumer>(consumerConfigTwo.Key);
        await consumerOne.StartAsync(CancellationToken.None);
        await consumerTwo.StartAsync(CancellationToken.None);

        var sender = services.GetRequiredKeyedService<ISender>(senderConfig.Key);

        try
        {
            var payload = new ShipOrder(Guid.CreateVersion7(), "ORD-123");
            await sender.SendAsync(payload, CancellationToken.None);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var received = await shipChannel.Reader.ReadAsync(cts.Token);
            received.OrderId.ShouldBe("ORD-123");

            using var negativeCts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            await Should.ThrowAsync<OperationCanceledException>(
                () => cancelChannel.Reader.ReadAsync(negativeCts.Token).AsTask());
        }
        finally
        {
            await consumerOne.StopAsync(CancellationToken.None);
            await consumerTwo.StopAsync(CancellationToken.None);
        }
    }

    private sealed class CapturingShipHandler(Channel<ShipOrder> channel) : IMessageHandler<ShipOrder>
    {
        public Task HandleAsync(ShipOrder message, CancellationToken cancellationToken)
            => channel.Writer.WriteAsync(message, cancellationToken).AsTask();
    }

    private sealed class CapturingCancelHandler(Channel<CancelOrder> channel) : IMessageHandler<CancelOrder>
    {
        public Task HandleAsync(CancelOrder message, CancellationToken cancellationToken)
            => channel.Writer.WriteAsync(message, cancellationToken).AsTask();
    }
}
