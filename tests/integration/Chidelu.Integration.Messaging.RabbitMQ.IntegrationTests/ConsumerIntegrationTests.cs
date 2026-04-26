using Chidelu.Integration.Messaging.RabbitMQ.Consumer;
using Chidelu.Integration.Messaging.RabbitMQ.Consumer.DependencyInjection;
using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Chidelu.Integration.Messaging.RabbitMQ.Core.Exceptions;
using Chidelu.Integration.Messaging.RabbitMQ.Publisher;
using Chidelu.Integration.Messaging.RabbitMQ.Publisher.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Shouldly;
using System.Threading.Channels;

namespace Chidelu.Integration.Messaging.RabbitMQ.IntegrationTests;

[Collection("rabbitmq")]
public sealed class ConsumerIntegrationTests(RabbitMqFixture fixture)
{
    private sealed record ShipOrder(Guid MessageId, string OrderId) : ICommand;

    private sealed class CapturingHandler(Channel<ShipOrder> channel) : IMessageHandler<ShipOrder>
    {
        public Task HandleAsync(ShipOrder message, CancellationToken cancellationToken)
            => channel.Writer.WriteAsync(message, cancellationToken).AsTask();
    }

    private sealed class AlwaysFailsHandler : IMessageHandler<ShipOrder>
    {
        public Task HandleAsync(ShipOrder message, CancellationToken cancellationToken)
            => throw new FailedToProcessMessageException("boom");
    }

    [Fact]
    public async Task Consumer_Receives_And_Acks_Command()
    {
        var id = Guid.NewGuid().ToString("N");
        var exchangeName = $"x.commands.{id}";
        var queueKey = $"orders-{id}";

        var channel = Channel.CreateUnbounded<ShipOrder>();

        var consumerConfig = new ConsumerConfig
        {
            ServiceName = "tests",
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost,
            QueueName = queueKey,
            ExchangeName = exchangeName,
            PrefetchCount = 1,
            MaxRetryCount = 10,
            ConcurrentMessageCount = 1
        };

        var senderConfig = new SenderConfig
        {
            ServiceName = "tests",
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost,
            CommandsExchange = exchangeName
        };

        var services = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .AddSingleton(channel)
            .AddConsumer(consumerConfig, b => b.AddHandler<ShipOrder, CapturingHandler>())
            .AddSender(senderConfig)
            .BuildServiceProvider();

        var consumer = services.GetRequiredKeyedService<IConsumer>(consumerConfig.Key);
        await consumer.StartAsync(CancellationToken.None);

        var sender = services.GetRequiredKeyedService<ISender>(senderConfig.Key);

        try
        {
            await sender.SendAsync(new ShipOrder(Guid.CreateVersion7(), "ORD-42"), CancellationToken.None);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var received = await channel.Reader.ReadAsync(cts.Token);
            received.OrderId.ShouldBe("ORD-42");
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await services.DisposeAsync().AsTask();
        }
    }

    [Fact]
    public async Task Consumer_FailedToProcess_Goes_To_DLQ()
    {
        var id = Guid.NewGuid().ToString("N");
        var exchangeName = $"x.commands.{id}";
        var queueKey = $"orders-{id}";
        var queueName = QueueNameConvention.BuildCommandQueueName(queueKey);
        var dlqName = QueueNameConvention.BuildDeadLetterQueueName(queueName);

        var consumerConfig = new ConsumerConfig
        {
            ServiceName = "tests",
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost,
            QueueName = queueKey,
            ExchangeName = exchangeName,
            PrefetchCount = 1,
            MaxRetryCount = 10,
            ConcurrentMessageCount = 1
        };

        var senderConfig = new SenderConfig
        {
            ServiceName = "tests",
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost,
            CommandsExchange = exchangeName
        };

        var services = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .AddConsumer(consumerConfig, b => b.AddHandler<ShipOrder, AlwaysFailsHandler>())
            .AddSender(senderConfig)
            .BuildServiceProvider();

        var consumer = services.GetRequiredKeyedService<IConsumer>(consumerConfig.Key);
        await consumer.StartAsync(CancellationToken.None);

        var sender = services.GetRequiredKeyedService<ISender>(senderConfig.Key);

        var cf = new ConnectionFactory
        {
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost
        };

        await using var conn = await cf.CreateConnectionAsync("consumer-dlq-test", TestContext.Current.CancellationToken);
        await using var ch = await conn.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);

        try
        {
            await sender.SendAsync(new ShipOrder(Guid.CreateVersion7(), "ORD-500"), TestContext.Current.CancellationToken);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            BasicGetResult? dlqMsg = null;

            while (!cts.IsCancellationRequested && dlqMsg is null)
            {
                dlqMsg = await ch.BasicGetAsync(dlqName, autoAck: true, cancellationToken: cts.Token);
                if (dlqMsg is null)
                {
                    await Task.Delay(200, cts.Token);
                }
            }

            dlqMsg.ShouldNotBeNull();
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await services.DisposeAsync().AsTask();
        }
    }
}
