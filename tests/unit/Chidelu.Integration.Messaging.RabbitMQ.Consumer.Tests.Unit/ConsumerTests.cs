using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Chidelu.Integration.Messaging.RabbitMQ.Core.Exceptions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shouldly;
using System.Collections.Concurrent;
using System.Reflection;
using CoreHeaders = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer.Tests.Unit;

public sealed class ConsumerTests
{
    private sealed record SampleMessage(int Value);

    private sealed class CapturingHandler(ConcurrentBag<SampleMessage> seen) : IMessageHandler<SampleMessage>
    {
        public Task HandleAsync(SampleMessage message, CancellationToken cancellationToken)
        {
            seen.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class FailedHandler : IMessageHandler<SampleMessage>
    {
        public Task HandleAsync(SampleMessage message, CancellationToken cancellationToken)
            => throw new FailedToProcessMessageException("boom");
    }

    private sealed class ThrowingHandler : IMessageHandler<SampleMessage>
    {
        public Task HandleAsync(SampleMessage message, CancellationToken cancellationToken)
            => throw new InvalidOperationException("boom");
    }

    [Fact]
    public async Task HandleMessageAsync_WhenHandlerSucceeds_Acks()
    {
        var seen = new ConcurrentBag<SampleMessage>();
        var services = new ServiceCollection()
            .AddSingleton(seen)
            .AddSingleton<CapturingHandler>()
            .AddSingleton<IMessageHandler<SampleMessage>>(sp => sp.GetRequiredService<CapturingHandler>())
            .BuildServiceProvider();

        var channel = Substitute.For<IChannel>();
        channel.BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);

        var consumer = CreateConsumer(services, channel);
        consumer.AddHandler<SampleMessage, CapturingHandler>();

        var args = BuildArgs(new SampleMessage(7));

        await InvokeHandleMessageAsync(consumer, args);

        await channel.Received(1).BasicAckAsync(args.DeliveryTag, false, Arg.Any<CancellationToken>());
        await channel.DidNotReceive().BasicNackAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        seen.ShouldContain(x => x.Value == 7);
    }

    [Fact]
    public async Task HandleMessageAsync_WhenHandlerThrowsFailedToProcess_NacksWithoutRequeue()
    {
        var services = new ServiceCollection()
            .AddSingleton<FailedHandler>()
            .AddSingleton<IMessageHandler<SampleMessage>>(sp => sp.GetRequiredService<FailedHandler>())
            .BuildServiceProvider();

        var channel = Substitute.For<IChannel>();
        channel.BasicNackAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);

        var consumer = CreateConsumer(services, channel);
        consumer.AddHandler<SampleMessage, FailedHandler>();

        var args = BuildArgs(new SampleMessage(1));

        await InvokeHandleMessageAsync(consumer, args);

        await channel.Received(1).BasicNackAsync(args.DeliveryTag, false, false, Arg.Any<CancellationToken>());
        await channel.DidNotReceive().BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleMessageAsync_WhenHandlerThrowsOtherException_NacksWithRequeue()
    {
        var services = new ServiceCollection()
            .AddSingleton<ThrowingHandler>()
            .AddSingleton<IMessageHandler<SampleMessage>>(sp => sp.GetRequiredService<ThrowingHandler>())
            .BuildServiceProvider();

        var channel = Substitute.For<IChannel>();
        channel.BasicNackAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);

        var consumer = CreateConsumer(services, channel);
        consumer.AddHandler<SampleMessage, ThrowingHandler>();

        var args = BuildArgs(new SampleMessage(2));

        await InvokeHandleMessageAsync(consumer, args);

        await channel.Received(1).BasicNackAsync(args.DeliveryTag, false, true, Arg.Any<CancellationToken>());
        await channel.DidNotReceive().BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleMessageAsync_WhenNoHandlerRegistered_NacksWithoutRequeue()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var channel = Substitute.For<IChannel>();
        channel.BasicNackAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);

        var consumer = CreateConsumer(services, channel);
        var args = BuildArgs(new SampleMessage(3));

        await InvokeHandleMessageAsync(consumer, args);

        await channel.Received(1).BasicNackAsync(args.DeliveryTag, false, false, Arg.Any<CancellationToken>());
        await channel.DidNotReceive().BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleMessageAsync_WhenTypeHeaderMissing_NacksWithoutRequeue()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var channel = Substitute.For<IChannel>();
        channel.BasicNackAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);

        var consumer = CreateConsumer(services, channel);
        var args = BuildArgs(new SampleMessage(4), includeTypeHeader: false);

        await InvokeHandleMessageAsync(consumer, args);

        await channel.Received(1).BasicNackAsync(args.DeliveryTag, false, false, Arg.Any<CancellationToken>());
        await channel.DidNotReceive().BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    private static Consumer CreateConsumer(IServiceProvider services, IChannel channel)
    {
        var config = new ConsumerRuntimeConfig
        {
            ServiceName = "svc",
            HostName = "localhost",
            Port = 5672,
            UserName = "user",
            Password = "pass",
            VirtualHost = "/",
            QueueName = "cmd.orders",
            DeadLetterQueue = "cmd.orders.dlq",
            ExchangeName = "x.commands",
            ExchangeType = ExchangeType.Direct,
            PrefetchCount = 10,
            MaxRetryCount = 10,
            DeadLetterExchange = "x.dlx.cmd.orders",
            ConcurrentMessageCount = 1
        };

        var options = new ConsumerOptions(config, new DefaultRabbitSerializer());
        var map = new ConsumerHandlerMap();
        var consumer = new Consumer(services, options, map);

        SetField(consumer, "_ch", channel);
        SetField(consumer, "_stopCts", new CancellationTokenSource());

        return consumer;
    }

    private static BasicDeliverEventArgs BuildArgs(SampleMessage message, bool includeTypeHeader = true)
    {
        var serializer = new DefaultRabbitSerializer();
        var body = serializer.Serialize(message);

        var props = new BasicProperties
        {
            Headers = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        };

        if (includeTypeHeader)
        {
            CoreHeaders.SetString(props.Headers, KnownMetadata.Type, typeof(SampleMessage).AssemblyQualifiedName!);
        }

        return new BasicDeliverEventArgs(
            "tag",
            1,
            false,
            "x",
            "rk",
            props,
            body,
            CancellationToken.None);
    }

    private static Task InvokeHandleMessageAsync(Consumer consumer, BasicDeliverEventArgs args)
    {
        var method = typeof(Consumer).GetMethod("HandleMessageAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        method.ShouldNotBeNull();
        return (Task)method!.Invoke(consumer, new object?[] { consumer, args })!;
    }

    private static void SetField(Consumer consumer, string name, object value)
    {
        var field = typeof(Consumer).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
        field.ShouldNotBeNull();
        field!.SetValue(consumer, value);
    }
}
