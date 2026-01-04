using Chidelu.Integration.Messaging.RabbitMQ.Core;
using NSubstitute;
using RabbitMQ.Client;
using Shouldly;
using System.Diagnostics;
using System.Reflection;
using CoreHeaders = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher.Tests.Unit;

public sealed class PublisherTests
{
    private sealed record OrderCreated(Guid MessageId) : IEvent;

    [Fact]
    public async Task PublishAsync_SetsHeaders_RoutingKey_And_IgnoresReservedHeaders()
    {
        var config = new PublisherConfig
        {
            ServiceName = "svc",
            HostName = "localhost",
            Port = 5672,
            UserName = "user",
            Password = "pass",
            VirtualHost = "/",
            EventsExchange = "x.events"
        };

        var options = new PublisherOptions(config, new DefaultRabbitSerializer());
        var sut = new Publisher(options);

        var channel = Substitute.For<IChannel>();
        BasicProperties? capturedProperties = null;
        string? capturedExchange = null;
        string? capturedRoutingKey = null;

        channel.BasicPublishAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<BasicProperties>(),
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask)
            .AndDoes(ci =>
            {
                capturedExchange = ci.ArgAt<string>(0);
                capturedRoutingKey = ci.ArgAt<string>(1);
                capturedProperties = ci.ArgAt<BasicProperties>(3);
            });

        SetChannel(sut, channel);

        var messageId = Guid.CreateVersion7();
        var payload = new OrderCreated(messageId);

        using var activity = new Activity("publish-test").Start();

        var extraHeaders = new Dictionary<string, string>
        {
            [KnownMetadata.CorrelationId] = "corr-1",
            [KnownMetadata.Type] = "override"
        };

        await sut.PublishAsync(payload, CancellationToken.None, extraHeaders);

        capturedExchange.ShouldBe("x.events");
        capturedRoutingKey.ShouldBe(typeof(OrderCreated).FullName);

        capturedProperties.ShouldNotBeNull();
        var headers = capturedProperties!.Headers.ShouldNotBeNull();

        CoreHeaders.GetString(headers, KnownMetadata.Type).ShouldBe(typeof(OrderCreated).AssemblyQualifiedName);
        CoreHeaders.TryGetGuid(headers, KnownMetadata.MessageId, out var parsed).ShouldBeTrue();
        parsed.ShouldBe(messageId);
        CoreHeaders.GetString(headers, KnownMetadata.CorrelationId).ShouldBe("corr-1");
        CoreHeaders.GetString(headers, KnownMetadata.OriginatingOperationId).ShouldBe(activity.Id);
    }

    [Fact]
    public async Task PublishAsync_WithNonGuid7_MessageId_Throws()
    {
        var config = new PublisherConfig
        {
            ServiceName = "svc",
            HostName = "localhost",
            Port = 5672,
            UserName = "user",
            Password = "pass",
            VirtualHost = "/",
            EventsExchange = "x.events"
        };

        var options = new PublisherOptions(config, new DefaultRabbitSerializer());
        var sut = new Publisher(options);

        var channel = Substitute.For<IChannel>();
        channel.BasicPublishAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<BasicProperties>(),
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);

        SetChannel(sut, channel);

        var payload = new OrderCreated(Guid.NewGuid());

        var ex = await Should.ThrowAsync<ArgumentException>(
            () => sut.PublishAsync(payload, CancellationToken.None));

        ex.Message.ShouldContain("UUIDv7");
    }

    [Fact]
    public async Task PublishAsync_WithEmptyMessageId_UsesGuid7()
    {
        var config = new PublisherConfig
        {
            ServiceName = "svc",
            HostName = "localhost",
            Port = 5672,
            UserName = "user",
            Password = "pass",
            VirtualHost = "/",
            EventsExchange = "x.events"
        };

        var options = new PublisherOptions(config, new DefaultRabbitSerializer());
        var sut = new Publisher(options);

        var channel = Substitute.For<IChannel>();
        BasicProperties? capturedProperties = null;

        channel.BasicPublishAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<BasicProperties>(),
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask)
            .AndDoes(ci => capturedProperties = ci.ArgAt<BasicProperties>(3));

        SetChannel(sut, channel);

        var payload = new OrderCreated(Guid.Empty);

        await sut.PublishAsync(payload, CancellationToken.None);

        capturedProperties.ShouldNotBeNull();
        var headers = capturedProperties!.Headers.ShouldNotBeNull();

        CoreHeaders.TryGetGuid(headers, KnownMetadata.MessageId, out var messageId).ShouldBeTrue();
        messageId.ShouldNotBe(Guid.Empty);
        messageId.ToString("D")[14].ShouldBe('7');
    }

    private static void SetChannel(Publisher target, IChannel channel)
    {
        var field = typeof(Publisher).GetField("_ch", BindingFlags.Instance | BindingFlags.NonPublic);
        field.ShouldNotBeNull();
        field!.SetValue(target, channel);
    }
}
