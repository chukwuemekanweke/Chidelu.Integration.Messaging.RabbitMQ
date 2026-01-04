using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Chidelu.Integration.Messaging.RabbitMQ.Publisher;
using NSubstitute;
using RabbitMQ.Client;
using Shouldly;
using System.Diagnostics;
using System.Reflection;
using CoreHeaders = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher.Tests.Unit;

public sealed class SenderTests
{
    private sealed record ShipOrder(Guid MessageId) : ICommand;

    [Fact]
    public async Task SendAsync_SetsHeaders_RoutingKey_And_UsesGuid7WhenMissing()
    {
        var config = new SenderConfig
        {
            ServiceName = "svc",
            HostName = "localhost",
            Port = 5672,
            UserName = "user",
            Password = "pass",
            VirtualHost = "/",
            CommandsExchange = "x.commands",
            ConcurrentMessageCount = 1
        };

        var options = new SenderOptions(config, new DefaultRabbitSerializer());
        var sut = new Sender(options);

        var channel = Substitute.For<IChannel>();
        BasicProperties? capturedProps = null;
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
                capturedProps = ci.ArgAt<BasicProperties>(3);
            });

        SetChannel(sut, channel);

        var payload = new ShipOrder(Guid.Empty);

        using var activity = new Activity("send-test").Start();

        var extraHeaders = new Dictionary<string, string>
        {
            [KnownMetadata.CausationId] = "cause-1",
            [KnownMetadata.Type] = "override"
        };

        await sut.SendAsync(payload, CancellationToken.None, extraHeaders);

        capturedExchange.ShouldBe("x.commands");
        capturedRoutingKey.ShouldBe(typeof(ShipOrder).AssemblyQualifiedName);

        capturedProps.ShouldNotBeNull();
        var headers = capturedProps!.Headers.ShouldNotBeNull();

        CoreHeaders.GetString(headers, KnownMetadata.Type).ShouldBe(typeof(ShipOrder).AssemblyQualifiedName);
        CoreHeaders.TryGetGuid(headers, KnownMetadata.MessageId, out var messageId).ShouldBeTrue();
        messageId.ShouldNotBe(Guid.Empty);
        messageId.ToString("D")[14].ShouldBe('7');
        CoreHeaders.GetString(headers, KnownMetadata.CausationId).ShouldBe("cause-1");
        CoreHeaders.GetString(headers, KnownMetadata.OriginatingOperationId).ShouldBe(activity.Id);
    }

    [Fact]
    public async Task SendAsync_WithEmptyMessageId_UsesGuid7()
    {
        var config = new SenderConfig
        {
            ServiceName = "svc",
            HostName = "localhost",
            Port = 5672,
            UserName = "user",
            Password = "pass",
            VirtualHost = "/",
            CommandsExchange = "x.commands",
            ConcurrentMessageCount = 1
        };

        var options = new SenderOptions(config, new DefaultRabbitSerializer());
        var sut = new Sender(options);

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

        var payload = new ShipOrder(Guid.Empty);

        using var activity = new Activity("send-test").Start();

        await sut.SendAsync(payload, CancellationToken.None);

        capturedProperties.ShouldNotBeNull();
        var headers = capturedProperties!.Headers.ShouldNotBeNull();

        CoreHeaders.GetString(headers, KnownMetadata.Type).ShouldBe(typeof(ShipOrder).AssemblyQualifiedName);
        CoreHeaders.TryGetGuid(headers, KnownMetadata.MessageId, out var messageId).ShouldBeTrue();
        messageId.ShouldNotBe(Guid.Empty);
        messageId.ToString("D")[14].ShouldBe('7');
        CoreHeaders.GetString(headers, KnownMetadata.OriginatingOperationId).ShouldBe(activity.Id);
    }

    [Fact]
    public async Task SendAsync_WithNonGuid7_MessageId_Throws()
    {
        var config = new SenderConfig
        {
            ServiceName = "svc",
            HostName = "localhost",
            Port = 5672,
            UserName = "user",
            Password = "pass",
            VirtualHost = "/",
            CommandsExchange = "x.commands",
            ConcurrentMessageCount = 1
        };

        var options = new SenderOptions(config, new DefaultRabbitSerializer());
        var sut = new Sender(options);

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

        var payload = new ShipOrder(Guid.NewGuid());

        var ex = await Should.ThrowAsync<ArgumentException>(
            () => sut.SendAsync(payload, CancellationToken.None));

        ex.Message.ShouldContain("UUIDv7");
    }

    private static void SetChannel(Sender target, IChannel channel)
    {
        var field = typeof(Sender).GetField("_ch", BindingFlags.Instance | BindingFlags.NonPublic);
        field.ShouldNotBeNull();
        field!.SetValue(target, channel);
    }
}
