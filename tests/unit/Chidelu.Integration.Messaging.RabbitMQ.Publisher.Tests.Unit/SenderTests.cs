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
            CommandsExchange = "x.commands"
        };

        var options = new SenderOptions(config, new DefaultRabbitSerializer(), new AsyncLocalMessageContextAccessor());
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
        capturedRoutingKey.ShouldBe(typeof(ShipOrder).FullName);

        capturedProps.ShouldNotBeNull();
        var headers = capturedProps!.Headers.ShouldNotBeNull();

        CoreHeaders.GetString(headers, KnownMetadata.Type).ShouldBe(typeof(ShipOrder).AssemblyQualifiedName);
        CoreHeaders.TryGetGuid(headers, KnownMetadata.MessageId, out var messageId).ShouldBeTrue();
        messageId.ShouldNotBe(Guid.Empty);
        messageId.ToString("D")[14].ShouldBe('7');
        CoreHeaders.GetString(headers, KnownMetadata.CausationId).ShouldBe("cause-1");
        CoreHeaders.GetString(headers, KnownMetadata.ParentOperationId).ShouldBe(activity.Id);
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
            CommandsExchange = "x.commands"
        };

        var options = new SenderOptions(config, new DefaultRabbitSerializer(), new AsyncLocalMessageContextAccessor());
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
        CoreHeaders.GetString(headers, KnownMetadata.ParentOperationId).ShouldBe(activity.Id);
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
            CommandsExchange = "x.commands"
        };

        var options = new SenderOptions(config, new DefaultRabbitSerializer(), new AsyncLocalMessageContextAccessor());
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

    [Fact]
    public async Task SendAsync_InheritsCorrelationId_AndSetsCausationId_FromIncomingContext()
    {
        var config = new SenderConfig
        {
            ServiceName = "svc",
            HostName = "localhost",
            Port = 5672,
            UserName = "user",
            Password = "pass",
            VirtualHost = "/",
            CommandsExchange = "x.commands"
        };

        var accessor = new AsyncLocalMessageContextAccessor();
        accessor.Current = new TestMessageMetadataContext(Guid.CreateVersion7(), "corr-inbound");

        var options = new SenderOptions(config, new DefaultRabbitSerializer(), accessor);
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

        await sut.SendAsync(new ShipOrder(Guid.CreateVersion7()), CancellationToken.None);

        var headers = capturedProperties!.Headers.ShouldNotBeNull();
        CoreHeaders.GetString(headers, KnownMetadata.CorrelationId).ShouldBe("corr-inbound");
        CoreHeaders.GetString(headers, KnownMetadata.CausationId).ShouldBe(accessor.Current!.MessageId!.Value.ToString());
    }

    [Fact]
    public async Task SendAsync_ExplicitHeadersOverrideIncomingContext()
    {
        var config = new SenderConfig
        {
            ServiceName = "svc",
            HostName = "localhost",
            Port = 5672,
            UserName = "user",
            Password = "pass",
            VirtualHost = "/",
            CommandsExchange = "x.commands"
        };

        var accessor = new AsyncLocalMessageContextAccessor();
        accessor.Current = new TestMessageMetadataContext(Guid.CreateVersion7(), "corr-inbound");

        var options = new SenderOptions(config, new DefaultRabbitSerializer(), accessor);
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

        var explicitHeaders = new Dictionary<string, string>
        {
            [KnownMetadata.CorrelationId] = "corr-explicit",
            [KnownMetadata.CausationId] = "cause-explicit"
        };

        await sut.SendAsync(new ShipOrder(Guid.CreateVersion7()), CancellationToken.None, explicitHeaders);

        var headers = capturedProperties!.Headers.ShouldNotBeNull();
        CoreHeaders.GetString(headers, KnownMetadata.CorrelationId).ShouldBe("corr-explicit");
        CoreHeaders.GetString(headers, KnownMetadata.CausationId).ShouldBe("cause-explicit");
    }

    private sealed class TestMessageMetadataContext(Guid? messageId, string? correlationId) : IMessageMetadataContext
    {
        public IReadOnlyDictionary<string, object?> Headers { get; } = new Dictionary<string, object?>();
        public string? MessageType => null;
        public Guid? MessageId { get; } = messageId;
        public string? CorrelationId { get; } = correlationId;
        public string? CausationId => null;
        public string? ParentOperationId => null;
    }
}
