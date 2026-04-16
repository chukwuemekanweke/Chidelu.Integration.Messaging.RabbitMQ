using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Chidelu.Integration.Messaging.RabbitMQ.Core.Exceptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using System.Collections.Concurrent;
using CoreHeaders = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer.Tests.Unit;

public sealed class MessageDispatcherTests
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

    private sealed class HeaderCapturingHandler(
        ConcurrentBag<string> seenHeaders,
        IMessageContext messageContext) : IMessageHandler<SampleMessage>
    {
        public Task HandleAsync(SampleMessage message, CancellationToken cancellationToken)
        {
            var value = messageContext.GetHeader("tenant-id");
            if (!string.IsNullOrWhiteSpace(value))
            {
                seenHeaders.Add(value);
            }

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
    public async Task DispatchAsync_WhenHandlerSucceeds_ReturnsAck()
    {
        var seen = new ConcurrentBag<SampleMessage>();
        var services = new ServiceCollection()
            .AddSingleton<IMessageContextAccessor, AsyncLocalMessageContextAccessor>()
            .AddScoped<MessageContext>()
            .AddScoped<IMessageContext>(sp => sp.GetRequiredService<MessageContext>())
            .AddSingleton(seen)
            .AddSingleton<CapturingHandler>()
            .AddSingleton<IMessageHandler<SampleMessage>>(sp => sp.GetRequiredService<CapturingHandler>())
            .BuildServiceProvider();

        var serializer = new DefaultRabbitSerializer();
        var handlers = new Dictionary<string, HandlerRegistry>(StringComparer.Ordinal)
        {
            [typeof(SampleMessage).FullName!] = CreateRegistry<SampleMessage, CapturingHandler>()
        };

        var dispatcher = CreateDispatcher(services, serializer, handlers);
        var envelope = BuildEnvelope(new SampleMessage(7), serializer);

        var outcome = await dispatcher.DispatchAsync(envelope, CancellationToken.None);

        outcome.ShouldBe(DispatchOutcome.Ack);
        seen.ShouldContain(x => x.Value == 7);
    }

    [Fact]
    public async Task DispatchAsync_WhenHandlerThrowsFailedToProcess_ReturnsNackDrop()
    {
        var services = new ServiceCollection()
            .AddSingleton<IMessageContextAccessor, AsyncLocalMessageContextAccessor>()
            .AddScoped<MessageContext>()
            .AddScoped<IMessageContext>(sp => sp.GetRequiredService<MessageContext>())
            .AddSingleton<FailedHandler>()
            .AddSingleton<IMessageHandler<SampleMessage>>(sp => sp.GetRequiredService<FailedHandler>())
            .BuildServiceProvider();

        var serializer = new DefaultRabbitSerializer();
        var handlers = new Dictionary<string, HandlerRegistry>(StringComparer.Ordinal)
        {
            [typeof(SampleMessage).FullName!] = CreateRegistry<SampleMessage, FailedHandler>()
        };

        var dispatcher = CreateDispatcher(services, serializer, handlers);
        var envelope = BuildEnvelope(new SampleMessage(1), serializer);

        var outcome = await dispatcher.DispatchAsync(envelope, CancellationToken.None);

        outcome.ShouldBe(DispatchOutcome.NackDrop);
    }

    [Fact]
    public async Task DispatchAsync_WhenHandlerThrowsOtherException_ReturnsNackRequeue()
    {
        var services = new ServiceCollection()
            .AddSingleton<IMessageContextAccessor, AsyncLocalMessageContextAccessor>()
            .AddScoped<MessageContext>()
            .AddScoped<IMessageContext>(sp => sp.GetRequiredService<MessageContext>())
            .AddSingleton<ThrowingHandler>()
            .AddSingleton<IMessageHandler<SampleMessage>>(sp => sp.GetRequiredService<ThrowingHandler>())
            .BuildServiceProvider();

        var serializer = new DefaultRabbitSerializer();
        var handlers = new Dictionary<string, HandlerRegistry>(StringComparer.Ordinal)
        {
            [typeof(SampleMessage).FullName!] = CreateRegistry<SampleMessage, ThrowingHandler>()
        };

        var dispatcher = CreateDispatcher(services, serializer, handlers);
        var envelope = BuildEnvelope(new SampleMessage(2), serializer);

        var outcome = await dispatcher.DispatchAsync(envelope, CancellationToken.None);

        outcome.ShouldBe(DispatchOutcome.NackRequeue);
    }

    [Fact]
    public async Task DispatchAsync_MakesHeadersAvailableThroughMessageContext()
    {
        var seenHeaders = new ConcurrentBag<string>();
        var services = new ServiceCollection()
            .AddSingleton<IMessageContextAccessor, AsyncLocalMessageContextAccessor>()
            .AddScoped<MessageContext>()
            .AddScoped<IMessageContext>(sp => sp.GetRequiredService<MessageContext>())
            .AddSingleton(seenHeaders)
            .AddScoped<HeaderCapturingHandler>()
            .AddScoped<IMessageHandler<SampleMessage>>(sp => sp.GetRequiredService<HeaderCapturingHandler>())
            .BuildServiceProvider();

        var serializer = new DefaultRabbitSerializer();
        var handlers = new Dictionary<string, HandlerRegistry>(StringComparer.Ordinal)
        {
            [typeof(SampleMessage).FullName!] = CreateRegistry<SampleMessage, HeaderCapturingHandler>()
        };

        var dispatcher = CreateDispatcher(services, serializer, handlers);
        var envelope = BuildEnvelope(
            new SampleMessage(9),
            serializer,
            extraHeaders: new Dictionary<string, string> { ["tenant-id"] = "tenant-42" });

        var outcome = await dispatcher.DispatchAsync(envelope, CancellationToken.None);

        outcome.ShouldBe(DispatchOutcome.Ack);
        seenHeaders.ShouldContain("tenant-42");
    }

    [Fact]
    public async Task DispatchAsync_WhenNoHandlerRegistered_ReturnsNackDrop()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var serializer = new DefaultRabbitSerializer();
        var handlers = new Dictionary<string, HandlerRegistry>(StringComparer.Ordinal);

        var dispatcher = CreateDispatcher(services, serializer, handlers);
        var envelope = BuildEnvelope(new SampleMessage(3), serializer);

        var outcome = await dispatcher.DispatchAsync(envelope, CancellationToken.None);

        outcome.ShouldBe(DispatchOutcome.NackDrop);
    }

    [Fact]
    public async Task DispatchAsync_WhenTypeHeaderMissing_ReturnsNackDrop()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var serializer = new DefaultRabbitSerializer();
        var handlers = new Dictionary<string, HandlerRegistry>(StringComparer.Ordinal);

        var dispatcher = CreateDispatcher(services, serializer, handlers);
        var envelope = BuildEnvelope(new SampleMessage(4), serializer, includeTypeHeader: false);

        var outcome = await dispatcher.DispatchAsync(envelope, CancellationToken.None);

        outcome.ShouldBe(DispatchOutcome.NackDrop);
    }

    private static MessageDispatcher CreateDispatcher(
        IServiceProvider services,
        IRabbitSerializer serializer,
        IDictionary<string, HandlerRegistry> handlers)
    {
        return new MessageDispatcher(
            services,
            serializer,
            NullLogger.Instance,
            key => handlers.TryGetValue(key, out var registration) ? registration : null);
    }

    private static HandlerRegistry CreateRegistry<TMessage, THandler>()
        where THandler : class, IMessageHandler<TMessage>
    {
        return new HandlerRegistry(
            typeof(TMessage),
            async (sp, obj, envelope, ct) =>
            {
                using var scope = sp.CreateScope();
                var messageContextAccessor = scope.ServiceProvider.GetRequiredService<IMessageContextAccessor>();
                var previous = messageContextAccessor.Current;
                scope.ServiceProvider.GetRequiredService<MessageContext>().SetHeaders(envelope.Headers);
                var handler = scope.ServiceProvider.GetRequiredService<THandler>();
                try
                {
                    await handler.HandleAsync((TMessage)obj, ct);
                }
                finally
                {
                    messageContextAccessor.Current = previous;
                }
            });
    }

    private static MessageEnvelope BuildEnvelope<T>(
        T message,
        DefaultRabbitSerializer serializer,
        bool includeTypeHeader = true,
        IDictionary<string, string>? extraHeaders = null)
    {
        var headers = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        if (includeTypeHeader)
        {
            CoreHeaders.SetString(headers, KnownMetadata.Type, typeof(T).AssemblyQualifiedName!);
        }

        if (extraHeaders is not null)
        {
            foreach (var header in extraHeaders)
            {
                CoreHeaders.SetString(headers, header.Key, header.Value);
            }
        }

        return new MessageEnvelope
        {
            Body = serializer.Serialize(message!),
            Headers = headers,
            DeliveryTag = 1
        };
    }
}
