namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal sealed class ConsumerHandlerMap
{
    internal sealed record HandlerDescriptor(
        Type MessageType,
        Type HandlerType,
        Func<IServiceProvider, object, MessageEnvelope, CancellationToken, Task> Invoker);

    private readonly List<HandlerDescriptor> _pairs = new();
    internal IReadOnlyList<HandlerDescriptor> Pairs => _pairs;

    internal void Add(HandlerDescriptor d) => _pairs.Add(d);
}
