namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

public sealed class ConsumerHandlerMap
{
    public sealed record HandlerDescriptor(
        Type MessageType,
        Type HandlerType,
        Func<IServiceProvider, object, CancellationToken, Task> Invoker);

    private readonly List<HandlerDescriptor> _pairs = new();
    public IReadOnlyList<HandlerDescriptor> Pairs => _pairs;

    internal void Add(HandlerDescriptor d) => _pairs.Add(d);
}
