namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal sealed class HandlerRegistry(Type messageType, Func<IServiceProvider, object, CancellationToken, Task> invoker)
{
    public Type MessageType { get; } = messageType;
    public Func<IServiceProvider, object, CancellationToken, Task> Invoke { get; } = invoker;
}
