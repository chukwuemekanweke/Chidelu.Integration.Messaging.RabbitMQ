using System.Threading;

namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public sealed class AsyncLocalMessageContextAccessor : IMessageContextAccessor
{
    private readonly AsyncLocal<IMessageMetadataContext?> _current = new();

    public IMessageMetadataContext? Current
    {
        get => _current.Value;
        set => _current.Value = value;
    }
}
