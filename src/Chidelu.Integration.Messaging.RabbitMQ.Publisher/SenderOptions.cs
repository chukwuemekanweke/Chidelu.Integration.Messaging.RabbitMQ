using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public sealed class SenderOptions(
    SenderConfig config,
    IRabbitSerializer serializer,
    IMessageContextAccessor messageContextAccessor)
{
    public SenderConfig Config { get; } = config;
    public IRabbitSerializer Serializer { get; } = serializer;
    public IMessageContextAccessor MessageContextAccessor { get; } = messageContextAccessor;
}
