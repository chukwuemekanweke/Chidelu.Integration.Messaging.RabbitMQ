using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public sealed class PublisherOptions(
    PublisherConfig config,
    IRabbitSerializer serializer,
    IMessageContextAccessor messageContextAccessor)
{
    public PublisherConfig Config { get; } = config;
    public IRabbitSerializer Serializer { get; } = serializer;
    public IMessageContextAccessor MessageContextAccessor { get; } = messageContextAccessor;
}
