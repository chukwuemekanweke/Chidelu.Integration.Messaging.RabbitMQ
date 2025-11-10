using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public sealed class PublisherOptions(PublisherConfig config, IRabbitSerializer serializer)
{
    public PublisherConfig Config { get; } = config;
    public IRabbitSerializer Serializer { get; } = serializer;
}
