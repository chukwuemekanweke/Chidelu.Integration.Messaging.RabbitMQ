using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal sealed class ConsumerOptions(ConsumerRuntimeConfig config, IRabbitSerializer serializer)
{
    public ConsumerRuntimeConfig Config { get; } = config;
    public IRabbitSerializer Serializer { get; } = serializer;
}
