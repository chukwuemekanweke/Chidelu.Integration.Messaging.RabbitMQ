using RabbitMQ.Client;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

internal static class TopologyInstaller
{
    public static void Ensure(IModel ch, PublisherConfig cfg)
    {
        ch.ExchangeDeclare(cfg.EventsExchange, ExchangeType.Topic, durable: true, autoDelete: false, arguments: null);
    }
}
