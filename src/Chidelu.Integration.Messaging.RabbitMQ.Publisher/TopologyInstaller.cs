using RabbitMQ.Client;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

internal static class TopologyInstaller
{
    public static async Task EnsureAsync(IChannel ch, PublisherConfig cfg)
    {
        if (string.IsNullOrWhiteSpace(cfg.EventsExchange))
        {
            throw new ArgumentException("EventsExchange is required.", nameof(cfg));
        }

        await ch.ExchangeDeclareAsync(cfg.EventsExchange, ExchangeType.Fanout, durable: true, autoDelete: false, arguments: null);
    }

    public static async Task EnsureAsync(IChannel ch, SenderConfig cfg)
    {
        if (string.IsNullOrWhiteSpace(cfg.CommandsExchange))
        {
            throw new ArgumentException("CommandsExchange is required.", nameof(cfg));
        }

        await ch.ExchangeDeclareAsync(cfg.CommandsExchange, ExchangeType.Direct, durable: true, autoDelete: false, arguments: null);
    }
}
