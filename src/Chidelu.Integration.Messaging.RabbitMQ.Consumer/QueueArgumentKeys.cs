namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal static class QueueArgumentKeys
{
    /// <summary>Declares the queue type (e.g., quorum).</summary>
    public const string QueueType = "x-queue-type";

    /// <summary>Maximum delivery attempts before dead-lettering.</summary>
    public const string DeliveryLimit = "x-delivery-limit";

    /// <summary>Exchange to which dead-lettered messages are routed.</summary>
    public const string DeadLetterExchange = "x-dead-letter-exchange";

    /// <summary>Routing key used when dead-lettering.</summary>
    public const string DeadLetterRoutingKey = "x-dead-letter-routing-key";

    /// <summary>Quorum queue type value.</summary>
    public const string QueueTypeQuorum = "quorum";
}
