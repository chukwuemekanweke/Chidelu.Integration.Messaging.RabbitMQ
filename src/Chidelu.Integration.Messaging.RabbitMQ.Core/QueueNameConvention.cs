namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public static class QueueNameConvention
{
    public static string BuildCommandQueueName(string name) => BuildQueueName("cmd", name);

    public static string BuildEventQueueName(string name) => BuildQueueName("evt", name);

    public static string BuildDeadLetterQueueName(string queueName)
        => $"{queueName}.dlq";

    public static string BuildDeadLetterExchangeName(string queueName)
        => $"x.dlx.{queueName}";

    private static string BuildQueueName(string prefix, string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Queue name cannot be empty.", nameof(name));
        }

        var normalized = name.Trim().ToLowerInvariant();
        var prefixWithDot = $"{prefix}.";

        return normalized.StartsWith(prefixWithDot, StringComparison.OrdinalIgnoreCase)
            ? normalized
            : $"{prefixWithDot}{normalized}";
    }
}
