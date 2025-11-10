namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public interface IPublisher
{
    Task PublishAsync<T>(
        T @event,
        string? routingKey = null,
        IDictionary<string, string>? extraHeaders = null,
        CancellationToken cancellationToken = default);

    Task PublishBatchAsync<T>(
        IEnumerable<T> events,
        string? routingKey = null,
        IDictionary<string, string>? sharedHeaders = null,
        CancellationToken cancellationToken = default);
}