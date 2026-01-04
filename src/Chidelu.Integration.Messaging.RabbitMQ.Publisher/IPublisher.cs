using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public interface IPublisher
{
    Task PublishAsync<T>(
        T @event,
        IDictionary<string, string>? extraHeaders = null,
        CancellationToken cancellationToken = default)
        where T : IEvent;

    Task PublishBatchAsync<T>(
        IEnumerable<T> events,
        IDictionary<string, string>? sharedHeaders = null,
        CancellationToken cancellationToken = default)
        where T : IEvent;
}
