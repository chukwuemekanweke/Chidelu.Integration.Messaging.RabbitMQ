using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public interface IPublisher
{
    Task PublishAsync<T>(
        T @event,
        CancellationToken cancellationToken,
        IDictionary<string, string>? extraHeaders = null)
        where T : IEvent;

    Task PublishBatchAsync<T>(
        IEnumerable<T> events,
        CancellationToken cancellationToken,
        IDictionary<string, string>? sharedHeaders = null)
        where T : IEvent;
}
