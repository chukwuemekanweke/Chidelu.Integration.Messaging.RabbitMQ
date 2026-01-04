using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public interface ISender
{
    Task SendAsync<T>(
        T command,
        IDictionary<string, string>? extraHeaders = null,
        CancellationToken cancellationToken = default)
        where T : ICommand;
}
