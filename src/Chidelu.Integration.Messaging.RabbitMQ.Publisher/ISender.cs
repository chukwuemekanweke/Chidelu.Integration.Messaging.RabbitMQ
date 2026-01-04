using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public interface ISender
{
    Task SendAsync<T>(
        T command,
        CancellationToken cancellationToken,
        IDictionary<string, string>? extraHeaders = null)
        where T : ICommand;
}
