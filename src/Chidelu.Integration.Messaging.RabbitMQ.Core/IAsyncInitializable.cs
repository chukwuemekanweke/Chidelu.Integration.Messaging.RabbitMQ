namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public interface IAsyncInitializable
{
    Task InitializeAsync(CancellationToken cancellationToken = default);
}
