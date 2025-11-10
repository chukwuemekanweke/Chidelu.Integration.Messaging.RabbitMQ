namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public interface IAsyncInitializable
{
    Task InitializeAsync(CancellationToken cancellationToken = default);
}
