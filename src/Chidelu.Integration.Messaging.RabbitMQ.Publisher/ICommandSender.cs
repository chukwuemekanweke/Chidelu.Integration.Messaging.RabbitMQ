namespace Chidelu.Integration.Messaging.RabbitMQ.Publisher;

public interface ICommandSender
{
    Task SendAsync<T>(
        T command,
        string queueName,
        IDictionary<string, string>? extraHeaders = null,
        CancellationToken cancellationToken = default);
}
