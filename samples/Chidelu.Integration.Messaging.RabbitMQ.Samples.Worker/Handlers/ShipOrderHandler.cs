using Chidelu.Integration.Messaging.RabbitMQ.Consumer;
using Chidelu.Integration.Messaging.RabbitMQ.Samples.Contracts;

namespace Chidelu.Integration.Messaging.RabbitMQ.Samples.Worker.Handlers;

public sealed class ShipOrderHandler(ILogger<ShipOrderHandler> logger)
    : IMessageHandler<ShipOrder>
{
    public Task HandleAsync(ShipOrder message, CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "ShipOrder received. OrderId={OrderId} MessageId={MessageId}",
            message.OrderId,
            message.MessageId);
        return Task.CompletedTask;
    }
}
