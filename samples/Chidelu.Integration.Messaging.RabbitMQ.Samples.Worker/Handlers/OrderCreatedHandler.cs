using Chidelu.Integration.Messaging.RabbitMQ.Consumer;
using Chidelu.Integration.Messaging.RabbitMQ.Samples.Contracts;

namespace Chidelu.Integration.Messaging.RabbitMQ.Samples.Worker.Handlers;

public sealed class OrderCreatedHandler(
    ILogger<OrderCreatedHandler> logger,
    IMessageContext messageContext)
    : IMessageHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "OrderCreated received. OrderId={OrderId} MessageId={MessageId} TenantId={TenantId}",
            message.OrderId,
            message.MessageId,
            messageContext.GetHeader("tenant-id"));
        return Task.CompletedTask;
    }
}
