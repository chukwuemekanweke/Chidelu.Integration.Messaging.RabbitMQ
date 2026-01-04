using Chidelu.Integration.Messaging.RabbitMQ.Publisher;
using Chidelu.Integration.Messaging.RabbitMQ.Samples.Contracts;
using Microsoft.AspNetCore.Mvc;

namespace Chidelu.Integration.Messaging.RabbitMQ.Samples.Api.Controllers;

[ApiController]
[Route("orders")]
public sealed class OrdersController : ControllerBase
{
    [HttpPost("events")]
    public async Task<IActionResult> PublishEvent(
        [FromKeyedServices(DependencyInjectionKeys.Publisher)] IPublisher publisher,
        CancellationToken cancellationToken)
    {
        var orderId = Guid.CreateVersion7().ToString("N");
        var message = new OrderCreated(Guid.CreateVersion7(), orderId);
        await publisher.PublishAsync(message, cancellationToken);
        return Accepted(new { message.MessageId, orderId });
    }

    [HttpPost("ship")]
    public async Task<IActionResult> SendCommand(
        [FromKeyedServices(DependencyInjectionKeys.Sender)] ISender sender,
        CancellationToken cancellationToken)
    {
        var orderId = Guid.CreateVersion7().ToString("N");
        var message = new ShipOrder(Guid.CreateVersion7(), orderId);
        await sender.SendAsync(message, cancellationToken);
        return Accepted(new { message.MessageId, orderId });
    }

    [HttpGet("health")]
    public IActionResult Health()
        => Ok(new { status = "ok" });
}
