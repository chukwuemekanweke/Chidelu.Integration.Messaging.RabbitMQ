# Chidelu.Integration.Messaging.RabbitMQ

RabbitMQ transport layer for events (pub/sub) and commands (point-to-point) with consistent routing, retry, and DLQ behavior.

## Features
- Events published to a fanout exchange, subscribers each receive their own copy.
- Commands sent to a direct exchange, bound to a single consumer queue.
- Message dispatch by header `cimr-assembly-type` (assembly-qualified type name).
- Routing key uses the message type `FullName` (not assembly-qualified).
- Quorum queues with broker-side `x-delivery-limit` and per-queue DLQ.
- Non-retryable failures are dead-lettered immediately.
- Automatic topology installation on first publish/send and on consumer start.

## Contracts
Message contracts must implement `IEvent` or `ICommand`.

```csharp
using Chidelu.Integration.Messaging.RabbitMQ.Core;

public sealed record OrderCreated(Guid MessageId, string OrderId) : IEvent;
public sealed record ShipOrder(Guid MessageId, string OrderId) : ICommand;
```

`MessageId` must be a Guid v7 when provided. If `Guid.Empty`, the library generates a Guid v7 automatically.

## Dependency Injection
### Publisher (events)
```csharp
services.AddPublisher(
    new PublisherConfig
    {
        ServiceName = "orders-api",
        HostName = "localhost",
        Port = 5672,
        UserName = "guest",
        Password = "guest",
        VirtualHost = "/",
        EventsExchange = "x.events.orders"
    });
```

### Sender (commands)
```csharp
services.AddSender(
    new SenderConfig
    {
        ServiceName = "orders-api",
        HostName = "localhost",
        Port = 5672,
        UserName = "guest",
        Password = "guest",
        VirtualHost = "/",
        CommandsExchange = "x.commands.orders"
    });
```

### Subscriber (events)
```csharp
services.AddSubscriber(
    new SubscriberConfig
    {
        ServiceName = "orders-worker",
        HostName = "localhost",
        Port = 5672,
        UserName = "guest",
        Password = "guest",
        VirtualHost = "/",
        SubscriptionName = "orders",
        ExchangeName = "x.events.orders",
        PrefetchCount = 8,
        MaxRetryCount = 10,
        ConcurrentMessageCount = 4
    },
    b => b.AddHandler<OrderCreated, OrderCreatedHandler>());
```

### Consumer (commands)
```csharp
services.AddConsumer(
    new ConsumerConfig
    {
        ServiceName = "orders-worker",
        HostName = "localhost",
        Port = 5672,
        UserName = "guest",
        Password = "guest",
        VirtualHost = "/",
        QueueName = "orders",
        ExchangeName = "x.commands.orders",
        PrefetchCount = 8,
        MaxRetryCount = 10,
        ConcurrentMessageCount = 4
    },
    b => b.AddHandler<ShipOrder, ShipOrderHandler>());
```

### Multiple instances with keyed DI
```csharp
services.AddSubscriber(subscriberConfig, b => b.AddHandler<OrderCreated, OrderCreatedHandler>(), "events-key");
services.AddConsumer(consumerConfig, b => b.AddHandler<ShipOrder, ShipOrderHandler>(), "commands-key");

var subscriber = services.GetRequiredKeyedService<ISubscriber>("events-key");
var consumer = services.GetRequiredKeyedService<IConsumer>("commands-key");
```

## Publishing and Sending
```csharp
var publisher = services.GetRequiredKeyedService<IPublisher>(publisherConfig.Key);
await publisher.PublishAsync(new OrderCreated(Guid.CreateVersion7(), "ORD-1"), CancellationToken.None);

var sender = services.GetRequiredKeyedService<ISender>(senderConfig.Key);
await sender.SendAsync(new ShipOrder(Guid.CreateVersion7(), "ORD-1"), CancellationToken.None);
```

Additional headers can still be supplied explicitly:

```csharp
await publisher.PublishAsync(
    new OrderCreated(Guid.CreateVersion7(), "ORD-1"),
    CancellationToken.None,
    new Dictionary<string, string>
    {
        ["tenant-id"] = "tenant-42"
    });
```

## Consuming
`AddHandler` registers message handlers and determines the routing keys used for direct exchanges.

```csharp
var subscriber = services.GetRequiredKeyedService<ISubscriber>(subscriberConfig.Key);
var consumer = services.GetRequiredKeyedService<IConsumer>(consumerConfig.Key);

await subscriber.StartAsync(stoppingToken);
await consumer.StartAsync(stoppingToken);
```

Handlers should be registered before `StartAsync` so the queue bindings are created correctly.

Handlers can access publish/send headers by injecting `IMessageContext`.

```csharp
public sealed class OrderCreatedHandler(IMessageContext messageContext)
    : IMessageHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, CancellationToken cancellationToken)
    {
        var tenantId = messageContext.GetHeader("tenant-id");
        return Task.CompletedTask;
    }
}
```

## Routing and Headers
- Routing key: `typeof(T).FullName` (assembly name is not included).
- Type header: `cimr-assembly-type` with the assembly-qualified type name.
- Additional headers can be provided via `extraHeaders`.
- Explicit `extraHeaders` values override automatically propagated metadata values.

For fanout exchanges, RabbitMQ ignores routing keys, but the library still sets the routing key for consistency.

## Observability
This library carries both business metadata and tracing metadata through RabbitMQ headers.

Known metadata headers:
- `cimr-message-id`: logical message id. Must be UUIDv7 when supplied. If the message contract contains `Guid.Empty`, the library generates a UUIDv7.
- `cimr-correlation-id`: end-to-end correlation id used to group related messages in the same workflow.
- `cimr-causation-id`: identifies the message that directly caused the new message to be published or sent.
- `cimr-parent-operation-id`: tracing parent operation id, derived from `Activity.Current.Id`.
- `cimr-assembly-type`: assembly-qualified message type used for dispatch.

Automatic propagation behavior:
- When publishing or sending inside a consumer/subscriber handler, `CorrelationId` is copied from the current inbound message if present.
- When publishing or sending inside a consumer/subscriber handler, `CausationId` is set to the current inbound message's `MessageId` if present.
- `ParentOperationId` is set from `Activity.Current.Id` when an activity is active.
- If `CorrelationId` or `CausationId` are passed explicitly in `extraHeaders`, the explicit values win.

Consumer-side behavior:
- Incoming handlers can access all headers by injecting `IMessageContext`.
- The consumer uses `cimr-parent-operation-id` to start a new `Activity` with that value as the parent, preserving trace continuity across message boundaries.

Example:

```csharp
public sealed class OrderCreatedHandler(
    IMessageContext messageContext,
    IPublisher publisher) : IMessageHandler<OrderCreated>
{
    public async Task HandleAsync(OrderCreated message, CancellationToken cancellationToken)
    {
        var tenantId = messageContext.GetHeader("tenant-id");

        await publisher.PublishAsync(
            new OrderProcessed(Guid.CreateVersion7(), message.OrderId),
            cancellationToken);
    }
}
```

In the example above:
- `tenant-id` is available from the inbound message through `IMessageContext`.
- `CorrelationId` automatically flows to `OrderProcessed` if it was present on `OrderCreated`.
- `CausationId` on `OrderProcessed` is automatically set to `OrderCreated.MessageId`.
- `ParentOperationId` flows through the current `Activity` when tracing is enabled.

## Queue Naming Conventions
The runtime queue names are derived from the user-provided identifiers:

| Input | Queue | DLQ | DLX |
| --- | --- | --- | --- |
| `QueueName = "Orders"` | `cmd.orders` | `cmd.orders.dlq` | `x.dlx.cmd.orders` |
| `SubscriptionName = "Payments"` | `evt.payments` | `evt.payments.dlq` | `x.dlx.evt.payments` |

Names are normalized to lowercase.

## Retry and DLQ Behavior
- All queues are quorum queues with `x-delivery-limit` set to `MaxRetryCount`.
- `FailedToProcessMessageException` is treated as non-retryable and dead-lettered immediately.
- `CannotProcessMessageNonTransientException` and deserialization errors are dead-lettered immediately.
- Other exceptions result in a requeue until the broker delivery limit is reached.
- DLQ queues dead-letter back to the main queue using the default exchange.

## Samples
This repo includes:
- `samples/Chidelu.Integration.Messaging.RabbitMQ.Samples.Api` for sending events/commands.
- `samples/Chidelu.Integration.Messaging.RabbitMQ.Samples.Worker` for subscriber/consumer processing.

The sample worker subscribes to the same exchanges used by the API.
