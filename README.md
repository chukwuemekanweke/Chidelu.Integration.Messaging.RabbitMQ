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

## Consuming
`AddHandler` registers message handlers and determines the routing keys used for direct exchanges.

```csharp
var subscriber = services.GetRequiredKeyedService<ISubscriber>(subscriberConfig.Key);
var consumer = services.GetRequiredKeyedService<IConsumer>(consumerConfig.Key);

await subscriber.StartAsync(stoppingToken);
await consumer.StartAsync(stoppingToken);
```

Handlers should be registered before `StartAsync` so the queue bindings are created correctly.

## Routing and Headers
- Routing key: `typeof(T).FullName` (assembly name is not included).
- Type header: `cimr-assembly-type` with the assembly-qualified type name.
- Additional headers can be provided via `extraHeaders`.

For fanout exchanges, RabbitMQ ignores routing keys, but the library still sets the routing key for consistency.

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
