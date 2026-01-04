using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Samples.Contracts;

public sealed record OrderCreated(Guid MessageId, string OrderId) : IEvent;
