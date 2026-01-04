using Chidelu.Integration.Messaging.RabbitMQ.Core;

namespace Chidelu.Integration.Messaging.RabbitMQ.Samples.Contracts;

public sealed record ShipOrder(Guid MessageId, string OrderId) : ICommand;
