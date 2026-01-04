namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal enum DispatchOutcome
{
    Ack = 1,
    NackRequeue = 2,
    NackDrop = 3
}
