namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal enum DispatchOutcome
{
    Ack,
    NackRequeue,
    NackDrop
}
