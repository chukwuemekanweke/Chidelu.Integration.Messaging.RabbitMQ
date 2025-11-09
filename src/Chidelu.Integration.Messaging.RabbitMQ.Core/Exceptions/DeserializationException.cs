namespace Chidelu.Integration.Messaging.RabbitMQ.Core.Exceptions;

public sealed class DeserializationException : Exception
{
    public DeserializationException(string msg) : base(msg) 
    {
    }

    public DeserializationException(string message, Exception inner) : base(message, inner) 
    {
    }
}
