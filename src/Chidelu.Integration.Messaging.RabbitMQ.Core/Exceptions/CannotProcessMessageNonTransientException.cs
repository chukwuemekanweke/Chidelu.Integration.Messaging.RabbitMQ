namespace Chidelu.Integration.Messaging.RabbitMQ.Core.Exceptions;

public sealed class CannotProcessMessageNonTransientException : Exception
{
    public CannotProcessMessageNonTransientException(string msg) : base(msg)
    { 
    }

    public CannotProcessMessageNonTransientException(string message, Exception inner) : base(message, inner) 
    {
    }
}
