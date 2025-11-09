namespace Chidelu.Integration.Messaging.RabbitMQ.Core.Exceptions;

public sealed class FailedToProcessMessageException : Exception
{
    public FailedToProcessMessageException(string msg) : base(msg)
    { 
    }

    public FailedToProcessMessageException(string message, Exception inner) : base(message, inner) 
    {
    }
}
