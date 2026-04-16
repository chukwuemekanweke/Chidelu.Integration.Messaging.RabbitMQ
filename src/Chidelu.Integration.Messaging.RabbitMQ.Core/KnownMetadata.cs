namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public static class KnownMetadata
{
    /// <summary>Trace/Operation id used for correlation.</summary>
    public const string ParentOperationId = "cimr-parent-operation-id";

    /// <summary>Assembly qualified type name for the message.</summary>
    public const string Type = "cimr-assembly-type";

    /// <summary>Logical message id.</summary>
    public const string MessageId = "cimr-message-id";

    /// <summary>Optional: end-to-end correlation id.</summary>
    public const string CorrelationId = "cimr-correlation-id";

    /// <summary>Optional: causation id for event sourcing pipelines.</summary>
    public const string CausationId = "cimr-causation-id";
}
