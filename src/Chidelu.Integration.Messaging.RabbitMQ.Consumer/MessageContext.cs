using Chidelu.Integration.Messaging.RabbitMQ.Core;
using CoreHeaders = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal sealed class MessageContext : IMessageContext
{
    private static readonly Dictionary<string, object?> EmptyHeaders =
        new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

    private Dictionary<string, object?> _headers = EmptyHeaders;

    public IReadOnlyDictionary<string, object?> Headers => _headers;

    public string? MessageType => GetHeader(KnownMetadata.Type);

    public Guid? MessageId
        => _headers.TryGetValue(KnownMetadata.MessageId, out _)
            && CoreHeaders.TryGetGuid(_headers, KnownMetadata.MessageId, out var value)
                ? value
                : null;

    public string? CorrelationId => GetHeader(KnownMetadata.CorrelationId);

    public string? CausationId => GetHeader(KnownMetadata.CausationId);

    public string? OriginatingOperationId => GetHeader(KnownMetadata.OriginatingOperationId);

    public string? GetHeader(string key) => CoreHeaders.GetString(_headers, key);

    internal void SetHeaders(IDictionary<string, object?>? headers)
    {
        _headers = headers is null
            ? EmptyHeaders
            : new Dictionary<string, object?>(headers, StringComparer.OrdinalIgnoreCase);
    }
}
