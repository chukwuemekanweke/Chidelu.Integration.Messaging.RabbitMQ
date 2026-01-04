using Chidelu.Integration.Messaging.RabbitMQ.Core.Exceptions;
using System.Text;

namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public static class Headers
{
    public static bool IsReservedHeader(string key)
        => string.Equals(key, KnownMetadata.Type, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.MessageId, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.CorrelationId, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.CausationId, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, KnownMetadata.OriginatingOperationId, StringComparison.OrdinalIgnoreCase);

    public static string? GetString(IDictionary<string, object?>? headers, string key)
    {
        if (headers is null || !headers.TryGetValue(key, out var v) || v is null) return null;
        return v switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> rom => Encoding.UTF8.GetString(rom.ToArray()),
            string s => s,
            _ => v.ToString()
        };
    }

    public static string GetRequiredString(IDictionary<string, object?>? headers, string key)
    {
        var s = GetString(headers, key);
        if (string.IsNullOrWhiteSpace(s))
        {
            throw new DeserializationException($"Missing or invalid header '{key}'.");
        }
        return s;
    }

    public static void SetString(IDictionary<string, object?> headers, string key, string value)
    {
        headers[key] = Encoding.UTF8.GetBytes(value);
    }

    public static bool TryGetGuid(IDictionary<string, object?>? headers, string key, out Guid value)
    {
        value = default;
        var s = GetString(headers, key);
        return s is not null && Guid.TryParse(s, out value);
    }

    public static void SetGuid(IDictionary<string, object?> headers, string key, Guid value)
    {
        SetString(headers, key, value.ToString());
    }
}
