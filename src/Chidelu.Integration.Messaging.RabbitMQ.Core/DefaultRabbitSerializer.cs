using System.Text.Json;
using System.Text.Json.Serialization;

namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public sealed class DefaultRabbitSerializer : IRabbitSerializer
{
    private readonly System.Text.Json.JsonSerializerOptions _json;
    public DefaultRabbitSerializer(JsonSerializerOptions? options = null) =>
         _json = options ?? new(JsonSerializerDefaults.Web)
         {
             WriteIndented = false,
             DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
             ReferenceHandler = ReferenceHandler.IgnoreCycles,
             Converters = { new JsonStringEnumConverter() }
         };

    public byte[] Serialize<T>(T value) =>
        JsonSerializer.SerializeToUtf8Bytes(value!, typeof(T), _json);

    public object? Deserialize(ReadOnlySpan<byte> data, Type type) =>
        JsonSerializer.Deserialize(data, type, _json);
}