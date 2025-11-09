namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public interface IRabbitSerializer
{
    byte[] Serialize<T>(T value);
    object? Deserialize(ReadOnlySpan<byte> data, Type type);
}
