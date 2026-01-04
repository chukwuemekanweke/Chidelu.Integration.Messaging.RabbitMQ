using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Chidelu.Integration.Messaging.RabbitMQ.Core.Exceptions;
using Shouldly;
using System.Text;

namespace Wicrypt.Integration.Messages.RabbitMQ.Core.Tests.Unit;

public sealed class HeadersTests
{
    [Fact]
    public void GetString_Should_Handle_String_And_Bytes()
    {
        var h = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["A"] = "hello",
            ["B"] = Encoding.UTF8.GetBytes("world"),
            ["C"] = 123 
        };

        Headers.GetString(h, "A").ShouldBe("hello");
        Headers.GetString(h, "B").ShouldBe("world");
        Headers.GetString(h, "C").ShouldBe("123");
        Headers.GetString(h, "missing").ShouldBeNull();
    }

    [Fact]
    public void GetRequiredString_Should_Throw_If_Missing()
    {
        var h = new Dictionary<string, object?>();
        Should.Throw<DeserializationException>(() => Headers.GetRequiredString(h, "x"));
    }

    [Fact]
    public void SetString_Should_Store_As_Utf8_Bytes()
    {
        var h = new Dictionary<string, object?>();
        Headers.SetString(h, "key", "value");
        var bytes = h["key"].ShouldBeOfType<byte[]>();
        Encoding.UTF8.GetString(bytes).ShouldBe("value");
    }

    [Fact]
    public void Guid_Roundtrip_Works()
    {
        var id = Guid.NewGuid();
        var h = new Dictionary<string, object?>();
        Headers.SetGuid(h, "id", id);

        Headers.TryGetGuid(h, "id", out var parsed).ShouldBeTrue();
        parsed.ShouldBe(id);
    }
}
