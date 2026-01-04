using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Shouldly;
using System.Text;

namespace Wicrypt.Integration.Messages.RabbitMQ.Core.Tests.Unit;

public sealed class DefaultRabbitSerializerTests
{
    private readonly DefaultRabbitSerializer _sut = new();

    private sealed record Sample(
        string OrderId,
        decimal Amount,
        PaymentMethod Method,
        string? OptionalNote = null);

    private enum PaymentMethod { Card, Transfer, Cash }

    [Fact]
    public void Serialize_Should_CreateUtf8Json_WithCamelCase_AndIgnoreNulls()
    {
        var model = new Sample("ORD-1", 42.50m, PaymentMethod.Transfer, OptionalNote: null);

        var bytes = _sut.Serialize(model);
        var json = Encoding.UTF8.GetString(bytes);

        json.ShouldContain("\"orderId\":\"ORD-1\"");
        json.ShouldContain("\"amount\":42.50");
        json.ShouldContain("\"method\":\"Transfer\"");
        json.ShouldNotContain("optionalNote");
    }

    [Fact]
    public void Deserialize_Should_Roundtrip_SimpleModel()
    {
        var original = new Sample("ORD-2", 100.01m, PaymentMethod.Card, "hello");
        var bytes = _sut.Serialize(original);

        var obj = (Sample)_sut.Deserialize(bytes, typeof(Sample))!;
        obj.ShouldNotBeNull();
        obj.OrderId.ShouldBe("ORD-2");
        obj.Amount.ShouldBe(100.01m);
        obj.Method.ShouldBe(PaymentMethod.Card);
        obj.OptionalNote.ShouldBe("hello");
    }

    [Fact]
    public void Deserialize_Should_Handle_ReadOnlySpan_Over_JsonBytes()
    {
        var original = new Sample("ORD-3", 10m, PaymentMethod.Cash);
        var bytes = _sut.Serialize(original);
        var span = bytes.AsSpan();

        var obj = (Sample)_sut.Deserialize(span, typeof(Sample))!;
        obj.OrderId.ShouldBe("ORD-3");
    }

    [Fact]
    public void Serializer_Should_Handle_DateTimeOffset_Roundtrip_As_Iso()
    {
        var now = DateTimeOffset.UtcNow.ToOffset(TimeSpan.Zero).ToString("O");
        var payload = $"{{\"when\":\"{now}\"}}";
        var bytes = Encoding.UTF8.GetBytes(payload);

        var model = (WithWhen)_sut.Deserialize(bytes, typeof(WithWhen))!;
        model.When.ToString("O").ShouldBe(now);

        var reBytes = _sut.Serialize(model);
        var roundTrip = (WithWhen)_sut.Deserialize(reBytes, typeof(WithWhen))!;
        roundTrip.When.ToString("O").ShouldBe(now);
    }

    private sealed record WithWhen(DateTimeOffset When);
}
