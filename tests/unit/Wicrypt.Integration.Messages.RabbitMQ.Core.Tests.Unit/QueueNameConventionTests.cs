using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Shouldly;

namespace Wicrypt.Integration.Messages.RabbitMQ.Core.Tests.Unit;

public sealed class QueueNameConventionTests
{
    [Fact]
    public void BuildCommandQueueName_Should_Normalize_And_AddPrefix()
    {
        QueueNameConvention.BuildCommandQueueName("Orders").ShouldBe("cmd.orders");
        QueueNameConvention.BuildCommandQueueName("cmd.Payments").ShouldBe("cmd.payments");
    }

    [Fact]
    public void BuildEventQueueName_Should_Normalize_And_AddPrefix()
    {
        QueueNameConvention.BuildEventQueueName("payments").ShouldBe("evt.payments");
        QueueNameConvention.BuildEventQueueName("evt.Notifications").ShouldBe("evt.notifications");
    }

    [Fact]
    public void DeadLetterNames_Should_Be_Derived_From_QueueName()
    {
        QueueNameConvention.BuildDeadLetterQueueName("cmd.orders").ShouldBe("cmd.orders.dlq");
        QueueNameConvention.BuildDeadLetterExchangeName("evt.payments").ShouldBe("x.dlx.evt.payments");
    }

    [Fact]
    public void BuildQueueName_Should_Throw_On_Whitespace()
    {
        Should.Throw<ArgumentException>(() => QueueNameConvention.BuildCommandQueueName(" "));
    }
}
