using System.Diagnostics;

namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public static class RabbitMqDiagnostics
{
    public static Activity StartActivity(string name, string? parentId = null)
    {
        var activity = new Activity(name);

        if (!string.IsNullOrWhiteSpace(parentId))
        {
            activity.SetParentId(parentId);
        }

        activity.Start();
        return activity;
    }
}
