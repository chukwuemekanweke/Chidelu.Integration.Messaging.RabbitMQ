using System.Diagnostics;

namespace Chidelu.Integration.Messaging.RabbitMQ.Core;

public static class RabbitMqDiagnostics
{
    public const string ActivitySourceName = "Chidelu.Integration.Messaging.RabbitMQ";
    public static readonly ActivitySource Source = new(ActivitySourceName);

    public static Activity StartActivity(string name, ActivityKind kind, string? parentId = null)
    {
        var activity = TryStartFromSource(name, kind, parentId);

        if (activity is not null)
        {
            return activity;
        }

        // Fallback: plain Activity for backward compatibility (always works)
        activity = new Activity(name);

        if (!string.IsNullOrWhiteSpace(parentId))
        {
            activity.SetParentId(parentId);
        }

        activity.Start();
        return activity;
    }

    private static Activity? TryStartFromSource(string name, ActivityKind kind, string? parentId)
    {
        if (string.IsNullOrWhiteSpace(parentId))
        {
            return Source.StartActivity(name, kind);
        }

        if (ActivityContext.TryParse(parentId, null, out var context))
        {
            return Source.StartActivity(name, kind, context);
        }

        return null;
    }
}
