using Chidelu.Integration.Messaging.RabbitMQ.Core;
using Chidelu.Integration.Messaging.RabbitMQ.Core.Exceptions;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Headers = Chidelu.Integration.Messaging.RabbitMQ.Core.Headers;

namespace Chidelu.Integration.Messaging.RabbitMQ.Consumer;

internal sealed class MessageDispatcher(
    IServiceProvider serviceProvider,
    IRabbitSerializer serializer,
    ILogger logger,
    Func<string, HandlerRegistry?> handlerResolver)
{
    public async Task<DispatchOutcome> DispatchAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        using var activity = BeginActivityScopeFromMetadata(envelope.Headers);

        try
        {
            var assemblyQualifiedType = Headers.GetRequiredString(envelope.Headers, KnownMetadata.Type);
            var messageType = ResolveMessageType(assemblyQualifiedType);
            var handlerKey = messageType.FullName ?? messageType.Name;

            var registration = handlerResolver(handlerKey)
                ?? throw new CannotProcessMessageNonTransientException($"No handler registered for '{handlerKey}'.");

            var deserialized = serializer.Deserialize(envelope.Body.Span, messageType)
                ?? throw new DeserializationException(
                    $"Deserialization returned null. MessageType: {messageType}");

            await registration.Invoke(serviceProvider, deserialized, envelope, cancellationToken);
            return DispatchOutcome.Ack;
        }
        catch (FailedToProcessMessageException ex)
        {
            logger.LogError(ex, "Message rejected without requeue. Reason: failed-to-process.");
            return DispatchOutcome.NackDrop;
        }
        catch (Exception ex) when (ex is CannotProcessMessageNonTransientException || ex is DeserializationException)
        {
            logger.LogError(ex, "Message rejected without requeue. Reason: non-transient.");
            return DispatchOutcome.NackDrop;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Message processing failed; requeueing.");
            return DispatchOutcome.NackRequeue;
        }
    }

    private static Type ResolveMessageType(string assemblyQualifiedType)
    {
        var resolved =
            Type.GetType(assemblyQualifiedType, throwOnError: false)
            ?? AppDomain.CurrentDomain.GetAssemblies()
                .Select(a => a.GetType(assemblyQualifiedType, throwOnError: false))
                .FirstOrDefault(t => t is not null);

        return resolved ?? throw new DeserializationException(
            $"Unable to resolve message type '{assemblyQualifiedType}'.");
    }

    private static IDisposable BeginActivityScopeFromMetadata(IDictionary<string, object?>? headers)
    {
        var parentId = Headers.GetString(headers, KnownMetadata.ParentOperationId);

        return RabbitMqDiagnostics.StartActivity("rabbitmq-consumer", ActivityKind.Consumer, parentId);
    }

    private sealed class NoopScope : IDisposable { public void Dispose() { } }
}
