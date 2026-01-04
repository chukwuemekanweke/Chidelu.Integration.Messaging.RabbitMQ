using Chidelu.Integration.Messaging.RabbitMQ.Consumer;

namespace Chidelu.Integration.Messaging.RabbitMQ.Samples.Worker;

public sealed class Worker(
    ISubscriber subscriber,
    IConsumer consumer,
    ILogger<Worker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Starting message consumers.");
        await subscriber.StartAsync(stoppingToken);
        await consumer.StartAsync(stoppingToken);

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown.
        }
        finally
        {
            logger.LogInformation("Stopping message consumers.");
            await subscriber.StopAsync(CancellationToken.None);
            await consumer.StopAsync(CancellationToken.None);
        }
    }
}
