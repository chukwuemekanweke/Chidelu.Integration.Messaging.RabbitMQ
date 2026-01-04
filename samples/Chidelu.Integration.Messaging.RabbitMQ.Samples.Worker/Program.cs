using Chidelu.Integration.Messaging.RabbitMQ.Consumer;
using Chidelu.Integration.Messaging.RabbitMQ.Consumer.DependencyInjection;
using Chidelu.Integration.Messaging.RabbitMQ.Samples.Contracts;
using Chidelu.Integration.Messaging.RabbitMQ.Samples.Worker.Handlers;
using Chidelu.Integration.Messaging.RabbitMQ.Samples.Worker;

var builder = Host.CreateApplicationBuilder(args);

var rabbit = builder.Configuration.GetRequiredSection("RabbitMQ").Get<RabbitMqOptions>()
    ?? throw new InvalidOperationException("RabbitMQ configuration section is missing.");

var serviceName = builder.Environment.ApplicationName;

var subscriberConfig = new SubscriberConfig
{
    ServiceName = serviceName,
    HostName = rabbit.HostName,
    Port = rabbit.Port,
    UserName = rabbit.UserName,
    Password = rabbit.Password,
    VirtualHost = rabbit.VirtualHost,
    SubscriptionName = "orders",
    ExchangeName = rabbit.EventsExchange,
    PrefetchCount = 5,
    MaxRetryCount = 10,
    ConcurrentMessageCount = 1
};

var consumerConfig = new ConsumerConfig
{
    ServiceName = serviceName,
    HostName = rabbit.HostName,
    Port = rabbit.Port,
    UserName = rabbit.UserName,
    Password = rabbit.Password,
    VirtualHost = rabbit.VirtualHost,
    QueueName = "orders",
    ExchangeName = rabbit.CommandsExchange,
    PrefetchCount = 5,
    MaxRetryCount = 10,
    ConcurrentMessageCount = 1
};

builder.Services
    .AddSubscriber(subscriberConfig, b => b.AddHandler<OrderCreated, OrderCreatedHandler>())
    .AddConsumer(consumerConfig, b => b.AddHandler<ShipOrder, ShipOrderHandler>());

builder.Services.AddHostedService(sp =>
    new Worker(
        sp.GetRequiredKeyedService<ISubscriber>(subscriberConfig.Key),
        sp.GetRequiredKeyedService<IConsumer>(consumerConfig.Key),
        sp.GetRequiredService<ILogger<Worker>>()));

var host = builder.Build();
host.Run();

internal sealed class RabbitMqOptions
{
    public required string HostName { get; init; }
    public required int Port { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
    public required string VirtualHost { get; init; }
    public required string EventsExchange { get; init; }
    public required string CommandsExchange { get; init; }
}
