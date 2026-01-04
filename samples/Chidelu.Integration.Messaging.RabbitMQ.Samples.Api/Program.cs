using Chidelu.Integration.Messaging.RabbitMQ.Publisher;
using Chidelu.Integration.Messaging.RabbitMQ.Publisher.DependencyInjection;
using Chidelu.Integration.Messaging.RabbitMQ.Samples.Contracts;
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

var rabbit = builder.Configuration.GetRequiredSection("RabbitMQ").Get<RabbitMqOptions>()
    ?? throw new InvalidOperationException("RabbitMQ configuration section is missing.");

var serviceName = builder.Environment.ApplicationName;

var publisherConfig = new PublisherConfig
{
    ServiceName = serviceName,
    HostName = rabbit.HostName,
    Port = rabbit.Port,
    UserName = rabbit.UserName,
    Password = rabbit.Password,
    VirtualHost = rabbit.VirtualHost,
    EventsExchange = rabbit.EventsExchange
};

var senderConfig = new SenderConfig
{
    ServiceName = serviceName,
    HostName = rabbit.HostName,
    Port = rabbit.Port,
    UserName = rabbit.UserName,
    Password = rabbit.Password,
    VirtualHost = rabbit.VirtualHost,
    CommandsExchange = rabbit.CommandsExchange
};

builder.Services
    .AddPublisher(publisherConfig, dependencyInjectionKey: DependencyInjectionKeys.Publisher)
    .AddSender(senderConfig, dependencyInjectionKey: DependencyInjectionKeys.Sender);

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapControllers();

app.Run();

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
