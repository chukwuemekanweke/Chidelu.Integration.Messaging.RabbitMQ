using Testcontainers.RabbitMq;

namespace Chidelu.Integration.Messaging.RabbitMQ.IntegrationTests;

public sealed class RabbitMqFixture : IAsyncLifetime
{
    public RabbitMqContainer Container { get; private set; } = default!;
    public string HostName => Container.Hostname;
    public int Port => Container.GetMappedPublicPort(5672);
    public string UserName => RabbitMqBuilder.DefaultUsername;
    public string Password => RabbitMqBuilder.DefaultPassword;
    public string VirtualHost => "/";

    public async ValueTask InitializeAsync()
    {
        Container = new RabbitMqBuilder("rabbitmq:3.13-management")
            .WithEnvironment("RABBITMQ_DEFAULT_VHOST", VirtualHost)
            .Build();

        await Container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (Container is not null)
        {
            await Container.DisposeAsync();
        }
    }

}

[CollectionDefinition("rabbitmq")]
public sealed class RabbitMqCollection : ICollectionFixture<RabbitMqFixture> { }
