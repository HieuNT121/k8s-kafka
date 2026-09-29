using Confluent.Kafka;
using System.Text.Json;
using OrderApi.Models;

namespace OrderApi.Services;

public class KafkaProducerService
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;

    public KafkaProducerService(
        IConfiguration configuration)
    {
        var bootstrapServers =
            configuration["Kafka:BootstrapServers"]
            ?? throw new InvalidOperationException(
                "Kafka bootstrap servers are not configured.");

        _topic =
            configuration["Kafka:Topic"]
            ?? throw new InvalidOperationException(
                "Kafka topic is not configured.");

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        _producer =
            new ProducerBuilder<string, string>(config)
                .Build();
    }

    public async Task ProduceAsync(
        OrderCreatedEvent orderEvent)
    {
        var message = new Message<string, string>
        {
            Key = orderEvent.OrderId,
            Value = JsonSerializer.Serialize(orderEvent)
        };

        await _producer.ProduceAsync(
            _topic,
            message);
    }
}