using Confluent.Kafka;
using Confluent.Kafka.Admin;

class Producer
{
    public async Task Main()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        var adminConfig = new AdminClientConfig { BootstrapServers = "localhost:9092" };
        using (var adminClient = new AdminClientBuilder(config).Build())
        {
            try
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[]
                {
            new TopicSpecification { Name = "demo-topic", NumPartitions = 3, ReplicationFactor = 1 }
                });

                Console.WriteLine("Topic 'demo-topic' created with 3 partitions.");
            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine($"Error creating topic: {e.Results[0].Error.Reason}");
            }
        }

        for (int i = 1; i <= 30; i++)
        {
            var key = (i % 3) switch
            {
                0 => "region-1",
                1 => "region-2",
                _ => "region-3"
            };

            var value = $"Message {i} from {key}";
            await producer.ProduceAsync("demo-topic", new Message<string, string> { Value = value });
            Console.WriteLine($"Produced: {value}");
        }

        producer.Flush(TimeSpan.FromSeconds(5));
        Console.WriteLine("All messages produced!");
    }
}
