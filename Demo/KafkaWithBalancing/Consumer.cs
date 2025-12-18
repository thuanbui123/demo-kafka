using Confluent.Kafka;

class Consumer
{
    public async Task Main()
    {
        var tasks = new List<Task>();

        for (int i = 1; i <= 3; i++)
        {
            int id = i;
            tasks.Add(Task.Run(() => RunConsumer(id)));
        }

        await Task.WhenAll(tasks);
    }

    static void RunConsumer(int id)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "sales-group",
            ClientId = $"consumer-{id}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("demo-topic");

        Console.WriteLine($"Consumer {id} started.");

        try
        {
            while (true)
            {
                var cr = consumer.Consume();
                Console.WriteLine($"Consumer {id} got message from partition {cr.Partition.Value}: {cr.Message.Value}");
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }
}
