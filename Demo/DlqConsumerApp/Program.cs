using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "dlq-reader",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe("products-dlq");

Console.WriteLine("👀 DLQ Reader started.");
while (true)
{
    var cr = consumer.Consume();
    Console.WriteLine($"DLQ >> {cr.Message.Value}");
}
