using CommonLib;
using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = KafkaConstants.Bootstrap,
    GroupId = "dlq-order-reader",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe(KafkaConstants.TopicDlqOrder);

Console.WriteLine("👀 DLQ Order Reader started.");
while (true)
{
    var cr = consumer.Consume();
    Console.WriteLine($"DLQ >> {cr.Message.Value}");
}
