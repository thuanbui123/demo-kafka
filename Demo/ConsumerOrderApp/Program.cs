using CommonLib;
using Confluent.Kafka;
using System.Text;
using System.Text.Json;

var config = new ConsumerConfig
{
    BootstrapServers = KafkaConstants.Bootstrap,
    GroupId = KafkaConstants.GroupOrders,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    SessionTimeoutMs = 10000,
    MaxPollIntervalMs = 300000,
    HeartbeatIntervalMs = 3000
};

var producerConfig = new ProducerConfig
{
    BootstrapServers = KafkaConstants.Bootstrap,
};

using var consumer = new ConsumerBuilder<string, Order>(config)
    .SetValueDeserializer(new KafkaJsonDeserializer<Order>())
    .Build();

using var dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();
using var retryProducer = new ProducerBuilder<string, Order>(producerConfig)
    .SetValueSerializer(new KafkaJsonSerializer<Order>())
    .Build();

var topics = new List<string> { KafkaConstants.TopicNewOrder, KafkaConstants.TopicCancelOrder };
consumer.Subscribe(topics);
Console.WriteLine("Consumer started. Listening...");

while(true)
{
    var cr = consumer.Consume();
    try
    {
        var msg = cr.Message;
        var order = msg.Value;
        if (order.Price < 0 || string.IsNullOrWhiteSpace(order.CustomerName) || string.IsNullOrWhiteSpace(order.ProductName))
        {
            throw new Exception("Business rule violated: invalid order.");
        }
        Console.WriteLine($"Processed: {order.CustomerName} | {order.ProductName} | {order.Price:N0} VND | {cr.TopicPartitionOffset}");
        consumer.Commit(cr);
    } catch (ConsumeException cex)
    {
        Console.WriteLine($"Kafka consume error: {cex.Error.Reason}");
    } catch (Exception ex)
    {
        int retryCount = 0;
        if (cr.Message.Headers != null)
        {
            var header = cr.Message.Headers.FirstOrDefault(h => h.Key == KafkaConstants.RetryHeader);
            if (header != null)
            {
                var s = Encoding.UTF8.GetString(header.GetValueBytes());
                int.TryParse(s, out retryCount);
            }
        }

        if(retryCount < KafkaConstants.MaxRetry)
        {
            var newHeaders = new Headers();
            newHeaders.Add(KafkaConstants.RetryHeader, Encoding.UTF8.GetBytes((retryCount + 1).ToString()));

            if(cr.Message.Value.Type == "new")
            {
                await retryProducer.ProduceAsync(KafkaConstants.TopicNewOrder, new Message<string, Order>
                {
                    Key = cr.Message.Key,
                    Value = cr.Message.Value,
                    Headers = newHeaders
                });
            } else if (cr.Message.Value.Type == "cancel")
            {
                await retryProducer.ProduceAsync(KafkaConstants.TopicNewOrder, new Message<string, Order>
                {
                    Key = cr.Message.Key,
                    Value = cr.Message.Value,
                    Headers = newHeaders
                });
            }

            Console.WriteLine($"🔁 Retry {retryCount + 1}/{KafkaConstants.MaxRetry} for topic={cr.Topic} key={cr.Message.Key}. Error={ex.Message}");
        } else
        {
            var dlqPayload = JsonSerializer.Serialize(new
            {
                Key = cr.Message.Key,
                Value = cr.Message.Value,
                Error = ex.Message,
                OriginalHeaders = cr.Message.Headers?.Select(h => new { h.Key, Value = Convert.ToBase64String(h.GetValueBytes()) }),
                TimestampUtc = DateTime.UtcNow
            });

            await dlqProducer.ProduceAsync(KafkaConstants.TopicProductsDlq, new Message<string, string>
            {
                Key = cr.Message.Key,
                Value = dlqPayload
            });

            Console.WriteLine($"🧨 Sent to DLQ | key={cr.Message.Key} | reason={ex.Message}");
        }
    }
}