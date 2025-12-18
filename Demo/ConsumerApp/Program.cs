using CommonLib;
using Confluent.Kafka;
using System.Text.Json;
using System.Text;

var config = new ConsumerConfig
{
    BootstrapServers = KafkaConstants.Bootstrap,
    GroupId = KafkaConstants.GroupProducts,
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

using var consumer = new ConsumerBuilder<string, Product>(config)
    .SetValueDeserializer(new KafkaJsonDeserializer<Product>())
    .Build();

using var dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();
using var retryProducer = new ProducerBuilder<string, Product>(producerConfig)
    .SetValueSerializer(new KafkaJsonSerializer<Product>())
    .Build();

consumer.Subscribe(KafkaConstants.TopicProducts);
Console.WriteLine("Consumer started. Listening...");

while(true)
{
    var cr = consumer.Consume();
    try
    {
        var msg = cr.Message;
        var product = msg.Value;

        if(product.Price < 0 || string.IsNullOrWhiteSpace(product.Name))
        {
            throw new Exception("Business rule violated: invalid product.");
        }

        Console.WriteLine($"Processed: {product.Name} | {product.Price:N0} VND | {cr.TopicPartitionOffset}");
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

        if (retryCount < KafkaConstants.MaxRetry)
        {
            // Tăng retry và gửi lại vào topic gốc
            var newHeaders = new Headers();
            newHeaders.Add(KafkaConstants.RetryHeader, Encoding.UTF8.GetBytes((retryCount + 1).ToString()));

            await retryProducer.ProduceAsync(KafkaConstants.TopicProducts, new Message<string, Product>
            {
                Key = cr.Message.Key,
                Value = cr.Message.Value,
                Headers = newHeaders
            });

            Console.WriteLine($"🔁 Retry {retryCount + 1}/{KafkaConstants.MaxRetry} for key={cr.Message.Key}. Error={ex.Message}");
        }
        else
        {
            // Đủ số lần retry → gửi DLQ dạng JSON string chứa nguyên message + lỗi
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