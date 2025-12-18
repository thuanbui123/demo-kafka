using CommonLib;
using Confluent.Kafka;
using Newtonsoft.Json;
using System.Collections.Concurrent;

const string bootstrap = "localhost:9092"; // Kafka broker của bạn
const string statusTopic = "status-topic";
var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = bootstrap,
    EnableIdempotence = true,
    Acks = Acks.All
}).Build();

var consumers = new ConcurrentDictionary<string, ConsumerStatus>();

// Thread 1: Lắng nghe trạng thái
var statusListener = Task.Run(() =>
{
    var cfg = new ConsumerConfig
    {
        BootstrapServers = bootstrap,
        GroupId = "main-worker-group",
        AutoOffsetReset = AutoOffsetReset.Earliest
    };

    using var consumer = new ConsumerBuilder<string, string>(cfg).Build();
    consumer.Subscribe(statusTopic);
    Console.WriteLine("📡 Listening consumer status...");

    while (true)
    {
        try
        {
            var cr = consumer.Consume();
            var status = JsonConvert.DeserializeObject<ConsumerStatus>(cr.Message.Value);
            consumers[status.ConsumerId] = status;
            Console.WriteLine($"🟢 {status.ConsumerId} ({status.Load}%)");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Status error: {ex.Message}");
        }
    }
});

// Thread 2: Gửi job cho consumer rảnh
while (true)
{
    await Task.Delay(2000);

    if (consumers.IsEmpty)
    {
        Console.WriteLine("⚠️ No consumers yet...");
        continue;
    }

    var availableConsumers = consumers.Values
        .Where(x => x.Load < 90)
        .OrderBy(x => x.Load);

    if (!availableConsumers.Any())
    {
        Console.WriteLine("⚠️ No available consumers, waiting...");
        await Task.Delay(2000);
        continue;
    }

    int minLoad = availableConsumers.First().Load;
    var candidates = availableConsumers.Where(x => x.Load == minLoad).ToList();
    var target = candidates[Random.Shared.Next(candidates.Count)];

    var msg = new WorkMessage
    {
        Id = Guid.NewGuid(),
        CreatedAt = DateTime.UtcNow,
        Payload = $"Work for {target.ConsumerId}"
    };

    await producer.ProduceAsync(target.Topic, new Message<string, string>
    {
        Key = msg.Id.ToString(),
        Value = JsonConvert.SerializeObject(msg)
    });

    Console.WriteLine($"📤 Sent work {msg.Id} -> {target.Topic}");
}
