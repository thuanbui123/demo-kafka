using CommonLib;
using Confluent.Kafka;
using Newtonsoft.Json;
using System.Collections.Concurrent;

if (args.Length < 2)
{
    Console.WriteLine("Usage: dotnet run <ConsumerId> <Topic>");
    return;
}

string consumerId = args[0];
string topic = args[1];
const string bootstrap = "localhost:9092";
const string statusTopic = "status-topic";
int maxWorkers = 3;

var producer = new ProducerBuilder<string, string>(
    new ProducerConfig { BootstrapServers = bootstrap }).Build();

var workers = new ConcurrentDictionary<Guid, Task>();

var cfg = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = consumerId, // mỗi consumer instance riêng group
    AutoOffsetReset = AutoOffsetReset.Earliest
};

try
{
    var adminService = new KafkaAdminService(bootstrap);
    await adminService.EnsureTopicExistsAsync(topic);

} catch (Exception e)
{
    Console.WriteLine(e.Message);
}

using var consumer = new ConsumerBuilder<string, string>(cfg).Build();
consumer.Subscribe(topic);
Console.WriteLine($"🔔 {consumerId} subscribed {topic}");

async Task ReportStatusAsync()
{
    int load = (int)((double)workers.Count / maxWorkers * 100);
    var status = new ConsumerStatus { ConsumerId = consumerId, Topic = topic, Load = load };
    await producer.ProduceAsync(statusTopic, new Message<string, string>
    {
        Value = JsonConvert.SerializeObject(status)
    });
}

await ReportStatusAsync();

while (true)
{
    var cr = consumer.Consume();
    var job = JsonConvert.DeserializeObject<WorkMessage>(cr.Message.Value);
    
    Console.WriteLine($"📥 {consumerId} received job {job.Id}");

    if (workers.Count >= maxWorkers)
    {
        Console.WriteLine($"⚠️ {consumerId} overloaded ({workers.Count}/{maxWorkers})");
        
    } else
    {
        Console.WriteLine($"⚠️ {consumerId} ({workers.Count}/{maxWorkers})");
        await ReportStatusAsync();
    }

    var task = Task.Run(async () =>
    {
        try
        {
            Console.WriteLine($"🧩 {consumerId} start job {job.Id}");
            await Task.Delay(Random.Shared.Next(20000, 60000)); // xử lý giả lập
            Console.WriteLine($"✅ {consumerId} done job {job.Id}");
        }
        finally
        {
            workers.TryRemove(job.Id, out _);
            await ReportStatusAsync(); // 🟢 Báo lại load giảm
        }
    });

    workers[job.Id] = task;
    await ReportStatusAsync();
}
