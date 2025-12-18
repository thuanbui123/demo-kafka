using CommonLib;
using Confluent.Kafka;
using System.Collections.Concurrent;
using System.Text.Json;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "batch-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

var importBuffer = new ConcurrentBag<Transaction>();
var exportBuffer = new ConcurrentBag<Transaction>();

Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(3000);
        if(importBuffer.Count > 0)
        {
            var batch = importBuffer.ToArray();
            importBuffer.Clear();

            Console.WriteLine($"Processing import batch: {batch.Length} transactions");
            await ProcessBatchAsync(batch, "IMPORT");
        }
    }
});

Task.Run(async () =>
{
    while(true)
    {
        await Task.Delay(3000);
        if(exportBuffer.Count > 0)
        {
            var batch = exportBuffer.ToArray();
            exportBuffer.Clear();

            Console.WriteLine($"Processing export batch: {batch.Length} transactions");
            await ProcessBatchAsync(batch, "EXPORT");
        }
    }
});

async Task ProcessBatchAsync(Transaction[] batch, string type)
{
    await Task.WhenAll(batch
        .GroupBy(t => t.PharmacyId)
        .Select(async group =>
        {
            double total = group.Sum(g => g.Amount);
            Console.WriteLine($"Pharmacy {group.Key} total {type} amount: {total}");
            await Task.Delay(200);
        }));
}

using var consumer = new ConsumerBuilder<string, string>(config).Build();

consumer.Subscribe(new[] { "transactions_import", "transactions_export" });

Console.WriteLine("Consumer started...");


try
{
    while (true)
    {
        var cr = consumer.Consume(TimeSpan.FromSeconds(10));

        if(cr == null)
        {
            Console.WriteLine("No message");
            continue;
        }
        var transaction = JsonSerializer.Deserialize<Transaction>(cr.Message.Value);

        if(cr.Topic == "transactions_import")
        {
            importBuffer.Add(transaction!);
        } else if (cr.Topic == "transactions_export")
        {
            exportBuffer.Add(transaction!);
        }
    }
} catch (Exception e)
{
    Console.WriteLine($"Error: {e.Message}");
}