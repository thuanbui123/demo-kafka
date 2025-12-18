using CommonLib;
using Confluent.Kafka;
using System.Text.Json;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

using var producer = new ProducerBuilder<string, string>(config).Build();

var random = new Random();
string[] pharmacies = { "A", "B", "C" };
string[] types = { "import", "export" };

while (true)
{
    var t = new Transaction
    {
        PharmacyId = pharmacies[random.Next(pharmacies.Length)],
        Type = types[random.Next(types.Length)],
        Amount = random.Next(1, 100)
    };

    var topic = t.Type == "import" ? "transactions_import" : "transactions_export";

    var message = new Message<string, string>
    {
        Key = t.PharmacyId,
        Value = JsonSerializer.Serialize(t)
    };

    await producer.ProduceAsync(topic, message);
    Console.WriteLine($"Sent to {topic}: {t.PharmacyId} - {t.Type} - {t.Amount}");
    await Task.Delay(300);
}