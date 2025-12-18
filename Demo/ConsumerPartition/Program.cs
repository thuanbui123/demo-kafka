using CommonLib;
using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = KafkaConstants.Bootstrap,
    GroupId = "test-load-balancing-group", // GroupId phải giống nhau ở các cửa sổ
    AutoOffsetReset = AutoOffsetReset.Latest,
    EnableAutoCommit = true
};

using var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsAssignedHandler((c, partitions) =>
    {
        Console.BackgroundColor = ConsoleColor.DarkBlue;
        Console.WriteLine($"\n>>> ĐÃ NHẬN QUYỀN ĐỌC PARTITION: {string.Join(", ", partitions)} ");
        Console.ResetColor();
    })
    .SetPartitionsRevokedHandler((c, partitions) =>
    {
        Console.WriteLine("\n<<< ĐANG THU HỒI PARTITION (Rebalancing)...");
    })
    .Build();

consumer.Subscribe("topic-A");

Console.WriteLine("Đang lắng nghe... Hãy gửi tin từ Producer.");

while (true)
{
    var res = consumer.Consume(TimeSpan.FromMilliseconds(500));
    if (res != null)
    {
        var now = DateTime.Now.ToString("HH:mm:ss.fff");
        Console.WriteLine($"[{now}]: [P-{res.Partition.Value}] Key: {res.Message.Key} | Val: {res.Message.Value}");
    }
}