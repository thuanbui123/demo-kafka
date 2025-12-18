using Confluent.Kafka;
using CommonLib;

var config = new ProducerConfig
{
    BootstrapServers = KafkaConstants.Bootstrap,
};

using var producer = new ProducerBuilder<string, string>(config).Build();
string topic = "topic-A";

Console.WriteLine("Nhấn Enter để gửi 100 tin nhắn từ các nhà thuốc khác nhau...");
Console.ReadLine();

for (int i = 1; i <= 100; i++)
{
    // Giả lập mã nhà thuốc ngẫu nhiên để test tính phân tán
    string pharmacyId = $"PHARMACY_{new Random().Next(1, 15000)}";
    string messageValue = $"Don-Hang-Thanh-Toan-{i}";

    var message = new Message<string, string> { Key = pharmacyId, Value = messageValue };

    producer.Produce(topic, message, (deliveryReport) =>
    {
        if (deliveryReport.Error.IsError)
        {
            Console.WriteLine($"Lỗi: {deliveryReport.Error.Reason}");
        }
        else
        {
            Console.WriteLine($"Gửi: {pharmacyId} -> Partition: {deliveryReport.Partition.Value} (Offset: {deliveryReport.Offset})");
        }
    });
}

producer.Flush(TimeSpan.FromSeconds(10));
Console.WriteLine("Hoàn tất gửi tin.");