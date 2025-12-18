using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Text;

// Cấu hình mã hóa để hiển thị tiếng Việt có dấu trong Console
Console.OutputEncoding = Encoding.UTF8;

var config = new AdminClientConfig { BootstrapServers = "localhost:9092" };
using var adminClient = new AdminClientBuilder(config).Build();

while (true)
{
    Console.Clear();
    Console.WriteLine("=== HỆ THỐNG QUẢN LÝ KAFKA PARTITION ===");
    Console.WriteLine("1. Kiểm tra số lượng Partition của Topic");
    Console.WriteLine("2. Tăng số lượng Partition cho Topic");
    Console.WriteLine("3. Thoát");
    Console.Write("\nChọn chức năng (1-3): ");

    var choice = Console.ReadLine();

    if (choice == "1")
    {
        Console.Write("Nhập tên topic cần kiểm tra: ");
        string topicName = Console.ReadLine() ?? "";
        try
        {
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            var topicMetadata = metadata.Topics.FirstOrDefault();

            if (topicMetadata != null && topicMetadata.Error.Code == ErrorCode.NoError)
            {
                Console.WriteLine($"\n[TOPIC: {topicMetadata.Topic}]");
                Console.WriteLine($"- Số lượng Partition hiện tại: {topicMetadata.Partitions.Count}");
            }
            else Console.WriteLine("\nLỗi: Không tìm thấy topic.");
        }
        catch (Exception ex) { Console.WriteLine($"Lỗi: {ex.Message}"); }
    }
    else if (choice == "2")
    {
        Console.Write("Nhập tên topic muốn tăng: ");
        string topicName = Console.ReadLine() ?? "";
        Console.Write("Nhập tổng số partition mong muốn: ");
        if (int.TryParse(Console.ReadLine(), out int newCount))
        {
            try
            {
                await adminClient.CreatePartitionsAsync(new List<PartitionsSpecification>
                {
                    new PartitionsSpecification { Topic = topicName, IncreaseTo = newCount }
                });
                Console.WriteLine($"\nThành công! Đã yêu cầu tăng lên {newCount}.");
            }
            catch (Exception ex) { Console.WriteLine($"Lỗi: {ex.Message}"); }
        }
    }
    else if (choice == "3") break;

    Console.WriteLine("\n-------------------------------------------");
    Console.WriteLine("Nhấn phím bất kỳ để quay lại Menu...");
    Console.ReadKey();
}