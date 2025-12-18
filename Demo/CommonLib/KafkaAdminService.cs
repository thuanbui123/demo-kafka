using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace CommonLib;

public class KafkaAdminService
{
    private IAdminClient _adminClient;
    private readonly string _bootstrapServer;

    public KafkaAdminService(string bootstrapServer)
    {
        _bootstrapServer = bootstrapServer;
        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServer };
        _adminClient = new AdminClientBuilder(adminConfig).Build();
    }

    public async Task EnsureTopicExistsAsync(string topicName, int numPartitions = 1, short replicationFactor = 1)
    {
        Console.WriteLine($"🔍 Checking if topic '{topicName}' exists on {_bootstrapServer}...");

        try
        {
            // 1. Lấy metadata để kiểm tra
            var metadata = _adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            var topicExists = metadata.Topics.Exists(t => t.Topic.Equals(topicName, StringComparison.OrdinalIgnoreCase));

            if (!topicExists)
            {
                Console.WriteLine($"⚠️ Topic '{topicName}' not found. Creating it now...");

                // 2. Định nghĩa và tạo Topic
                var topicSpec = new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = numPartitions,
                    ReplicationFactor = replicationFactor
                };

                await _adminClient.CreateTopicsAsync(new TopicSpecification[] { topicSpec });
                Console.WriteLine($"✅ Topic '{topicName}' created successfully (Partitions: {numPartitions}, Replicas: {replicationFactor}).");
            }
            else
            {
                Console.WriteLine($"✅ Topic '{topicName}' already exists.");
            }
        }
        catch (CreateTopicsException e)
        {
            // Xử lý trường hợp Topic đã tồn tại (thường xảy ra nếu nhiều tiến trình cùng cố gắng tạo)
            if (e.Results[0].Error.Code == ErrorCode.TopicAlreadyExists)
            {
                Console.WriteLine($"✅ Topic '{topicName}' already exists (Caught exception: {e.Results[0].Error.Reason}).");
            }
            else
            {
                Console.WriteLine($"❌ Error creating topic '{topicName}': {e.Results[0].Error.Reason}");
                throw; // Ném lại lỗi nếu đó là lỗi nghiêm trọng khác
            }
        }
        catch (KafkaException e)
        {
            // Xử lý lỗi kết nối chung
            Console.WriteLine($"❌ Kafka Error during topic check/creation: {e.Message}");
            Console.WriteLine($"Please ensure your Kafka Broker is running and accessible at {_bootstrapServer}.");
            throw; // Ném lại để dừng ứng dụng nếu không kết nối được
        }
    }
}
