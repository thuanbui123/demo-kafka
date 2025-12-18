
namespace KafkaWithBalancing;

public class Program
{
    public static async Task Main (string[] args)
    {
        Producer producer = new Producer();
        await producer.Main();

        Consumer consumer = new Consumer();
        await consumer.Main();
    }
}
