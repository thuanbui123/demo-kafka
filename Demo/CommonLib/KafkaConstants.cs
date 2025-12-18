namespace CommonLib;

public class KafkaConstants
{
    public const string Bootstrap = "localhost:9092";
    public const string TopicProducts = "products";
    public const string TopicProductsDlq = "products-dlq";
    public const string GroupProducts = "products-group";

    public const string TopicNewOrder = "orders-new";
    public const string TopicCancelOrder = "orders-cancel";
    public const string TopicDlqOrder = "orders-dlp";
    public const string GroupOrders = "orders-group";

    public const string RetryHeader = "retry-count";
    public const int MaxRetry = 3;
}
