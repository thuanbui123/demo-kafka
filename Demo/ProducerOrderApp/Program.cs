using CommonLib;
using Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = KafkaConstants.Bootstrap,
};

var producer = new ProducerBuilder<string, Order>(config)
        .SetValueSerializer(new KafkaJsonSerializer<Order>()).Build();

Console.WriteLine("Enter order info.");

while (true)
{
    Console.WriteLine("Enter customer name: ");
    var customerName = Console.ReadLine();
    if (customerName?.ToLower() == "exit") break;

    Console.WriteLine("Enter order type: ");
    var orderType = Console.ReadLine();

    Console.WriteLine("Enter product name: ");
    var productName = Console.ReadLine();

    Console.WriteLine("Enter product price: ");
    if(!decimal.TryParse(Console.ReadLine(), out var productPrice))
    {
        Console.WriteLine("Invalid product! Try again.");
        continue;
    }

    Console.WriteLine("Enter product quantity: ");
    if(!int.TryParse(Console.ReadLine(), out var quantity))
    {
        Console.WriteLine("Invalid product quantity! Try again.");
        continue;
    }

    var order = new Order
    {
        Id = Guid.NewGuid(),
        CustomerName = customerName ?? string.Empty,
        ProductName = productName ?? string.Empty,
        Type = orderType ?? string.Empty,
        Quantity = quantity,
        Price = productPrice,
        CreatedAt = DateTime.Now
    };

    string topicToSend = (orderType?.ToLower() == "new")
                ? KafkaConstants.TopicNewOrder
                : (orderType?.ToLower() == "cancel")
                    ? KafkaConstants.TopicCancelOrder
                    : string.Empty;
    var rs = await producer.ProduceAsync(topicToSend, new Message<string, Order>
    {
        Key = order.Id.ToString(),
        Value = order
    });

    Console.WriteLine($"Send to {rs.Topic}, {rs.TopicPartitionOffset} | key = {order.Id.ToString()}, order = {order}");
}