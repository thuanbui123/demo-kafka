using CommonLib;
using Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = KafkaConstants.Bootstrap,
    Acks = Acks.All
};

using var producer = new ProducerBuilder<string, Product>(config)
    .SetValueSerializer(new KafkaJsonSerializer<Product>()).Build();

Console.WriteLine("Enter the product (type 'exit' to exit).");

while (true)
{
    Console.WriteLine("Enter product name: ");
    var name = Console.ReadLine();
    if (name?.ToLower() == "exit") break;

    Console.WriteLine("Enter product price: ");
    if(!decimal.TryParse(Console.ReadLine(), out var price))
    {
        Console.WriteLine("Invalid product! Try again.");
        continue;
    }

    var product = new Product
    {
        Name = name ?? string.Empty,
        Price = price,
        CreatedAt = DateTime.Now,
    };

    var rs = await producer.ProduceAsync(
        KafkaConstants.TopicProducts,
        new Message<string, Product> { Key = product.Name, Value = product }
    );

    Console.WriteLine($"Send to {rs.TopicPartitionOffset} | key = {product.Name}, price = {product.Price}");
}

