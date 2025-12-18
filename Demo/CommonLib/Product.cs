namespace CommonLib;

public class Product
{
    public string Name { get; set; } = default!;
    public decimal Price { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}
