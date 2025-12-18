namespace CommonLib;

public class Order
{
    public Guid Id { get; set; }
    public string Type { get; set; } = "new";
    public string CustomerName { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Quantity { get; set; }   
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}
