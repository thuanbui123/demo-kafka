namespace CommonLib;

public class Transaction
{
    public string PharmacyId { get; set; }
    public string Type { get; set; }  // import / export
    public double Amount { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
