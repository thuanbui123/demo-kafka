namespace CommonLib
{
    public class WorkMessage
    {
        public Guid Id { get; set; }
        public string Payload { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
    }
}
