namespace CommonLib
{
    public class ConsumerStatus
    {
        public string ConsumerId { get; set; } = string.Empty;
        public string Topic { get; set; } = string.Empty;
        public int Load { get; set; }
    }

}
