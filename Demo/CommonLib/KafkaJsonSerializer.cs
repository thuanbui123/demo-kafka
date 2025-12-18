using Confluent.Kafka;
using System.Text.Json;

namespace CommonLib;

public class KafkaJsonSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
        => JsonSerializer.SerializeToUtf8Bytes(data);
}
