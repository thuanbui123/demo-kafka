using Confluent.Kafka;
using System.Text.Json;

namespace CommonLib;

public class KafkaJsonDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        => JsonSerializer.Deserialize<T>(data) ?? throw new InvalidOperationException("Cannot deserialize JSON to object.");
}
