using System.Text.Json;
using System.Text.Json.Serialization;

namespace aws_backup;

[JsonConverter(typeof(ByteArrayKeyConverter))]
public readonly struct ByteArrayKey : IEquatable<ByteArrayKey>
{
    private readonly byte[] _data;
    private readonly int _hash;

    public ByteArrayKey(byte[] data)
    {
        _data = data ?? throw new ArgumentNullException(nameof(data));
        // compute once
        var h = data.Aggregate(2166136261u, (current, b) => (current ^ b) * 16777619u);
        _hash = unchecked((int)h);
    }

    public bool Equals(ByteArrayKey other)
    {
        // first compare hashes, then contents
        return _hash == other._hash
               && _data.AsSpan().SequenceEqual(other._data);
    }

    public override bool Equals(object? obj)
    {
        return obj is ByteArrayKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return _hash;
    }

    public static bool operator ==(ByteArrayKey a, ByteArrayKey b)
    {
        return a.Equals(b);
    }

    public static bool operator !=(ByteArrayKey a, ByteArrayKey b)
    {
        return !a.Equals(b);
    }

    public ReadOnlySpan<byte> AsSpan()
    {
        return _data.AsSpan();
    }

    public byte[] ToArray()
    {
        return _data;
    }

    public override string ToString()
    {
        return Base64Url.Encode(_data);
    }
}

public class ByteArrayKeyConverter : JsonConverter<ByteArrayKey>
{
    public override ByteArrayKey Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options)
    {
        // Expect the key as a Base64 string
        var b64 = reader.GetString()
                  ?? throw new JsonException("Expected Base64 string for ByteArrayKey");
        var bytes = Base64Url.Decode(b64);
        return new ByteArrayKey(bytes);
    }

    public override void Write(
        Utf8JsonWriter writer,
        ByteArrayKey value,
        JsonSerializerOptions options)
    {
        // Emit the underlying bytes as Base64
        writer.WriteStringValue(Base64Url.Encode(value.ToArray()));
    }

    public override ByteArrayKey ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var b64 = reader.GetString()
                  ?? throw new JsonException("Expected Base64 string for ByteArrayKey");
        var bytes = Base64Url.Decode(b64);
        return new ByteArrayKey(bytes);
    }
    
    public override void WriteAsPropertyName(Utf8JsonWriter writer, ByteArrayKey value, JsonSerializerOptions options)
    {
        // Emit the underlying bytes as Base64
        writer.WritePropertyName(Base64Url.Encode(value.ToArray()));
    }
}