namespace aws_backup;

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
}