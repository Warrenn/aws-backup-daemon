using System.Numerics;

namespace aws_backup;

public sealed class RollingBuzhash32
{
    public const int WindowSize = 64;

    // 256-entry table of pseudo-random 32-bit constants (deterministic)
    private static readonly uint[] T = BuildTable();
    private static uint[] BuildTable()
    {
        var t = new uint[256];
        var x = 0x9E3779B9u; // simple xorShift32 stream
        for (var i = 0; i < 256; i++)
        {
            x ^= x << 13; x ^= x >> 17; x ^= x << 5;
            t[i] = x;
        }
        return t;
    }

    private readonly byte[] _ring = new byte[WindowSize];
    private int _ringPos;
    private int _count; // number of bytes seen so far (caps at WindowSize)

    public void Reset()
    {
        Array.Clear(_ring, 0, _ring.Length);
        _ringPos = 0; 
        _count = 0; 
        Value = 0;
    }

    public int Filled => Math.Min(_count, WindowSize);
    public uint Value { get; private set; }

    // Feed one byte. Before the window fills, we "warm up".
    public void Push(byte b)
    {
        if (_count < WindowSize)
        {
            Value = BitOperations.RotateLeft(Value, 1) ^ T[b];
            _ring[_ringPos] = b;
            _ringPos = (_ringPos + 1) & (WindowSize - 1); // WindowSize is power-of-two
            _count++;
            return;
        }

        // Rolling step: remove oldest, add newest, O(1).
        var old = _ring[_ringPos];
        _ring[_ringPos] = b;
        _ringPos = (_ringPos + 1) & (WindowSize - 1);

        // buzhash rolling update:
        // h' = rotl(h,1) ^ T[new] ^ rotl(T[old], WindowSize)
        Value = BitOperations.RotateLeft(Value, 1) ^ T[b] ^ BitOperations.RotateLeft(T[old], WindowSize);
    }
}