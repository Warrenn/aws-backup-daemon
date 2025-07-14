using System.Threading.Channels;

namespace aws_backup_common;

public interface ISignalHub<T>
{
    /// <summary>
    ///     Await the next value signaled by any caller of SignalAsync.
    /// </summary>
    ValueTask<T> WaitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Signal a value, unblocking exactly one waiter (or queue it for the next waiter).
    /// </summary>
    ValueTask SignalAsync(T value, CancellationToken cancellationToken = default);

    bool Signal(T value);
}

public sealed class SignalHub<T> : ISignalHub<T>
{
    private readonly Channel<T> _channel =
        Channel.CreateUnbounded<T>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });

    public ValueTask<T> WaitAsync(CancellationToken cancellationToken = default)
    {
        return _channel.Reader.ReadAsync(cancellationToken);
    }

    public ValueTask SignalAsync(T value, CancellationToken cancellationToken = default)
    {
        return _channel.Writer.WriteAsync(value, cancellationToken);
    }

    public bool Signal(T value)
    {
        return _channel.Writer.TryWrite(value);
    }
}