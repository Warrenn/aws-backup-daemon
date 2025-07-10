using System.Threading.Channels;

namespace aws_backup;

public interface ISignalHub<T>
{
    /// <summary>
    ///     Await the next value signaled by any caller of SignalAsync.
    /// </summary>
    Task<T> WaitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Signal a value, unblocking exactly one waiter (or queue it for the next waiter).
    /// </summary>
    Task SignalAsync(T value, CancellationToken cancellationToken = default);

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

    public Task<T> WaitAsync(CancellationToken cancellationToken = default)
    {
        return _channel.Reader.ReadAsync(cancellationToken).AsTask();
    }

    public Task SignalAsync(T value, CancellationToken cancellationToken = default)
    {
        return _channel.Writer.WriteAsync(value, cancellationToken).AsTask();
    }

    public bool Signal(T value)
    {
        return _channel.Writer.TryWrite(value);
    }
}