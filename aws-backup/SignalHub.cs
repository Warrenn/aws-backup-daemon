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
}

public sealed class SignalHub<T> : ISignalHub<T>
{
    // unbounded so producers never block, and we can signal more times
    private readonly Channel<T> _channel =
        Channel.CreateBounded<T>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true, // only one consumer
                SingleWriter = true // only one producer
            });

    public Task<T> WaitAsync(CancellationToken cancellationToken = default)
    {
        return _channel.Reader.ReadAsync(cancellationToken).AsTask();
    }

    public Task SignalAsync(T value, CancellationToken cancellationToken = default)
    {
        return _channel.Writer.WriteAsync(value, cancellationToken).AsTask();
    }
}