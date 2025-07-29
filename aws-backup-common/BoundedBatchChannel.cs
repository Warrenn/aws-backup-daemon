using System.Threading.Channels;

namespace aws_backup_common;

public class BoundedBatchChannel<T>(BoundedChannelOptions options)
{
    private readonly TaskCompletionSource _tcs = new();

    private int _expectedReaders;
    public Channel<T> Channel { get; } = System.Threading.Channels.Channel.CreateBounded<T>(options);

    public void RegisterReader()
    {
        Interlocked.Increment(ref _expectedReaders);
    }

    public void SignalReaderCompleted()
    {
        if (Interlocked.Decrement(ref _expectedReaders) > 0) return;
        _expectedReaders = 0;
        _tcs.TrySetResult();
    }

    public Task WaitForAllReadersAsync()
    {
        return _tcs.Task;
    }
}

public class ChannelManager<T>
{
    private readonly Lock _lock = new();
    private readonly BoundedChannelOptions _options;
    private BoundedBatchChannel<T> _current = null!;
    
    public bool ChannelIsOpen
    {
        get
        {
            lock (_lock)
            {
                return _current.Channel.Reader.Completion.IsCompleted == false;
            }
        }
    }

    public ChannelManager(BoundedChannelOptions options)
    {
        _options = options;
        Reset();
    }

    public BoundedBatchChannel<T> Current
    {
        get
        {
            lock (_lock)
            {
                return _current;
            }
        }
    }

    public void Reset()
    {
        lock (_lock)
        {
            _current = new BoundedBatchChannel<T>(_options);
        }
    }
}