namespace aws_backup_common;

public interface ICountDownEvent
{
    void AddCount();
    void Signal();
    Task Wait(CancellationToken cancellationToken);
    void Reset();
}

public interface IChunkCountDownEvent : ICountDownEvent;

public interface IFileCountDownEvent : ICountDownEvent;

public sealed class InternalCountDownEvent : IFileCountDownEvent, IChunkCountDownEvent
{
    private int _currentCount;
    private readonly Lock _lock = new();
    private TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void AddCount()
    {
        lock (_lock)
        {
            Interlocked.Increment(ref _currentCount);
        }
    }

    public void Signal()
    {
        lock (_lock)
        {
            if (Interlocked.Decrement(ref _currentCount) > 0) return;
            _tcs.TrySetResult();
        }
    }

    public async Task Wait(CancellationToken cancellationToken)
    {
        await _tcs.Task.WaitAsync(cancellationToken);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _currentCount = 0;
            _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
}