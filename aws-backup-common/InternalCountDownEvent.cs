namespace aws_backup_common;

public interface ICountDownEvent
{
    void AddCount();
    bool Signal();
    void Wait(CancellationToken cancellationToken);
    void Reset();
}

public interface IChunkCountDownEvent : ICountDownEvent;

public interface IFileCountDownEvent : ICountDownEvent;

public sealed class InternalCountDownEvent(int initialCount)
    : CountdownEvent(initialCount), IFileCountDownEvent, IChunkCountDownEvent;