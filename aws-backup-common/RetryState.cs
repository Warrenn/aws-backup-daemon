namespace aws_backup_common;

public abstract record RetryState
{
    public Exception? Exception { get; set; }
    public DateTimeOffset? NextAttemptAt { get; set; }
    public int AttemptCount { get; set; }
    public int RetryLimit { get; set; }
    public Func<RetryState, CancellationToken, Task>? Retry { get; set; }
    public Func<RetryState, CancellationToken, Task>? LimitExceeded { get; set; }
}
