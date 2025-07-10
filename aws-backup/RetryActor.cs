using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public abstract record RetryState
{
    public Exception? Exception { get; set; }
    public DateTimeOffset? NextAttemptAt { get; set; }
    public int AttemptCount { get; set; }
    public int RetryLimit { get; set; }
    public Func<RetryState, CancellationToken, Task>? Retry { get; set; }
    public Func<RetryState, CancellationToken, Task>? LimitExceeded { get; set; }
}

public interface IRetryMediator
{
    IAsyncEnumerable<RetryState> GetRetries(CancellationToken cancellationToken);
    Task RetryAttempt(RetryState attempt, CancellationToken cancellationToken);
}

public sealed class RetryActor(
    IRetryMediator mediator,
    IContextResolver contextResolver,
    ILogger<RetryActor> logger,
    TimeProvider provider) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting retry Actor");
        await foreach (var state in mediator.GetRetries(cancellationToken))
            try
            {
                state.NextAttemptAt ??= contextResolver.NextRetryTime(state.AttemptCount);
                var limit = state.RetryLimit <= 0 ? contextResolver.GeneralRetryLimit() : state.RetryLimit;

                if (state.AttemptCount > limit)
                {
                    logger.LogInformation("Retry Limit Exceeded for {State}", state);
                    await (state.LimitExceeded?.Invoke(state, cancellationToken) ?? Task.CompletedTask);
                    continue;
                }

                if (provider.GetUtcNow() < state.NextAttemptAt)
                {
                    var interval = contextResolver.RetryCheckIntervalMs();
                    //some breathing room between reads and retries
                    await Task.Delay(interval, cancellationToken);
                    await mediator.RetryAttempt(state, cancellationToken);
                    continue;
                }
                
                logger.LogInformation("Retry Attempt: {State}", state);
                state.AttemptCount += 1;
                state.NextAttemptAt = contextResolver.NextRetryTime(state.AttemptCount);

                if (state.Retry is not null) await state.Retry(state, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error retrying request: {Message}", ex.Message);
            }
    }
}