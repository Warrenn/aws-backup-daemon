using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public abstract record RetryState
{
    public Exception? Exception { get; set; } = null;
    public DateTimeOffset? NextAttemptAt { get; set; }
    public int AttemptCount { get; set; }
    public int RetryLimit { get; set; }
    public Func<RetryState, CancellationToken, Task>? Retry { get; set; }
    public Func<RetryState, CancellationToken, Task>? LimitExceeded { get; set; }
}

public interface IRetryMediator
{
    IAsyncEnumerable<RetryState> GetRetries(CancellationToken cancellationToken);
    ValueTask RetryAttempt(RetryState attempt, CancellationToken cancellationToken);
}

public class RetryOrchestration(
    IRetryMediator mediator,
    IContextResolver contextResolver,
    ILogger<RetryOrchestration> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await foreach (var state in mediator.GetRetries(cancellationToken))
        {
            state.NextAttemptAt ??= contextResolver.NextRetryTime(state.AttemptCount);
            var limit = state.RetryLimit <= 0 ? contextResolver.GeneralRetryLimit() : state.RetryLimit;
            
            if (state.AttemptCount > limit)
            {
                await (state.LimitExceeded?.Invoke(state, cancellationToken) ?? Task.CompletedTask);
                continue;
            }

            if (TimeProvider.System.GetUtcNow() < state.NextAttemptAt)
            {
                //some breathing room between reads and retries
                await Task.Delay(contextResolver.RetryCheckIntervalSeconds(), cancellationToken);
                await mediator.RetryAttempt(state, cancellationToken);
                continue;
            }

            state.AttemptCount += 1;
            state.NextAttemptAt = contextResolver.NextRetryTime(state.AttemptCount);

            if (state.Retry is not null) await state.Retry(state, cancellationToken);
        }
    }
}