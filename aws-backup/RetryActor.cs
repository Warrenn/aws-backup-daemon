using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

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
        logger.LogInformation("RetryActor started");

        await foreach (var state in mediator.GetRetries(cancellationToken))
            try
            {
                logger.LogInformation("Retrying {state}", state);

                var limit = state.RetryLimit <= 0 ? contextResolver.GeneralRetryLimit() : state.RetryLimit;
                state.AttemptCount += 1;

                if (state.AttemptCount > limit)
                {
                    logger.LogInformation("Retry Limit Exceeded for {State}", state);
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            if (state.LimitExceeded is null)
                            {
                                logger.LogWarning("LimitExceeded function not set for {State}", state);
                                return;
                            }

                            await state.LimitExceeded(state, cancellationToken);
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Error in LimitExceeded handler for {State}: {Message}", state,
                                ex.Message);
                        }
                    }, cancellationToken);
                    continue;
                }

                var delay = contextResolver.NextRetryTimeSpan(state.AttemptCount);
                logger.LogInformation("Delaying retry for {Delay}ms for {State}", delay.TotalMilliseconds, state);

                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(delay, provider, cancellationToken);
                        if (state.Retry is null)
                        {
                            logger.LogWarning("Retry function not set for {State}", state);
                            return;
                        }

                        await state.Retry(state, cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Error retrying request: {Message} for {State}", ex.Message, state);
                        await mediator.RetryAttempt(state, cancellationToken);
                    }
                }, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing retry state {State}: {Message}", state, ex.Message);
                await mediator.RetryAttempt(state, cancellationToken);
            }
    }
}