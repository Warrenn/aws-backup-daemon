using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

internal record FailedUploadAttempt(
    string RunId,
    string FilePath,
    DateTimeOffset NextAttemptAt,
    Exception Exception,
    int AttemptNo);

public class RetryFileOrchestration(
    IMediator mediator,
    IContextResolver contextResolver,
    IArchiveService archiveService,
    ILogger<RetryFileOrchestration> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<string, FailedUploadAttempt> _retryAttempts = [];
    private Task[] _workers = [];

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _workers = new Task[2];
        var currentRunId = "";

        _workers[0] = Task.Run(async () =>
        {
            await foreach (var (runId, filePath, exception) in mediator.GetRetries(cancellationToken))
            {
                if (currentRunId != runId)
                {
                    currentRunId = runId;
                    _retryAttempts.Clear();
                }

                var key = $"{runId}::{filePath}";
                var uploadAttemptLimit = contextResolver.ResolveUploadAttemptLimit();
                var uploadRetryDelay = contextResolver.ResolveUploadRetryDelay();

                if (_retryAttempts.TryGetValue(key, out var existingAttempt) &&
                    existingAttempt.AttemptNo > uploadAttemptLimit)
                {
                    await archiveService.RecordFailedFile(runId, filePath, exception, cancellationToken);
                    logger.LogWarning("Download attempt limit reached for {FilePath} in archive Run {RestoreId}",
                        filePath, runId);

                    _retryAttempts.TryRemove(key, out _);
                    continue;
                }

                var addedAttempt = new FailedUploadAttempt(
                    runId,
                    filePath,
                    DateTimeOffset.UtcNow.AddSeconds(uploadRetryDelay),
                    exception,
                    1);

                if (_retryAttempts.TryRemove(key, out var retryAttempt))
                    addedAttempt = addedAttempt with
                    {
                        AttemptNo = retryAttempt.AttemptNo + 1
                    };

                _retryAttempts.TryAdd(key, addedAttempt);
            }
        }, cancellationToken);


        _workers[1] = Task.Run(async () =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var attempts = _retryAttempts.Values.ToList();
                foreach (var attempt in attempts)
                {
                    var now = DateTimeOffset.UtcNow;
                    if (now < attempt.NextAttemptAt) continue;

                    await mediator.ProcessFile(attempt.RunId, attempt.FilePath, cancellationToken);
                }

                await Task.Delay(contextResolver.ResolveRetryCheckInterval(), cancellationToken);
            }
        }, cancellationToken);

        await Task.WhenAll(_workers);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal no more items
        // (Producer side should call sharedChannel.Writer.Complete())
        await base.StopAsync(cancellationToken);

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts =
            new CancellationTokenSource(TimeSpan.FromSeconds(contextResolver.ResolveShutdownTimeoutSeconds()));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}