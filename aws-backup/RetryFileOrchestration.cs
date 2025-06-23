using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class RetryFileOrchestration(
    IMediator mediator,
    IContextResolver contextResolver,
    Configuration configuration,
    IArchiveService archiveService,
    ILogger<RetryFileOrchestration> logger) : BackgroundService
{
    private readonly Dictionary<string, Exception> _failedFiles = [];
    private readonly Dictionary<string, int> _retryAttempts = [];
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _timers = [];

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var currentRunId = "";
        await foreach (var (runId, filePath, exception) in mediator.GetRetries(cancellationToken))
        {
            if (currentRunId != runId)
            {
                currentRunId = runId;
                _failedFiles.Clear();
                _retryAttempts.Clear();
                foreach (var (_, t) in _timers)
                {
                    await t.CancelAsync();
                    t.Dispose();
                }

                _timers.Clear();
            }

            if (_failedFiles.ContainsKey(filePath)) continue;

            var attemptNo = 0;
            if (_retryAttempts.TryGetValue(filePath, out var attempts))
                attemptNo = attempts + 1;

            if (attemptNo > configuration.RetryLimit)
            {
                _failedFiles.Add(filePath, exception);
                await archiveService.RecordFailedFile(runId, filePath, exception, cancellationToken);
                continue;
            }

            var timeAlg = contextResolver.ResolveRetryTimeAlgorithm(configuration);
            _retryAttempts[filePath] = attemptNo;
            var retryDelay = timeAlg(attemptNo, filePath, exception);

            if (_timers.TryRemove(filePath, out var existingSource))
            {
                await existingSource.CancelAsync();
                existingSource.Dispose();
            }

            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _timers[filePath] = cts;
            var key = filePath;

            // fire‐and‐forget the async work
            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(retryDelay, cts.Token);
                    await mediator.ProcessFile(runId, key, cts.Token);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Error processing retry for file {FilePath} in archive {runId}",
                        key, runId);
                }
                finally
                {
                    if (_timers.TryRemove(key, out var oldCts)) oldCts.Dispose();
                }
            }, cts.Token);
        }
    }
}