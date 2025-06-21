using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class RetryFileProcessor(
    IMediator mediator,
    IContextFactory contextFactory,
    Configuration configuration,
    IArchiveService archiveService,
    ILogger<RetryFileProcessor> logger) : BackgroundService
{
    private readonly Dictionary<string, Exception> _failedFiles = [];
    private readonly Dictionary<string, int> _retryAttempts = [];
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _timers = [];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var archiveId = "";
        await foreach (var (archive, filePath, exception) in mediator.GetRetries(stoppingToken))
        {
            if (archiveId != archive.RunId)
            {
                archiveId = archive.RunId;
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
                await archiveService.RecordFailedFile(archive.RunId, filePath, exception, stoppingToken);
                continue;
            }

            var timeAlg = contextFactory.ResolveRetryTimeAlgorithm(configuration);
            _retryAttempts[filePath] = attemptNo;
            var retryDelay = timeAlg(attemptNo, filePath, exception);

            if (_timers.TryRemove(filePath, out var existingSource))
            {
                await existingSource.CancelAsync();
                existingSource.Dispose();
            }

            var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            _timers[filePath] = cts;
            var key = filePath;

            // fire‐and‐forget the async work
            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(retryDelay, cts.Token);
                    await mediator.ProcessFile(archive.RunId, key, cts.Token);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Error processing retry for file {FilePath} in archive {ArchiveId}",
                        key, archive.RunId);
                }
                finally
                {
                    if (_timers.TryRemove(key, out var oldCts)) oldCts.Dispose();
                }
            }, cts.Token);
        }
    }
}