using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class DownloadFileOrchestration(
    IMediator mediator,
    Configuration configuration,
    IS3ChunkedFileReconstructor reconstructor,
    IRestoreService restoreService,
    ILogger<DownloadFileOrchestration> logger,
    IContextResolver contextResolver) : BackgroundService
{
    private readonly ConcurrentDictionary<string, DownloadFileFromS3Request> _requestsRetried = [];
    private readonly ConcurrentDictionary<string, int> _retryAttempts = [];
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _timers = [];
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        // Spin up N worker loops
        _workers = new Task[configuration.RestoreS3Concurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var downloadRequest in mediator.GetDownloadRequests(cancellationToken))
        {
            var key = $"{downloadRequest.RestoreId}::{downloadRequest.FilePath}";

            _requestsRetried.TryAdd(key, downloadRequest);
            try
            {
                await reconstructor.ReconstructAsync(downloadRequest, cancellationToken);

                _requestsRetried.TryRemove(key, out _);
                _retryAttempts.TryRemove(key, out _);
                _timers.TryRemove(key, out _);

                await restoreService.ReportDownloadComplete(downloadRequest, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                var attemptNo = 0;
                if (_retryAttempts.TryGetValue(key, out var attempts))
                    attemptNo = attempts + 1;

                if (attemptNo > configuration.DownloadRetryLimit)
                {
                    await restoreService.ReportDownloadFailed(downloadRequest, exception, cancellationToken);
                    continue;
                }

                var timeAlg = contextResolver.ResolveRetryTimeAlgorithm(configuration);
                _retryAttempts[key] = attemptNo;
                var retryDelay = timeAlg(attemptNo, key, exception);

                if (_timers.TryRemove(key, out var existingSource))
                {
                    await existingSource.CancelAsync();
                    existingSource.Dispose();
                }

                var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _timers[key] = cts;
                var retryKey = key;

                // fire‐and‐forget the async work
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var retryReq = _requestsRetried.GetValueOrDefault(retryKey, downloadRequest);
                        await Task.Delay(retryDelay, cts.Token);
                        await mediator.DownloadFileFromS3(retryReq, cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e, "Error processing restore download for file {FilePath}",
                            key);
                    }
                    finally
                    {
                        if (_timers.TryRemove(retryKey, out var oldCts)) oldCts.Dispose();
                    }
                }, cts.Token);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal no more items
        // (Producer side should call sharedChannel.Writer.Complete())
        await base.StopAsync(cancellationToken);

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(configuration.ShutdownTimeoutSeconds));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}