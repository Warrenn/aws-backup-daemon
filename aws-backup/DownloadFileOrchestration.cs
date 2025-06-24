using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

internal record FailedAttempt(
    DownloadFileFromS3Request Request,
    DateTimeOffset NextAttemptAt,
    Exception Exception,
    int AttemptNo);

public class DownloadFileOrchestration(
    IMediator mediator,
    IS3ChunkedFileReconstructor reconstructor,
    IRestoreService restoreService,
    ILogger<DownloadFileOrchestration> logger,
    IContextResolver contextResolver) : BackgroundService
{
    private readonly ConcurrentDictionary<string, FailedAttempt> _retryAttempts = [];
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var restoreS3Concurrency = contextResolver.ResolveRestoreS3Concurrency();
        // Spin up N worker loops
        _workers = new Task[restoreS3Concurrency + 1];
        _workers[0] = Task.Run(() => RetryFailedAttempts(cancellationToken), cancellationToken);
        for (var i = 1; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var downloadRequest in mediator.GetDownloadRequests(cancellationToken))
        {
            var key = $"{downloadRequest.RestoreId}::{downloadRequest.FilePath}";
            var downloadRetryDelay = contextResolver.ResolveDownloadRetryDelay();
            var keepTimeStamps = contextResolver.ResolveKeepTimeStamps();
            var keepOwnerGroup = contextResolver.ResolveKeepOwnerGroup();
            var keepAclEntries = contextResolver.ResolveKeepAclEntries();
            var downloadAttemptLimit = contextResolver.ResolveDownloadAttemptLimit();

            if (_retryAttempts.TryGetValue(key, out var existingAttempt) &&
                existingAttempt.AttemptNo > downloadAttemptLimit)
            {
                await restoreService.ReportDownloadFailed(
                    downloadRequest,
                    existingAttempt.Exception,
                    cancellationToken);
                logger.LogWarning("Download attempt limit reached for {FilePath} in restore {RestoreId}",
                    downloadRequest.FilePath, downloadRequest.RestoreId);

                _retryAttempts.TryRemove(key, out _);
                continue;
            }

            try
            {
                var localFilePath = await reconstructor.ReconstructAsync(downloadRequest, cancellationToken);
                var checkDownloadHash = contextResolver.ResolveCheckDownloadHash();
                var hashPassed = !checkDownloadHash ||
                                 await reconstructor.VerifyDownloadHashAsync(downloadRequest, localFilePath,
                                     cancellationToken);

                if (!hashPassed)
                {
                    logger.LogWarning("Hash verification failed for {FilePath} in restore {RestoreId}",
                        downloadRequest.FilePath, downloadRequest.RestoreId);

                    var addedAttempt = new FailedAttempt(
                        downloadRequest,
                        DateTimeOffset.UtcNow.AddSeconds(downloadRetryDelay),
                        new InvalidOperationException("Hash verification failed"),
                        1);

                    if (_retryAttempts.TryRemove(key, out var retryAttempt))
                        addedAttempt = addedAttempt with
                        {
                            AttemptNo = retryAttempt.AttemptNo + 1
                        };

                    _retryAttempts.TryAdd(key, addedAttempt);
                    continue;
                }

                await restoreService.ReportDownloadComplete(downloadRequest, cancellationToken);

                if (keepTimeStamps)
                    FileHelper.SetTimestamps(localFilePath, downloadRequest.Created ?? DateTimeOffset.UtcNow,
                        downloadRequest.LastModified ?? DateTimeOffset.UtcNow);

                if (keepOwnerGroup)
                    await FileHelper.SetOwnerGroupAsync(
                        localFilePath,
                        downloadRequest.Owner ?? "",
                        downloadRequest.Group ?? "",
                        cancellationToken);

                if (keepAclEntries) FileHelper.ApplyAcl(downloadRequest.AclEntries ?? [], localFilePath);

                _retryAttempts.TryRemove(key, out _);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                var addedAttempt = new FailedAttempt(
                    downloadRequest,
                    DateTimeOffset.UtcNow.AddSeconds(downloadRetryDelay),
                    exception,
                    1);

                if (_retryAttempts.TryRemove(key, out var retryAttempt))
                    addedAttempt = addedAttempt with
                    {
                        AttemptNo = retryAttempt.AttemptNo + 1
                    };

                _retryAttempts.TryAdd(key, addedAttempt);
            }
        }
    }

    private async Task RetryFailedAttempts(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var attempts = _retryAttempts.Values.ToList();
            foreach (var attempt in attempts)
            {
                var now = DateTimeOffset.UtcNow;
                if (now < attempt.NextAttemptAt) continue;

                await mediator.DownloadFileFromS3(attempt.Request, cancellationToken);
            }

            await Task.Delay(contextResolver.ResolveRetryCheckInterval(), cancellationToken);
        }
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