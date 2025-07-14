using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public sealed class DownloadFileActor(
    IRetryMediator retryMediator,
    IDownloadFileMediator mediator,
    IS3ChunkedFileReconstructor reconstructor,
    IRestoreService restoreService,
    ILogger<DownloadFileActor> logger,
    IContextResolver contextResolver) : BackgroundService
{
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("DownloadFileActor started");
        var restoreS3Concurrency = contextResolver.NoOfS3FilesToDownloadConcurrently();
        // Spin up N worker loops
        _workers = new Task[restoreS3Concurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var downloadFileRequest in mediator.GetDownloadFileRequests(cancellationToken))
            try
            {
                downloadFileRequest.Retry ??= (req, ct) =>
                    mediator.DownloadFileFromS3((DownloadFileFromS3Request)req, ct);
                downloadFileRequest.RetryLimit = contextResolver.DownloadAttemptLimit();
                downloadFileRequest.LimitExceeded ??= (req, ct) =>
                    restoreService.ReportDownloadFailed(
                        (DownloadFileFromS3Request)req,
                        req.Exception ?? new Exception("Exceeded limit"),
                        ct);

                var keepTimeStamps = contextResolver.KeepTimeStamps();
                var keepOwnerGroup = contextResolver.KeepOwnerGroup();
                var keepAclEntries = contextResolver.KeepAclEntries();

                logger.LogInformation("Processing download request {FilePath}", downloadFileRequest.FilePath);
                var (localFilePath, error) = await reconstructor.ReconstructAsync(downloadFileRequest, cancellationToken);
                if (error is not null || string.IsNullOrWhiteSpace(localFilePath))
                {
                    logger.LogError(error, "Failed to reconstruct file {FilePath}", downloadFileRequest.FilePath);
                    downloadFileRequest.Exception = error;
                    await retryMediator.RetryAttempt(downloadFileRequest, cancellationToken);
                    continue;
                }

                var checkDownloadHash = contextResolver.CheckDownloadHash();
                var hashPassed = !checkDownloadHash ||
                                 await reconstructor.VerifyDownloadHashAsync(downloadFileRequest, localFilePath,
                                     cancellationToken);

                if (!hashPassed)
                {
                    logger.LogWarning("Download hash verification failed for {FilePath}", downloadFileRequest.FilePath);
                    downloadFileRequest.Exception = new InvalidOperationException("Download hash verification failed");
                    await retryMediator.RetryAttempt(downloadFileRequest, cancellationToken);
                    continue;
                }

                logger.LogInformation("Download completed for {FilePath}", downloadFileRequest.FilePath);
                if (keepTimeStamps)
                    FileHelper.SetTimestamps(localFilePath, downloadFileRequest.Created ?? DateTimeOffset.UtcNow,
                        downloadFileRequest.LastModified ?? DateTimeOffset.UtcNow);

                if (keepOwnerGroup)
                    await FileHelper.SetOwnerGroupAsync(
                        localFilePath,
                        downloadFileRequest.Owner ?? "",
                        downloadFileRequest.Group ?? "",
                        cancellationToken);

                if (keepAclEntries) FileHelper.ApplyAcl(downloadFileRequest.AclEntries ?? [], localFilePath);

                await restoreService.ReportDownloadComplete(downloadFileRequest, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Error processing download request {FilePath}", downloadFileRequest.FilePath);
                //queue to the failed queue;
                downloadFileRequest.Exception = exception;
                await retryMediator.RetryAttempt(downloadFileRequest, cancellationToken);
            }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal no more items
        // (Producer side should call sharedChannel.Writer.Complete())
        await base.StopAsync(cancellationToken);

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts =
            new CancellationTokenSource(TimeSpan.FromSeconds(contextResolver.ShutdownTimeoutSeconds()));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}