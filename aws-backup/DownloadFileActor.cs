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
        var restoreS3Concurrency = contextResolver.NoOfS3FilesToDownloadConcurrently();
        // Spin up N worker loops
        _workers = new Task[restoreS3Concurrency];
        for (var i = 1; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var downloadRequest in mediator.GetDownloadRequests(cancellationToken))
            try
            {
                downloadRequest.Retry ??= (req, ct) =>
                    mediator.DownloadFileFromS3((DownloadFileFromS3Request)req, ct);
                downloadRequest.RetryLimit = contextResolver.DownloadAttemptLimit();
                downloadRequest.LimitExceeded ??= (req, ct) =>
                    restoreService.ReportDownloadFailed(
                        (DownloadFileFromS3Request)req,
                        req.Exception ?? new Exception("Exceeded limit"),
                        ct);

                var keepTimeStamps = contextResolver.KeepTimeStamps();
                var keepOwnerGroup = contextResolver.KeepOwnerGroup();
                var keepAclEntries = contextResolver.KeepAclEntries();

                logger.LogInformation("Processing download request {FilePath}", downloadRequest.FilePath);
                var localFilePath = await reconstructor.ReconstructAsync(downloadRequest, cancellationToken);
                var checkDownloadHash = contextResolver.CheckDownloadHash();
                var hashPassed = !checkDownloadHash ||
                                 await reconstructor.VerifyDownloadHashAsync(downloadRequest, localFilePath,
                                     cancellationToken);

                if (!hashPassed)
                {
                    logger.LogWarning("Download hash verification failed for {FilePath}", downloadRequest.FilePath);
                    downloadRequest.Exception = new InvalidOperationException("Download hash verification failed");
                    await retryMediator.RetryAttempt(downloadRequest, cancellationToken);
                    continue;
                }

                logger.LogInformation("Download completed for {FilePath}", downloadRequest.FilePath);
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
                
                await restoreService.ReportDownloadComplete(downloadRequest, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Error processing download request {FilePath}", downloadRequest.FilePath);
                //queue to the failed queue;
                downloadRequest.Exception = exception;
                await retryMediator.RetryAttempt(downloadRequest, cancellationToken);
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