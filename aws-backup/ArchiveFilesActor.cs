using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IArchiveFileMediator
{
    IAsyncEnumerable<ArchiveFileRequest> GetArchiveFiles(CancellationToken cancellationToken);
    Task ProcessFile(ArchiveFileRequest request, CancellationToken cancellationToken);
}

public sealed record ArchiveFileRequest(
    string RunId,
    string FilePath
) : RetryState;

public sealed class ArchiveFilesActor(
    IArchiveFileMediator mediator,
    IRetryMediator retryMediator,
    IChunkedEncryptingFileProcessor processor,
    IArchiveService archiveService,
    ILogger<ArchiveFilesActor> logger,
    IContextResolver contextResolver)
    : BackgroundService
{
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("ArchiveFilesActor started");
        var concurrency = contextResolver.NoOfConcurrentFileUploads();

        _workers = new Task[concurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var request in mediator.GetArchiveFiles(cancellationToken))
            try
            {
                request.Retry ??= (r, ct) => mediator.ProcessFile((ArchiveFileRequest)r, ct);
                request.LimitExceeded ??= (state, token) =>
                    archiveService.RecordFailedFile(
                        ((ArchiveFileRequest)state).RunId,
                        ((ArchiveFileRequest)state).FilePath,
                        state.Exception ?? new Exception("Exceeded limit"),
                        token);

                var runId = request.RunId;
                var filePath = request.FilePath;
                var keepTimeStamps = contextResolver.KeepTimeStamps();
                var keepOwnerGroup = contextResolver.KeepOwnerGroup();
                var keepAclEntries = contextResolver.KeepAclEntries();

                var requireProcessing =
                    await archiveService.DoesFileRequireProcessing(runId, filePath, cancellationToken);
                if (!requireProcessing)
                {
                    logger.LogInformation("Skipping {File} for {ArchiveRunId} - already processed", filePath, runId);
                    continue;
                }

                while (true)
                {
                    var cacheFolderSizeLimit = contextResolver.CacheFolderSizeLimitBytes();
                    if (cacheFolderSizeLimit <= 0) break; // no limit, skip cleanup

                    var cacheFolder = contextResolver.LocalCacheFolder();
                    var cacheSize = FileHelper.FolderSize(cacheFolder);
                    var checkTimeSpan = TimeSpan.FromSeconds(contextResolver.CacheSizeCheckTimeoutSeconds());
                    if (cacheSize < cacheFolderSizeLimit) break;

                    logger.LogWarning(
                        "Cache folder {cacheFolder} with size {cacheSize} exceeds limit {Limit}, waiting {checkTimeSpan} ",
                        cacheFolder, cacheSize, cacheFolderSizeLimit, checkTimeSpan);
                    await Task.Delay(checkTimeSpan, cancellationToken);
                }

                logger.LogInformation("Processing {File} for {ArchiveRunId}", filePath, runId);
                var task = processor.ProcessFileAsync(runId, filePath, cancellationToken);

                try
                {
                    if (keepTimeStamps)
                    {
                        FileHelper.GetTimestamps(filePath, out var created, out var modified);
                        await archiveService.UpdateTimeStamps(runId, filePath, created, modified,
                            cancellationToken);
                    }

                    if (keepOwnerGroup)
                    {
                        var (owner, group) = await FileHelper.GetOwnerGroupAsync(filePath, cancellationToken);
                        await archiveService.UpdateOwnerGroup(runId, filePath, owner, group, cancellationToken);
                    }

                    if (keepAclEntries)
                    {
                        var aclEntries = FileHelper.GetFileAcl(filePath);
                        await archiveService.UpdateAclEntries(runId, filePath, aclEntries, cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to update metadata for {File} in {ArchiveRunId}", filePath, runId);
                    await task;
                    request.Exception = ex;
                    await retryMediator.RetryAttempt(request, cancellationToken);
                    continue;
                }

                var result = await task;

                if (result.Error is not null)
                {
                    request.Exception = result.Error;
                    await retryMediator.RetryAttempt(request, cancellationToken);
                    continue;
                }

                logger.LogInformation("File {File} processed successfully for {ArchiveRunId}", filePath, runId);
                await archiveService.ReportProcessingResult(runId, result, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                request.Exception = ex;
                await retryMediator.RetryAttempt(request, cancellationToken);
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