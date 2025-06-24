using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class ArchiveFilesOrchestration(
    IMediator mediator,
    IChunkedEncryptingFileProcessor processor,
    IArchiveService archiveService,
    ILogger<ArchiveFilesOrchestration> logger,
    IContextResolver contextResolver)
    : BackgroundService
{
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var readConcurrency = contextResolver.NoOfConcurrentDownloadsPerFile();

        _workers = new Task[readConcurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var (runId, filePath) in mediator.GetArchiveFiles(cancellationToken))
            try
            {
                var keepTimeStamps = contextResolver.ResolveKeepTimeStamps();
                var keepOwnerGroup = contextResolver.ResolveKeepOwnerGroup();
                var keepAclEntries = contextResolver.ResolveKeepAclEntries();

                var requireProcessing =
                    await archiveService.DoesFileRequireProcessing(runId, filePath, cancellationToken);
                if (!requireProcessing)
                {
                    logger.LogInformation("Skipping {File} for {RunId} - already processed", filePath, runId);
                    continue;
                }

                logger.LogInformation("Processing {File} for {RunId}", filePath, runId);
                var result = await processor.ProcessFileAsync(runId, filePath, cancellationToken);
                await archiveService.ReportProcessingResult(runId, result, cancellationToken);
                if (keepTimeStamps)
                {
                    FileHelper.GetTimestamps(filePath, out var created, out var modified);
                    await archiveService.UpdateTimeStamps(runId, result.LocalFilePath, created, modified,
                        cancellationToken);
                }

                if (keepOwnerGroup)
                {
                    var (owner, group) = await FileHelper.GetOwnerGroupAsync(filePath, cancellationToken);
                    await archiveService.UpdateOwnerGroup(runId, result.LocalFilePath, owner, group, cancellationToken);
                }

                if (!keepAclEntries) continue;

                var aclEntries = FileHelper.GetFileAcl(filePath);
                await archiveService.UpdateAclEntries(runId, result.LocalFilePath, aclEntries, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                await mediator.RetryFile(runId, filePath, ex, cancellationToken);
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