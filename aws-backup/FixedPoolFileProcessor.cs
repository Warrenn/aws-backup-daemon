using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public abstract class FixedPoolFileProcessor(
    IMediator mediator,
    IChunkedEncryptingFileProcessor processor,
    IArchiveService archiveService,
    ILogger<FixedPoolFileProcessor> logger,
    Configuration configuration)
    : BackgroundService
{
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Spin up N worker loops
        _workers = new Task[configuration.ReadConcurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(stoppingToken), stoppingToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken ct)
    {
        await foreach (var (runId, filePath) in mediator.GetArchiveFiles(ct))
            try
            {
                var requireProcessing = await archiveService.FileRequiresProcessing(runId, filePath, ct);
                if (!requireProcessing)
                {
                    logger.LogInformation("Skipping {File} for {RunId} - already processed", filePath, runId);
                    continue;
                }

                logger.LogInformation("Processing {File} for {RunId}", filePath, runId);
                var result = await processor.ProcessFileAsync(filePath, ct);
                var requireMetaData = await archiveService.ReportProcessingResult(runId, result, ct);
                if (!requireMetaData) continue;
                if (configuration.KeepTimeStamps)
                {
                    FileHelper.GetTimestamps(filePath, out var created, out var modified);
                    await archiveService.UpdateTimeStamps(runId, result.LocalFilePath, created, modified, ct);
                }

                if (configuration.KeepOwnerGroup)
                {
                    var (owner, group) = await FileHelper.GetOwnerGroupAsync(filePath, ct);
                    await archiveService.UpdateOwnerGroup(runId, result.LocalFilePath, owner, group, ct);
                }

                if (!configuration.KeepAcl) return;

                var aclEntries = FileHelper.GetFileAcl(filePath);
                await archiveService.UpdateAclEntries(runId, result.LocalFilePath, aclEntries, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                await mediator.RetryFile(runId, filePath, ex, ct);
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