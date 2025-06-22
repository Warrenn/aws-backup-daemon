using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class HotStorageUploadProcessor(
    IHotStorageService hotStorageService,
    IMediator mediator,
    ILogger<HotStorageUploadProcessor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var archiveTask = Task.Run(async () =>
        {
            await foreach (var (archive, key) in mediator.GetArchiveState(stoppingToken))
                try
                {
                    await hotStorageService.UploadAsync(key, archive, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // Handle cancellation gracefully
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error processing hot storage upload for archive {ArchiveId} and key {Key}",
                        archive.RunId, key);
                    // Optionally, you can rethrow or handle the exception as needed
                }
        }, stoppingToken);
        
        var uploadManifestTask = Task.Run(async () =>
        {
            await foreach (var (key, manifest) in mediator.DataChunksManifest(stoppingToken))
                try
                {
                    await hotStorageService.UploadAsync(key, manifest, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // Handle cancellation gracefully
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error processing hot storage upload manifest for key {Key}", key);
                    // Optionally, you can rethrow or handle the exception as needed
                }
        }, stoppingToken);

        await Task.WhenAll(uploadManifestTask, archiveTask);
    }
}