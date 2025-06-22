using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class StateUploadOrchestration(
    IHotStorageService hotStorageService,
    IMediator mediator,
    ILogger<StateUploadOrchestration> logger,
    Configuration configuration) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var archiveTask = Task.Run(async () =>
        {
            await foreach (var (archive, key) in mediator.GetArchiveState(stoppingToken))
                try
                {
                    await hotStorageService.UploadAsync(key, archive, stoppingToken);
                    await Task.Delay(configuration.MsDelayBetweenArchiveSaves, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
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
            await foreach (var (key, manifest) in mediator.GetDataChunksManifest(stoppingToken))
                try
                {
                    await hotStorageService.UploadAsync(key, manifest, stoppingToken);
                    await Task.Delay(configuration.MsDelayBetweenManifestSaves, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
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