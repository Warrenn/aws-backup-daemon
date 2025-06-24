using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class StateUploadOrchestration(
    IHotStorageService hotStorageService,
    IMediator mediator,
    IContextResolver contextResolver,
    ILogger<StateUploadOrchestration> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var delayBetweenUploads = contextResolver.ResolveDelayBetweenUploads();
        var archiveTask = Task.Run(async () =>
        {
            await foreach (var (archive, key) in mediator.GetArchiveState(cancellationToken))
                try
                {
                    await hotStorageService.UploadAsync(key, archive, cancellationToken);
                    await Task.Delay(delayBetweenUploads, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error processing hot storage upload for archive {ArchiveId} and key {Key}",
                        archive.RunId, key);
                    // Optionally, you can rethrow or handle the exception as needed
                }
        }, cancellationToken);

        var uploadManifestTask = Task.Run(async () =>
        {
            await foreach (var (key, manifest) in mediator.GetDataChunksManifest(cancellationToken))
                try
                {
                    await hotStorageService.UploadAsync(key, manifest, cancellationToken);
                    await Task.Delay(delayBetweenUploads, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error processing hot storage upload manifest for key {Key}", key);
                    // Optionally, you can rethrow or handle the exception as needed
                }
        }, cancellationToken);

        await Task.WhenAll(uploadManifestTask, archiveTask);
    }
}