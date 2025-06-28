using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class UploadOrchestration(
    IHotStorageService hotStorageService,
    IArchiveRunMediator runMediator,
    IChunkManifestMediator chunkManifestMediator,
    IRestoreManifestMediator restoreManifestMediator,
    IRestoreRunMediator restoreRequestsMediator,
    IContextResolver contextResolver,
    ILogger<UploadOrchestration> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var delayBetweenUploads = contextResolver.DelayBetweenUploadsSeconds();
        var workers = new[]
        {
            RunUploadAsync(runMediator.GetArchiveRuns, delayBetweenUploads, cancellationToken),
            RunUploadAsync(chunkManifestMediator.GetDataChunksManifest, delayBetweenUploads, cancellationToken),
            RunUploadAsync(restoreManifestMediator.GetRestoreManifest, delayBetweenUploads, cancellationToken),
            RunUploadAsync(restoreRequestsMediator.GetRestoreRuns, delayBetweenUploads, cancellationToken)
        };

        await Task.WhenAll(workers);
    }

    private Task RunUploadAsync<T>(
        Func<CancellationToken, IAsyncEnumerable<KeyValuePair<string, T>>> getData,
        int delayBetweenUploads,
        CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            await foreach (var (bucketKey, data) in getData(cancellationToken))
                try
                {
                    await hotStorageService.UploadAsync(bucketKey, data, cancellationToken);
                    await Task.Delay(delayBetweenUploads, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error processing archive run upload for run ID");
                    // Optionally, you can rethrow or handle the exception as needed
                }
        }, cancellationToken);
    }
}