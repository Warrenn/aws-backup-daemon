using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public sealed class UploadOrchestration(
    IHotStorageService hotStorageService,
    IArchiveRunMediator runMediator,
    IChunkManifestMediator chunkManifestMediator,
    IRestoreManifestMediator restoreManifestMediator,
    IRestoreRunMediator restoreRunMediator,
    IRestoreRequestsMediator restoreRequestsMediator,
    ISnsOrchestrationMediator snsOrchestrationMediator,
    IContextResolver contextResolver,
    ILogger<UploadOrchestration> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var workers = new[]
        {
            RunUploadAsync(runMediator.GetArchiveRuns, cancellationToken),
            RunUploadAsync(chunkManifestMediator.GetDataChunksManifest, cancellationToken),
            RunUploadAsync(restoreManifestMediator.GetRestoreManifest, cancellationToken),
            RunUploadAsync(restoreRunMediator.GetRestoreRuns, cancellationToken),
            RunUploadAsync(runMediator.GetCurrentArchiveRuns, cancellationToken),
            RunUploadAsync(restoreRequestsMediator.GetRunningRequests, cancellationToken)
        };
        //GetCurrentArchiveRuns
        await Task.WhenAll(workers);
    }

    private Task RunUploadAsync<T>(
        Func<CancellationToken, IAsyncEnumerable<KeyValuePair<string, T>>> getData,
        CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            await foreach (var (bucketKey, data) in getData(cancellationToken))
                try
                {
                    var delayBetweenUploads = contextResolver.DelayBetweenUploadsSeconds();
                    await hotStorageService.UploadAsync(bucketKey, data, cancellationToken);
                    await Task.Delay(delayBetweenUploads, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    await snsOrchestrationMediator.PublishMessage(
                        new ExceptionMessage($"Error processing upload {bucketKey}", ex.ToString()), cancellationToken);
                    logger.LogError(ex, "Error processing upload {BucketKey}", bucketKey);
                    // Optionally, you can rethrow or handle the exception as needed
                }
        }, cancellationToken);
    }
}