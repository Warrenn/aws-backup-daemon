using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public sealed class UploadActor(
    IHotStorageService hotStorageService,
    IArchiveRunMediator runMediator,
    IChunkManifestMediator chunkManifestMediator,
    IRestoreManifestMediator restoreManifestMediator,
    IRestoreRunMediator restoreRunMediator,
    IRestoreRequestsMediator restoreRequestsMediator,
    ISnsMessageMediator snsMessageMediator,
    IContextResolver contextResolver,
    ILogger<UploadActor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var workers = new[]
        {
            RunUploadAsync(runMediator.GetArchiveRuns, cancellationToken),
            RunUploadAsync(chunkManifestMediator.GetDataChunksManifest, cancellationToken),
            RunUploadAsync(restoreManifestMediator.GetRestoreManifest, cancellationToken),
            RunUploadAsync(restoreRunMediator.GetRestoreRuns, cancellationToken),
            RunUploadAsync(runMediator.GetCurrentArchiveRunRequests, cancellationToken),
            RunUploadAsync(restoreRequestsMediator.GetRunningRequests, cancellationToken)
        };

        //GetCurrentArchiveRuns
        await Task.WhenAll(workers);
    }

    private Task RunUploadAsync<T>(
        Func<CancellationToken, IAsyncEnumerable<S3LocationAndValue<T>>> getData,
        CancellationToken cancellationToken) where T : notnull
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
                    await snsMessageMediator.PublishMessage(
                        new ExceptionMessage($"Error processing upload {bucketKey}", ex.ToString()), cancellationToken);
                    logger.LogError(ex, "Error processing upload {BucketKey}", bucketKey);
                    // Optionally, you can rethrow or handle the exception as needed
                }
        }, cancellationToken);
    }
}