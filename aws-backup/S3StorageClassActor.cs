using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public sealed class S3StorageClassActor(
    IS3Service s3Service,
    IRestoreService restoreService,
    IContextResolver resolver,
    ILogger<S3StorageClassActor> logger
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var storageCheckDelay = resolver.StorageCheckDelaySeconds();
        logger.LogInformation("Starting storage class actor");
        while (!cancellationToken.IsCancellationRequested)
            try
            {
                await foreach (var s3StorageInfo in s3Service.GetStorageClasses(cancellationToken))
                {
                    await restoreService.ReportS3Storage(s3StorageInfo.BucketName, s3StorageInfo.Key,
                        s3StorageInfo.StorageClass, cancellationToken);
                    var storagePageDelay = resolver.StoragePageDelayMilliseconds();
                    await Task.Delay(storagePageDelay,
                        cancellationToken); // small delay to avoid overwhelming the service
                }

                storageCheckDelay = resolver.StorageCheckDelaySeconds();
                await Task.Delay(storageCheckDelay, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in S3 storage class actor: {Message}", ex.Message);
                // Optionally, you could add a delay here to avoid rapid retries
                await Task.Delay(storageCheckDelay, cancellationToken); // delay on error
            }
    }
}