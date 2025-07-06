using Microsoft.Extensions.Hosting;

namespace aws_backup;

public sealed class S3StorageClassActor(
    IS3Service s3Service,
    IRestoreService restoreService,
    IContextResolver resolver
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await foreach (var s3StorageInfo in s3Service.GetStorageClasses(cancellationToken))
            {
                await restoreService.ReportS3Storage(s3StorageInfo.BucketName, s3StorageInfo.Key,
                    s3StorageInfo.StorageClass, cancellationToken);
                var storagePageDelay = resolver.StoragePageDelayMilliseconds();
                await Task.Delay(storagePageDelay, cancellationToken); // small delay to avoid overwhelming the service
            }

            var storageCheckDelay = resolver.StorageCheckDelaySeconds();
            await Task.Delay(storageCheckDelay, cancellationToken);
        }
    }
}