using Microsoft.Extensions.Hosting;

namespace aws_backup;

public class S3StorageClassOrchestration(
    IS3Service s3Service,
    IRestoreService restoreService,
    IContextResolver resolver
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var storageCheckDelay = resolver.ResolveStorageCheckDelay();

        while (!cancellationToken.IsCancellationRequested)
        {
            var storageClasses = await s3Service.GetStorageClasses(cancellationToken);
            foreach (var s3StorageInfo in storageClasses)
                await restoreService.ReportS3Storage(s3StorageInfo.BucketName, s3StorageInfo.Key,
                    s3StorageInfo.StorageClass, cancellationToken);

            await Task.Delay(storageCheckDelay, cancellationToken);
        }
    }
}