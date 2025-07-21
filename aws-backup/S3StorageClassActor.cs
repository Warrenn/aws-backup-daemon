using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IS3StorageClassMediator
{
    IAsyncEnumerable<string> GetStorageClassesRequests(CancellationToken cancellationToken);
    Task QueryStorageClass(string key, CancellationToken cancellationToken);
}

public sealed class S3StorageClassActor(
    IS3Service s3Service,
    IRestoreService restoreService,
    IContextResolver resolver,
    IS3StorageClassMediator mediator,
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
                await foreach (var s3Key in mediator.GetStorageClassesRequests(cancellationToken))
                {
                    var storage = await s3Service.GetStorageClass(s3Key, cancellationToken);
                    await restoreService.ReportS3Storage(s3Key, storage, cancellationToken);
                    storageCheckDelay = resolver.StorageCheckDelaySeconds();
                    var timespan = TimeSpan.FromSeconds(storageCheckDelay);
                    await Task.Delay(timespan, cancellationToken);
                }
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