using System.Runtime.CompilerServices;
using Amazon.S3;
using Amazon.S3.Model;

namespace aws_backup;

public sealed record S3StorageInfo(
    string BucketName,
    string Key,
    S3StorageClass StorageClass
);

public interface IS3Service
{
    Task<bool> RunExists(string runId, CancellationToken cancellationToken);
    Task<ArchiveRun> GetArchive(string runId, CancellationToken cancellationToken);
    Task<bool> RestoreExists(string restoreId, CancellationToken cancellationToken);
    Task<RestoreRun> GetRestoreRun(string restoreId, CancellationToken cancellationToken);
    Task ScheduleDeepArchiveRecovery(CloudChunkDetails cloudChunkDetails, CancellationToken cancellationToken);
    IAsyncEnumerable<S3StorageInfo> GetStorageClasses(CancellationToken cancellationToken);
}

public sealed class S3Service(
    IAwsClientFactory awsClientFactory,
    IContextResolver contextResolver,
    IHotStorageService hotStorageService
) : IS3Service
{
    public async Task<bool> RunExists(string runId, CancellationToken cancellationToken)
    {
        var bucketId = contextResolver.S3BucketId();
        var key = contextResolver.RunIdBucketKey(runId);
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        return await hotStorageService.S3ObjectExistsAsync(s3Client, bucketId, key, cancellationToken);
    }

    public async Task<ArchiveRun> GetArchive(string runId, CancellationToken cancellationToken)
    {
        var key = contextResolver.RunIdBucketKey(runId);
        return (await hotStorageService.DownloadAsync<ArchiveRun>(key, cancellationToken))!;
    }

    public async Task<bool> RestoreExists(string restoreId, CancellationToken cancellationToken)
    {
        var bucketId = contextResolver.S3BucketId();
        var key = contextResolver.RestoreIdBucketKey(restoreId);
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        return await hotStorageService.S3ObjectExistsAsync(s3Client, bucketId, key, cancellationToken);
    }

    public async Task<RestoreRun> GetRestoreRun(string restoreId, CancellationToken cancellationToken)
    {
        var key = contextResolver.RestoreIdBucketKey(restoreId);
        return (await hotStorageService.DownloadAsync<RestoreRun>(key, cancellationToken))!;
    }

    public async Task ScheduleDeepArchiveRecovery(CloudChunkDetails cloudChunkDetails,
        CancellationToken cancellationToken)
    {
        var chunkS3Key = cloudChunkDetails.S3Key;
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = contextResolver.S3BucketId();
        var daysToKeepRestoredCopy = contextResolver.DaysToKeepRestoredCopy();
        var restoreRequest = new RestoreObjectRequest
        {
            BucketName = bucketName,
            Key = chunkS3Key,
            Days = daysToKeepRestoredCopy,
            RetrievalTier = GlacierJobTier.Standard
        };
        await s3Client.RestoreObjectAsync(restoreRequest, cancellationToken);
    }

    public async IAsyncEnumerable<S3StorageInfo> GetStorageClasses(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = contextResolver.S3BucketId();
        var prefix = contextResolver.S3DataPrefix();
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = prefix
            // You can also set MaxKeys if you want smaller pages
        };

        ListObjectsV2Response response;
        do
        {
            response = await s3Client.ListObjectsV2Async(request, cancellationToken);

            foreach (var s3Object in response.S3Objects)
                yield return new S3StorageInfo(
                    s3Object.BucketName,
                    s3Object.Key,
                    s3Object.StorageClass
                );

            // If the response is truncated, set the token to get the next page
            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated ?? false);
    }
}