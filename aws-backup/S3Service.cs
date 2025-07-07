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

    Task<S3ChunkRestoreStatus> ScheduleDeepArchiveRecovery(string chunkS3Key,
        CancellationToken cancellationToken);

    IAsyncEnumerable<S3StorageInfo> GetStorageClasses(CancellationToken cancellationToken);
}

public sealed class S3Service(
    IAwsClientFactory awsClientFactory,
    IContextResolver contextResolver,
    IHotStorageService hotStorageService,
    AwsConfiguration awsConfiguration
) : IS3Service
{
    public async Task<bool> RunExists(string runId, CancellationToken cancellationToken)
    {
        var bucketId = awsConfiguration.BucketName;
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
        var bucketId = awsConfiguration.BucketName;
        var key = contextResolver.RestoreIdBucketKey(restoreId);
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        return await hotStorageService.S3ObjectExistsAsync(s3Client, bucketId, key, cancellationToken);
    }

    public async Task<RestoreRun> GetRestoreRun(string restoreId, CancellationToken cancellationToken)
    {
        var key = contextResolver.RestoreIdBucketKey(restoreId);
        return (await hotStorageService.DownloadAsync<RestoreRun>(key, cancellationToken))!;
    }

    public async Task<S3ChunkRestoreStatus> ScheduleDeepArchiveRecovery(string chunkS3Key,
        CancellationToken cancellationToken)
    {
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = awsConfiguration.BucketName;

        var (storageClass, restoreInProgress) =
            await GetStorageClass(s3Client, bucketName, chunkS3Key, cancellationToken);
        if (storageClass != S3StorageClass.DeepArchive) return S3ChunkRestoreStatus.ReadyToRestore;
        if (restoreInProgress) return S3ChunkRestoreStatus.PendingDeepArchiveRestore;

        var daysToKeepRestoredCopy = contextResolver.DaysToKeepRestoredCopy();
        var restoreRequest = new RestoreObjectRequest
        {
            BucketName = bucketName,
            Key = chunkS3Key,
            Days = daysToKeepRestoredCopy,
            RetrievalTier = GlacierJobTier.Standard
        };
        await s3Client.RestoreObjectAsync(restoreRequest, cancellationToken);

        return S3ChunkRestoreStatus.PendingDeepArchiveRestore;
    }

    public async IAsyncEnumerable<S3StorageInfo> GetStorageClasses(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = awsConfiguration.BucketName;
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

    private static async Task<(S3StorageClass storageClass, bool restoreInProgress)> GetStorageClass(IAmazonS3 s3Client,
        string bucketName, string s3Key,
        CancellationToken cancellationToken)
    {
        var request = new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = s3Key
        };

        var metadata = await s3Client.GetObjectMetadataAsync(request, cancellationToken);

        // StorageClass is a nullable enum; if AWS returned no header it will be null
        return (metadata.StorageClass, metadata.RestoreInProgress ?? false);
    }
}