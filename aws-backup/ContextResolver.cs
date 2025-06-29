using Amazon.S3;

namespace aws_backup;

public interface IContextResolver
{
    string ChunkS3Key(
        string localFilePath,
        int chunkIndex,
        long chunkSize,
        byte[] hashKey,
        long size
    );

    string RestoreId(
        string archiveRunId,
        string restorePaths,
        DateTimeOffset requestedAt
    );

    string LocalRestoreFolder(string requestRestoreId);
    string ArchiveRunId(DateTimeOffset utcNow);
    DateTimeOffset NextRetryTime(int attemptCount);

    // s3
    string S3BucketId();
    S3StorageClass ColdStorage();
    S3StorageClass HotStorage();
    ServerSideEncryptionMethod ServerSideEncryptionMethod();
    int ReadBufferSize();
    long ChunkSizeBytes();
    long S3PartSize();
    Task<byte[]> AesFileEncryptionKey(CancellationToken cancellationToken);
    Task<byte[]> SqsEncryptionKey(CancellationToken cancellationToken);
    bool KeepTimeStamps();
    bool KeepOwnerGroup();
    bool KeepAclEntries();
    bool CheckDownloadHash();
    string LocalCacheFolder();
    string? LocalIgnoreFile();
    int NoOfConcurrentDownloadsPerFile();
    int NoOfS3FilesToDownloadConcurrently();
    int NoOfS3FilesToUploadConcurrently();
    int DownloadRetryDelaySeconds();
    int UploadRetryDelaySeconds();
    int ShutdownTimeoutSeconds();
    int RetryCheckIntervalMs();
    int StorageCheckDelaySeconds();
    int DelayBetweenUploadsSeconds();
    int DownloadAttemptLimit();
    int UploadAttemptLimit();
    string SqsQueueUrl();
    int? SqsWaitTimeSeconds();
    int? SqsMaxNumberOfMessages();
    int? SqsVisibilityTimeout();
    long SqsRetryDelaySeconds();
    bool EncryptSqs();
    int GeneralRetryLimit();
}

public class ContextResolver : IContextResolver
{
    public string S3BucketId()
    {
        throw new NotImplementedException(nameof(S3BucketId));
    }

    public S3StorageClass ColdStorage()
    {
        throw new NotImplementedException(nameof(ColdStorage));
    }

    public ServerSideEncryptionMethod ServerSideEncryptionMethod()
    {
        throw new NotImplementedException(nameof(ServerSideEncryptionMethod));
    }

    public string LocalCacheFolder()
    {
        throw new NotImplementedException(nameof(LocalCacheFolder));
    }

    public Task<byte[]> AesFileEncryptionKey(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public S3StorageClass HotStorage()
    {
        throw new NotImplementedException();
    }

    public Task<byte[]> SqsEncryptionKey(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public string ChunkS3Key(string localFilePath, int chunkIndex, long chunkSize, byte[] hashKey, long size)
    {
        throw new NotImplementedException();
    }

    public string RestoreId(string archiveRunId, string restorePaths, DateTimeOffset requestedAt)
    {
        throw new NotImplementedException();
    }

    public string LocalRestoreFolder(string requestRestoreId)
    {
        throw new NotImplementedException();
    }

    public int NoOfConcurrentDownloadsPerFile()
    {
        throw new NotImplementedException();
    }

    public bool KeepTimeStamps()
    {
        throw new NotImplementedException();
    }

    public bool KeepOwnerGroup()
    {
        throw new NotImplementedException();
    }

    public bool KeepAclEntries()
    {
        throw new NotImplementedException();
    }

    public int ShutdownTimeoutSeconds()
    {
        throw new NotImplementedException();
    }

    public string? LocalIgnoreFile()
    {
        throw new NotImplementedException();
    }

    public int ReadBufferSize()
    {
        throw new NotImplementedException();
    }

    public long ChunkSizeBytes()
    {
        throw new NotImplementedException();
    }

    public int NoOfS3FilesToDownloadConcurrently()
    {
        throw new NotImplementedException();
    }

    public int DownloadRetryDelaySeconds()
    {
        throw new NotImplementedException();
    }

    public int DownloadAttemptLimit()
    {
        throw new NotImplementedException();
    }

    public int RetryCheckIntervalMs()
    {
        throw new NotImplementedException();
    }

    public bool CheckDownloadHash()
    {
        throw new NotImplementedException();
    }

    public int UploadAttemptLimit()
    {
        throw new NotImplementedException();
    }

    public int UploadRetryDelaySeconds()
    {
        throw new NotImplementedException();
    }

    public int NoOfS3FilesToUploadConcurrently()
    {
        throw new NotImplementedException();
    }

    public long S3PartSize()
    {
        throw new NotImplementedException();
    }

    public int StorageCheckDelaySeconds()
    {
        throw new NotImplementedException();
    }

    public string SqsQueueUrl()
    {
        throw new NotImplementedException();
    }

    public int? SqsWaitTimeSeconds()
    {
        throw new NotImplementedException();
    }

    public int? SqsMaxNumberOfMessages()
    {
        throw new NotImplementedException();
    }

    public int? SqsVisibilityTimeout()
    {
        throw new NotImplementedException();
    }

    public long SqsRetryDelaySeconds()
    {
        throw new NotImplementedException();
    }

    public bool EncryptSqs()
    {
        throw new NotImplementedException();
    }

    public int GeneralRetryLimit()
    {
        throw new NotImplementedException();
    }

    public string ArchiveRunId(DateTimeOffset utcNow)
    {
        throw new NotImplementedException();
    }

    public DateTimeOffset NextRetryTime(int attemptCount)
    {
        throw new NotImplementedException();
    }

    public int DelayBetweenUploadsSeconds()
    {
        throw new NotImplementedException();
    }

}