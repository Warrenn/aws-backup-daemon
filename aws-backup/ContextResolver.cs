using Amazon.S3;

namespace aws_backup;

public interface IContextResolver
{
    string ResolveS3BucketName();
    S3StorageClass ResolveColdStorage();
    ServerSideEncryptionMethod ResolveServerSideEncryptionMethod();
    string ResolveS3Key(DataChunkDetails chunk);
    string ResolveCacheFolder();
    Task<byte[]> ResolveAesKey(CancellationToken cancellationToken);
    Func<int, string, Exception, TimeSpan> ResolveRetryTimeAlgorithm();
    S3StorageClass ResolveHotStorage();
    string ResolveArchiveKey(string runId);
    string ResolveChunkManifestKey();
    Task<byte[]> ResolveSqsDecryptionKey(CancellationToken cancellationToken);
    string ResolveRestoreLocation(string file);
    string ResolveRestoreFolder(string requestRestoreId);
    int ResolveReadConcurrency();
    bool ResolveKeepTimeStamps();
    bool ResolveKeepOwnerGroup();
    bool ResolveKeepAclEntries();
    int ResolveShutdownTimeoutSeconds();
    string? ResolveIgnoreFilePath();
    int ResolveReadBufferSize();
    long ResolveChunkSizeBytes();
    int ResolveRestoreS3Concurrency();
    int ResolveDownloadRetryDelay();
    int ResolveDownloadAttemptLimit();
    int ResolveRetryCheckInterval();
    bool ResolveCheckDownloadHash();
    int ResolveUploadAttemptLimit();
    int ResolveUploadRetryDelay();
}

public class ContextResolver : IContextResolver
{
    public string ResolveS3BucketName()
    {
        throw new NotImplementedException(nameof(ResolveS3BucketName));
    }

    public S3StorageClass ResolveColdStorage()
    {
        throw new NotImplementedException(nameof(ResolveColdStorage));
    }

    public ServerSideEncryptionMethod ResolveServerSideEncryptionMethod()
    {
        throw new NotImplementedException(nameof(ResolveServerSideEncryptionMethod));
    }

    public string ResolveS3Key(DataChunkDetails chunk)
    {
        throw new NotImplementedException(nameof(ResolveS3Key));
    }

    public string ResolveCacheFolder()
    {
        throw new NotImplementedException(nameof(ResolveCacheFolder));
    }

    public Task<byte[]> ResolveAesKey(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Func<int, string, Exception, TimeSpan> ResolveRetryTimeAlgorithm()
    {
        throw new NotImplementedException(nameof(ResolveRetryTimeAlgorithm));
    }

    public S3StorageClass ResolveHotStorage()
    {
        throw new NotImplementedException();
    }

    public string ResolveArchiveKey(string runId)
    {
        throw new NotImplementedException();
    }

    public string ResolveChunkManifestKey()
    {
        throw new NotImplementedException();
    }

    public Task<byte[]> ResolveSqsDecryptionKey(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public string ResolveRestoreLocation(string file)
    {
        throw new NotImplementedException();
    }

    public string ResolveRestoreFolder(string requestRestoreId)
    {
        throw new NotImplementedException();
    }

    public int ResolveReadConcurrency()
    {
        throw new NotImplementedException();
    }

    public bool ResolveKeepTimeStamps()
    {
        throw new NotImplementedException();
    }

    public bool ResolveKeepOwnerGroup()
    {
        throw new NotImplementedException();
    }

    public bool ResolveKeepAclEntries()
    {
        throw new NotImplementedException();
    }

    public int ResolveShutdownTimeoutSeconds()
    {
        throw new NotImplementedException();
    }

    public string? ResolveIgnoreFilePath()
    {
        throw new NotImplementedException();
    }

    public int ResolveReadBufferSize()
    {
        throw new NotImplementedException();
    }

    public long ResolveChunkSizeBytes()
    {
        throw new NotImplementedException();
    }

    public int ResolveRestoreS3Concurrency()
    {
        throw new NotImplementedException();
    }

    public int ResolveDownloadRetryDelay()
    {
        throw new NotImplementedException();
    }

    public int ResolveDownloadAttemptLimit()
    {
        throw new NotImplementedException();
    }

    public int ResolveRetryCheckInterval()
    {
        throw new NotImplementedException();
    }

    public bool ResolveCheckDownloadHash()
    {
        throw new NotImplementedException();
    }

    public int ResolveUploadAttemptLimit()
    {
        throw new NotImplementedException();
    }

    public int ResolveUploadRetryDelay()
    {
        throw new NotImplementedException();
    }
}