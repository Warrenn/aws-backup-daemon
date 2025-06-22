using Amazon.S3;

namespace aws_backup;

public interface IContextResolver
{
    string ResolveS3BucketName(Configuration configuration);
    S3StorageClass ResolveColdStorage(Configuration configuration);
    ServerSideEncryptionMethod ResolveServerSideEncryptionMethod(Configuration configuration);
    string ResolveS3Key(DataChunkDetails chunk, Configuration configuration);
    string ResolveCacheFolder(Configuration configuration);
    Task<byte[]> ResolveAesKey(Configuration configuration);
    Func<int, string, Exception, TimeSpan> ResolveRetryTimeAlgorithm(Configuration configuration);
    S3StorageClass ResolveHotStorage(Configuration configuration);
    string ResolveArchiveKey(string runId);
    string ResolveChunkManifestKey();
    Task<byte[]> ResolveSqsDecryptionKey(Configuration configuration);
}

public class ContextResolver : IContextResolver
{
    public string ResolveS3BucketName(Configuration configuration) => configuration.S3BucketName;

    public S3StorageClass ResolveColdStorage(Configuration configuration)
    {
        throw new NotImplementedException(nameof(ResolveColdStorage));
    }

    public ServerSideEncryptionMethod ResolveServerSideEncryptionMethod(Configuration configuration) 
    {
        throw new NotImplementedException(nameof(ResolveServerSideEncryptionMethod));
    }
    public string ResolveS3Key(DataChunkDetails chunk, Configuration configuration) 
    {
        throw new NotImplementedException(nameof(ResolveS3Key));
    }
    public string ResolveCacheFolder(Configuration configuration)
    {
        throw new NotImplementedException(nameof(ResolveCacheFolder));
    }

    public Task<byte[]> ResolveAesKey(Configuration configuration) 
    {
        throw new NotImplementedException(nameof(ResolveAesKey));
    }

    public Func<int, string, Exception, TimeSpan> ResolveRetryTimeAlgorithm(Configuration configuration) 
    {
        throw new NotImplementedException(nameof(ResolveRetryTimeAlgorithm));
    }

    public S3StorageClass ResolveHotStorage(Configuration configuration)
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

    public Task<byte[]> ResolveSqsDecryptionKey(Configuration configuration)
    {
        throw new NotImplementedException();
    }
}