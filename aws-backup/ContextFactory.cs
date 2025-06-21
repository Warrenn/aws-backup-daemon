using Amazon.S3;

namespace aws_backup;

public interface IContextFactory
{
    string ResolveS3BucketName(Configuration configuration);
    S3StorageClass ResolveColdStorage(Configuration configuration);
    ServerSideEncryptionMethod ResolveServerSideEncryptionMethod(Configuration configuration);
    string ResolveS3Key(ChunkData chunk, Configuration configuration);
    string ResolveCacheFolder(Configuration configuration);
    byte[] ResolveAesKey(Configuration configuration);
    Func<int, string, Exception, TimeSpan> ResolveRetryTimeAlgorithm(Configuration configuration);
    S3StorageClass ResolveHotStorage(Configuration configuration);
}

public class ContextFactory : IContextFactory
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
    public string ResolveS3Key(ChunkData chunk, Configuration configuration) 
    {
        throw new NotImplementedException(nameof(ResolveS3Key));
    }
    public string ResolveCacheFolder(Configuration configuration)
    {
        throw new NotImplementedException(nameof(ResolveCacheFolder));
    }

    public byte[] ResolveAesKey(Configuration configuration) 
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
}