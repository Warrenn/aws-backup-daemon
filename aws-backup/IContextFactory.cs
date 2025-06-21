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
}