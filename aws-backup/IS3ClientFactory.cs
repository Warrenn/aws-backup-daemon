using Amazon.S3;

namespace aws_backup;

public interface IS3ClientFactory
{
    IAmazonS3 CreateS3Client(Configuration configuration);
}