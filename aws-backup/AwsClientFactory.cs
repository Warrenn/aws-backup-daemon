using Amazon.S3;

namespace aws_backup;

public interface IAwsClientFactory
{
    IAmazonS3 CreateS3Client(Configuration configuration);
}

public class AwsClientFactory : IAwsClientFactory
{
    public IAmazonS3 CreateS3Client(Configuration configuration)
    {
        throw new NotImplementedException();
    }
}