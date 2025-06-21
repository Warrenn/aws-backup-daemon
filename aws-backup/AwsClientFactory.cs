using Amazon.S3;
using Amazon.SimpleSystemsManagement;

namespace aws_backup;

public interface IAwsClientFactory
{
    IAmazonS3 CreateS3Client(Configuration configuration);
    IAmazonSimpleSystemsManagement CreateSsmClient(Configuration configuration);
}

public class AwsClientFactory : IAwsClientFactory
{
    public IAmazonS3 CreateS3Client(Configuration configuration)
    {
        throw new NotImplementedException();
    }

    public IAmazonSimpleSystemsManagement CreateSsmClient(Configuration configuration)
    {
        throw new NotImplementedException();
    }
}