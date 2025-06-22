using Amazon.S3;
using Amazon.SimpleSystemsManagement;

namespace aws_backup;

public interface IAwsClientFactory
{
    Task<IAmazonS3> CreateS3Client(Configuration configuration);
    Task<IAmazonSimpleSystemsManagement> CreateSsmClient(Configuration configuration);
}

public class AwsClientFactory : IAwsClientFactory
{
    public Task<IAmazonS3> CreateS3Client(Configuration configuration)
    {
        throw new NotImplementedException();
    }

    public Task<IAmazonSimpleSystemsManagement> CreateSsmClient(Configuration configuration)
    {
        throw new NotImplementedException();
    }
}