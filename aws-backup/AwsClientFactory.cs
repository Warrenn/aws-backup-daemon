using Amazon.S3;
using Amazon.SimpleSystemsManagement;
using Amazon.SQS;

namespace aws_backup;

public interface IAwsClientFactory
{
    Task<IAmazonS3> CreateS3Client(Configuration configuration, CancellationToken cancellationToken);
    Task<IAmazonSimpleSystemsManagement> CreateSsmClient(Configuration configuration, CancellationToken cancellationToken);
    Task<IAmazonSQS> CreateSqsClient(Configuration configuration, CancellationToken cancellationToken);
}

public class AwsClientFactory : IAwsClientFactory
{
    public Task<IAmazonS3> CreateS3Client(Configuration configuration, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task<IAmazonSimpleSystemsManagement> CreateSsmClient(Configuration configuration, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task<IAmazonSQS> CreateSqsClient(Configuration configuration, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}