using Amazon.S3;
using Amazon.SimpleSystemsManagement;
using Amazon.SQS;

namespace aws_backup;

public interface IAwsClientFactory
{
    Task<IAmazonS3> CreateS3Client(CancellationToken cancellationToken);
    Task<IAmazonSimpleSystemsManagement> CreateSsmClient(CancellationToken cancellationToken);
    Task<IAmazonSQS> CreateSqsClient(CancellationToken cancellationToken);
}

public class AwsClientFactory : IAwsClientFactory
{
    public async Task<IAmazonS3> CreateS3Client(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task<IAmazonSimpleSystemsManagement> CreateSsmClient(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task<IAmazonSQS> CreateSqsClient(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}