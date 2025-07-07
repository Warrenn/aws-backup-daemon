using Amazon.SimpleSystemsManagement.Model;

namespace aws_backup;

public interface IAesContextResolver
{
    Task<byte[]> SqsEncryptionKey(CancellationToken cancellationToken);
    Task<byte[]> FileEncryptionKey(CancellationToken cancellationToken);
}

public sealed class AesContextResolver(
    IAwsClientFactory awsClientFactory,
    AwsConfiguration configuration) : IAesContextResolver
{
    private byte[]? _fileEncryptionKey;
    private byte[]? _sqsEncryptionKey;

    public async Task<byte[]> SqsEncryptionKey(CancellationToken cancellationToken)
    {
        return _sqsEncryptionKey ??= await GetEncryptionKey(
            configuration.AesSqsEncryptionPath, cancellationToken);
    }

    public async Task<byte[]> FileEncryptionKey(CancellationToken cancellationToken)
    {
        return _fileEncryptionKey ??= await GetEncryptionKey(
            configuration.AesFileEncryptionPath, cancellationToken);
    }

    private async Task<byte[]> GetEncryptionKey(string path, CancellationToken cancellationToken)
    {
        using var ssmClient = await awsClientFactory.CreateSsmClient(cancellationToken);
        // Retrieve from SSM Parameter Store
        // Implementation depends on your key management strategy
        // Example:
        var response = await ssmClient.GetParameterAsync(new GetParameterRequest
        {
            Name = path,
            WithDecryption = true
        }, cancellationToken);

        return Convert.FromBase64String(response.Parameter.Value);
    }
}