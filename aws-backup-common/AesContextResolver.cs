using Amazon.SimpleSystemsManagement.Model;

namespace aws_backup_common;

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
        var ssmClient = await awsClientFactory.CreateSsmClient(cancellationToken);
        var response = await ssmClient.GetParameterAsync(new GetParameterRequest
        {
            Name = path,
            WithDecryption = true
        }, cancellationToken);

        return Convert.FromBase64String(response.Parameter.Value);
    }
}