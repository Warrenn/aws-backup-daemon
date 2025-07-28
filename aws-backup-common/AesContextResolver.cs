using Amazon.SimpleSystemsManagement.Model;

namespace aws_backup_common;

public interface IAesContextResolver
{
    Task<byte[]> SqsEncryptionKey(CancellationToken cancellationToken);
    Task<byte[]> FileEncryptionKey(CancellationToken cancellationToken);
}

public sealed class AesContextResolver(
    IAwsClientFactory awsClientFactory,
    IContextResolver resolver) : IAesContextResolver
{
    private byte[]? _fileEncryptionKey;
    private byte[]? _sqsEncryptionKey;

    public async Task<byte[]> SqsEncryptionKey(CancellationToken cancellationToken)
    {
        var aesSqsEncryptionPath = $"{resolver.ParamBasePath()}/{resolver.ClientId()}/aes-sqs-encryption";
        return _sqsEncryptionKey ??= await GetEncryptionKey(aesSqsEncryptionPath, cancellationToken);
    }

    public async Task<byte[]> FileEncryptionKey(CancellationToken cancellationToken)
    {
        var aesSqsEncryptionPath = $"{resolver.ParamBasePath()}/{resolver.ClientId()}/aes-file-encryption";
        return _fileEncryptionKey ??= await GetEncryptionKey(aesSqsEncryptionPath, cancellationToken);
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