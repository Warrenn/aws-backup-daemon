using System.Text.Json;
using Amazon.SimpleSystemsManagement.Model;

namespace aws_backup_common;

//settings that client should not change
public sealed record AwsConfiguration(
    long ChunkSizeBytes,
    string AesSqsEncryptionPath,
    string AesFileEncryptionPath,
    string BucketName,
    string SqsInboxQueueUrl,
    string ArchiveCompleteTopicArn,
    string ArchiveCompleteErrorsTopicArn,
    string RestoreCompleteTopicArn,
    string RestoreCompleteErrorsTopicArn,
    string ExceptionTopicArn,
    string DynamoDbTableName);

public interface IAwsConfigurationFactory
{
    Task<AwsConfiguration> GetAwsConfiguration(CancellationToken cancellationToken);
}

public sealed class AwsConfigurationFactory(
    IAwsClientFactory clientFactory,
    IContextResolver contextResolver) : IAwsConfigurationFactory
{
    public async Task<AwsConfiguration> GetAwsConfiguration(CancellationToken cancellationToken)
    {
        var ssm = await clientFactory.CreateSsmClient(cancellationToken);
        var paramPath = contextResolver.ParamBasePath();
        var clientId = contextResolver.ClientId();
        var region = contextResolver.GetAwsRegion().ToString().ToLowerInvariant();
        var configParam = await ssm.GetParameterAsync(new GetParameterRequest { Name = paramPath }, cancellationToken);
        if (configParam.Parameter is null)
            throw new InvalidOperationException(
                $"Parameter {paramPath} not found in SSM Parameter Store for client {clientId} in region {region}");
                
        var config = configParam.Parameter.Value;
        var returnValue =
            JsonSerializer.Deserialize<AwsConfiguration>(config, SourceGenerationContext.Default.AwsConfiguration);

        return returnValue!;
    }
}