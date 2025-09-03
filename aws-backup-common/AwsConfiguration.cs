using System.Collections.Frozen;
using System.Text;
using Amazon.CloudFormation.Model;

namespace aws_backup_common;

public sealed record AwsConfiguration(
    string BucketName, // required
    string SqsInboxQueueUrl,
    string SqsOutboxQueueUrl,
    string ArchiveCompleteTopicArn,
    string RestoreCompleteTopicArn,
    string ArchiveCompleteErrorsTopicArn,
    string RestoreCompleteErrorsTopicArn,
    string ExceptionTopicArn,
    string ParamBasePath,
    string DynamoDbTableName);

public interface IAwsConfigurationFactory
{
    Task<(AwsConfiguration? configuration, string? errorMessage)> GetAwsConfiguration(
        CancellationToken cancellationToken);
}

public sealed class AwsConfigurationFactory(
    IAwsClientFactory clientFactory,
    string clientId,
    Configuration configuration) : IAwsConfigurationFactory
{
    public async Task<(AwsConfiguration? configuration, string? errorMessage)> GetAwsConfiguration(
        CancellationToken cancellationToken)
    {
        try
        {
            var stackName = GetDefault(configuration.StackName, $"per-client-{clientId}");
            var cloudFormationClient = await clientFactory.CreateCloudFormationClient(cancellationToken);

            var resp = await cloudFormationClient.DescribeStacksAsync(
                new DescribeStacksRequest { StackName = stackName }, cancellationToken);
            var stack = resp.Stacks?.FirstOrDefault();
            var outputs = (stack?.Outputs ?? [])
                .ToFrozenDictionary(o => o.OutputKey, o => o.OutputValue ?? string.Empty,
                    StringComparer.InvariantCultureIgnoreCase);

            var errorBuilder = new StringBuilder();
            var returnValue = new AwsConfiguration(
                BucketName: GetValue(outputs, errorBuilder, "BucketName", configuration.BucketName),
                SqsInboxQueueUrl: GetValue(outputs, errorBuilder, "SqsInboxQueueUrl", configuration.SqsInboxQueueUrl),
                SqsOutboxQueueUrl: GetValue(outputs, errorBuilder,"SqsOutboxQueueUrl", configuration.SqsOutboxQueueUrl),
                ArchiveCompleteTopicArn: GetValue(outputs, errorBuilder, "ArchiveCompleteTopicArn", configuration.ArchiveCompleteTopicArn),
                RestoreCompleteTopicArn: GetValue(outputs, errorBuilder, "RestoreCompleteTopicArn", configuration.RestoreCompleteTopicArn),
                ArchiveCompleteErrorsTopicArn: GetValue(outputs, errorBuilder, "ArchiveCompleteErrorsTopicArn", configuration.ArchiveCompleteErrorsTopicArn),
                RestoreCompleteErrorsTopicArn: GetValue(outputs, errorBuilder, "RestoreCompleteErrorsTopicArn", configuration.RestoreCompleteErrorsTopicArn),
                ExceptionTopicArn: GetValue(outputs, errorBuilder, "ExceptionTopicArn", configuration.ExceptionTopicArn),
                ParamBasePath: GetValue(outputs, errorBuilder, "ParamBasePath", configuration.ParamBasePath),
                DynamoDbTableName: GetValue(outputs, errorBuilder, "DynamoDbTableName", configuration.DynamoDbTableName)
            );
            
            if (errorBuilder.Length > 0)
                return (null, errorBuilder.ToString()); ;

            return (returnValue, null);
        }
        catch (Exception e)
        {
            return (null, e.Message);
        }
    }

    private static string GetDefault(string? value, string defaultValue)
    {
        return string.IsNullOrWhiteSpace(value) ? defaultValue : value;
    }

    private static string GetValue(FrozenDictionary<string, string> d, StringBuilder errorBuilder, string key,
        string? defaultValue)
    {
        if (!string.IsNullOrWhiteSpace(defaultValue)) return defaultValue;
        if (d.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v)) return v;
        errorBuilder.AppendLine($"Required CloudFormation Output '{key}' is missing or empty.");
        return "";
    }
}