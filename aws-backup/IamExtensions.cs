using Amazon.IdentityManagement;
using Amazon.IdentityManagement.Model;

namespace aws_backup;

public static class IamExtensions
{
    /// <summary>
    ///     Fetches the "config-path" tag value for the specified IAM role, or null if not present.
    /// </summary>
    public static async Task<AwsConfiguration> GetAwsConfigurationAsync(
        this IAmazonIdentityManagementService iam,
        string roleName,
        CancellationToken cancellationToken = default)
    {
        string? marker = null;
        var tags = new Dictionary<string, string>();

        do
        {
            var req = new ListRoleTagsRequest
            {
                RoleName = roleName,
                Marker = marker
                // MaxItems defaults to 100; adjust if you have >100 tags
            };

            var resp = await iam.ListRoleTagsAsync(req, cancellationToken);

            foreach (var tag in resp.Tags.Where(tag => !tags.ContainsKey(tag.Key))) tags[tag.Key] = tag.Value;
            marker = resp.IsTruncated ?? false ? resp.Marker : null;
        } while (marker != null);

        return new AwsConfiguration(
            tags.TryGetValue("chunk-size-bytes", out var chunkSize) ? long.Parse(chunkSize) : 524288000L,
            tags.GetValueOrDefault("aes-sqs-encryption-path", string.Empty),
            tags.GetValueOrDefault("aes-file-encryption-path", string.Empty),
            tags.GetValueOrDefault("bucket-name", string.Empty),
            tags.GetValueOrDefault("bucket-region", string.Empty),
            tags.GetValueOrDefault("sqs-inbox-queue-url", string.Empty),
            tags.GetValueOrDefault("sqs-outbox-queue-url", string.Empty),
            tags.GetValueOrDefault("archive-complete-topic-arn", string.Empty),
            tags.GetValueOrDefault("archive-complete-errors-topic-arn", string.Empty),
            tags.GetValueOrDefault("restore-complete-topic-arn", string.Empty),
            tags.GetValueOrDefault("restore-complete-errors-topic-arn", string.Empty),
            tags.GetValueOrDefault("exception-topic-arn", string.Empty)
        );
    }
}