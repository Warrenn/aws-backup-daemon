namespace aws_backup_common;

//settings that client should not change
public sealed record AwsConfiguration(
    long ChunkSizeBytes,
    string AesSqsEncryptionPath,
    string AesFileEncryptionPath,
    string BucketName,
    string BucketRegion,
    string SqsInboxQueueUrl,
    string SqsOutboxQueueUrl,
    string ArchiveCompleteTopicArn,
    string ArchiveCompleteErrorsTopicArn,
    string RestoreCompleteTopicArn,
    string RestoreCompleteErrorsTopicArn,
    string ExceptionTopicArn);