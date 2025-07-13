using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Options;

namespace aws_backup;

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


//settings that can change
public sealed record Configuration
{
    [Required(AllowEmptyStrings = false)]
    public string ClientId { get; set; }

    [Required(AllowEmptyStrings = false)]
    public string CronSchedule { get; set; }

    [Required(AllowEmptyStrings = false)]
    public string PathsToArchive { get; set; }

    public string? ColdStorage { get; set; }
    public string? HotStorage { get; set; }
    public string? LowCostStorage { get; set; }
    public string? AwsRegion { get; set; }
    public string? ServerSideEncryption { get; set; }
    public string? LocalCacheFolder { get; set; }
    public string? LocalIgnoreFile { get; set; }
    public string? LocalRestoreFolderBase { get; set; }
    public string? AwsRetryMode { get; set; }
    public string? RolesAnyWhereProfileArn { get; set; }
    public string? RolesAnyWhereRoleArn { get; set; }
    public string? RolesAnyWhereTrustAnchorArn { get; set; }
    public string? RolesAnyWhereCertificateFileName { get; set; }
    public string? RolesAnyWherePrivateKeyFileName { get; set; }
    public int? AwsCredentialsTimeoutSeconds { get; set; }
    public int? ReadBufferSize { get; set; }
    public long? S3PartSize { get; set; }
    public int? NoOfConcurrentFileUploads { get; set; }
    public int? NoOfConcurrentDownloadsPerFile { get; set; }
    public int? NoOfS3FilesToDownloadConcurrently { get; set; }
    public int? NoOfS3FilesToUploadConcurrently { get; set; }
    public int? ShutdownTimeoutSeconds { get; set; }
    public int? RetryCheckIntervalMs { get; set; }
    public int? StorageCheckDelaySeconds { get; set; }
    public int? StoragePageDelayMilliseconds { get; set; }
    public int? DelayBetweenUploadsSeconds { get; set; }
    public int? DownloadAttemptLimit { get; set; }
    public int? UploadAttemptLimit { get; set; }
    public int? SqsWaitTimeSeconds { get; set; }
    public int? SqsMaxNumberOfMessages { get; set; }
    public int? SqsVisibilityTimeout { get; set; }
    public long? SqsRetryDelaySeconds { get; set; }
    public int? GeneralRetryLimit { get; set; }
    public bool? KeepTimeStamps { get; set; }
    public bool? KeepOwnerGroup { get; set; }
    public bool? KeepAclEntries { get; set; }
    public bool? CheckDownloadHash { get; set; }
    public bool? EncryptSqs { get; set; }
    public bool? UseS3Accelerate { get; set; }
    public bool? NotifyOnArchiveComplete { get; set; }
    public bool? NotifyOnArchiveCompleteErrors { get; set; }
    public bool? NotifyOnRestoreComplete { get; set; }
    public bool? NotifyOnRestoreCompleteErrors { get; set; }
    public bool? NotifyOnException { get; set; }
    public int? DaysToKeepRestoredCopy { get; set; }
    public string? RollingLogFolder { get; set; }
    public long? CacheFolderSizeLimitBytes { get; set; }
    public long? CacheSizeCheckTimeoutSeconds { get; set; }
    public string? CompressionLevel { get; set; }
}