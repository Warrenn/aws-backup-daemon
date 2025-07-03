using System.ComponentModel.DataAnnotations;

namespace aws_backup;

//settings for everyone
public sealed record GlobalConfiguration(
    long ChunkSizeBytes,
    string KmsBasePath,
    string BucketId,
    string SqsQueueBaseName,
    string SnsBaseArn);

//settings that can change
public sealed record Configuration(
    [property: Required] string ClientId,
    [property: Required] string CronSchedule,
    [property: Required] string PathsToArchive,
    string? ColdStorage = null,
    string? HotStorage = null,
    string? AwsRegion = null,
    string? ServerSideEncryption = null,
    string? LocalCacheFolder = null,
    string? LocalIgnoreFile = null,
    string? LocalRestoreFolderBase = null,
    string? AwsRetryMode = null,
    string? RolesAnyWhereProfileArn = null,
    string? RolesAnyWhereRoleArn = null,
    string? RolesAnyWhereTrustAnchorArn = null,
    string? RolesAnyWhereCertificateFileName = null,
    string? RolesAnyWherePrivateKeyFileName = null,
    int? ReadBufferSize = null,
    long? S3PartSize = null,
    int? NoOfConcurrentDownloadsPerFile = null,
    int? NoOfS3FilesToDownloadConcurrently = null,
    int? NoOfS3FilesToUploadConcurrently = null,
    int? ShutdownTimeoutSeconds = null,
    int? RetryCheckIntervalMs = null,
    int? StorageCheckDelaySeconds = null,
    int? DelayBetweenUploadsSeconds = null,
    int? DownloadAttemptLimit = null,
    int? UploadAttemptLimit = null,
    int? SqsWaitTimeSeconds = null,
    int? SqsMaxNumberOfMessages = null,
    int? SqsVisibilityTimeout = null,
    long? SqsRetryDelaySeconds = null,
    int? GeneralRetryLimit = null,
    bool? KeepTimeStamps = null,
    bool? KeepOwnerGroup = null,
    bool? KeepAclEntries = null,
    bool? CheckDownloadHash = null,
    bool? EncryptSqs = null,
    bool? UseS3Accelerate = null,
    bool? EncryptFiles = null,
    bool? NotifyOnArchiveComplete = null,
    bool? NotifyOnArchiveCompleteErrors = null,
    bool? NotifyOnRestoreComplete = null,
    bool? NotifyOnRestoreCompleteErrors = null,
    bool? NotifyOnException = null,
    int? DaysToKeepRestoredCopy = null
);