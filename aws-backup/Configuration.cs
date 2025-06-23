namespace aws_backup;

public class Configuration
{
    public bool EncryptSQS { get; set; }
    public string RepositoryName { get; set; }
    public string PassphrasePath { get; set; }
    public string IgnoreFile { get; set; }
    public int ReadConcurrency { get; set; }
    public int UploadS3Concurrency { get; set; }
    public int RestoreS3Concurrency { get; set; }
    public long ChunkSizeBytes { get; set; }
    public int ReadBufferSize { get; set; }
    public long FileRetryDelay { get; set; }
    public string RetryStrategy { get; set; }
    public string CacheFolder { get; set; }
    public string ArchiveNameTemplate { get; set; } //date
    public string AwsTrustRoleArn { get; set; }
    public string AwsTrustProfileArn { get; set; }
    public string AwsTrustRoleAnchor { get; set; }
    public string AwsTrustCertificate { get; set; }
    public string AwsTrustPrivateKey { get; set; }
    public string ColdStorageType { get; set; }
    public string HotStorageType { get; set; }
    public string AwsS3SecurityConfig { get; set; }
    public string SnsTopicArn { get; set; }
    public string AlertEvents { get; set; }
    public string LogVerbosity { get; set; }
    public string LogFilePath { get; set; }
    public string LogFileMaxSize { get; set; } // in MB
    public string CronSchedule { get; set; }
    public double ShutdownTimeoutSeconds { get; set; }
    public string PathsToArchive { get; set; }
    public bool KeepTimeStamps { get; set; }
    public bool KeepOwnerGroup { get; set; }
    public bool KeepAcl { get; set; }
    public string S3BucketName { get; set; }
    public string S3RepoPrefix { get; set; }
    public long S3PartSize { get; set; }
    public int RetryLimit { get; set; }
    public int MsDelayBetweenArchiveSaves { get; set; }
    public int MsDelayBetweenManifestSaves { get; set; }
    public string QueueUrl { get; set; }
    public int SqsWaitTimeSeconds { get; set; }
    public int SqsMaxNumberOfMessages { get; set; }
    public int SqsVisibilityTimeout { get; set; }
    public int RestoreConcurrency { get; set; }
    public long SqsRetryDelaySeconds { get; set; }
    public int StorageClassCheckDelay { get; set; }
    public int MaxDownloadConcurrency { get; set; }
}