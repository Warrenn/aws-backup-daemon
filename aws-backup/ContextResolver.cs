using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using Microsoft.Extensions.Options;

namespace aws_backup;

public interface IContextResolver
{
    string ChunkS3Key(
        string localFilePath,
        int chunkIndex,
        long chunkSize,
        byte[] hashKey,
        long size
    );

    string RestoreId(
        string archiveRunId,
        string restorePaths,
        DateTimeOffset requestedAt
    );

    string LocalRestoreFolder(string requestRestoreId);
    string ArchiveRunId(DateTimeOffset utcNow);
    DateTimeOffset NextRetryTime(int attemptCount);

    // s3
    string S3BucketId();
    S3StorageClass ColdStorage();
    S3StorageClass HotStorage();
    ServerSideEncryptionMethod ServerSideEncryption();
    int ReadBufferSize();
    long ChunkSizeBytes();
    long S3PartSize();
    Task<byte[]> AesFileEncryptionKey(CancellationToken cancellationToken);
    Task<byte[]> SqsEncryptionKey(CancellationToken cancellationToken);
    bool KeepTimeStamps();
    bool KeepOwnerGroup();
    bool KeepAclEntries();
    bool CheckDownloadHash();
    string LocalCacheFolder();
    string? LocalIgnoreFile();
    int NoOfConcurrentDownloadsPerFile();
    int NoOfS3FilesToDownloadConcurrently();
    int NoOfS3FilesToUploadConcurrently();
    int ShutdownTimeoutSeconds();
    int RetryCheckIntervalMs();
    int StorageCheckDelaySeconds();
    int DelayBetweenUploadsSeconds();
    int DownloadAttemptLimit();
    int UploadAttemptLimit();
    string SqsQueueUrl();
    int? SqsWaitTimeSeconds();
    int? SqsMaxNumberOfMessages();
    int? SqsVisibilityTimeout();
    long SqsRetryDelaySeconds();
    bool EncryptSqs();
    int GeneralRetryLimit();
    bool UseS3Accelerate();
    RegionEndpoint GetAwsRegion();
    RequestRetryMode GetAwsRetryMode();
    string RolesAnyWhereProfileArn();
    string RolesAnyWhereRoleArn();
    string RolesAnyWhereTrustAnchorArn();
    string RolesAnyWhereCertificateFileName();
    string RolesAnyWherePrivateKeyFileName();
    public void SetSsmClientFactory(Func<CancellationToken, Task<IAmazonSimpleSystemsManagement>> ssmFactory);
    string CurrentRestoreBucketKey();
    string CurrentArchiveRunsBucketKey();
    string ChunkManifestBucketKey();
    string RestoreManifestBucketKey();
    bool NotifyOnArchiveComplete();
    bool NotifyOnArchiveCompleteErrors();
    bool NotifyOnRestoreComplete();
    bool NotifyOnRestoreCompleteErrors();
    bool NotifyOnException();
    string ArchiveCompleteSnsArn();
    string RestoreCompleteSnsArn();
    string ArchiveCompleteErrorSnsArn();
    string RestoreCompleteErrorSnsArn();
    string ExceptionSnsArn();
}

public class ContextResolver : IContextResolver
{
    private readonly string _clientId;
    private readonly IOptionsMonitor<Configuration> _configOptions;
    private readonly GlobalConfiguration _globalConfigOptions;

    // Cached values
    private RegionEndpoint? _awsRegion;
    private RequestRetryMode? _awsRetryMode;
    private bool? _checkDownloadHash;
    private S3StorageClass? _coldStorageClass;
    private int? _delayBetweenUploadsSeconds;
    private int? _downloadAttemptLimit;
    private bool? _encryptFiles;
    private bool? _encryptSqs;
    private byte[]? _fileEncryptionKey;
    private int? _generalRetryLimit;
    private S3StorageClass? _hotStorageClass;
    private bool? _keepAclEntries;
    private bool? _keepOwnerGroup;
    private bool? _keepTimeStamps;
    private string? _localCacheFolder;
    private string? _localRestoreFolder;
    private int? _noOfConcurrentDownloadsPerFile;
    private int? _noOfS3FilesToDownloadConcurrently;
    private int? _noOfS3FilesToUploadConcurrently;
    private int? _readBufferSize;
    private int? _retryCheckIntervalMs;
    private long? _s3PartSize;
    private ServerSideEncryptionMethod? _serverSideEncryptionMethod;
    private int? _shutdownTimeoutSeconds;
    private byte[]? _sqsEncryptionKey;

    private Func<CancellationToken, Task<IAmazonSimpleSystemsManagement>>
        _ssmClientFactory = _ => Task.FromResult<IAmazonSimpleSystemsManagement>(null!);

    private int? _storageCheckDelaySeconds;
    private int? _uploadAttemptLimit;
    private bool? _useS3Accelerate;

    public ContextResolver(
        IOptionsMonitor<Configuration> configOptions,
        GlobalConfiguration globalConfigOptions)
    {
        _configOptions = configOptions;
        _globalConfigOptions = globalConfigOptions;
        _configOptions.OnChange((_, _) =>
        {
            // Clear cached values on configuration change
            ClearCache();
        });
        _clientId = ScrubClientId(_configOptions.CurrentValue.ClientId);
    }

    public string S3BucketId()
    {
        return _globalConfigOptions.BucketId;
    }

    public S3StorageClass ColdStorage()
    {
        if (_coldStorageClass is not null) return _coldStorageClass;
        _coldStorageClass = ResolveStorageClass(_configOptions.CurrentValue.ColdStorage, S3StorageClass.DeepArchive);
        return _coldStorageClass;
    }

    public S3StorageClass HotStorage()
    {
        if (_hotStorageClass is not null) return _hotStorageClass;
        _hotStorageClass = ResolveStorageClass(_configOptions.CurrentValue.HotStorage, S3StorageClass.Standard);
        return _hotStorageClass;
    }

    public ServerSideEncryptionMethod ServerSideEncryption()
    {
        if (_serverSideEncryptionMethod is not null) return _serverSideEncryptionMethod;
        var encryptionMethod = _configOptions.CurrentValue.ServerSideEncryption;
        _serverSideEncryptionMethod = string.IsNullOrWhiteSpace(encryptionMethod)
            ? ServerSideEncryptionMethod.AES256
            : ServerSideEncryptionMethod.FindValue(encryptionMethod);
        return _serverSideEncryptionMethod;
    }

    public RegionEndpoint GetAwsRegion()
    {
        if (_awsRegion is not null) return _awsRegion;
        var configRegion = _configOptions.CurrentValue.AwsRegion;
        _awsRegion = string.IsNullOrWhiteSpace(configRegion)
            ? string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("AWS_DEFAULT_REGION"))
                ? string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("AWS_REGION"))
                    ? RegionEndpoint.USEast1
                    : RegionEndpoint.GetBySystemName(Environment.GetEnvironmentVariable("AWS_REGION")!)
                : RegionEndpoint.GetBySystemName(Environment.GetEnvironmentVariable("AWS_DEFAULT_REGION")!)
            : RegionEndpoint.GetBySystemName(configRegion);
        return _awsRegion;
    }

    public RequestRetryMode GetAwsRetryMode()
    {
        if (_awsRetryMode is not null) return _awsRetryMode.Value;
        var retryMode = _configOptions.CurrentValue.AwsRetryMode;
        _awsRetryMode = string.IsNullOrWhiteSpace(retryMode)
            ? RequestRetryMode.Adaptive
            : Enum.TryParse<RequestRetryMode>(retryMode, true, out var mode)
                ? mode
                : RequestRetryMode.Adaptive;
        return _awsRetryMode.Value;
    }

    public string LocalCacheFolder()
    {
        if (!string.IsNullOrWhiteSpace(_localCacheFolder) && Directory.Exists(_localCacheFolder))
            return _localCacheFolder;

        var configFolder = _configOptions.CurrentValue.LocalCacheFolder;
        if (string.IsNullOrWhiteSpace(configFolder) || !Directory.Exists(configFolder))
            _localCacheFolder = Path.GetTempPath();
        else
            _localCacheFolder = configFolder;

        return _localCacheFolder;
    }

    public string? LocalIgnoreFile()
    {
        var ignoreFile = _configOptions.CurrentValue.LocalIgnoreFile;
        return string.IsNullOrWhiteSpace(ignoreFile) ? null : ignoreFile;
    }

    public string LocalRestoreFolder(string requestRestoreId)
    {
        if (_localRestoreFolder is not null)
            return Path.Combine(_localRestoreFolder, requestRestoreId);

        var configFolder = _configOptions.CurrentValue.LocalRestoreFolderBase;
        _localRestoreFolder = string.IsNullOrWhiteSpace(configFolder)
            ? Path.Combine(Path.GetTempPath(), "restores")
            : configFolder;

        return Path.Combine(_localRestoreFolder, requestRestoreId);
    }

    public string SqsQueueUrl()
    {
        return $"{_globalConfigOptions.SqsQueueBaseName}/{_clientId}";
    }

    // Roles Anywhere methods
    public string RolesAnyWhereProfileArn()
    {
        return _configOptions.CurrentValue.RolesAnyWhereProfileArn ?? "";
    }

    public string RolesAnyWhereRoleArn()
    {
        return _configOptions.CurrentValue.RolesAnyWhereRoleArn ?? "";
    }

    public string RolesAnyWhereTrustAnchorArn()
    {
        return _configOptions.CurrentValue.RolesAnyWhereTrustAnchorArn ?? "";
    }

    public string RolesAnyWhereCertificateFileName()
    {
        return _configOptions.CurrentValue.RolesAnyWhereCertificateFileName ?? "";
    }

    public string RolesAnyWherePrivateKeyFileName()
    {
        return _configOptions.CurrentValue.RolesAnyWherePrivateKeyFileName ?? "";
    }

    // Integer configuration methods with defaults
    public int ReadBufferSize()
    {
        return _readBufferSize ??= _configOptions.CurrentValue.ReadBufferSize ?? 65536;
        // 64KB default
    }

    public int NoOfConcurrentDownloadsPerFile()
    {
        return _noOfConcurrentDownloadsPerFile ??= _configOptions.CurrentValue.NoOfConcurrentDownloadsPerFile ?? 4;
    }

    public int NoOfS3FilesToDownloadConcurrently()
    {
        return _noOfS3FilesToDownloadConcurrently ??=
            _configOptions.CurrentValue.NoOfS3FilesToDownloadConcurrently ?? 10;
    }

    public int NoOfS3FilesToUploadConcurrently()
    {
        return _noOfS3FilesToUploadConcurrently ??= _configOptions.CurrentValue.NoOfS3FilesToUploadConcurrently ?? 10;
    }

    public int ShutdownTimeoutSeconds()
    {
        return _shutdownTimeoutSeconds ??= _configOptions.CurrentValue.ShutdownTimeoutSeconds ?? 30;
    }

    public int RetryCheckIntervalMs()
    {
        return _retryCheckIntervalMs ??= _configOptions.CurrentValue.RetryCheckIntervalMs ?? 5000;
    }

    public int StorageCheckDelaySeconds()
    {
        return _storageCheckDelaySeconds ??= _configOptions.CurrentValue.StorageCheckDelaySeconds ?? 300;
    }

    public int DelayBetweenUploadsSeconds()
    {
        return _delayBetweenUploadsSeconds ??= _configOptions.CurrentValue.DelayBetweenUploadsSeconds ?? 1;
    }

    public int DownloadAttemptLimit()
    {
        return _downloadAttemptLimit ??= _configOptions.CurrentValue.DownloadAttemptLimit ?? 3;
    }

    public int UploadAttemptLimit()
    {
        return _uploadAttemptLimit ??= _configOptions.CurrentValue.UploadAttemptLimit ?? 3;
    }

    public int GeneralRetryLimit()
    {
        return _generalRetryLimit ??= _configOptions.CurrentValue.GeneralRetryLimit ?? 3;
    }

    public int? SqsWaitTimeSeconds()
    {
        return _configOptions.CurrentValue.SqsWaitTimeSeconds ?? 20;
    }

    public int? SqsMaxNumberOfMessages()
    {
        return _configOptions.CurrentValue.SqsMaxNumberOfMessages ?? 10;
    }

    public int? SqsVisibilityTimeout()
    {
        return _configOptions.CurrentValue.SqsVisibilityTimeout ?? 300;
    }

    // Long configuration methods with defaults
    public long ChunkSizeBytes()
    {
        return _globalConfigOptions.ChunkSizeBytes <= 0 ? 524288000L : _globalConfigOptions.ChunkSizeBytes;
        // 500MB default
    }

    public long S3PartSize()
    {
        return _s3PartSize ??= _configOptions.CurrentValue.S3PartSize ?? 5242880L;
        // 5MB default
    }

    public long SqsRetryDelaySeconds()
    {
        return _configOptions.CurrentValue.SqsRetryDelaySeconds ?? 60L;
    }

    // Boolean configuration methods with defaults
    public bool KeepTimeStamps()
    {
        return _keepTimeStamps ??= _configOptions.CurrentValue.KeepTimeStamps ?? false;
    }

    public bool KeepOwnerGroup()
    {
        return _keepOwnerGroup ??= _configOptions.CurrentValue.KeepOwnerGroup ?? false;
    }

    public bool KeepAclEntries()
    {
        return _keepAclEntries ??= _configOptions.CurrentValue.KeepAclEntries ?? false;
    }

    public bool CheckDownloadHash()
    {
        return _checkDownloadHash ??= _configOptions.CurrentValue.CheckDownloadHash ?? true;
    }

    public bool EncryptSqs()
    {
        return _encryptSqs ??= _configOptions.CurrentValue.EncryptSqs ?? true;
    }

    public bool UseS3Accelerate()
    {
        return _useS3Accelerate ??= _configOptions.CurrentValue.UseS3Accelerate ?? false;
    }

    // Key generation methods
    public async Task<byte[]> AesFileEncryptionKey(CancellationToken cancellationToken)
    {
        var encryptFiles = _encryptFiles ??= _configOptions.CurrentValue.EncryptFiles ?? true;
        if (!encryptFiles) return [];

        if (_fileEncryptionKey is not null && _fileEncryptionKey.Length > 0)
            return _fileEncryptionKey;

        // Implementation would retrieve or generate encryption key
        // This could involve calling SSM Parameter Store or generating a key
        using var ssmClient = await _ssmClientFactory(cancellationToken);
        // Retrieve from SSM Parameter Store
        // Implementation depends on your key management strategy
        // Example:
        var response = await ssmClient.GetParameterAsync(new GetParameterRequest
        {
            Name = $"{_globalConfigOptions.KmsBasePath}{_clientId}/aes-file-encryption-key",
            WithDecryption = true
        }, cancellationToken);

        _fileEncryptionKey = Convert.FromBase64String(response.Parameter.Value);

        return _fileEncryptionKey;
    }

    public async Task<byte[]> SqsEncryptionKey(CancellationToken cancellationToken)
    {
        if (EncryptSqs() && _sqsEncryptionKey is not null && _sqsEncryptionKey.Length > 0)
            return _sqsEncryptionKey;

        using var ssmClient = await _ssmClientFactory(cancellationToken);
        var response = await ssmClient.GetParameterAsync(new GetParameterRequest
        {
            Name = $"{_globalConfigOptions.KmsBasePath}{_clientId}/aes-sqs-encryption-key",
            WithDecryption = true
        }, cancellationToken);

        _sqsEncryptionKey = Convert.FromBase64String(response.Parameter.Value);

        return _sqsEncryptionKey;
    }

    // ID generation methods
    public string ChunkS3Key(string localFilePath, int chunkIndex, long chunkSize, byte[] hashKey, long size)
    {
        var hash = Base64Url.Encode(hashKey); // Use first 8 chars of hash
        return $"{_clientId}/data/{hash}";
    }

    public string RestoreId(string archiveRunId, string restorePaths, DateTimeOffset requestedAt)
    {
        var pathsHash = ComputeSimpleHash(restorePaths);
        var timestamp = requestedAt.ToString("yyyy-MM-dd-HH-mm-ss");
        return $"restore_{archiveRunId}_{pathsHash}_{timestamp}";
    }

    public string ArchiveRunId(DateTimeOffset utcNow)
    {
        var timestamp = utcNow.ToString("yyyy-MM-dd-HH-mm-ss");
        return $"{timestamp}";
    }

    public DateTimeOffset NextRetryTime(int attemptCount)
    {
        // Exponential backoff: 2^attemptCount seconds, max 300 seconds (5 minutes)
        var delaySeconds = Math.Min(Math.Pow(2, attemptCount), 300);
        return DateTimeOffset.UtcNow.AddSeconds(delaySeconds);
    }

    public void SetSsmClientFactory(Func<CancellationToken, Task<IAmazonSimpleSystemsManagement>> ssmFactory)
    {
        _ssmClientFactory = ssmFactory;
    }

    public string CurrentRestoreBucketKey()
    {
        return $"{_clientId}/restores.json.gz";
    }

    public string CurrentArchiveRunsBucketKey()
    {
        return $"{_clientId}/archive-runs.json.gz";
    }

    public string ChunkManifestBucketKey()
    {
        return $"{_clientId}/chunk-manifest.json.gz";
    }

    public string RestoreManifestBucketKey()
    {
        return $"{_clientId}/restore-manifest.json.gz";
    }

    public bool NotifyOnArchiveComplete()
    {
        return _configOptions.CurrentValue.NotifyOnArchiveComplete ?? false;
    }

    public bool NotifyOnArchiveCompleteErrors()
    {
        return _configOptions.CurrentValue.NotifyOnArchiveCompleteErrors ?? false;
    }

    public bool NotifyOnRestoreComplete()
    {
        return _configOptions.CurrentValue.NotifyOnRestoreComplete ?? false;
    }

    public bool NotifyOnRestoreCompleteErrors()
    {
        return _configOptions.CurrentValue.NotifyOnRestoreCompleteErrors ?? false;
    }

    public bool NotifyOnException()
    {
        return _configOptions.CurrentValue.NotifyOnException ?? false;
    }

    public string ArchiveCompleteSnsArn()
    {
        return $"{_globalConfigOptions.SnsBaseArn}{_clientId}-archive-complete";
    }

    public string RestoreCompleteSnsArn()
    {
        return $"{_globalConfigOptions.SnsBaseArn}{_clientId}-restore-complete";
    }

    public string ArchiveCompleteErrorSnsArn()
    {
        return $"{_globalConfigOptions.SnsBaseArn}{_clientId}-archive-complete-errors";
    }

    public string RestoreCompleteErrorSnsArn()
    {
        return $"{_globalConfigOptions.SnsBaseArn}{_clientId}-restore-complete-errors";
    }

    public string ExceptionSnsArn()
    {
        return $"{_globalConfigOptions.SnsBaseArn}{_clientId}-exception";
    }

    private void ClearCache()
    {
        _awsRegion = null;
        _coldStorageClass = null;
        _hotStorageClass = null;
        _serverSideEncryptionMethod = null;
        _awsRetryMode = null;
        _localCacheFolder = null;
        _localRestoreFolder = null;

        // Clear primitive caches
        _readBufferSize = null;
        _s3PartSize = null;
        _noOfConcurrentDownloadsPerFile = null;
        _noOfS3FilesToDownloadConcurrently = null;
        _noOfS3FilesToUploadConcurrently = null;
        _shutdownTimeoutSeconds = null;
        _retryCheckIntervalMs = null;
        _storageCheckDelaySeconds = null;
        _delayBetweenUploadsSeconds = null;
        _downloadAttemptLimit = null;
        _uploadAttemptLimit = null;
        _generalRetryLimit = null;
        _keepTimeStamps = null;
        _keepOwnerGroup = null;
        _keepAclEntries = null;
        _checkDownloadHash = null;
        _encryptSqs = null;
        _useS3Accelerate = null;
        _encryptFiles = null;
    }

    private static S3StorageClass ResolveStorageClass(string? storageClassName, S3StorageClass defaultStorageClass)
    {
        if (string.IsNullOrWhiteSpace(storageClassName))
            return defaultStorageClass;

        var storageClass = S3StorageClass.FindValue(storageClassName);
        return storageClass ?? defaultStorageClass;
    }

    private static string ScrubClientId(string clientId)
    {
        return RegexHelper
            .NonAlphanumericRegex()
            .Replace(clientId, "")
            .ToLowerInvariant();
    }

    private static string ComputeSimpleHash(string input)
    {
        // Simple hash for generating IDs - use a proper hash function in production
        return Math.Abs(input.GetHashCode()).ToString("X8");
    }
}