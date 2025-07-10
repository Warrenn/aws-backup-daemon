using System.Text.Json;
using System.Text.Json.Nodes;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace aws_backup;

public interface IContextResolver
{
    string ChunkS3Key(byte[] hashKey);

    string RestoreId(
        string archiveRunId,
        string restorePaths,
        DateTimeOffset requestedAt
    );

    string LocalRestoreFolder(string requestRestoreId);
    string ArchiveRunId(DateTimeOffset utcNow);
    DateTimeOffset NextRetryTime(int attemptCount);

    // s3
    S3StorageClass ColdStorage();
    S3StorageClass HotStorage();
    S3StorageClass LowCostStorage();
    ServerSideEncryptionMethod ServerSideEncryption();
    int ReadBufferSize();
    long S3PartSize();
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
    int? SqsWaitTimeSeconds();
    int? SqsMaxNumberOfMessages();
    int? SqsVisibilityTimeout();
    long SqsRetryDelaySeconds();
    bool EncryptSqs();
    int GeneralRetryLimit();
    bool UseS3Accelerate();
    RequestRetryMode GetAwsRetryMode();
    RegionEndpoint GetAwsRegion();
    string RolesAnyWhereProfileArn();
    string RolesAnyWhereRoleArn();
    string RolesAnyWhereTrustAnchorArn();
    string RolesAnyWhereCertificateFileName();
    string RolesAnyWherePrivateKeyFileName();
    string CurrentRestoreBucketKey();
    string CurrentArchiveRunsBucketKey();
    string ChunkManifestBucketKey();
    string RestoreManifestBucketKey();
    bool NotifyOnArchiveComplete();
    bool NotifyOnArchiveCompleteErrors();
    bool NotifyOnRestoreComplete();
    bool NotifyOnRestoreCompleteErrors();
    bool NotifyOnException();
    string RunIdBucketKey(string runId);
    string RestoreIdBucketKey(string restoreId);
    int DaysToKeepRestoredCopy();
    string S3DataPrefix();
    string S3LogFolder();
    int StoragePageDelayMilliseconds();
    string PathsToArchive();
    string ClientId();
    string RollingLogFolder();
    Task UpdateConfiguration(Configuration configOptions, CancellationToken cancellationToken);
    string CronSchedule();
    int AwsCredentialsTimeoutSeconds();
}

public sealed class ContextResolver : IContextResolver
{
    private readonly string _appSettingsPath;
    private readonly string _clientId;
    private readonly ILogger<ContextResolver> _logger;
    private readonly ISignalHub<string> _signalHub;
    private RegionEndpoint? _awsRegion;

    // Cached values
    private RequestRetryMode? _awsRetryMode;
    private S3StorageClass? _coldStorageClass;
    private Configuration _configOptions;
    private S3StorageClass? _hotStorageClass;
    private string? _ignoreFile;
    private string? _localCacheFolder;
    private string? _localRestoreFolder;
    private S3StorageClass? _lowCostStorage;
    private ServerSideEncryptionMethod? _serverSideEncryptionMethod;

    public ContextResolver(
        string appSettingsPath,
        IOptionsMonitor<Configuration> configOptions,
        ISignalHub<string> signalHub,
        ILogger<ContextResolver> logger)
    {
        _appSettingsPath = appSettingsPath;
        _logger = logger;
        _configOptions = configOptions.CurrentValue;
        configOptions.OnChange((newConfig, _) =>
        {
            logger.LogInformation("Configuration changed, updating ContextResolver.");
            logger.LogInformation("new config {config}", newConfig);
            _configOptions = newConfig;
            ResetCache();
            logger.LogInformation("Configuration updated in ContextResolver.");
            signalHub.Signal(_configOptions.CronSchedule);
        });
        _signalHub = signalHub;
        _clientId = ScrubClientId(_configOptions.ClientId);
    }

    public S3StorageClass ColdStorage()
    {
        return _coldStorageClass ??=
            ResolveStorageClass(_configOptions.ColdStorage, S3StorageClass.DeepArchive);
    }

    public S3StorageClass HotStorage()
    {
        return _hotStorageClass ??=
            ResolveStorageClass(_configOptions.HotStorage, S3StorageClass.Standard);
    }

    public S3StorageClass LowCostStorage()
    {
        return _lowCostStorage ??= ResolveStorageClass(
            _configOptions.LowCostStorage,
            S3StorageClass.IntelligentTiering);
    }

    public ServerSideEncryptionMethod ServerSideEncryption()
    {
        if (_serverSideEncryptionMethod is not null) return _serverSideEncryptionMethod;
        var encryptionMethod = _configOptions.ServerSideEncryption;
        _serverSideEncryptionMethod = string.IsNullOrWhiteSpace(encryptionMethod)
            ? ServerSideEncryptionMethod.AES256
            : ServerSideEncryptionMethod.FindValue(encryptionMethod);
        return _serverSideEncryptionMethod;
    }

    public RegionEndpoint GetAwsRegion()
    {
        if (_awsRegion is not null) return _awsRegion;
        var configRegion = _configOptions.AwsRegion;
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
        var retryMode = _configOptions.AwsRetryMode;
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

        _localCacheFolder = _configOptions.LocalCacheFolder;
        if (string.IsNullOrWhiteSpace(_localCacheFolder) || !IsValidPath(_localCacheFolder))
            _localCacheFolder = Path.Combine(Path.GetTempPath(), "cache");

        if (!Path.IsPathRooted(_localCacheFolder))
            _localCacheFolder = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, _localCacheFolder));

        Directory.CreateDirectory(_localCacheFolder);

        return _localCacheFolder;
    }

    public string? LocalIgnoreFile()
    {
        if (!string.IsNullOrWhiteSpace(_ignoreFile) && File.Exists(_ignoreFile))
            return _ignoreFile;
        _ignoreFile = _configOptions.LocalIgnoreFile;
        return string.IsNullOrWhiteSpace(_ignoreFile) ? null : _ignoreFile;
    }

    public string LocalRestoreFolder(string requestRestoreId)
    {
        if (string.IsNullOrWhiteSpace(_localRestoreFolder) || !Directory.Exists(_localRestoreFolder))
            _localRestoreFolder = _configOptions.LocalRestoreFolderBase;

        if (string.IsNullOrWhiteSpace(_localRestoreFolder) || !IsValidPath(_localRestoreFolder))
            _localRestoreFolder = Path.Combine(AppContext.BaseDirectory, "restores");

        if (!Path.IsPathRooted(_localRestoreFolder))
            _localRestoreFolder = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, _localRestoreFolder));

        if (!Directory.Exists(_localRestoreFolder))
            Directory.CreateDirectory(_localRestoreFolder);

        if (string.IsNullOrWhiteSpace(requestRestoreId))
            requestRestoreId = DateTime.UtcNow.ToString("yyyyMMddHHmmss");

        var returnValue = Path.Combine(_localRestoreFolder, requestRestoreId);

        if (!Directory.Exists(returnValue))
            Directory.CreateDirectory(returnValue);

        return returnValue;
    }

    // Roles Anywhere methods
    public string RolesAnyWhereProfileArn()
    {
        return _configOptions.RolesAnyWhereProfileArn ?? "";
    }

    public string RolesAnyWhereRoleArn()
    {
        return _configOptions.RolesAnyWhereRoleArn ?? "";
    }

    public string RolesAnyWhereTrustAnchorArn()
    {
        return _configOptions.RolesAnyWhereTrustAnchorArn ?? "";
    }

    public string RolesAnyWhereCertificateFileName()
    {
        return _configOptions.RolesAnyWhereCertificateFileName ?? "";
    }

    public string RolesAnyWherePrivateKeyFileName()
    {
        return _configOptions.RolesAnyWherePrivateKeyFileName ?? "";
    }

    // Integer configuration methods with defaults
    public int ReadBufferSize()
    {
        return _configOptions.ReadBufferSize ?? 65536;
        // 64KB default
    }

    public int NoOfConcurrentDownloadsPerFile()
    {
        return _configOptions.NoOfConcurrentDownloadsPerFile ?? 4;
    }

    public int NoOfS3FilesToDownloadConcurrently()
    {
        return _configOptions.NoOfS3FilesToDownloadConcurrently ?? 10;
    }

    public int NoOfS3FilesToUploadConcurrently()
    {
        return _configOptions.NoOfS3FilesToUploadConcurrently ?? 10;
    }

    public int ShutdownTimeoutSeconds()
    {
        return _configOptions.ShutdownTimeoutSeconds ?? 30;
    }

    public int RetryCheckIntervalMs()
    {
        return _configOptions.RetryCheckIntervalMs ?? 5000;
    }

    public int StorageCheckDelaySeconds()
    {
        return _configOptions.StorageCheckDelaySeconds ?? 300;
    }

    public int DelayBetweenUploadsSeconds()
    {
        return _configOptions.DelayBetweenUploadsSeconds ?? 60;
    }

    public int DownloadAttemptLimit()
    {
        return _configOptions.DownloadAttemptLimit ?? 3;
    }

    public int UploadAttemptLimit()
    {
        return _configOptions.UploadAttemptLimit ?? 3;
    }

    public int GeneralRetryLimit()
    {
        return _configOptions.GeneralRetryLimit ?? 3;
    }

    public int? SqsWaitTimeSeconds()
    {
        return _configOptions.SqsWaitTimeSeconds ?? 20;
    }

    public int? SqsMaxNumberOfMessages()
    {
        return _configOptions.SqsMaxNumberOfMessages ?? 10;
    }

    public int? SqsVisibilityTimeout()
    {
        return _configOptions.SqsVisibilityTimeout ?? 300;
    }

    public long S3PartSize()
    {
        return _configOptions.S3PartSize ?? 5242880L;
        // 5MB default
    }

    public long SqsRetryDelaySeconds()
    {
        return _configOptions.SqsRetryDelaySeconds ?? 60L;
    }

    // Boolean configuration methods with defaults
    public bool KeepTimeStamps()
    {
        return _configOptions.KeepTimeStamps ?? false;
    }

    public bool KeepOwnerGroup()
    {
        return _configOptions.KeepOwnerGroup ?? false;
    }

    public bool KeepAclEntries()
    {
        return _configOptions.KeepAclEntries ?? false;
    }

    public bool CheckDownloadHash()
    {
        return _configOptions.CheckDownloadHash ?? true;
    }

    public bool EncryptSqs()
    {
        return _configOptions.EncryptSqs ?? true;
    }

    public bool UseS3Accelerate()
    {
        return _configOptions.UseS3Accelerate ?? false;
    }

    // ID generation methods
    public string ChunkS3Key(byte[] hashKey)
    {
        var hash = Base64Url.Encode(hashKey); // Use first 8 chars of hash
        var prefix = S3DataPrefix();
        return $"{prefix}/{hash}";
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

    public string CurrentRestoreBucketKey()
    {
        return $"{_clientId}/restores.json.tar.gz";
    }

    public string CurrentArchiveRunsBucketKey()
    {
        return $"{_clientId}/archive-requests.json.tar.gz";
    }

    public string ChunkManifestBucketKey()
    {
        return $"{_clientId}/chunk-manifest.json.tar.gz";
    }

    public string RestoreManifestBucketKey()
    {
        return $"{_clientId}/restore-manifest.json.tar.gz";
    }

    public bool NotifyOnArchiveComplete()
    {
        return _configOptions.NotifyOnArchiveComplete ?? false;
    }

    public bool NotifyOnArchiveCompleteErrors()
    {
        return _configOptions.NotifyOnArchiveCompleteErrors ?? false;
    }

    public bool NotifyOnRestoreComplete()
    {
        return _configOptions.NotifyOnRestoreComplete ?? false;
    }

    public bool NotifyOnRestoreCompleteErrors()
    {
        return _configOptions.NotifyOnRestoreCompleteErrors ?? false;
    }

    public bool NotifyOnException()
    {
        return _configOptions.NotifyOnException ?? false;
    }

    public string RunIdBucketKey(string runId)
    {
        return $"{_clientId}/archive-runs/{runId}.json.gz";
    }

    public string RestoreIdBucketKey(string restoreId)
    {
        return $"{_clientId}/restore-runs/{restoreId}.json.gz";
    }

    public int DaysToKeepRestoredCopy()
    {
        return _configOptions.DaysToKeepRestoredCopy ?? 7;
    }

    public string S3DataPrefix()
    {
        return $"{_clientId}/data";
    }

    public string S3LogFolder()
    {
        return $"{_clientId}/logs";
    }

    public int StoragePageDelayMilliseconds()
    {
        return _configOptions.StoragePageDelayMilliseconds ?? 5;
    }

    public string PathsToArchive()
    {
        return _configOptions.PathsToArchive;
    }

    public string ClientId()
    {
        return _clientId;
    }

    public string RollingLogFolder()
    {
        return _configOptions.RollingLogFolder ?? "";
    }

    public async Task UpdateConfiguration(Configuration configOptions, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Updating configuration in ContextResolver from UpdateConfiguration.");
        if (!IsValidPath(_appSettingsPath)) return;
        var configString = await File.ReadAllTextAsync(_appSettingsPath, cancellationToken);
        var root = JsonNode.Parse(configString);
        if (root is null)
        {
            _logger.LogError("the existing configuration file {appSettingsPath} is not valid JSON, skipping update",
                _appSettingsPath);
            return;
        }

        root["Configuration"] =
            JsonSerializer.SerializeToNode(configOptions, SourceGenerationContext.Default.Configuration);

        _logger.LogInformation("Writing updated configuration to {appSettingsPath}", _appSettingsPath);
        await File.WriteAllTextAsync(_appSettingsPath, root.ToJsonString(SourceGenerationContext.Default.Options),
            cancellationToken);
    }

    public string CronSchedule()
    {
        return _configOptions.CronSchedule;
    }

    public int AwsCredentialsTimeoutSeconds()
    {
        return _configOptions.AwsCredentialsTimeoutSeconds ?? 3600;
    }

    private void ResetCache()
    {
        _awsRegion = null; // Reset cached region
        _awsRetryMode = null; // Reset cached retry mode
        _coldStorageClass = null; // Reset cached storage classes)
        _hotStorageClass = null;
        _lowCostStorage = null;
        _serverSideEncryptionMethod = null; // Reset encryption method
        _localCacheFolder = null; // Reset local cache folder
        _ignoreFile = null; // Reset ignore file
        _localRestoreFolder = null; // Reset restore folder
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

    private static bool IsValidPath(string path)
    {
        try
        {
            Path.GetFullPath(path);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }
}