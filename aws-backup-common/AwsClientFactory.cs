using System.Globalization;
using Amazon.DynamoDBv2;
using Amazon.IdentityManagement;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleSystemsManagement;
using Amazon.SQS;
using Microsoft.Extensions.Logging;

namespace aws_backup_common;

public interface IAwsClientFactory
{
    Task<IAmazonS3> CreateS3Client(CancellationToken cancellationToken);
    Task<IAmazonSimpleSystemsManagement> CreateSsmClient(CancellationToken cancellationToken = default);
    Task<IAmazonSQS> CreateSqsClient(CancellationToken cancellationToken);
    Task<IAmazonSimpleNotificationService> CreateSnsClient(CancellationToken cancellationToken);
    Task<AmazonIdentityManagementServiceClient> CreateIamClient(CancellationToken cancellationToken = default);
    Task<AmazonDynamoDBClient> CreateDynamoDbClient(CancellationToken cancellationToken = default);
    void ResetCachedCredentials();
}

public sealed class AwsClientFactory(
    IContextResolver resolver,
    ILogger<AwsClientFactory> logger,
    ITemporaryCredentialsServer temporaryCredentialsServer,
    TimeProvider timeProvider)
    : IAwsClientFactory
{
    private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);
    private AWSCredentials? _cachedCredentials;

    public async Task<IAmazonS3> CreateS3Client(CancellationToken cancellationToken)
    {
        var config = CreateS3Config();
        var credentials = await GetCredentialsAsync(cancellationToken);

        return credentials != null
            ? new AmazonS3Client(credentials, config)
            : new AmazonS3Client(config);
    }

    public async Task<IAmazonSimpleSystemsManagement> CreateSsmClient(CancellationToken cancellationToken)
    {
        var config = CreateSsmConfig();
        var credentials = await GetCredentialsAsync(cancellationToken);

        return credentials != null
            ? new AmazonSimpleSystemsManagementClient(credentials, config)
            : new AmazonSimpleSystemsManagementClient(config);
    }

    public async Task<IAmazonSQS> CreateSqsClient(CancellationToken cancellationToken)
    {
        var config = CreateSqsConfig();
        var credentials = await GetCredentialsAsync(cancellationToken);

        return credentials != null
            ? new AmazonSQSClient(credentials, config)
            : new AmazonSQSClient(config);
    }

    public async Task<IAmazonSimpleNotificationService> CreateSnsClient(CancellationToken cancellationToken)
    {
        var config = CreateSnsConfig();
        var credentials = await GetCredentialsAsync(cancellationToken);

        return credentials != null
            ? new AmazonSimpleNotificationServiceClient(credentials, config)
            : new AmazonSimpleNotificationServiceClient(config);
    }

    public async Task<AmazonIdentityManagementServiceClient> CreateIamClient(CancellationToken cancellationToken)
    {
        var config = new AmazonIdentityManagementServiceConfig
        {
            MaxErrorRetry = resolver.GeneralRetryLimit(),
            RetryMode = resolver.GetAwsRetryMode(),
            Timeout = TimeSpan.FromSeconds(resolver.ShutdownTimeoutSeconds()),
            RegionEndpoint = resolver.GetAwsRegion()
        };

        var credentials = await GetCredentialsAsync(cancellationToken);
        return credentials != null
            ? new AmazonIdentityManagementServiceClient(credentials, config)
            : new AmazonIdentityManagementServiceClient(config);
    }

    public async Task<AmazonDynamoDBClient> CreateDynamoDbClient(CancellationToken cancellationToken = default)
    {
        var config = new AmazonDynamoDBConfig
        {
            MaxErrorRetry = resolver.GeneralRetryLimit(),
            RetryMode = resolver.GetAwsRetryMode(),
            Timeout = TimeSpan.FromSeconds(resolver.ShutdownTimeoutSeconds()),
            RegionEndpoint = resolver.GetAwsRegion()
        };

        var credentials = await GetCredentialsAsync(cancellationToken);
        return credentials != null
            ? new AmazonDynamoDBClient(credentials, config)
            : new AmazonDynamoDBClient(config);
    }

    public void ResetCachedCredentials()
    {
        _cachedCredentials = null;
        logger.LogDebug("Cached AWS credentials have been reset");
    }

    private async Task<bool> ValidateCredentialsAsync(AWSCredentials? credentials, CancellationToken cancellationToken)
    {
        try
        {
            if (credentials is null) return false;

            using var stsClient = new AmazonSecurityTokenServiceClient(credentials);
            var response = await stsClient.GetCallerIdentityAsync(new GetCallerIdentityRequest(), cancellationToken);
            if (response != null && !string.IsNullOrWhiteSpace(response.UserId)) return true;

            logger.LogWarning("Invalid AWS credentials: GetCallerIdentity returned null or empty UserId");
            return false;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to validate AWS credentials");
            return false;
        }
    }

    private static DateTime GetUniversalTime(DateTime? dateTime)
    {
        if (dateTime is null) return DateTime.UtcNow;
        return dateTime.Value.Kind == DateTimeKind.Utc ? dateTime.Value : dateTime.Value.ToUniversalTime();
    }

    private async Task<AWSCredentials?> GetCredentialsAsync(CancellationToken cancellationToken)
    {
        var credentialsValid = _cachedCredentials?.Expiration is not null &&
                               timeProvider.GetUtcNow() < GetUniversalTime(_cachedCredentials.Expiration);

        if (credentialsValid) return _cachedCredentials;
        await _semaphoreSlim.WaitAsync(cancellationToken);
        try
        {
            // double-checked locking
            credentialsValid = _cachedCredentials?.Expiration is not null &&
                               timeProvider.GetUtcNow() < GetUniversalTime(_cachedCredentials.Expiration);

            if (credentialsValid) return _cachedCredentials;

            var expiresIn = resolver.AwsCredentialsTimeoutSeconds();

            var credentialSources = new List<(string Name, Func<CancellationToken, Task<AWSCredentials?>> Factory)>
            {
                ("Roles Anywhere", TryGetRolesAnyWhereCredentials),
                ("Environment Variables", _ => Task.FromResult(TryGetEnvironmentCredentials(timeProvider, expiresIn))),
                ("AWS Profile", _ => Task.FromResult(TryGetProfileCredentials(timeProvider, expiresIn))),
                ("IAM Role", c => TryGetInstanceProfileCredentialsAsync(timeProvider, expiresIn, c)),
                ("Web Identity Token", _ => Task.FromResult(TryGetWebIdentityCredentials(timeProvider, expiresIn)))
            };

            foreach (var (name, factory) in credentialSources)
                try
                {
                    logger.LogDebug("Attempting to resolve AWS credentials using {CredentialSource}", name);
                    var credentials = await factory(cancellationToken);

                    if (credentials == null) continue;

                    logger.LogInformation("Successfully resolved AWS credentials using {CredentialSource}", name);
                    var settingsExpiry = timeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(expiresIn)).DateTime;
                    var credExpiry = GetUniversalTime(credentials.Expiration);

                    credentials.Expiration =
                        DateTime.SpecifyKind(credExpiry >= settingsExpiry ? settingsExpiry : credExpiry,
                            DateTimeKind.Utc);

                    _cachedCredentials = credentials;
                    return credentials;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to resolve AWS credentials using {CredentialSource}", name);
                }

            logger.LogWarning("No valid AWS credentials found, falling back to default credential chain");
            return null; // Let AWS SDK handle default credential chain
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    private async Task<AWSCredentials?> TryGetRolesAnyWhereCredentials(CancellationToken cancellationToken)
    {
        //if all the variables needed for temporary credentials are set, use them
        var profileArn = resolver.RolesAnyWhereProfileArn();
        var roleArn = resolver.RolesAnyWhereRoleArn();
        var trustAnchorArn = resolver.RolesAnyWhereTrustAnchorArn();
        var certificateFileName = resolver.RolesAnyWhereCertificateFileName();
        var privateKeyFileName = resolver.RolesAnyWherePrivateKeyFileName();
        var credentialsTimeout = resolver.AwsCredentialsTimeoutSeconds();

        var region = resolver.GetAwsRegion();

        if (string.IsNullOrWhiteSpace(profileArn)) return null;
        if (string.IsNullOrWhiteSpace(roleArn)) return null;
        if (string.IsNullOrWhiteSpace(trustAnchorArn)) return null;
        if (!File.Exists(certificateFileName)) return null;
        if (!File.Exists(privateKeyFileName)) return null;

        var (accessKey, secretKey, sessionToken, expirationString) = await temporaryCredentialsServer.GetCredentials(
            profileArn,
            roleArn,
            trustAnchorArn,
            certificateFileName,
            privateKeyFileName,
            region.SystemName,
            credentialsTimeout,
            cancellationToken);

        if (string.IsNullOrWhiteSpace(accessKey) || string.IsNullOrWhiteSpace(secretKey))
            return null;

        AWSCredentials newCredentials = string.IsNullOrWhiteSpace(sessionToken)
            ? new BasicAWSCredentials(accessKey, secretKey)
            : new SessionAWSCredentials(accessKey, secretKey, sessionToken);
        if (!string.IsNullOrWhiteSpace(expirationString) && DateTime.TryParse(expirationString,
                CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out var expiration))
            newCredentials.Expiration = expiration;

        newCredentials.Expiration ??= DateTime.SpecifyKind(
            timeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(credentialsTimeout)).DateTime, DateTimeKind.Utc);
        return newCredentials;
    }

    private static AWSCredentials? TryGetEnvironmentCredentials(TimeProvider timeProvider,
        int credentialsTimeout = 3600)
    {
        //if all the variables needed for temporary credentials are set, use them

        var accessKey = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
        var secretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");
        var sessionToken = Environment.GetEnvironmentVariable("AWS_SESSION_TOKEN");

        if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey))
            return null;

        AWSCredentials credentials = string.IsNullOrEmpty(sessionToken)
            ? new BasicAWSCredentials(accessKey, secretKey)
            : new SessionAWSCredentials(accessKey, secretKey, sessionToken);
        credentials.Expiration ??= DateTime.SpecifyKind(
            timeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(credentialsTimeout)).DateTime, DateTimeKind.Utc);
        return credentials;
    }

    private static AWSCredentials? TryGetProfileCredentials(TimeProvider timeProvider, int credentialsTimeout = 3600)
    {
        try
        {
            var profileName = Environment.GetEnvironmentVariable("AWS_PROFILE") ?? "default";
            var chain = new CredentialProfileStoreChain();

            chain.TryGetAWSCredentials(profileName, out var credentials);
            if (credentials is null) return null;
            credentials.Expiration ??= DateTime.SpecifyKind(
                timeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(credentialsTimeout)).DateTime, DateTimeKind.Utc);
            return credentials;
        }
        catch
        {
            return null;
        }
    }

    private static async Task<AWSCredentials?> TryGetInstanceProfileCredentialsAsync(
        TimeProvider timeProvider,
        int credentialsTimeout = 3600,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var credentials = new InstanceProfileAWSCredentials();
            // Test if we can get credentials
            await credentials.GetCredentialsAsync();
            credentials.Expiration ??= DateTime.SpecifyKind(
                timeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(credentialsTimeout)).DateTime, DateTimeKind.Utc);
            return credentials;
        }
        catch
        {
            return null;
        }
    }

    private static AWSCredentials? TryGetWebIdentityCredentials(TimeProvider timeProvider,
        int credentialsTimeout = 3600)
    {
        try
        {
            var roleArn = Environment.GetEnvironmentVariable("AWS_ROLE_ARN");
            var webIdentityTokenFile = Environment.GetEnvironmentVariable("AWS_WEB_IDENTITY_TOKEN_FILE");
            var roleSessionName = Environment.GetEnvironmentVariable("AWS_ROLE_SESSION_NAME");

            if (string.IsNullOrEmpty(roleArn) || string.IsNullOrEmpty(webIdentityTokenFile))
                return null;

            var credentials = new AssumeRoleWithWebIdentityCredentials(
                webIdentityTokenFile,
                roleArn,
                roleSessionName ?? "aws-client-factory-session");
            credentials.Expiration ??= DateTime.SpecifyKind(
                timeProvider.GetUtcNow().Add(TimeSpan.FromSeconds(credentialsTimeout)).DateTime, DateTimeKind.Utc);
            return credentials;
        }
        catch
        {
            return null;
        }
    }

    private AmazonS3Config CreateS3Config()
    {
        var config = new AmazonS3Config
        {
            MaxErrorRetry = resolver.GeneralRetryLimit(),
            RetryMode = resolver.GetAwsRetryMode(),
            Timeout = TimeSpan.FromSeconds(resolver.AwsTimeoutSeconds()),
            RegionEndpoint = resolver.GetAwsRegion(),
            BufferSize = resolver.ReadBufferSize(),
            UseAccelerateEndpoint = resolver.UseS3Accelerate(),
            UseHttp = false, // Always use HTTPS
            ForcePathStyle = false // Use virtual-hosted-style requests by default
        };

        return config;
    }

    private AmazonSimpleSystemsManagementConfig CreateSsmConfig()
    {
        var config = new AmazonSimpleSystemsManagementConfig
        {
            MaxErrorRetry = resolver.GeneralRetryLimit(),
            RetryMode = resolver.GetAwsRetryMode(),
            Timeout = TimeSpan.FromSeconds(resolver.AwsTimeoutSeconds()),
            RegionEndpoint = resolver.GetAwsRegion()
        };
        return config;
    }

    private AmazonSQSConfig CreateSqsConfig()
    {
        var config = new AmazonSQSConfig
        {
            MaxErrorRetry = resolver.GeneralRetryLimit(),
            RetryMode = resolver.GetAwsRetryMode(),
            Timeout = TimeSpan.FromSeconds(resolver.AwsTimeoutSeconds()),
            RegionEndpoint = resolver.GetAwsRegion()
        };
        return config;
    }

    private AmazonSimpleNotificationServiceConfig CreateSnsConfig()
    {
        var config = new AmazonSimpleNotificationServiceConfig
        {
            MaxErrorRetry = resolver.GeneralRetryLimit(),
            RetryMode = resolver.GetAwsRetryMode(),
            Timeout = TimeSpan.FromSeconds(resolver.AwsTimeoutSeconds()),
            RegionEndpoint = resolver.GetAwsRegion()
        };
        return config;
    }
}