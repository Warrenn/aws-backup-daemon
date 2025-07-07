using System.Collections.Concurrent;
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

namespace aws_backup;

public interface IAwsClientFactory : IDisposable
{
    Task<IAmazonS3> CreateS3Client(CancellationToken cancellationToken);
    Task<IAmazonSimpleSystemsManagement> CreateSsmClient(CancellationToken cancellationToken = default);
    Task<IAmazonSQS> CreateSqsClient(CancellationToken cancellationToken);
    Task<IAmazonSimpleNotificationService> CreateSnsClient(CancellationToken cancellationToken);

    Task<AmazonIdentityManagementServiceClient> CreateIamClient(CancellationToken cancellationToken = default);
}

public sealed class AwsClientFactory(
    IContextResolver resolver,
    ILogger<AwsClientFactory> logger,
    ITemporaryCredentialsServer temporaryCredentialsServer)
    : IAwsClientFactory
{
    private readonly ConcurrentDictionary<Type, object> _clientCache = new();
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private AWSCredentials? _cachedCredentials;

    public async Task<IAmazonS3> CreateS3Client(CancellationToken cancellationToken)
    {
        return await GetOrCreateClient<IAmazonS3>(async () =>
        {
            var config = CreateS3Config();
            var credentials = await GetCredentialsAsync(cancellationToken);

            return credentials != null
                ? new AmazonS3Client(credentials, config)
                : new AmazonS3Client(config);
        }, cancellationToken);
    }

    public async Task<IAmazonSimpleSystemsManagement> CreateSsmClient(CancellationToken cancellationToken)
    {
        return await GetOrCreateClient<IAmazonSimpleSystemsManagement>(async () =>
        {
            var config = CreateSsmConfig();
            var credentials = await GetCredentialsAsync(cancellationToken);

            return credentials != null
                ? new AmazonSimpleSystemsManagementClient(credentials, config)
                : new AmazonSimpleSystemsManagementClient(config);
        }, cancellationToken);
    }

    public async Task<IAmazonSQS> CreateSqsClient(CancellationToken cancellationToken)
    {
        return await GetOrCreateClient<IAmazonSQS>(async () =>
        {
            var config = CreateSqsConfig();
            var credentials = await GetCredentialsAsync(cancellationToken);

            return credentials != null
                ? new AmazonSQSClient(credentials, config)
                : new AmazonSQSClient(config);
        }, cancellationToken);
    }

    public async Task<IAmazonSimpleNotificationService> CreateSnsClient(CancellationToken cancellationToken)
    {
        return await GetOrCreateClient<IAmazonSimpleNotificationService>(async () =>
        {
            var config = CreateSnsConfig();
            var credentials = await GetCredentialsAsync(cancellationToken);

            return credentials != null
                ? new AmazonSimpleNotificationServiceClient(credentials, config)
                : new AmazonSimpleNotificationServiceClient(config);
        }, cancellationToken);
    }

    public async Task<AmazonIdentityManagementServiceClient> CreateIamClient(CancellationToken cancellationToken)

    {
        return await GetOrCreateClient(async () =>
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
        }, cancellationToken);
    }

    public void Dispose()
    {
        foreach (var client in _clientCache.Values.OfType<IDisposable>())
            try
            {
                client.Dispose();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Error disposing AWS client");
            }

        _clientCache.Clear();
        _semaphore.Dispose();
    }

    private async Task<T> GetOrCreateClient<T>(Func<Task<T>> factory, CancellationToken cancellationToken)
        where T : class
    {
        if (_clientCache.TryGetValue(typeof(T), out var cachedClient)) return (T)cachedClient;

        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            // Double-check locking pattern
            if (_clientCache.TryGetValue(typeof(T), out cachedClient)) return (T)cachedClient;

            logger.LogDebug("Creating new AWS client of type {ClientType}", typeof(T).Name);
            var client = await factory();
            _clientCache.TryAdd(typeof(T), client);
            return client;
        }
        finally
        {
            _semaphore.Release();
        }
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

    private async Task<AWSCredentials?> GetCredentialsAsync(CancellationToken cancellationToken)
    {
        var credentialsValid = _cachedCredentials is not null &&
                               await ValidateCredentialsAsync(_cachedCredentials, cancellationToken);
        if (credentialsValid) return _cachedCredentials;

        var credentialSources = new List<(string Name, Func<CancellationToken, Task<AWSCredentials?>> Factory)>
        {
            ("Roles Anywhere", TryGetRolesAnyWhereCredentials),
            ("Environment Variables", _ => Task.FromResult(TryGetEnvironmentCredentials())),
            ("AWS Profile", _ => Task.FromResult(TryGetProfileCredentials())),
            ("IAM Role", TryGetInstanceProfileCredentialsAsync),
            ("Web Identity Token", _ => Task.FromResult(TryGetWebIdentityCredentials()))
        };

        foreach (var (name, factory) in credentialSources)
            try
            {
                logger.LogDebug("Attempting to resolve AWS credentials using {CredentialSource}", name);
                var credentials = await factory(cancellationToken);

                if (credentials == null) continue;
                // Test credentials by attempting to get caller identity
                if (!await ValidateCredentialsAsync(credentials, cancellationToken)) continue;

                logger.LogInformation("Successfully resolved AWS credentials using {CredentialSource}", name);
                _cachedCredentials = credentials; // Cache the valid credentials
                return credentials;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to resolve AWS credentials using {CredentialSource}", name);
            }

        logger.LogWarning("No valid AWS credentials found, falling back to default credential chain");
        return null; // Let AWS SDK handle default credential chain
    }

    private async Task<AWSCredentials?> TryGetRolesAnyWhereCredentials(CancellationToken cancellationToken)
    {
        //if all the variables needed for temporary credentials are set, use them
        var profileArn = resolver.RolesAnyWhereProfileArn();
        var roleArn = resolver.RolesAnyWhereRoleArn();
        var trustAnchorArn = resolver.RolesAnyWhereTrustAnchorArn();
        var certificateFileName = resolver.RolesAnyWhereCertificateFileName();
        var privateKeyFileName = resolver.RolesAnyWherePrivateKeyFileName();
        var region = resolver.GetAwsRegion();

        if (string.IsNullOrWhiteSpace(profileArn)) return null;
        if (string.IsNullOrWhiteSpace(roleArn)) return null;
        if (string.IsNullOrWhiteSpace(trustAnchorArn)) return null;
        if (!File.Exists(certificateFileName)) return null;
        if (!File.Exists(privateKeyFileName)) return null;

        var (accessKey, secretKey, sessionToken) = await temporaryCredentialsServer.GetCredentials(
            profileArn,
            roleArn,
            trustAnchorArn,
            certificateFileName,
            privateKeyFileName,
            region.SystemName,
            cancellation: cancellationToken);

        if (string.IsNullOrWhiteSpace(accessKey) || string.IsNullOrWhiteSpace(secretKey))
            return null;

        return string.IsNullOrWhiteSpace(sessionToken)
            ? new BasicAWSCredentials(accessKey, secretKey)
            : new SessionAWSCredentials(accessKey, secretKey, sessionToken);
    }

    private static AWSCredentials? TryGetEnvironmentCredentials()
    {
        //if all the variables needed for temporary credentials are set, use them

        var accessKey = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
        var secretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");
        var sessionToken = Environment.GetEnvironmentVariable("AWS_SESSION_TOKEN");

        if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey))
            return null;

        return string.IsNullOrEmpty(sessionToken)
            ? new BasicAWSCredentials(accessKey, secretKey)
            : new SessionAWSCredentials(accessKey, secretKey, sessionToken);
    }

    private static AWSCredentials? TryGetProfileCredentials()
    {
        try
        {
            var profileName = Environment.GetEnvironmentVariable("AWS_PROFILE") ?? "default";
            var chain = new CredentialProfileStoreChain();

            return chain.TryGetAWSCredentials(profileName, out var credentials)
                ? credentials
                : null;
        }
        catch
        {
            return null;
        }
    }

    private static async Task<AWSCredentials?> TryGetInstanceProfileCredentialsAsync(
        CancellationToken cancellationToken)
    {
        try
        {
            var credentials = new InstanceProfileAWSCredentials();
            // Test if we can get credentials
            await credentials.GetCredentialsAsync();
            return credentials;
        }
        catch
        {
            return null;
        }
    }

    private static AWSCredentials? TryGetWebIdentityCredentials()
    {
        try
        {
            var roleArn = Environment.GetEnvironmentVariable("AWS_ROLE_ARN");
            var webIdentityTokenFile = Environment.GetEnvironmentVariable("AWS_WEB_IDENTITY_TOKEN_FILE");
            var roleSessionName = Environment.GetEnvironmentVariable("AWS_ROLE_SESSION_NAME");

            if (string.IsNullOrEmpty(roleArn) || string.IsNullOrEmpty(webIdentityTokenFile))
                return null;

            return new AssumeRoleWithWebIdentityCredentials(
                webIdentityTokenFile,
                roleArn,
                roleSessionName ?? "aws-client-factory-session");
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
            Timeout = TimeSpan.FromSeconds(resolver.ShutdownTimeoutSeconds()),
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
            Timeout = TimeSpan.FromSeconds(resolver.ShutdownTimeoutSeconds()),
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
            Timeout = TimeSpan.FromSeconds(resolver.ShutdownTimeoutSeconds()),
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
            Timeout = TimeSpan.FromSeconds(resolver.ShutdownTimeoutSeconds()),
            RegionEndpoint = resolver.GetAwsRegion()
        };
        return config;
    }
}