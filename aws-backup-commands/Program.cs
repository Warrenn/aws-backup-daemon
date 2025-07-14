// See https://aka.ms/new-console-template for more information

using aws_backup_commands;
using aws_backup_common;
using Cocona;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Serilog;

var appSettingsPath = GetValueFromArgs("app-settings", "a", args) ??
                      Path.Combine(AppContext.BaseDirectory, "appsettings.json");

var clientId = GetValueFromArgs("client-id", "c", args);

if (!Path.IsPathRooted(appSettingsPath))
    appSettingsPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, appSettingsPath));
if (string.IsNullOrWhiteSpace(appSettingsPath) || !File.Exists(appSettingsPath))
{
    await Console.Error.WriteLineAsync(
        $"Application settings file not found: {appSettingsPath} ");
    return -1;
}

var configBuilder = new ConfigurationBuilder();
configBuilder
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile(appSettingsPath, false)
    .AddEnvironmentVariables();

var configurationRoot = configBuilder.Build();
var configuration = configurationRoot.GetSection("Configuration").Get<Configuration>();

if (configuration is null)
{
    await Console.Error.WriteLineAsync("Configuration section 'Configuration' not found in appsettings.json.");
    return -1;
}

if (string.IsNullOrWhiteSpace(configuration.ClientId) && string.IsNullOrWhiteSpace(clientId))
{
    await Console.Error.WriteLineAsync(
        "Client ID is required. Please provide it via --client-id or in appsettings.json.");
    return -1;
}

configuration.ClientId = clientId ?? configuration.ClientId;

var builder = CoconaApp.CreateBuilder(args);
builder.Configuration.AddConfiguration(configurationRoot);
builder
    .Services
    .AddSerilog(config => config.ReadFrom.Configuration(configurationRoot));

builder
    .Services
    .AddSingleton<IContextResolver>(_ => new ContextResolver(configuration))
    .AddSingleton(configuration)
    .AddSingleton(sp =>
    {
        var factory = sp.GetRequiredService<IAwsClientFactory>();
        var iamClient = factory.CreateIamClient().GetAwaiter().GetResult();
        var awsConfig = iamClient.GetAwsConfigurationAsync(configuration.ClientId).GetAwaiter().GetResult();
        return awsConfig;
    })
    .AddSingleton(provider =>
    {
        var resolver = provider.GetRequiredService<IContextResolver>();
        return provider.GetRequiredService<IS3Service>()
            .DownloadCompressedObject<CurrentArchiveRunRequests>(resolver.CurrentArchiveRunsBucketKey(),
                CancellationToken.None).GetAwaiter().GetResult() ?? new CurrentArchiveRunRequests();
    })
    .AddSingleton(provider =>
    {
        var resolver = provider.GetRequiredService<IContextResolver>();
        return provider.GetRequiredService<IS3Service>()
            .DownloadCompressedObject<DataChunkManifest>(resolver.ChunkManifestBucketKey(),
                CancellationToken.None).GetAwaiter().GetResult() ?? new DataChunkManifest();
    })
    .AddSingleton(provider =>
    {
        var resolver = provider.GetRequiredService<IContextResolver>();
        return provider.GetRequiredService<IS3Service>()
            .DownloadCompressedObject<CurrentRestoreRequests>(resolver.CurrentRestoreBucketKey(),
                CancellationToken.None).GetAwaiter().GetResult() ?? new CurrentRestoreRequests();
    })
    .AddSingleton(provider =>
    {
        var resolver = provider.GetRequiredService<IContextResolver>();
        return provider.GetRequiredService<IS3Service>()
            .DownloadCompressedObject<S3RestoreChunkManifest>(resolver.RestoreManifestBucketKey(),
                CancellationToken.None).GetAwaiter().GetResult() ?? new S3RestoreChunkManifest();
    })
    .AddSingleton<ISignalHub<string>, SignalHub<string>>()
    .AddSingleton<ITemporaryCredentialsServer, RolesAnywhere>()
    .AddSingleton<IAwsClientFactory, AwsClientFactory>()
    .AddSingleton<IAesContextResolver, AesContextResolver>()
    .AddSingleton<IS3Service, S3Service>()
    .AddSingleton<TimeProvider>(_ => TimeProvider.System)
    .AddSingleton<CurrentArchiveRuns>();

var host = builder.Build();

await host.RunAsync<BackupCommands>();
return 0;

static string? GetValueFromArgs(string key, string keyAlt, string[] args)
{
    for (var i = 0; i < args.Length; i++)
    {
        if (args[i] == $"--{key}" && i + 1 < args.Length)
            return args[i + 1].Trim();

        if (args[i].StartsWith($"--{key}="))
            return args[i].Split('=')[1].Trim();

        if (args[i] == $"-{keyAlt}" && i + 1 < args.Length)
            return args[i + 1].Trim();

        if (args[i].StartsWith($"-{keyAlt}="))
            return args[i].Split('=')[1].Trim();
    }

    return null;
}