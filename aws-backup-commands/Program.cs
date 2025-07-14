// See https://aka.ms/new-console-template for more information

using aws_backup_common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

var appSettingsPath = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
if (!Path.IsPathRooted(appSettingsPath))
    appSettingsPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, appSettingsPath));
if (!File.Exists(appSettingsPath))
{
    await Console.Error.WriteLineAsync($"Application settings file not found: {appSettingsPath}");
    return -1;
}


var configBuilder = new ConfigurationBuilder();
configBuilder
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile(appSettingsPath, false, true)
    .AddInMemoryCollection([
        new KeyValuePair<string, string?>("Configuration:SettingsPath", appSettingsPath)
    ])
    .AddEnvironmentVariables();

var configuration = configBuilder.Build();

var builder = Host.CreateApplicationBuilder(args);
builder.Configuration.AddConfiguration(configuration);
builder
    .Services
    .AddSerilog(config => config.ReadFrom.Configuration(configuration));

builder
    .Services
    .AddSingleton(sp =>
    {
        var resolver = sp.GetRequiredService<IContextResolver>();
        var factory = sp.GetRequiredService<IAwsClientFactory>();
        var iamClient = factory.CreateIamClient().GetAwaiter().GetResult();
        var clientId = resolver.ClientId();
        var awsConfig = iamClient.GetAwsConfigurationAsync(clientId).GetAwaiter().GetResult();
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

await host.RunAsync();
return 0;