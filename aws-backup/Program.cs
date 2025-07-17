// See https://aka.ms/new-console-template for more information

using System.CommandLine;
using aws_backup_common;
using aws_backup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serilog;

var appConfigOpt = new Option<string?>("--app-settings", "-a")
{
    Description = "Path to the application settings file (appsettings.json).",
    Required = false
};

var rootCommand = new RootCommand("AWS Backup Tool - Archive and restore files to/from AWS S3")
{
    appConfigOpt
};

var cancellationToken = new CancellationTokenSource().Token;
var parsedArgs = rootCommand.Parse(args);
var exit = await parsedArgs.InvokeAsync(cancellationToken);
if (exit != 0) return exit;

var appSettingsPath = parsedArgs.GetValue(appConfigOpt) ??
                      Path.Combine(AppContext.BaseDirectory, "appsettings.json");
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
    .AddSerilog(config =>
        config
            .Enrich.FromLogContext()
            .WriteTo.File(
                // use {Date} to get YYYY-MM-DD in the filename:
                "logs/log-.log",
                outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}",
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: 1, // keep the last 31 days of logs
                rollOnFileSizeLimit: true,
                fileSizeLimitBytes: 10 * 1024 * 1024 // 10 MB per file
            )
            .ReadFrom.Configuration(configuration).MinimumLevel.Debug())
    .AddWindowsService()
    .AddSystemd()
    .AddSingleton<Mediator>()
    .AddSingleton<IContextResolver>(sp =>
        new ContextResolver(
            appSettingsPath,
            sp.GetRequiredService<IOptionsMonitor<Configuration>>(),
            sp.GetRequiredService<ISignalHub<string>>(),
            sp.GetRequiredService<ILogger<ContextResolver>>()))
    .AddSingleton<IUpdateConfiguration, ContextResolver>()
    .AddSingleton(sp =>
    {
        var resolver = sp.GetRequiredService<IContextResolver>();
        var factory = sp.GetRequiredService<IAwsClientFactory>();
        var iamClient = factory.CreateIamClient(cancellationToken).GetAwaiter().GetResult();
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
    .AddSingleton<ISnsMessageMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<IArchiveFileMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<IRunRequestMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<IArchiveRunMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<IChunkManifestMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<IDownloadFileMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<IRetryMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<IRestoreRequestsMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<IRestoreManifestMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<IRestoreRunMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<IUploadChunksMediator>(sp => sp.GetRequiredService<Mediator>())
    .AddSingleton<ISignalHub<string>, SignalHub<string>>()
    .AddSingleton<ITemporaryCredentialsServer, RolesAnywhere>()
    .AddSingleton<IAwsClientFactory, AwsClientFactory>()
    .AddSingleton<IAesContextResolver, AesContextResolver>()
    .AddSingleton<IArchiveService, ArchiveService>()
    .AddSingleton<IChunkedEncryptingFileProcessor, ChunkedEncryptingFileProcessor>()
    .AddSingleton<ICronScheduler, CronScheduler>()
    .AddSingleton<ICronSchedulerFactory, CronSchedulerFactory>()
    .AddSingleton<IDataChunkService, DataChunkService>()
    .AddSingleton<IFileLister, FileLister>()
    .AddSingleton<IRestoreService, RestoreService>()
    .AddSingleton<IS3ChunkedFileReconstructor, S3ChunkedFileReconstructor>()
    .AddSingleton<IS3Service, S3Service>()
    .AddSingleton(_ => TimeProvider.System)
    .AddSingleton<CurrentArchiveRuns>()
    .AddHostedService<CronJobActor>()
    .AddHostedService<ArchiveFilesActor>()
    .AddHostedService<ArchiveRunActor>()
    .AddHostedService<DownloadFileActor>()
    .AddHostedService<RestoreRunActor>()
    .AddHostedService<RetryActor>()
    .AddHostedService<S3StorageClassActor>()
    .AddHostedService<SqsPollingActor>()
    .AddHostedService<UploadChunkDataActor>()
    .AddHostedService<UploadActor>()
    .AddHostedService<SnsActor>()
    .AddHostedService<RollingFileActor>()
    .AddOptions<Configuration>()
    .Bind(configuration.GetSection("Configuration"))
    .ValidateOnStart();

var host = builder.Build();

await host.RunAsync();
return 0;