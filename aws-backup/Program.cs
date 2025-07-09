// See https://aka.ms/new-console-template for more information

using System.CommandLine;
using aws_backup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.File;

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
if (!File.Exists(appSettingsPath))
{
    await Console.Error.WriteLineAsync($"Application settings file not found: {appSettingsPath}");
    return -1;
}

if (!Path.IsPathRooted(appSettingsPath))
    appSettingsPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, appSettingsPath));

var configBuilder = new ConfigurationBuilder();
configBuilder
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile(appSettingsPath, false, true)
    .AddEnvironmentVariables();

var rootConfig = configBuilder.Build();
var configuration = rootConfig.Get<Configuration>();

if (configuration is null)
{
    await Console.Error.WriteLineAsync("No configuration found in appsettings.json");
    return -1;
}

configuration.AppSettingsPath = appSettingsPath;

var serviceCollection = new ServiceCollection();
serviceCollection
    .AddLogging(builder =>
    {
        builder
            .ClearProviders() // Clear default logging providers
            .AddConsole()
            .AddDebug()
            .AddSerilog(); // Use Serilog for structured logging
    })
    .AddSingleton(configuration)
    .AddSingleton<ISignalHub<string>, SignalHub<string>>()
    .AddSingleton<IContextResolver, ContextResolver>()
    .AddSingleton<ITemporaryCredentialsServer, RolesAnywhere>()
    .AddSingleton<IAwsClientFactory, AwsClientFactory>();

IServiceProvider provider = serviceCollection.BuildServiceProvider();
var resolver = provider.GetRequiredService<IContextResolver>();
var factory = provider.GetRequiredService<IAwsClientFactory>();
var iamClient = await factory.CreateIamClient(cancellationToken);
var clientId = resolver.ClientId();
var awsConfiguration = await iamClient.GetAwsConfigurationAsync(clientId, cancellationToken);

serviceCollection
    .AddSingleton(awsConfiguration)
    .AddSingleton<IS3Service, S3Service>();

provider = serviceCollection.BuildServiceProvider();
var hotStorageService = provider.GetRequiredService<IS3Service>();

var archiveRunRequests =
    await hotStorageService.DownloadCompressedObject<CurrentArchiveRunRequests>(resolver.CurrentArchiveRunsBucketKey(),
        CancellationToken.None) ?? new CurrentArchiveRunRequests();

var dataChunkManifest =
    await hotStorageService.DownloadCompressedObject<DataChunkManifest>(resolver.ChunkManifestBucketKey(),
        CancellationToken.None) ?? new DataChunkManifest();

var currentRestoreRequests =
    await hotStorageService.DownloadCompressedObject<CurrentRestoreRequests>(resolver.CurrentRestoreBucketKey(),
        CancellationToken.None) ?? new CurrentRestoreRequests();

var chunkManifest =
    await hotStorageService.DownloadCompressedObject<S3RestoreChunkManifest>(resolver.RestoreManifestBucketKey(),
        CancellationToken.None) ?? new S3RestoreChunkManifest();

var builder = Host.CreateDefaultBuilder(args)
    .UseWindowsService() // ← this enables service integration
    .UseSystemd() // ← this enables systemd integration
    .ConfigureAppConfiguration(builder =>
    {
        foreach (var source in configBuilder.Sources) builder.Add(source);
    })
    .ConfigureServices(services =>
    {
        foreach (var descriptor in serviceCollection) services.Add(descriptor);
        services
            .AddSingleton<Mediator>()
            .AddSingleton(awsConfiguration)
            .AddSingleton(archiveRunRequests)
            .AddSingleton(dataChunkManifest)
            .AddSingleton(currentRestoreRequests)
            .AddSingleton(chunkManifest)
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
            .AddSingleton<IRollingFileMediator>(sp => sp.GetRequiredService<Mediator>())
            .AddSingleton<IAesContextResolver, AesContextResolver>()
            .AddSingleton<IAwsClientFactory, AwsClientFactory>()
            .AddSingleton<IArchiveService, ArchiveService>()
            .AddSingleton<IChunkedEncryptingFileProcessor, ChunkedEncryptingFileProcessor>()
            .AddSingleton<ICronScheduler, CronScheduler>()
            .AddSingleton<ICronSchedulerFactory, CronSchedulerFactory>()
            .AddSingleton<IDataChunkService, DataChunkService>()
            .AddSingleton<IFileLister, FileLister>()
            .AddSingleton<IRestoreService, RestoreService>()
            .AddSingleton<IS3ChunkedFileReconstructor, S3ChunkedFileReconstructor>()
            .AddSingleton<IS3Service, S3Service>()
            .AddSingleton<FileLifecycleHooks, UploadToS3Hooks>()
            .AddSingleton<TimeProvider>(_ => TimeProvider.System)
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
            .AddHostedService<RollingFileActor>();
    });
var host = builder.Build();
provider = host.Services;

ConfigureSeriLogging(configuration, provider);

await host.RunAsync();
return 0;

static void ConfigureSeriLogging(Configuration logConfig, IServiceProvider logProvider)
{
    var logFolder = logConfig.LogFolder;
    var logLevelStr = logConfig.LogLevel;
    var hooks = logProvider.GetService<FileLifecycleHooks>();

    if (string.IsNullOrWhiteSpace(logFolder))
        logFolder = Path.Combine(AppContext.BaseDirectory, "logs");
    else if (!Path.IsPathRooted(logFolder))
        logFolder = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, logFolder));

    if (!Directory.Exists(logFolder))
        Directory.CreateDirectory(logFolder);

    if (string.IsNullOrWhiteSpace(logLevelStr) ||
        !Enum.TryParse(logLevelStr, true, out LogEventLevel logLevel))
        logLevel = LogEventLevel.Warning; // default log level if not set.

    Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Is(logLevel)
        .Enrich.FromLogContext()
        .WriteTo.File(
            Path.Combine(logFolder, "{Date}.log"),
            outputTemplate:
            "[{Timestamp:yyyy-MM-dd HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}",
            rollingInterval: RollingInterval.Day,
            retainedFileCountLimit: 1, // keep the last 1 day of logs
            rollOnFileSizeLimit: false,
            hooks: hooks // add the custom hooks for file lifecycle events
        )
        .CreateLogger();
}