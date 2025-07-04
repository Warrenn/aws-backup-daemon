// See https://aka.ms/new-console-template for more information

using System.CommandLine;
using System.Text.Json;
using aws_backup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Core;
using Serilog.Sinks.File;

// Required options (non-nullable fields)
var clientIdOpt = new Option<string>("--client-id", "-i")
{
    Description = "Unique identifier for this backup client.",
    Required = true
};

var globalConfigOpt = new Option<string?>("--global-config", "-g")
{
    Description = "Path to the global configuration file (global.json).",
    Required = false
};

var appConfigOpt = new Option<string?>("--app-settings", "-a")
{
    Description = "Path to the application settings file (appsettings.json).",
    Required = false
};

// Create the root command
var rootCommand = new RootCommand("AWS Backup Tool - Archive and restore files to/from AWS S3")
{
    clientIdOpt,
    globalConfigOpt,
    appConfigOpt
};

var parsedArgs = rootCommand.Parse(args);
var exit = await parsedArgs.InvokeAsync();
if (exit != 0) return exit;

var globalConfigPath = parsedArgs.GetValue(globalConfigOpt) ?? "global.json";
if (!File.Exists(globalConfigPath))
{
    Console.Error.WriteLine($"Global configuration file not found: {globalConfigPath}");
    return -1;
}

var json = File.ReadAllText(globalConfigPath);
var globalConfig = JsonSerializer.Deserialize<GlobalConfiguration>(json)!;

var appSettingsPath = parsedArgs.GetValue(appConfigOpt) ?? "appsettings.json";
if (!File.Exists(appSettingsPath))
{
    Console.Error.WriteLine($"Application settings file not found: {appSettingsPath}");
    return -1;
}

var builder = Host.CreateDefaultBuilder(args)
    .UseWindowsService() // ← this enables service integration
    .UseSystemd() // ← this enables systemd integration
    .ConfigureAppConfiguration(config =>
    {
        // Load configuration from appsettings.json, environment variables, etc.
        config
            .AddJsonFile(appSettingsPath, true, true)
            .AddEnvironmentVariables()
            .AddCommandLine(args, new Dictionary<string, string>
            {
                { "--client-id", "ClientId" } // Map command line option to configuration key
            });
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders(); // Clear default providers
        logging.AddConsole(); // Add console logging
        logging.AddDebug(); // Add debug logging
        // Optionally add other loggers like File, Azure, etc.
    })
    .ConfigureServices(services =>
    {
        // Register the global configuration
        services
            .AddLogging()
            .AddSingleton<IContextResolver, ContextResolver>()
            .AddSingleton<ITemporaryCredentialsServer, RolesAnywhere>()
            .AddSingleton<IHotStorageService, HotStorageService>()
            .AddSingleton(globalConfig)
            .AddOptions<Configuration>()
            .ValidateOnStart();
    });

var provider = builder.Build().Services;

var archiveRunRequests =
    await GetS3File<CurrentArchiveRunRequests>(provider, ctx => ctx.CurrentArchiveRunsBucketKey()) ??
    new CurrentArchiveRunRequests();
var dataChunkManifest = await GetS3File<DataChunkManifest>(provider, ctx => ctx.ChunkManifestBucketKey()) ??
                        new DataChunkManifest();
var currentRestoreRequests =
    await GetS3File<CurrentRestoreRequests>(provider, ctx => ctx.CurrentRestoreBucketKey()) ??
    new CurrentRestoreRequests();
var chunkManifest = await GetS3File<S3RestoreChunkManifest>(provider, ctx => ctx.RestoreManifestBucketKey()) ??
                    new S3RestoreChunkManifest();

builder
    .ConfigureServices(services =>
    {
        services
            .AddSingleton<Mediator>()
            .AddSingleton(archiveRunRequests)
            .AddSingleton(dataChunkManifest)
            .AddSingleton(currentRestoreRequests)
            .AddSingleton(chunkManifest)
            .AddSingleton<ISnsOrchestrationMediator>(sp => sp.GetRequiredService<Mediator>())
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
            .AddSingleton<IAwsClientFactory, AwsClientFactory>()
            .AddSingleton<IArchiveService, ArchiveService>()
            .AddSingleton<IChunkedEncryptingFileProcessor, ChunkedEncryptingFileProcessor>()
            .AddSingleton<IContextResolver, ContextResolver>()
            .AddSingleton<ICronScheduler, CronScheduler>()
            .AddSingleton<ICronSchedulerFactory, CronSchedulerFactory>()
            .AddSingleton<IDataChunkService, DataChunkService>()
            .AddSingleton<IFileLister, FileLister>()
            .AddSingleton<IRestoreService, RestoreService>()
            .AddSingleton<IS3ChunkedFileReconstructor, S3ChunkedFileReconstructor>()
            .AddSingleton<IS3Service, S3Service>()
            .AddSingleton<FileLifecycleHooks, UploadToS3Hooks>()
            .AddSingleton<TimeProvider>(_ => TimeProvider.System)
            .AddHostedService<CronJobOrchestration>()
            .AddHostedService<ArchiveFilesOrchestration>()
            .AddHostedService<ArchiveRunOrchestration>()
            .AddHostedService<DownloadFileOrchestration>()
            .AddHostedService<RestoreRunOrchestration>()
            .AddHostedService<RetryOrchestration>()
            .AddHostedService<S3StorageClassOrchestration>()
            .AddHostedService<SqsPollingOrchestration>()
            .AddHostedService<UploadChunkDataOrchestration>()
            .AddHostedService<UploadOrchestration>()
            .AddHostedService<SnsOrchestration>()
            .AddHostedService<RollingFileOrchestration>()
            .AddSingleton<Logger>(sp =>
            {
                var resolver = sp.GetService<IContextResolver>()!;
                var logFolder = resolver.LogPath();
                Directory.CreateDirectory(logFolder);

                var logLevel = resolver.LogLevel();

                var hooks = sp.GetRequiredService<FileLifecycleHooks>();

                var logger = new LoggerConfiguration()
                    .MinimumLevel.Is(logLevel) // adjust as you like
                    .Enrich.FromLogContext()
                    .WriteTo.File(
                        // use {Date} to get YYYY-MM-DD in the filename:
                        Path.Combine(logFolder, "{Date}.log"),
                        outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}",
                        rollingInterval: RollingInterval.Day,
                        retainedFileCountLimit: 1, // keep the last 31 days of logs
                        rollOnFileSizeLimit: false,
                        hooks: hooks // add the custom hooks for file lifecycle events
                    )
                    .CreateLogger();
                Log.Logger = logger;
                return logger;
            });
    });

var host = builder.Build();
await host.RunAsync();
return 0;

async Task<T?> GetS3File<T>(IServiceProvider sp, Func<IContextResolver, string> getKey)
    where T : notnull
{
    var resolver = sp.GetService<IContextResolver>();
    var service = sp.GetService<IHotStorageService>();
    var key = getKey(resolver!);

    var data = await service!.DownloadAsync<T>(key, CancellationToken.None);
    return data;
}