// See https://aka.ms/new-console-template for more information

using System.CommandLine;
using System.Text.Json;
using aws_backup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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

//CurrentArchiveRuns
//DataChunkManifest
//CurrentRestoreRequests
//S3RestoreChunkManifest
//Configuration
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
    .ConfigureServices((hosting, services) =>
    {
        services
            .AddSingleton<Mediator>()
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
            .AddSingleton<IAwsClientFactory, AwsClientFactory>()
            .AddSingleton<IArchiveService, ArchiveService>()
            .AddSingleton<IChunkedEncryptingFileProcessor, ChunkedEncryptingFileProcessor>()
            .AddSingleton<IContextResolver, ContextResolver>()
            .AddSingleton<ICronScheduler, CronScheduler>()
            .AddSingleton<ICronSchedulerFactory, CronSchedulerFactory>()
            .AddSingleton<IDataChunkService, DataChunkService>()
            .AddSingleton<IFileLister, FileLister>()
            .AddSingleton<IHotStorageService, HotStorageService>()
            .AddSingleton<IRestoreService, RestoreService>()
            .AddSingleton<IS3ChunkedFileReconstructor, S3ChunkedFileReconstructor>()
            .AddSingleton<IS3Service, S3Service>()
            .AddSingleton<ITemporaryCredentialsServer, RolesAnywhere>()
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
            .AddSingleton(globalConfig)
            .AddOptions<Configuration>()
            .ValidateOnStart();
    });

var host = builder.Build();
await host.RunAsync();
return 0;