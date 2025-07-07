// See https://aka.ms/new-console-template for more information

using System.CommandLine;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.File;

namespace aws_backup;

public static class Program
{
    /// <summary>
    ///     The main entry point for the application.
    /// </summary>
    /// <param name="args">Command line arguments.</param>
    /// <param name="cancellationToken">Cancellation token for graceful shutdown.</param>
    /// <returns>Exit code.</returns>
    public static async Task<int> Main(string[] args, CancellationToken cancellationToken)
    {
        var appConfigOpt = new Option<string?>("--app-settings", "-a")
        {
            Description = "Path to the application settings file (appsettings.json).",
            Required = false
        };

        var rootCommand = new RootCommand("AWS Backup Tool - Archive and restore files to/from AWS S3")
        {
            appConfigOpt
        };

        var parsedArgs = rootCommand.Parse(args);
        var exit = await parsedArgs.InvokeAsync(cancellationToken);
        if (exit != 0) return exit;

        var appSettingsPath = parsedArgs.GetValue(appConfigOpt) ?? "appsettings.json";
        if (!File.Exists(appSettingsPath))
        {
            await Console.Error.WriteLineAsync($"Application settings file not found: {appSettingsPath}");
            return -1;
        }

        var configuration =
            await JsonSerializer.DeserializeAsync<Configuration>(File.OpenRead(appSettingsPath), Json.Options,
                cancellationToken);
        if (configuration == null)
        {
            await Console.Error.WriteLineAsync($"Failed to parse configuration from {appSettingsPath}");
            return -1;
        }

        var builder = Host.CreateDefaultBuilder(args)
            .UseWindowsService() // ← this enables service integration
            .UseSystemd() // ← this enables systemd integration
            .ConfigureAppConfiguration(config =>
            {
                // Load configuration from appsettings.json, environment variables, etc.
                config.AddEnvironmentVariables();
            })
            .ConfigureServices(services =>
            {
                services
                    .AddLogging(builder =>
                    {
                        builder
                            .ClearProviders() // Clear default logging providers
                            .AddConsole()
                            .AddDebug()
                            .AddSerilog(); // Use Serilog for structured logging
                    })
                    .AddSingleton<IContextResolver, ContextResolver>()
                    .AddSingleton<ITemporaryCredentialsServer, RolesAnywhere>()
                    .AddSingleton<IAwsClientFactory, AwsClientFactory>();
            });

        var provider = builder.Build().Services;

        var archiveRunRequests =
            await GetS3File<CurrentArchiveRunRequests>(provider, ctx => ctx.CurrentArchiveRunsBucketKey()) ??
            new CurrentArchiveRunRequests();

        var dataChunkManifest =
            await GetS3File<DataChunkManifest>(provider, ctx => ctx.ChunkManifestBucketKey()) ??
            new DataChunkManifest();

        var currentRestoreRequests =
            await GetS3File<CurrentRestoreRequests>(provider, ctx => ctx.CurrentRestoreBucketKey()) ??
            new CurrentRestoreRequests();

        var chunkManifest =
            await GetS3File<S3RestoreChunkManifest>(provider, ctx => ctx.RestoreManifestBucketKey()) ??
            new S3RestoreChunkManifest();

        var resolver = provider.GetRequiredService<IContextResolver>();
        var iamClient = await provider.GetRequiredService<IAwsClientFactory>().CreateIamClient(cancellationToken);
        var clientId = resolver.ClientId();
        var awsConfiguration = await iamClient.GetAwsConfigurationAsync(clientId, cancellationToken);

        builder
            .ConfigureServices(services =>
            {
                services
                    .AddSingleton<Mediator>()
                    .AddSingleton(awsConfiguration)
                    .AddSingleton(configuration)
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
                    .ValidateOnStart();
            });
        var host = builder.Build();
        provider = host.Services;

        ConfigureSeriLogging(configuration, provider);

        await host.RunAsync(token: cancellationToken);
        return 0;
    }

    private static async Task<T?> GetS3File<T>(IServiceProvider sp, Func<IContextResolver, string> getKey)
        where T : notnull
    {
        var resolver = sp.GetService<IContextResolver>();
        var service = sp.GetService<IHotStorageService>();
        var key = getKey(resolver!);

        var data = await service!.DownloadAsync<T>(key, CancellationToken.None);
        return data;
    }

    private static void ConfigureSeriLogging(Configuration logConfig, IServiceProvider logProvider)
    {
        var logFolder = logConfig.LogFolder;
        var logLevelStr = logConfig.LogLevel;
        var hooks = logProvider.GetService<FileLifecycleHooks>();

        if (string.IsNullOrWhiteSpace(logFolder))
            logFolder = Path.Combine(AppContext.BaseDirectory, "logs");
        else if (!Path.IsPathRooted(logFolder))
            logFolder = Path.Combine(AppContext.BaseDirectory, logFolder);

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
}