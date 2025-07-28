// See https://aka.ms/new-console-template for more information

using aws_backup_commands;
using aws_backup_common;
using Cocona;
using Cocona.Help;
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
        $"Application settings file not found: {appSettingsPath} to specify use --app-settings or -a");

    await CoconaApp.CreateBuilder([..args, "--help"]).Build().RunAsync<BackupCommands>();
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
    await CoconaApp.CreateBuilder([..args, "--help"]).Build().RunAsync<BackupCommands>();
    return -1;
}

if (string.IsNullOrWhiteSpace(configuration.ClientId) && string.IsNullOrWhiteSpace(clientId))
{
    await Console.Error.WriteLineAsync(
        "Client ID is required. Please provide it via --client-id or in appsettings.json.");
    await CoconaApp.CreateBuilder([..args, "--help"]).Build().RunAsync<BackupCommands>();
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
    .AddSingleton<IAwsConfigurationFactory, AwsConfigurationFactory>()
    .AddSingleton<IArchiveDataStore, DynamoDbDataStore>()
    .AddSingleton<AwsConfiguration>(sp =>
    {
        var (awsConfiguration, errorMessage) = sp
            .GetService<IAwsConfigurationFactory>()!
            .GetAwsConfiguration(CancellationToken.None)
            .GetAwaiter()
            .GetResult();
        if (awsConfiguration is not null) return awsConfiguration;
        var ex = new InvalidOperationException("AWS configuration could not be loaded.");
        errorMessage ??= "Failed to load AWS configuration.";
        Log.Error(ex, errorMessage);
        throw ex;
    })
    .AddSingleton<ITemporaryCredentialsServer, RolesAnywhere>()
    .AddSingleton<IAwsClientFactory, AwsClientFactory>()
    .AddSingleton<IAesContextResolver, AesContextResolver>()
    .AddSingleton<IS3Service, S3Service>()
    .AddSingleton<TimeProvider>(_ => TimeProvider.System);

var host = builder.Build();
host.AddCommand((
    [Option("client-id", ['c'], Description = "The client ID for the restore operation")]
    string? clientId,
    [Option("app-settings", ['a'], Description = "Path to the application settings file")]
    string? appSettings,
    [FromService] ICoconaHelpMessageBuilder helpMessageBuilder) =>
{
    Console.WriteLine(helpMessageBuilder.BuildAndRenderForCurrentContext());
});

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