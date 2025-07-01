// See https://aka.ms/new-console-template for more information

using System.CommandLine;
using System.Text.Json;
using aws_backup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

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

var host = Host.CreateDefaultBuilder(args)
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
    .ConfigureServices(services =>
    {
        // ... your AddHostedService<...>() etc.
    })
    .Build();

await host.RunAsync();
return 0;