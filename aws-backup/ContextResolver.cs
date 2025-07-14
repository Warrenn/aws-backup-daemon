using System.Text.Json;
using System.Text.Json.Nodes;
using aws_backup_common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace aws_backup;

public interface IUpdateConfiguration
{
    Task UpdateConfiguration(Configuration configOptions, CancellationToken cancellationToken);
}

public sealed class ContextResolver : ContextResolverBase, IContextResolver, IUpdateConfiguration
{
    private readonly string _appSettingsPath;
    private readonly ILogger<ContextResolver> _logger;

    public ContextResolver(
        string appSettingsPath,
        IOptionsMonitor<Configuration> configOptions,
        ISignalHub<string> signalHub,
        ILogger<ContextResolver> logger) : base(configOptions.CurrentValue, configOptions.CurrentValue.ClientId)
    {
        _appSettingsPath = appSettingsPath;
        _logger = logger;
        _configOptions = configOptions.CurrentValue;
        configOptions.OnChange((newConfig, _) =>
        {
            logger.LogInformation("Configuration changed, updating ContextResolver.");
            logger.LogInformation("new config {config}", newConfig);
            _configOptions = newConfig;
            ResetCache();
            logger.LogInformation("Configuration updated in ContextResolver.");
            signalHub.Signal(((Configuration)_configOptions).CronSchedule);
        });
    }

    public override string PathsToArchive()
    {
        return ((Configuration)_configOptions).PathsToArchive;
    }

    public override string CronSchedule()
    {
        return ((Configuration)_configOptions).CronSchedule;
    }

    public async Task UpdateConfiguration(Configuration configOptions, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Updating configuration in ContextResolver from UpdateConfiguration.");
        if (!IsValidPath(_appSettingsPath) && File.Exists(_appSettingsPath)) return;
        var configString = await File.ReadAllTextAsync(_appSettingsPath, cancellationToken);
        var root = JsonNode.Parse(configString);
        if (root is null)
        {
            _logger.LogError("the existing configuration file {appSettingsPath} is not valid JSON, skipping update",
                _appSettingsPath);
            return;
        }

        root["Configuration"] =
            JsonSerializer.SerializeToNode(configOptions, ConfigurationGenerationContext.Default.Configuration);

        _logger.LogInformation("Writing updated configuration to {appSettingsPath}", _appSettingsPath);
        await File.WriteAllTextAsync(_appSettingsPath, root.ToJsonString(SourceGenerationContext.Default.Options),
            cancellationToken);
    }

    private void ResetCache()
    {
        _awsRegion = null; // Reset cached region
        _awsRetryMode = null; // Reset cached retry mode
        _coldStorageClass = null; // Reset cached storage classes
        _hotStorageClass = null;
        _lowCostStorage = null;
        _serverSideEncryptionMethod = null; // Reset encryption method
        _localCacheFolder = null; // Reset local cache folder
        _ignoreFile = null; // Reset ignore file
        _localRestoreFolder = null; // Reset restore folder
        _compressionLevel = null;
    }
}