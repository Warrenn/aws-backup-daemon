using aws_backup_common;

namespace aws_backup_commands;

public record Configuration : CommonConfiguration
{
    public string ClientId { get; set; } = string.Empty;
    public string AppSettingsPath { get; set; } = string.Empty;
}