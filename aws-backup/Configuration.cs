using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;
using aws_backup_common;

namespace aws_backup;

[JsonSerializable(typeof(Configuration))]
public partial class ConfigurationGenerationContext : JsonSerializerContext
{
}

//settings that can change
public sealed record Configuration : CommonConfiguration
{
    [Required(AllowEmptyStrings = false)] public required string ClientId { get; set; }

    [Required(AllowEmptyStrings = false)] public required string CronSchedule { get; set; }

    [Required(AllowEmptyStrings = false)] public required string PathsToArchive { get; set; }
}