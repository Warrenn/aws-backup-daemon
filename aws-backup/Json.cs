using System.Text.Json;
using System.Text.Json.Serialization;

namespace aws_backup;

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(RestoreRequest))]
internal partial class SourceGenerationContext : JsonSerializerContext
{
}

public static class Json
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };
}