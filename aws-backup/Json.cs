using System.Text.Json;
using System.Text.Json.Serialization;

namespace aws_backup;

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(RestoreRequest))]
[JsonSerializable(typeof(ArchiveRun))]
[JsonSerializable(typeof(FileMetaData))]
[JsonSerializable(typeof(RunRequest))]
[JsonSerializable(typeof(DataChunkManifest))]
[JsonSerializable(typeof(CloudChunkDetails))]
[JsonSerializable(typeof(DataChunkDetails))]
[JsonSerializable(typeof(ByteArrayKey))]
[JsonSerializable(typeof(AclEntry))]
[JsonSerializable(typeof(S3RestoreChunkManifest))]
[JsonSerializable(typeof(RestoreFileMetaData))]
[JsonSerializable(typeof(RestoreRun))]
internal partial class SourceGenerationContext : JsonSerializerContext
{
}

public static class Json
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = true
    };
}