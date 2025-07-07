using System.Collections.Concurrent;
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
[JsonSerializable(typeof(AwsConfiguration))]
[JsonSerializable(typeof(CurrentRestoreRequests))]
[JsonSerializable(typeof(CurrentArchiveRuns))]
[JsonSerializable(typeof(Configuration))]
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

public sealed class JsonDictionaryConverter<T> : JsonConverter<ConcurrentDictionary<string, T>>
{
    public override ConcurrentDictionary<string, T> Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        var manifest = new ConcurrentDictionary<string, T>();
        if (reader.TokenType != JsonTokenType.StartArray)
            throw new JsonException();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                break;

            // Each entry is [ keyAsBase64, detailsObj ]
            if (reader.TokenType != JsonTokenType.StartArray)
                throw new JsonException();

            reader.Read();
            var key = reader.GetString()!;
            reader.Read();

            var details = JsonSerializer.Deserialize<T>(ref reader, options)!;

            reader.Read(); // EndArray

            manifest[key] = details;
        }

        return manifest;
    }

    public override void Write(Utf8JsonWriter writer, ConcurrentDictionary<string, T> value,
        JsonSerializerOptions options)
    {
        writer.WriteStartArray();
        foreach (var kv in value)
        {
            writer.WriteStartArray();
            writer.WriteStringValue(kv.Key); // assuming ByteArrayKey exposes the raw bytes
            JsonSerializer.Serialize(writer, kv.Value, options);
            writer.WriteEndArray();
        }

        writer.WriteEndArray();
    }
}