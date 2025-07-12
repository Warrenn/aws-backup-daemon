using System.Text.Json;
using System.Text.Json.Serialization;

namespace aws_backup;

[JsonSourceGenerationOptions(
    WriteIndented = true,
    Converters = [typeof(JsonStringEnumConverter)],
    PropertyNameCaseInsensitive = true
)]
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
[JsonSerializable(typeof(CurrentArchiveRunRequests))]
[JsonSerializable(typeof(Configuration))]
[JsonSerializable(typeof(ArchiveRunStatus))]
[JsonSerializable(typeof(FileStatus))]
[JsonSerializable(typeof(ChunkStatus))]
internal partial class SourceGenerationContext : JsonSerializerContext
{
}

public sealed class JsonDictionaryConverter<TKey, TValue, TDict> : JsonConverter<TDict>
    where TDict : IDictionary<TKey, TValue>, new()
    where TKey : notnull
    where TValue : notnull
{

    public override TDict Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        var keyTypeInfo = SourceGenerationContext.Default.GetTypeInfo(typeof(TKey)) ??
                          throw new InvalidOperationException(
                              $"Type {typeof(TKey)} is not registered in the source generation context.");

        var keyConverter = (JsonConverter<TKey>)keyTypeInfo.Converter;

        var valueTypeInfo = SourceGenerationContext.Default.GetTypeInfo(typeof(TValue)) ??
                            throw new InvalidOperationException(
                                $"Type {typeof(TValue)} is not registered in the source generation context.");

        var valueConverter = (JsonConverter<TValue>)valueTypeInfo.Converter;

        var manifest = new TDict();
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException();

        TKey? key = default;
        TValue? value = default;

        while (reader.Read())
        {
            if (reader.TokenType is JsonTokenType.EndObject)
                break;

            if (reader.TokenType == JsonTokenType.PropertyName)
                key = keyConverter.Read(ref reader, typeof(TKey), options) ?? throw new JsonException(
                    $"Expected property name token, but got {reader.TokenType}.");

            if (reader.TokenType == JsonTokenType.StartObject)
                value = valueConverter.Read(ref reader, typeof(TValue), options) ?? throw new JsonException(
                    $"Expected object token for value, but got {reader.TokenType}.");

            if (key is null || value is null) continue;

            manifest[key] = value;
            
            key = default;
            value = default;
        }

        return manifest;
    }

    public override void Write(Utf8JsonWriter writer, TDict value,
        JsonSerializerOptions options)
    {
        var keyTypeInfo = SourceGenerationContext.Default.GetTypeInfo(typeof(TKey)) ??
                          throw new InvalidOperationException(
                              $"Type {typeof(TKey)} is not registered in the source generation context.");
        var keyOptions = keyTypeInfo.Options;

        var keyConverter = (JsonConverter<TKey>)keyTypeInfo.Converter;

        var valueTypeInfo = SourceGenerationContext.Default.GetTypeInfo(typeof(TValue)) ??
                            throw new InvalidOperationException(
                                $"Type {typeof(TValue)} is not registered in the source generation context.");

        var valueOptions = valueTypeInfo.Options;

        var valueConverter = (JsonConverter<TValue>)valueTypeInfo.Converter;

        writer.WriteStartObject();
        foreach (var kv in value)
        {
            keyConverter.WriteAsPropertyName(writer, kv.Key, keyOptions);
            valueConverter.Write(writer, kv.Value, valueOptions);
        }

        writer.WriteEndObject();
    }
}