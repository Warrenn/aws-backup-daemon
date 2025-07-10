using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace aws_backup;

public interface IChunkManifestMediator
{
    IAsyncEnumerable<S3LocationAndValue<DataChunkManifest>> GetDataChunksManifest(
        CancellationToken cancellationToken);

    Task SaveChunkManifest(DataChunkManifest manifest, CancellationToken cancellationToken);
}

public sealed record CloudChunkDetails(
    string S3Key, // S3 key for the chunk
    string BucketName, // S3 bucket name
    long ChunkSize,
    byte[] Hash);

[JsonConverter(typeof(DataChunkManifestConverter))]
public sealed class DataChunkManifest : ConcurrentDictionary<ByteArrayKey, CloudChunkDetails>;

public sealed record DataChunkDetails(
    string LocalFilePath,
    int ChunkIndex,
    long ChunkSize,
    byte[] HashKey,
    long Size
);

public interface IDataChunkService
{
    bool ChunkAlreadyUploaded(DataChunkDetails chunk);

    Task MarkChunkAsUploaded(DataChunkDetails chunk, string key, string bucketName,
        CancellationToken cancellationToken);
}

public sealed class DataChunkService(
    IChunkManifestMediator mediator,
    DataChunkManifest dataChunkManifest
) : IDataChunkService
{
    public bool ChunkAlreadyUploaded(DataChunkDetails chunk)
    {
        var key = new ByteArrayKey(chunk.HashKey);
        return dataChunkManifest.ContainsKey(key);
    }

    public async Task MarkChunkAsUploaded(DataChunkDetails chunk, string s3Key, string bucketName,
        CancellationToken cancellationToken)
    {
        var hashKey = new ByteArrayKey(chunk.HashKey);
        if (dataChunkManifest.ContainsKey(hashKey))
            // If the chunk is already in the manifest, we don't need to re-add it
            return;
        var cloudChunkDetails = new CloudChunkDetails(
            s3Key,
            bucketName,
            chunk.ChunkSize,
            chunk.HashKey);
        dataChunkManifest.TryAdd(hashKey, cloudChunkDetails);

        await mediator.SaveChunkManifest(dataChunkManifest, cancellationToken);
    }
}

public sealed class DataChunkManifestConverter : JsonConverter<DataChunkManifest>
{
    public override DataChunkManifest Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var manifest = new DataChunkManifest();
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
            var keyBase64 = reader.GetString()!;
            var keyBytes = Convert.FromBase64String(keyBase64);
            var key = new ByteArrayKey(keyBytes);

            reader.Read();
            var details = JsonSerializer.Deserialize<CloudChunkDetails>(ref reader, options)!;

            reader.Read(); // EndArray

            manifest[key] = details;
        }

        return manifest;
    }

    public override void Write(Utf8JsonWriter writer, DataChunkManifest value, JsonSerializerOptions options)
    {
        writer.WriteStartArray();
        foreach (var kv in value)
        {
            writer.WriteStartArray();
            writer.WriteStringValue(
                Convert.ToBase64String(kv.Key.ToArray())); // assuming ByteArrayKey exposes the raw bytes
            JsonSerializer.Serialize(writer, kv.Value, options);
            writer.WriteEndArray();
        }

        writer.WriteEndArray();
    }
}