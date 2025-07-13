using System.Collections.Concurrent;
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

[JsonConverter(
    typeof(JsonDictionaryConverter<ByteArrayKey, CloudChunkDetails, DataChunkManifest>))]
public sealed class DataChunkManifest : ConcurrentDictionary<ByteArrayKey, CloudChunkDetails>;

public sealed record DataChunkDetails(
    string LocalFilePath,
    int ChunkIndex,
    long ChunkSize,
    byte[] HashKey,
    long Size,
    byte[] CompressedHashKey)
{
    public ChunkStatus Status { get; set; } = ChunkStatus.Added;
}

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