using System.Collections.Concurrent;
using Amazon.S3;

namespace aws_backup;

public record CloudChunkDetails(
    string S3Key, // S3 key for the chunk
    string BucketName, // S3 bucket name
    ByteArrayKey Hash);

public class DataChunkManifest : ConcurrentDictionary<ByteArrayKey, CloudChunkDetails>
{
    public static DataChunkManifest Current { get; } = new();
}

public record DataChunkDetails(
    string LocalFilePath,
    int ChunkIndex,
    ByteArrayKey Key,
    long Size
);

public interface IDataChunkService
{
    bool ChunkRequiresUpload(DataChunkDetails chunk);

    Task MarkChunkAsUploaded(DataChunkDetails chunk, string key, string bucketName,
        CancellationToken cancellationToken);
}

public class DataChunkService(
    IMediator mediator
) : IDataChunkService
{
    public bool ChunkRequiresUpload(DataChunkDetails chunk)
    {
        var dataChunkManifest = DataChunkManifest.Current;
        return dataChunkManifest.ContainsKey(chunk.Key);
    }

    public async Task MarkChunkAsUploaded(DataChunkDetails chunk, string key, string bucketName,
        CancellationToken cancellationToken)
    {
        var dataChunkManifest = DataChunkManifest.Current;
        if (dataChunkManifest.ContainsKey(chunk.Key))
            // If the chunk is already in the manifest, we don't need to re-add it
            return;
        var cloudChunkDetails = new CloudChunkDetails(
            key,
            bucketName,
            chunk.Key);
        dataChunkManifest.TryAdd(chunk.Key, cloudChunkDetails);

        await mediator.SaveChunkManifest(dataChunkManifest, cancellationToken);
    }
}