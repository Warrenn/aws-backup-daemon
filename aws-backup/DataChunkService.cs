namespace aws_backup;

public record CloudChunkDetails(
    string S3Key, // S3 key for the chunk
    string BucketName, // S3 bucket name
    byte[] Hash, // SHA-256 hash of the chunk
    long Size);

public class DataChunkManifest : SortedDictionary<ByteArrayKey, CloudChunkDetails>
{
    public static DataChunkManifest Current { get; } = new();
}

public record DataChunkDetails(
    string LocalFilePath,
    int ChunkIndex,
    byte[] Hash, // SHA-256 hash of the chunk
    long Size
)
{
    public ByteArrayKey Key { get; } = new(Hash);
}

public interface IDataChunkService
{
    bool ChunkRequiresUpload(DataChunkDetails chunk);
    Task MarkChunkAsUploaded(DataChunkDetails chunk, string key, string bucketName, CancellationToken stoppingToken);
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
        CancellationToken stoppingToken)
    {
        var dataChunkManifest = DataChunkManifest.Current;
        if (dataChunkManifest.ContainsKey(chunk.Key))
            // If the chunk is already in the manifest, we don't need to re-add it
            return;
        var cloudChunkDetails = new CloudChunkDetails(
            key,
            bucketName,
            chunk.Hash,
            chunk.Size);
        dataChunkManifest.Add(chunk.Key, cloudChunkDetails);

        await mediator.SaveChunkManifest(dataChunkManifest, stoppingToken);
    }
}