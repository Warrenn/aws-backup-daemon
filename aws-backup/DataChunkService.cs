namespace aws_backup;

public record CloudChunkDetails(
    string S3Key, // S3 key for the chunk
    string BucketName, // S3 bucket name
    byte[] Hash, // SHA-256 hash of the chunk
    long Size);

public class DataChunkManifest : Dictionary<ByteArrayKey, CloudChunkDetails>
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
    public ByteArrayKey Key { get; init; } = new(Hash);
}

public interface IDataChunkService
{
    Task<bool> ChunkRequiresUpload(DataChunkDetails chunk, CancellationToken stoppingToken);
    Task MarkChunkAsUploaded(DataChunkDetails chunk, string key, string bucketName, CancellationToken stoppingToken);
}

public class DataChunkService  : IDataChunkService
{
    public Task<bool> ChunkRequiresUpload(DataChunkDetails chunk, CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }

    public Task MarkChunkAsUploaded(DataChunkDetails chunk, string key, string bucketName,
        CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }
}