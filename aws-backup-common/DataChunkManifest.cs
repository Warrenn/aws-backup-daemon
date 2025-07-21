namespace aws_backup_common;

public enum ChunkStatus
{
    Added,
    Uploaded,
    Failed
}

public record CloudChunkDetails(
    string S3Key, // S3 key for the chunk
    string BucketName, // S3 bucket name
    long ChunkSize,
    long Offset,
    long Size,
    byte[] HashKey);

public sealed record DataChunkDetails(
    string LocalFilePath,
    int ChunkIndex,
    long ChunkSize,
    byte[] HashKey,
    long Size)
{
    public ChunkStatus Status { get; set; } = ChunkStatus.Added;
}