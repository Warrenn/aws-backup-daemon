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
    long OffsetInS3BatchFile,
    long CompressedSize,
    long Size,
    byte[] HashId);

public sealed record DataChunkDetails(
    string LocalFilePath,
    long CompressedSize,
    long Offset,
    long Size,
    byte[] HashId)
{
    public ChunkStatus Status { get; set; } = ChunkStatus.Added;
}