using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace aws_backup_common;

public enum ChunkStatus
{
    Added,
    Uploaded,
    Failed
}

public sealed record CloudChunkDetails(
    string S3Key, // S3 key for the chunk
    string BucketName, // S3 bucket name
    long ChunkSize,
    byte[] Hash);

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

[JsonConverter(
    typeof(JsonDictionaryConverter<ByteArrayKey, CloudChunkDetails, DataChunkManifest>))]
public sealed class DataChunkManifest : ConcurrentDictionary<ByteArrayKey, CloudChunkDetails>;
