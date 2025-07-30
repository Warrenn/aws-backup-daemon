using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace aws_backup_common;

public enum RestoreRunStatus
{
    Processing,
    AllFilesListed,
    Completed
}

public enum S3ChunkRestoreStatus
{
    PendingDeepArchiveRestore,
    ReadyToRestore
}

public enum FileRestoreStatus
{
    PendingDeepArchiveRestore,
    PendingS3Download,
    Completed,
    Failed
}

public enum RestorePathStrategy
{
    Flatten,
    Nested
}

public sealed record RestoreChunkDetails(
    string S3Key, // S3 key for the chunk
    string BucketName, // S3 bucket name
    long ChunkSize,
    long Offset,
    long Size,
    byte[] HashKey,
    int Index) : CloudChunkDetails(S3Key, BucketName, ChunkSize, Offset, Size, HashKey)
{
    public S3ChunkRestoreStatus Status { get; set; } = S3ChunkRestoreStatus.PendingDeepArchiveRestore;
}

public sealed record RestoreRequest(
    string ArchiveRunId,
    string RestorePaths,
    DateTimeOffset RequestedAt,
    RestorePathStrategy RestorePathStrategy = RestorePathStrategy.Flatten,
    string? RestoreDestination = null);

public sealed record RestoreFileMetaData(
    string FilePath)
{
    public FileRestoreStatus Status { get; set; } = FileRestoreStatus.PendingDeepArchiveRestore;
    public string? FailedMessage { get; set; }
    public long Size { get; set; }
    public ConcurrentDictionary<ByteArrayKey, RestoreChunkDetails> CloudChunkDetails { get; set; } = [];
    public DateTimeOffset? LastModified { get; set; }
    public DateTimeOffset? Created { get; set; }
    public AclEntry[]? AclEntries { get; set; }
    public string? Owner { get; set; }
    public string? Group { get; set; }
    public byte[]? Sha256Checksum { get; set; }
    public RestorePathStrategy RestorePathStrategy { get; set; }
    public string? RestoreFolder { get; set; }
}

public sealed record DownloadFileFromS3Request(
    string RestoreId,
    string FilePath,
    RestoreChunkDetails[] CloudChunkDetails,
    long Size,
    RestorePathStrategy RestorePathStrategy = RestorePathStrategy.Flatten,
    string? RestoreFolder = null) : RetryState
{
    public DateTimeOffset? LastModified { get; init; }
    public DateTimeOffset? Created { get; init; }
    public AclEntry[]? AclEntries { get; init; }
    public string? Owner { get; init; }
    public string? Group { get; init; }
    public byte[]? Sha256Checksum { get; init; }
}

public sealed class RestoreRun
{
    public required string RestoreId { get; init; }
    public required string RestorePaths { get; init; }
    public required string ArchiveRunId { get; init; }
    public required RestoreRunStatus Status { get; set; } = RestoreRunStatus.Processing;
    public required DateTimeOffset RequestedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? CompletedAt { get; set; }
    [JsonInclude] public ConcurrentDictionary<string, RestoreFileMetaData> RequestedFiles { get; init; } = new();
}