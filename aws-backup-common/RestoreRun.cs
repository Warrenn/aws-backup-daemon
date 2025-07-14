using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace aws_backup_common;

public enum RestoreRunStatus
{
    Processing,
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

public sealed record RestoreRequest(
    string ArchiveRunId,
    string RestorePaths,
    DateTimeOffset RequestedAt);

public sealed record RestoreFileMetaData(
    ByteArrayKey[] Chunks,
    string FilePath,
    long Size
)
{
    public DateTimeOffset? LastModified { get; set; }
    public DateTimeOffset? Created { get; set; }
    public AclEntry[]? AclEntries { get; set; }
    public string? Owner { get; set; }
    public string? Group { get; set; }
    public byte[]? Checksum { get; set; }
    public FileRestoreStatus Status { get; set; } = FileRestoreStatus.PendingDeepArchiveRestore;
}

public sealed record DownloadFileFromS3Request(
    string RestoreId,
    string FilePath,
    CloudChunkDetails[] CloudChunkDetails,
    long Size) : RetryState
{
    public DateTimeOffset? LastModified { get; init; }
    public DateTimeOffset? Created { get; init; }
    public AclEntry[]? AclEntries { get; init; }
    public string? Owner { get; init; }
    public string? Group { get; init; }
    public byte[]? Checksum { get; init; }
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
    [JsonInclude] public ConcurrentDictionary<string, string> FailedFiles { get; init; } = new();
}

[JsonConverter(typeof(JsonDictionaryConverter<string, RestoreRequest, CurrentRestoreRequests>))]
public class CurrentRestoreRequests : ConcurrentDictionary<string, RestoreRequest>;
public sealed class S3RestoreChunkManifest : ConcurrentDictionary<ByteArrayKey, S3ChunkRestoreStatus>;