using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace aws_backup_common;

public enum ArchiveRunStatus
{
    Processing,
    AllFilesListed,
    Completed
}

public enum FileStatus
{
    Added,
    ChunkingComplete,
    UploadComplete,
    Skipped
}

public sealed record AclEntry(
    string Identity,
    string Permissions,
    string Type);

public sealed record FileMetaData(
    string LocalFilePath
)
{
    public AclEntry[]? AclEntries { get; set; }
    public long? CompressedSize { get; set; }
    public DateTimeOffset? Created { get; set; }
    public string? Group { get; set; }
    public byte[] HashKey { get; set; } = [];
    public DateTimeOffset? LastModified { get; set; }
    public long? OriginalSize { get; set; }
    public string? Owner { get; set; }

    public FileStatus Status { get; set; } = FileStatus.Added;

    public ConcurrentDictionary<ByteArrayKey, DataChunkDetails> Chunks { get; set; } = [];
    public string SkipReason { get; set; } = "";
}

public sealed record RunRequest(
    string RunId,
    string PathsToArchive,
    string CronSchedule);

public sealed class ArchiveRun
{
    public required string PathsToArchive { get; init; }
    public required string RunId { get; init; }
    public required string CronSchedule { get; init; }
    public required ArchiveRunStatus Status { get; set; } = ArchiveRunStatus.Processing;
    public required DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;

    public DateTimeOffset? CompletedAt { get; set; }
    public long? CompressedSize { get; set; }
    public long? OriginalSize { get; set; }
    public int? TotalFiles { get; set; }
    public int? TotalSkippedFiles { get; set; }

    [JsonConverter(typeof(JsonDictionaryConverter<string, FileMetaData, ConcurrentDictionary<string, FileMetaData>>))]
    [JsonInclude]
    public ConcurrentDictionary<string, FileMetaData> Files { get; init; } = new();
}

public sealed class CurrentArchiveRuns : ConcurrentDictionary<string, ArchiveRun>;

public class CurrentArchiveRunRequests : ConcurrentDictionary<string, RunRequest>;