namespace aws_backup;

public enum ArchiveRunStatus
{
    Processing,
    Completed
}

public record FileMetaData(
    byte[] HashId,
    long CompressedSize,
    long OriginalSize,
    string CronSchedule,
    ChunkData[] Chunks
);

public abstract record RunRequest(
    string RunId,
    string PathsToArchive,
    string CronSchedule
);

public record ArchiveRun(
    string RunId,
    string PathsToArchive,
    string CronSchedule)
{
    public long? CompressedSize { get; set; } = null;
    public long? OriginalSize { get; set; } = null;
    public int? TotalFiles { get; set; } = null;
    public int? TotalSkippedFiles { get; set; } = null;
    public ArchiveRunStatus Status { get; set; } = ArchiveRunStatus.Processing;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? CompletedAt { get; set; } = null;
}

public interface IArchiveService
{
    Task<ArchiveRun?> GetArchiveRun(string archiveRequestRunId, CancellationToken stoppingToken);
    Task SaveArchiveRun(ArchiveRun archiveRun, CancellationToken stoppingToken);
    Task<bool> FileRequiresProcessing(string archiveRunId, string filePath, CancellationToken ct);
    Task<bool> ReportProcessingResult(string archiveRunId, FileProcessResult result, CancellationToken ct);
    Task UpdateTimeStamps(byte[] resultFullFileHash, DateTime created, DateTime modified, CancellationToken ct);
    Task UpdateOwnerGroup(byte[] resultFullFileHash, string owner, string group, CancellationToken ct);
    Task UpdateAclEntries(byte[] resultFullFileHash, AclEntry[] aclEntries, CancellationToken ct);
    Task<bool> ChunkRequiresUpload(ChunkData chunk, CancellationToken ct);
    Task RecordFailedFile(string archiveRunId, string filePath, Exception exception, CancellationToken stoppingToken);
}