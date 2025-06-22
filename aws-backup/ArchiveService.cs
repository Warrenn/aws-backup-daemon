namespace aws_backup;

public enum ArchiveRunStatus
{
    Processing,
    Completed
}

public enum FileStatus
{
    Added,
    Processed,
    Skipped
}


public record AclEntry(
    string Identity, 
    string Permissions, 
    string Type);

public record FileMetaData(
    string LocalFilePath,
    long CompressedSize,
    long OriginalSize,
    DateTimeOffset LastModified,
    DateTimeOffset Created,
    AclEntry[] AclEntries,
    string Owner,
    string Group,
    FileStatus Status,
    ByteArrayKey? HashKey,
    DataChunkDetails[]? Chunks);

public record RunRequest(
    string RunId,
    string PathsToArchive,
    string CronSchedule);

public record ArchiveRun(
    string RunId,
    string PathsToArchive,
    string CronSchedule)
{
    public Configuration Configuration { get; set; } = new();
    public long? CompressedSize { get; set; } = null;
    public long? OriginalSize { get; set; } = null;
    public int? TotalFiles { get; set; } = null;
    public int? TotalSkippedFiles { get; set; } = null;
    public FileMetaData[] Files { get; set; } = [];
    public ArchiveRunStatus Status { get; init; } = ArchiveRunStatus.Processing;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? CompletedAt { get; set; } = null;
}

public interface IArchiveService
{
    Task<ArchiveRun?> LookupArchiveRun(string archiveRequestRunId, CancellationToken stoppingToken);
    Task StartNewArchiveRun(ArchiveRun archiveRun, Configuration configuration, CancellationToken stoppingToken);
    Task<bool> FileRequiresProcessing(string archiveRunId, string filePath, CancellationToken ct);
    Task<bool> ReportProcessingResult(string archiveRunId, FileProcessResult result, CancellationToken ct);
    Task UpdateTimeStamps(string runId, string fullFilePath, DateTimeOffset created, DateTimeOffset modified,
        CancellationToken ct);
    Task UpdateOwnerGroup(string runId, string localFilePath, string owner, string group, CancellationToken ct);
    Task UpdateAclEntries(string runId, string filePath, AclEntry[] aclEntries, CancellationToken ct);
    Task RecordFailedFile(string archiveRunId, string filePath, Exception exception, CancellationToken stoppingToken);
    Task RecordLocalFile(string archiveRunRunId, string filePath, CancellationToken stoppingToken);
    Task CompleteArchiveRun(string archiveRunRunId, CancellationToken stoppingToken);
}

public class ArchiveService : IArchiveService
{
    public Task<ArchiveRun?> LookupArchiveRun(string archiveRequestRunId, CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }

    public Task StartNewArchiveRun(ArchiveRun archiveRun, Configuration configuration, CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }

    public Task<bool> FileRequiresProcessing(string archiveRunId, string filePath, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task<bool> ReportProcessingResult(string archiveRunId, FileProcessResult result, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task UpdateTimeStamps(string runId, string fullFilePath, DateTimeOffset created, DateTimeOffset modified,
        CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task UpdateOwnerGroup(string runId, string localFilePath, string owner, string group, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task UpdateAclEntries(string runId, string filePath, AclEntry[] aclEntries, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task<bool> ChunkRequiresUpload(DataChunkDetails chunk, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public Task RecordFailedFile(string archiveRunId, string filePath, Exception exception,
        CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }

    public Task RecordLocalFile(string archiveRunRunId, string filePath, CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }

    public Task CompleteArchiveRun(string archiveRunRunId, CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }
}