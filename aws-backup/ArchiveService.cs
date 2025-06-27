using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace aws_backup;

public interface IArchiveRunMediator
{
    IAsyncEnumerable<(ArchiveRun archiveRun, string key)> GetArchiveState(CancellationToken cancellationToken);
    ValueTask SaveArchiveRun(ArchiveRun currentArchiveRun, CancellationToken cancellationToken);
}

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
    public DataChunkDetails[] Chunks { get; set; } = [];
}

public record RunRequest(
    string RunId,
    string PathsToArchive,
    string CronSchedule);

public class ArchiveRun
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

    [JsonInclude] public ConcurrentDictionary<string, FileMetaData> Files { get; init; } = new();
}

public interface IArchiveService
{
    Task<ArchiveRun?> LookupArchiveRun(string runId, CancellationToken cancellationToken);

    Task<ArchiveRun> StartNewArchiveRun(RunRequest request, CancellationToken cancellationToken);

    Task<bool> DoesFileRequireProcessing(string archiveRunId, string filePath, CancellationToken cancellationToken);
    Task ReportProcessingResult(string archiveRunId, FileProcessResult result, CancellationToken cancellationToken);

    Task UpdateTimeStamps(string runId, string localFilePath, DateTimeOffset created, DateTimeOffset modified,
        CancellationToken cancellationToken);

    Task UpdateOwnerGroup(string runId, string localFilePath, string owner, string group,
        CancellationToken cancellationToken);

    Task UpdateAclEntries(string runId, string filePath, AclEntry[] aclEntries, CancellationToken cancellationToken);

    Task RecordFailedFile(string archiveRunId, string filePath, Exception exception,
        CancellationToken cancellationToken);

    Task RecordLocalFile(string archiveRunRunId, string filePath, CancellationToken cancellationToken);
    Task CompleteArchiveRun(string archiveRunRunId, CancellationToken cancellationToken);
    bool IsTheFileSkipped(string parentFile);
}

public class ArchiveService(
    IS3Service s3Service,
    IArchiveRunMediator mediator
) : IArchiveService
{
    private ArchiveRun _currentArchiveRun = null!;
    private TaskCompletionSource _tcs = null!;

    public async Task<ArchiveRun?> LookupArchiveRun(string runId, CancellationToken cancellationToken)
    {
        //first look it up local cache
        if (await s3Service.RunExists(runId, cancellationToken))
            return await Task.FromResult<ArchiveRun?>(null);

        //add it to local cache if not complete
        return await s3Service.GetArchive(runId, cancellationToken);
    }

    public async Task<ArchiveRun> StartNewArchiveRun(RunRequest request, CancellationToken cancellationToken)
    {
        _currentArchiveRun = new ArchiveRun
        {
            RunId = request.RunId,
            CronSchedule = request.CronSchedule,
            PathsToArchive = request.PathsToArchive,
            CreatedAt = DateTimeOffset.UtcNow,
            Status = ArchiveRunStatus.Processing
        };
        _tcs = new TaskCompletionSource();
        await mediator.SaveArchiveRun(_currentArchiveRun, cancellationToken);
        return _currentArchiveRun;
    }

    public async Task<bool> DoesFileRequireProcessing(string archiveRunId, string filePath,
        CancellationToken cancellationToken)
    {
        if (_currentArchiveRun.Files.TryGetValue(filePath, out var fileMeta))
            return fileMeta.Status is not FileStatus.Processed and not FileStatus.Skipped;

        fileMeta = new FileMetaData(filePath)
        {
            Status = FileStatus.Added
        };
        _currentArchiveRun.Files[filePath] = fileMeta;
        await CheckIfRunComplete(cancellationToken);
        return true;
    }

    public async Task ReportProcessingResult(string archiveRunId, FileProcessResult result,
        CancellationToken cancellationToken)
    {
        var filePath = result.LocalFilePath;
        if (!_currentArchiveRun.Files.TryGetValue(filePath, out var fileMeta)) return;

        fileMeta.Status = FileStatus.Processed;
        fileMeta.CompressedSize = result.Chunks.Sum(c => c.Size);
        fileMeta.OriginalSize = result.OriginalSize;
        fileMeta.HashKey = result.FullFileHash;
        fileMeta.Chunks = result.Chunks;

        await CheckIfRunComplete(cancellationToken);
    }

    public async Task UpdateTimeStamps(string runId, string localFilePath, DateTimeOffset created,
        DateTimeOffset modified,
        CancellationToken cancellationToken)
    {
        if (!_currentArchiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return;
        var updatedMeta = fileMeta with
        {
            Created = created,
            LastModified = modified
        };
        _currentArchiveRun.Files[localFilePath] = updatedMeta;
        await CheckIfRunComplete(cancellationToken);
    }

    public async Task UpdateOwnerGroup(string runId, string localFilePath, string owner, string group,
        CancellationToken cancellationToken)
    {
        if (!_currentArchiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return;
        var updatedMeta = fileMeta with
        {
            Owner = owner,
            Group = group
        };
        _currentArchiveRun.Files[localFilePath] = updatedMeta;
        await CheckIfRunComplete(cancellationToken);
    }

    public async Task UpdateAclEntries(string runId, string localFilePath, AclEntry[] aclEntries,
        CancellationToken cancellationToken)
    {
        if (!_currentArchiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return;
        var updatedMeta = fileMeta with
        {
            AclEntries = aclEntries
        };
        _currentArchiveRun.Files[localFilePath] = updatedMeta;
        await CheckIfRunComplete(cancellationToken);
    }

    public async Task RecordFailedFile(string archiveRunId, string localFilePath, Exception exception,
        CancellationToken cancellationToken)
    {
        if (!_currentArchiveRun.Files.TryGetValue(localFilePath, out var fileMeta))
            fileMeta = new FileMetaData(localFilePath);

        fileMeta.Status = FileStatus.Skipped;
        _currentArchiveRun.Files[localFilePath] = fileMeta;
        await CheckIfRunComplete(cancellationToken);
    }

    public async Task RecordLocalFile(string archiveRunRunId, string localFilePath, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(localFilePath)) return;
        if (_currentArchiveRun.Files.TryGetValue(localFilePath, out _)) return;

        var fileMeta = new FileMetaData(localFilePath)
        {
            Status = FileStatus.Added
        };

        _currentArchiveRun.Files[localFilePath] = fileMeta;
        await CheckIfRunComplete(cancellationToken);
    }

    public async Task CompleteArchiveRun(string archiveRunRunId, CancellationToken cancellationToken)
    {
        await CheckIfRunComplete(cancellationToken);
        if (cancellationToken.IsCancellationRequested && !_tcs.Task.IsCompleted) _tcs.TrySetCanceled(cancellationToken);
        await _tcs.Task;
    }

    public bool IsTheFileSkipped(string localFilePath)
    {
        if (!_currentArchiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return false;
        return fileMeta.Status == FileStatus.Skipped;
    }

    private async Task CheckIfRunComplete(CancellationToken cancellationToken)
    {
        var complete = _currentArchiveRun.Files.Values.All(f => f.Status is FileStatus.Processed or FileStatus.Skipped);
        if (complete)
        {
            _currentArchiveRun.Status = ArchiveRunStatus.Completed;
            _currentArchiveRun.CompletedAt = DateTimeOffset.UtcNow;
            _currentArchiveRun.CompressedSize = _currentArchiveRun.Files.Values
                .Where(f => f.Status == FileStatus.Processed)
                .Sum(f => f.CompressedSize ?? 0);
            _currentArchiveRun.OriginalSize = _currentArchiveRun.Files.Values
                .Where(f => f.Status == FileStatus.Processed)
                .Sum(f => f.OriginalSize ?? 0);
            _currentArchiveRun.TotalFiles = _currentArchiveRun.Files.Count;
            _currentArchiveRun.TotalSkippedFiles = _currentArchiveRun.Files.Values
                .Count(f => f.Status == FileStatus.Skipped);

            _tcs.TrySetResult();
        }

        await mediator.SaveArchiveRun(_currentArchiveRun, cancellationToken);
    }
}