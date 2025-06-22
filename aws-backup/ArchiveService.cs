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
    long? CompressedSize,
    long? OriginalSize,
    DateTimeOffset? LastModified,
    DateTimeOffset? Created,
    AclEntry[]? AclEntries,
    string? Owner,
    string? Group,
    FileStatus Status,
    byte[] HashKey,
    DataChunkDetails[] Chunks);

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
    public required Configuration Configuration { get; init; } = new();
    public required DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;

    public DateTimeOffset? CompletedAt { get; set; }
    public long? CompressedSize { get; set; }
    public long? OriginalSize { get; set; }
    public int? TotalFiles { get; set; }
    public int? TotalSkippedFiles { get; set; }
    public Dictionary<string, FileMetaData> Files { get; } = new();
}

public interface IArchiveService
{
    Task<ArchiveRun?> LookupArchiveRun(string runId, CancellationToken stoppingToken);

    Task<ArchiveRun> StartNewArchiveRun(RunRequest request, Configuration configuration,
        CancellationToken stoppingToken);

    Task<bool> FileRequiresProcessing(string archiveRunId, string filePath, CancellationToken ct);
    Task ReportProcessingResult(string archiveRunId, FileProcessResult result, CancellationToken ct);

    Task UpdateTimeStamps(string runId, string localFilePath, DateTimeOffset created, DateTimeOffset modified,
        CancellationToken ct);

    Task UpdateOwnerGroup(string runId, string localFilePath, string owner, string group, CancellationToken ct);
    Task UpdateAclEntries(string runId, string filePath, AclEntry[] aclEntries, CancellationToken ct);
    Task RecordFailedFile(string archiveRunId, string filePath, Exception exception, CancellationToken stoppingToken);
    Task RecordLocalFile(string archiveRunRunId, string filePath, CancellationToken stoppingToken);
    Task CompleteArchiveRun(string archiveRunRunId, CancellationToken stoppingToken);
}

public class ArchiveService(
    IS3Service s3Service,
    IMediator mediator
) : IArchiveService
{
    private ArchiveRun _currentArchiveRun = null!;
    private TaskCompletionSource _tcs = null!;

    public async Task<ArchiveRun?> LookupArchiveRun(string runId, CancellationToken stoppingToken)
    {
        if (await s3Service.RunExists(runId, stoppingToken))
            return await Task.FromResult<ArchiveRun?>(null);
        return await s3Service.GetArchive(runId, stoppingToken);
    }

    public async Task<ArchiveRun> StartNewArchiveRun(RunRequest request, Configuration configuration,
        CancellationToken stoppingToken)
    {
        _currentArchiveRun = new ArchiveRun
        {
            RunId = request.RunId,
            CronSchedule = request.CronSchedule,
            PathsToArchive = request.PathsToArchive,
            Configuration = configuration,
            CreatedAt = DateTimeOffset.UtcNow,
            Status = ArchiveRunStatus.Processing
        };
        _tcs = new TaskCompletionSource();
        await mediator.SaveArchiveRun(_currentArchiveRun, stoppingToken);
        return _currentArchiveRun;
    }

    public async Task<bool> FileRequiresProcessing(string archiveRunId, string filePath, CancellationToken ct)
    {
        if (_currentArchiveRun.Files.TryGetValue(filePath, out var fileMeta))
            return fileMeta.Status is not FileStatus.Processed and not FileStatus.Skipped;
        fileMeta = new FileMetaData(
            filePath,
            Status: FileStatus.Added,
            CompressedSize: null,
            OriginalSize: null,
            LastModified: null,
            Created: null,
            AclEntries: null,
            Owner: null,
            Group: null,
            HashKey: [],
            Chunks: []
        );
        _currentArchiveRun.Files[filePath] = fileMeta;
        await CheckIfRunComplete(ct);
        return false;
    }

    public async Task ReportProcessingResult(string archiveRunId, FileProcessResult result, CancellationToken ct)
    {
        var fileMeta = new FileMetaData(
            result.LocalFilePath,
            Status: FileStatus.Processed,
            CompressedSize: result.Chunks.Sum(c => c.Size),
            OriginalSize: result.OriginalSize,
            LastModified: null,
            Created: null,
            AclEntries: null,
            Owner: null,
            Group: null,
            HashKey: result.FullFileHash,
            Chunks: result.Chunks
        );
        _currentArchiveRun.Files[result.LocalFilePath] = fileMeta;
        await CheckIfRunComplete(ct);
    }

    public async Task UpdateTimeStamps(string runId, string localFilePath, DateTimeOffset created,
        DateTimeOffset modified,
        CancellationToken ct)
    {
        if (!_currentArchiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return;
        var updatedMeta = fileMeta with
        {
            Created = created,
            LastModified = modified
        };
        _currentArchiveRun.Files[localFilePath] = updatedMeta;
        await CheckIfRunComplete(ct);
    }

    public async Task UpdateOwnerGroup(string runId, string localFilePath, string owner, string group,
        CancellationToken ct)
    {
        if (!_currentArchiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return;
        var updatedMeta = fileMeta with
        {
            Owner = owner,
            Group = group
        };
        _currentArchiveRun.Files[localFilePath] = updatedMeta;
        await CheckIfRunComplete(ct);
    }

    public async Task UpdateAclEntries(string runId, string localFilePath, AclEntry[] aclEntries, CancellationToken ct)
    {
        if (!_currentArchiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return;
        var updatedMeta = fileMeta with
        {
            AclEntries = aclEntries
        };
        _currentArchiveRun.Files[localFilePath] = updatedMeta;
        await CheckIfRunComplete(ct);
    }

    public async Task RecordFailedFile(string archiveRunId, string localFilePath, Exception exception,
        CancellationToken stoppingToken)
    {
        if (!_currentArchiveRun.Files.TryGetValue(localFilePath, out var fileMeta))
            fileMeta = new FileMetaData(
                localFilePath,
                Status: FileStatus.Skipped,
                CompressedSize: null,
                OriginalSize: null,
                LastModified: null,
                Created: null,
                AclEntries: null,
                Owner: null,
                Group: null,
                HashKey: [],
                Chunks: []
            );
        var updatedMeta = fileMeta with
        {
            Status = FileStatus.Skipped
        };
        _currentArchiveRun.Files[localFilePath] = updatedMeta;
        await CheckIfRunComplete(stoppingToken);
    }

    public async Task RecordLocalFile(string archiveRunRunId, string localFilePath, CancellationToken stoppingToken)
    {
        if (!_currentArchiveRun.Files.TryGetValue(localFilePath, out var fileMeta))
            fileMeta = new FileMetaData(
                localFilePath,
                Status: FileStatus.Added,
                CompressedSize: null,
                OriginalSize: null,
                LastModified: null,
                Created: null,
                AclEntries: null,
                Owner: null,
                Group: null,
                HashKey: [],
                Chunks: []
            );

        _currentArchiveRun.Files[localFilePath] = fileMeta;
        await CheckIfRunComplete(stoppingToken);
    }

    public async Task CompleteArchiveRun(string archiveRunRunId, CancellationToken stoppingToken)
    {
        await CheckIfRunComplete(stoppingToken);
        if (stoppingToken.IsCancellationRequested && !_tcs.Task.IsCompleted) _tcs.TrySetCanceled(stoppingToken);
        await _tcs.Task;
    }

    private async Task CheckIfRunComplete(CancellationToken stoppingToken)
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

        await mediator.SaveArchiveRun(_currentArchiveRun, stoppingToken);
    }
}

public interface IS3Service
{
    Task<bool> RunExists(string runId, CancellationToken stoppingToken);
    Task<ArchiveRun> GetArchive(string runId, CancellationToken stoppingToken);
}