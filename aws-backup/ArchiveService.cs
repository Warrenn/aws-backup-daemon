using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace aws_backup;

public interface IArchiveRunMediator
{
    IAsyncEnumerable<KeyValuePair<string, ArchiveRun>> GetArchiveRuns(CancellationToken cancellationToken);

    IAsyncEnumerable<KeyValuePair<string, CurrentArchiveRuns>> GetCurrentArchiveRuns(
        CancellationToken cancellationToken);

    Task SaveArchiveRun(ArchiveRun currentArchiveRun, CancellationToken cancellationToken);
    Task SaveCurrentArchiveRuns(CurrentArchiveRuns currentArchiveRuns, CancellationToken cancellationToken);
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
    [JsonInclude] public ConcurrentDictionary<string, string> SkipReason { get; init; } = new();
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

    Task RecordLocalFile(ArchiveRun archiveRun, string filePath, CancellationToken cancellationToken);
    Task CompleteArchiveRun(string archiveRunId, CancellationToken cancellationToken);
    bool IsTheFileSkipped(string archiveRunId, string parentFile);
}

[JsonConverter(typeof(JsonDictionaryConverter<ArchiveRun>))]
public class CurrentArchiveRuns : ConcurrentDictionary<string, ArchiveRun>
{
    public static CurrentArchiveRuns Current { get; } = new();
}

public class ArchiveService(
    IS3Service s3Service,
    IArchiveRunMediator mediator,
    CurrentArchiveRuns currentArchiveRuns
) : IArchiveService
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _tcs = [];

    public async Task<ArchiveRun?> LookupArchiveRun(string runId, CancellationToken cancellationToken)
    {
        if (currentArchiveRuns.TryGetValue(runId, out var archiveRun)) return archiveRun;
        if (!await s3Service.RunExists(runId, cancellationToken)) return null;

        archiveRun = await s3Service.GetArchive(runId, cancellationToken);
        if (!currentArchiveRuns.TryAdd(runId, archiveRun)) archiveRun = currentArchiveRuns[runId];

        await mediator.SaveCurrentArchiveRuns(currentArchiveRuns, cancellationToken);
        return archiveRun;
    }

    public async Task<ArchiveRun> StartNewArchiveRun(RunRequest request, CancellationToken cancellationToken)
    {
        var archiveRun = new ArchiveRun
        {
            RunId = request.RunId,
            CronSchedule = request.CronSchedule,
            PathsToArchive = request.PathsToArchive,
            CreatedAt = DateTimeOffset.UtcNow,
            Status = ArchiveRunStatus.Processing
        };
        var tcs = new TaskCompletionSource();

        if (!_tcs.TryAdd(request.RunId, tcs))
            _tcs[request.RunId] = tcs;
        if (!currentArchiveRuns.TryAdd(request.RunId, archiveRun))
            currentArchiveRuns[request.RunId] = archiveRun;

        await mediator.SaveCurrentArchiveRuns(currentArchiveRuns, cancellationToken);
        await mediator.SaveArchiveRun(archiveRun, cancellationToken);
        return archiveRun;
    }

    public async Task<bool> DoesFileRequireProcessing(string archiveRunId, string filePath,
        CancellationToken cancellationToken)
    {
        var archiveRun = await LookupArchiveRun(archiveRunId, cancellationToken);
        if (archiveRun is null) return false;

        if (archiveRun.Files.TryGetValue(filePath, out var fileMeta))
            return fileMeta.Status is not FileStatus.Processed and not FileStatus.Skipped;

        fileMeta = new FileMetaData(filePath)
        {
            Status = FileStatus.Added
        };
        archiveRun.Files[filePath] = fileMeta;
        await CheckIfRunComplete(archiveRun, cancellationToken);
        return true;
    }

    public async Task ReportProcessingResult(string archiveRunId, FileProcessResult result,
        CancellationToken cancellationToken)
    {
        var archiveRun = await LookupArchiveRun(archiveRunId, cancellationToken);
        if (archiveRun is null) return;

        var filePath = result.LocalFilePath;
        if (!archiveRun.Files.TryGetValue(filePath, out var fileMeta)) return;

        fileMeta.Status = FileStatus.Processed;
        fileMeta.CompressedSize = result.Chunks.Sum(c => c.Size);
        fileMeta.OriginalSize = result.OriginalSize;
        fileMeta.HashKey = result.FullFileHash;
        fileMeta.Chunks = result.Chunks;

        await CheckIfRunComplete(archiveRun, cancellationToken);
    }

    public async Task UpdateTimeStamps(string runId, string localFilePath, DateTimeOffset created,
        DateTimeOffset modified,
        CancellationToken cancellationToken)
    {
        var archiveRun = await LookupArchiveRun(runId, cancellationToken);
        if (archiveRun is null) return;

        if (!archiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return;
        var updatedMeta = fileMeta with
        {
            Created = created,
            LastModified = modified
        };
        archiveRun.Files[localFilePath] = updatedMeta;
        await CheckIfRunComplete(archiveRun, cancellationToken);
    }

    public async Task UpdateOwnerGroup(string runId, string localFilePath, string owner, string group,
        CancellationToken cancellationToken)
    {
        var archiveRun = await LookupArchiveRun(runId, cancellationToken);
        if (archiveRun is null) return;

        if (!archiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return;
        var updatedMeta = fileMeta with
        {
            Owner = owner,
            Group = group
        };
        archiveRun.Files[localFilePath] = updatedMeta;
        await CheckIfRunComplete(archiveRun, cancellationToken);
    }

    public async Task UpdateAclEntries(string runId, string localFilePath, AclEntry[] aclEntries,
        CancellationToken cancellationToken)
    {
        var archiveRun = await LookupArchiveRun(runId, cancellationToken);
        if (archiveRun is null) return;

        if (!archiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return;
        var updatedMeta = fileMeta with
        {
            AclEntries = aclEntries
        };
        archiveRun.Files[localFilePath] = updatedMeta;
        await CheckIfRunComplete(archiveRun, cancellationToken);
    }

    public async Task RecordFailedFile(string archiveRunId, string localFilePath, Exception exception,
        CancellationToken cancellationToken)
    {
        var archiveRun = await LookupArchiveRun(archiveRunId, cancellationToken);
        if (archiveRun is null) return;

        if (!archiveRun.Files.TryGetValue(localFilePath, out var fileMeta))
            fileMeta = new FileMetaData(localFilePath);

        fileMeta.Status = FileStatus.Skipped;
        archiveRun.Files[localFilePath] = fileMeta;

        archiveRun.SkipReason[localFilePath] = exception.Message;
        await CheckIfRunComplete(archiveRun, cancellationToken);
    }

    public async Task RecordLocalFile(ArchiveRun archiveRun, string localFilePath, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(localFilePath)) return;
        if (archiveRun.Files.ContainsKey(localFilePath)) return;

        var fileMeta = new FileMetaData(localFilePath)
        {
            Status = FileStatus.Added
        };

        if (!archiveRun.Files.TryAdd(localFilePath, fileMeta)) return;
        await mediator.SaveArchiveRun(archiveRun, cancellationToken);
    }

    public async Task CompleteArchiveRun(string archiveRunId, CancellationToken cancellationToken)
    {
        var archiveRun = await LookupArchiveRun(archiveRunId, cancellationToken);
        if (archiveRun is null) return;

        await CheckIfRunComplete(archiveRun, cancellationToken);
        if (!_tcs.TryGetValue(archiveRunId, out var tc)) tc = new TaskCompletionSource();
        if (cancellationToken.IsCancellationRequested && !tc.Task.IsCompleted) tc.TrySetCanceled(cancellationToken);
        await tc.Task;
    }

    public bool IsTheFileSkipped(string archiveRunId, string localFilePath)
    {
        if (!currentArchiveRuns.TryGetValue(archiveRunId, out var archiveRun)) return false;
        if (!archiveRun.Files.TryGetValue(localFilePath, out var fileMeta)) return false;
        return fileMeta.Status == FileStatus.Skipped;
    }

    private async Task CheckIfRunComplete(ArchiveRun archiveRun, CancellationToken cancellationToken)
    {
        var complete =
            archiveRun.Files.Values.All(f => f.Status is FileStatus.Processed or FileStatus.Skipped);

        if (!_tcs.TryGetValue(archiveRun.RunId, out var tc)) tc = new TaskCompletionSource();
        if (complete)
        {
            archiveRun.Status = ArchiveRunStatus.Completed;
            archiveRun.CompletedAt = DateTimeOffset.UtcNow;
            archiveRun.CompressedSize = archiveRun.Files.Values
                .Where(f => f.Status == FileStatus.Processed)
                .Sum(f => f.CompressedSize ?? 0);
            archiveRun.OriginalSize = archiveRun.Files.Values
                .Where(f => f.Status == FileStatus.Processed)
                .Sum(f => f.OriginalSize ?? 0);
            archiveRun.TotalFiles = archiveRun.Files.Count;
            archiveRun.TotalSkippedFiles = archiveRun.Files.Values
                .Count(f => f.Status == FileStatus.Skipped);

            if (currentArchiveRuns.TryRemove(archiveRun.RunId, out _))
                await mediator.SaveCurrentArchiveRuns(currentArchiveRuns, cancellationToken);

            tc.TrySetResult();
        }

        await mediator.SaveArchiveRun(archiveRun, cancellationToken);
    }
}