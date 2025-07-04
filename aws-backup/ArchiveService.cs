using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IArchiveRunMediator
{
    IAsyncEnumerable<S3LocationAndValue<ArchiveRun>> GetArchiveRuns(CancellationToken cancellationToken);

    IAsyncEnumerable<S3LocationAndValue<CurrentArchiveRunRequests>> GetCurrentArchiveRunRequests(
        CancellationToken cancellationToken);

    Task SaveArchiveRun(ArchiveRun currentArchiveRun, CancellationToken cancellationToken);

    Task SaveCurrentArchiveRunRequests(CurrentArchiveRunRequests currentArchiveRuns,
        CancellationToken cancellationToken);
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
    public DataChunkDetails[] Chunks { get; set; } = [];
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
    bool IsTheFileSkipped(string archiveRunId, string parentFile);
}

[JsonConverter(typeof(JsonDictionaryConverter<ArchiveRun>))]
public sealed class CurrentArchiveRuns : ConcurrentDictionary<string, ArchiveRun>;

[JsonConverter(typeof(JsonDictionaryConverter<RunRequest>))]
public class CurrentArchiveRunRequests : ConcurrentDictionary<string, RunRequest>;

public sealed class ArchiveService(
    IS3Service s3Service,
    IArchiveRunMediator runMed,
    ISnsMessageMediator snsMed,
    CurrentArchiveRuns currentRuns,
    CurrentArchiveRunRequests currentRequests,
    ILogger<ArchiveService> logger)
    : IArchiveService, IDisposable
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _runLocks = new();
    
    public async Task<ArchiveRun?> LookupArchiveRun(string runId, CancellationToken ct)
    {
        
        if (currentRuns.TryGetValue(runId, out var cached))
        {
            logger.LogDebug("LookupArchiveRun({RunId}) => cached", runId);
            return cached;
        }

        if (!await s3Service.RunExists(runId, ct))
        {
            logger.LogInformation("LookupArchiveRun({RunId}) => not found in S3", runId);
            return null;
        }

        var run = await s3Service.GetArchive(runId, ct);
        logger.LogInformation("LookupArchiveRun({RunId}) => loaded from S3", runId);

        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            currentRuns[runId] = run;
            currentRequests[runId] = new RunRequest(runId, run.PathsToArchive, run.CronSchedule);
            await runMed.SaveCurrentArchiveRunRequests(currentRequests, ct);
        }
        finally
        {
            lockSlim.Release();
        }

        return run;
    }

    public async Task<ArchiveRun> StartNewArchiveRun(RunRequest request, CancellationToken ct)
    {
        
        var runId = request.RunId;
        var run = new ArchiveRun
        {
            RunId = runId,
            CronSchedule = request.CronSchedule,
            PathsToArchive = request.PathsToArchive,
            CreatedAt = DateTimeOffset.UtcNow,
            Status = ArchiveRunStatus.Processing
        };

        logger.LogInformation("Starting new archive run {RunId}", runId);

        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            currentRuns[runId] = run;
            currentRequests[runId] = request;
            await runMed.SaveCurrentArchiveRunRequests(currentRequests, ct);
            await runMed.SaveArchiveRun(run, ct);
        }
        finally
        {
            lockSlim.Release();
        }

        return run;
    }

    public async Task<bool> DoesFileRequireProcessing(
        string runId, string filePath, CancellationToken ct)
    {
        
        var run = await LookupArchiveRun(runId, ct);
        if (run is null) return false;

        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            if (run.Files.TryGetValue(filePath, out var meta))
            {
                var need = meta.Status is not (FileStatus.Processed or FileStatus.Skipped);
                logger.LogDebug("DoesFileRequireProcessing({Run},{File}) => {Need}", runId, filePath, need);
                return need;
            }

            logger.LogDebug("DoesFileRequireProcessing({Run},{File}) => Added", runId, filePath);
            run.Files[filePath] = new FileMetaData(filePath) { Status = FileStatus.Added };
            await SaveAndFinalizeIfComplete(run, ct);
            return true;
        }
        finally
        {
            lockSlim.Release();
        }
    }

    public async Task ReportProcessingResult(
        string runId, FileProcessResult result, CancellationToken ct)
    {
        
        var run = await LookupArchiveRun(runId, ct);
        if (run is null) return;

        var filePath = result.LocalFilePath;
        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            if (!run.Files.TryGetValue(filePath, out var meta)) return;

            logger.LogInformation("ReportProcessingResult: {Run}/{File} => Processed", runId, filePath);
            meta = meta with
            {
                Status = FileStatus.Processed,
                CompressedSize = result.Chunks.Sum(c => c.Size),
                OriginalSize = result.OriginalSize,
                HashKey = result.FullFileHash,
                Chunks = result.Chunks
            };
            run.Files[filePath] = meta;

            await SaveAndFinalizeIfComplete(run, ct);
        }
        finally
        {
            lockSlim.Release();
        }
    }

    public async Task UpdateTimeStamps(
        string runId, string localFilePath, DateTimeOffset created, DateTimeOffset modified,
        CancellationToken ct)
    {
        
        var run = await LookupArchiveRun(runId, ct);
        if (run is null) return;

        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            if (!run.Files.TryGetValue(localFilePath, out var meta)) return;

            logger.LogDebug("UpdateTimeStamps: {Run}/{File}", runId, localFilePath);
            run.Files[localFilePath] = meta with { Created = created, LastModified = modified };
            await SaveAndFinalizeIfComplete(run, ct);
        }
        finally
        {
            lockSlim.Release();
        }
    }

    public async Task UpdateOwnerGroup(
        string runId, string localFilePath, string owner, string group,
        CancellationToken ct)
    {
        
        var run = await LookupArchiveRun(runId, ct);
        if (run is null) return;

        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            if (!run.Files.TryGetValue(localFilePath, out var meta)) return;

            logger.LogDebug("UpdateOwnerGroup: {Run}/{File}", runId, localFilePath);
            run.Files[localFilePath] = meta with { Owner = owner, Group = group };
            await SaveAndFinalizeIfComplete(run, ct);
        }
        finally
        {
            lockSlim.Release();
        }
    }

    public async Task UpdateAclEntries(
        string runId, string localFilePath, AclEntry[] aclEntries,
        CancellationToken ct)
    {
        
        var run = await LookupArchiveRun(runId, ct);
        if (run is null) return;

        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            if (!run.Files.TryGetValue(localFilePath, out var meta)) return;

            logger.LogDebug("UpdateAclEntries: {Run}/{File}", runId, localFilePath);
            run.Files[localFilePath] = meta with { AclEntries = aclEntries };
            await SaveAndFinalizeIfComplete(run, ct);
        }
        finally
        {
            lockSlim.Release();
        }
    }

    public async Task RecordFailedFile(
        string runId, string localFilePath, Exception exception,
        CancellationToken ct)
    {
        
        var run = await LookupArchiveRun(runId, ct);
        if (run is null) return;

        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            logger.LogWarning(exception, "RecordFailedFile: {Run}/{File}", runId, localFilePath);

            var meta = run.Files.GetValueOrDefault(localFilePath)
                       ?? new FileMetaData(localFilePath);
            meta = meta with { Status = FileStatus.Skipped };
            run.Files[localFilePath] = meta;
            run.SkipReason[localFilePath] = exception.Message;

            // notify via SNS
            await snsMed.PublishMessage(new ExceptionMessage(
                $"File Skipped: {localFilePath} in run {runId}",
                $"Skipped due to: {exception.Message}"
            ), ct);

            await SaveAndFinalizeIfComplete(run, ct);
        }
        finally
        {
            lockSlim.Release();
        }
    }

    public async Task RecordLocalFile(
        ArchiveRun run, string localFilePath,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(localFilePath)) return;

        var runId = run.RunId;
        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            if (run.Files.ContainsKey(localFilePath)) return;

            logger.LogDebug("RecordLocalFile: {Run}/{File}", runId, localFilePath);
            run.Files[localFilePath] = new FileMetaData(localFilePath) { Status = FileStatus.Added };
            await runMed.SaveArchiveRun(run, ct);
        }
        finally
        {
            lockSlim.Release();
        }
    }

    public bool IsTheFileSkipped(string runId, string localFilePath)
    {
        if (!currentRuns.TryGetValue(runId, out var run)) return false;
        if (!run.Files.TryGetValue(localFilePath, out var meta)) return false;
        return meta.Status == FileStatus.Skipped;
    }

    public void Dispose()
    {
        foreach (var sem in _runLocks.Values) sem.Dispose();
    }

    private async Task SaveAndFinalizeIfComplete(ArchiveRun run, CancellationToken ct)
    {
        // persist current state
        await runMed.SaveArchiveRun(run, ct);

        // check for completion
        if (run.Files.Values.All(f => f.Status is FileStatus.Processed or FileStatus.Skipped))
            await FinalizeRun(run, ct);
    }

    private async Task FinalizeRun(ArchiveRun run, CancellationToken ct)
    {
        var runId = run.RunId;
        logger.LogInformation("Finalizing run {RunId}", runId);

        // update summary fields
        run.Status = ArchiveRunStatus.Completed;
        run.CompletedAt = DateTimeOffset.UtcNow;
        run.CompressedSize = run.Files.Values.Where(f => f.Status == FileStatus.Processed)
            .Sum(f => f.CompressedSize ?? 0);
        run.OriginalSize = run.Files.Values.Where(f => f.Status == FileStatus.Processed).Sum(f => f.OriginalSize ?? 0);
        run.TotalFiles = run.Files.Count;
        run.TotalSkippedFiles = run.Files.Values.Count(f => f.Status == FileStatus.Skipped);

        // publish summary
        if (run.TotalSkippedFiles > 0)
            await snsMed.PublishMessage(new ArchiveCompleteErrorMessage(
                runId,
                $"Archive run {runId} completed with errors",
                "Some files were skipped.",
                run
            ), ct);
        else
            await snsMed.PublishMessage(new ArchiveCompleteMessage(
                runId,
                $"Archive run {runId} completed successfully",
                "All files processed.",
                run
            ), ct);

        // persist final
        await runMed.SaveArchiveRun(run, ct);

        // remove from in‐memory caches
        currentRuns.TryRemove(runId, out _);
        currentRequests.TryRemove(runId, out _);
        logger.LogInformation("Run {RunId} removed from in‐memory cache", runId);
    }
}