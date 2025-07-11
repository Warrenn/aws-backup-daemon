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

public enum ChunkStatus
{
    Added,
    Uploaded,
    Failed
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
    public ConcurrentDictionary<ByteArrayKey, ChunkStatus> ChunkStatus { get; set; } = [];
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

    Task UpdateTimeStamps(string runId, string localFilePath, DateTimeOffset created, DateTimeOffset modified,
        CancellationToken cancellationToken);

    Task UpdateOwnerGroup(string runId, string localFilePath, string owner, string group,
        CancellationToken cancellationToken);

    Task UpdateAclEntries(string runId, string filePath, AclEntry[] aclEntries, CancellationToken cancellationToken);

    Task RecordFailedFile(string archiveRunId, string filePath, Exception exception,
        CancellationToken cancellationToken);

    Task RecordFailedChunk(string archiveRunId, string filePath, byte[] chunkHash, Exception exception,
        CancellationToken cancellationToken);

    Task RecordLocalFile(ArchiveRun run, string filePath, CancellationToken cancellationToken);
    bool IsTheFileSkipped(string archiveRunId, string parentFile);
    Task TryRemove(string archiveRunId, CancellationToken cancellationToken);
    Task ResetFileStatus(string runId, string inputPath, CancellationToken cancellationToken);

    Task RecordChunkUpload(string requestArchiveRunId, string parentFile, byte[] chunkHashKey,
        CancellationToken cancellationToken);

    Task AddChunkToFile(string runId, string localFilePath, byte[] chunkHasherHash,
        CancellationToken cancellationToken);

    Task ReportProcessingResult(string runId, FileProcessResult result, CancellationToken cancellationToken);
}

public sealed class CurrentArchiveRuns : ConcurrentDictionary<string, ArchiveRun>;

public class CurrentArchiveRunRequests : ConcurrentDictionary<string, RunRequest>;

public sealed class ArchiveService(
    IS3Service s3Service,
    IArchiveRunMediator runMed,
    ISnsMessageMediator snsMed,
    CurrentArchiveRuns currentRunsCache,
    CurrentArchiveRunRequests currentRequests,
    ILogger<ArchiveService> logger)
    : IArchiveService, IDisposable
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _runLocks = new();

    public async Task<ArchiveRun?> LookupArchiveRun(string runId, CancellationToken ct)
    {
        if (currentRunsCache.TryGetValue(runId, out var cached))
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
            currentRunsCache[runId] = run;
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
            currentRunsCache[runId] = run;
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
        var need = false;
        await UpdateFileMetaData(runId, filePath, (_, meta, _) =>
        {
            need = meta.Status is not FileStatus.Processed;
            logger.LogDebug("DoesFileRequireProcessing({Run},{File}) => {Need}", runId, filePath, need);
            return Task.CompletedTask;
        }, ct);
        return need;
    }

    public async Task UpdateTimeStamps(
        string runId, string localFilePath, DateTimeOffset created, DateTimeOffset modified,
        CancellationToken ct)
    {
        await UpdateFileMetaData(runId, localFilePath, (run, meta, _) =>
        {
            logger.LogDebug("UpdateTimeStamps: {Run}/{File}", runId, localFilePath);
            run.Files[localFilePath] = meta with { Created = created, LastModified = modified };
            return Task.CompletedTask;
        }, ct);
    }

    public async Task UpdateOwnerGroup(
        string runId, string localFilePath, string owner, string group,
        CancellationToken ct)
    {
        await UpdateFileMetaData(runId, localFilePath, (run, meta, _) =>
        {
            logger.LogDebug("UpdateOwnerGroup: {Run}/{File}", runId, localFilePath);
            run.Files[localFilePath] = meta with { Owner = owner, Group = group };
            return Task.CompletedTask;
        }, ct);
    }

    public async Task UpdateAclEntries(
        string runId, string localFilePath, AclEntry[] aclEntries,
        CancellationToken ct)
    {
        await UpdateFileMetaData(runId, localFilePath, (run, meta, _) =>
        {
            logger.LogDebug("UpdateAclEntries: {Run}/{File}", runId, localFilePath);
            run.Files[localFilePath] = meta with { AclEntries = aclEntries };
            return Task.CompletedTask;
        }, ct);
    }

    public async Task RecordFailedFile(
        string runId, string localFilePath, Exception exception,
        CancellationToken ct)
    {
        await UpdateFileMetaData(runId, localFilePath, async (run, meta, token) =>
        {
            logger.LogWarning(exception, "RecordFailedFile: {Run}/{File}", runId, localFilePath);

            meta = meta with { Status = FileStatus.Skipped };
            run.Files[localFilePath] = meta;
            run.SkipReason[localFilePath] = exception.Message;

            // notify via SNS
            await snsMed.PublishMessage(new ExceptionMessage(
                $"File Skipped: {localFilePath} in run {runId}",
                $"Skipped due to: {exception.Message}"), token);
        }, ct);
    }

    public async Task RecordFailedChunk(string archiveRunId, string filePath, byte[] chunkHash, Exception exception,
        CancellationToken cancellationToken)
    {
        await UpdateFileMetaData(archiveRunId, filePath, (run, meta, _) =>
        {
            var hashKey = new ByteArrayKey(chunkHash);
            meta.ChunkStatus.TryUpdate(hashKey, ChunkStatus.Failed, ChunkStatus.Added);
            meta.ChunkStatus.TryUpdate(hashKey, ChunkStatus.Failed, ChunkStatus.Uploaded);

            run.Files[filePath] = meta with
            {
                Status = FileStatus.Skipped
            };
            run.SkipReason[filePath] = exception.Message;
            return Task.CompletedTask;
        }, cancellationToken);
    }

    public async Task RecordLocalFile(
        ArchiveRun run, string localFilePath,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(localFilePath)) return;

        await UpdateFileMetaData(run.RunId, localFilePath, (_, _, _) =>
        {
            logger.LogDebug("RecordLocalFile: {Run}/{File}", run, localFilePath);
            return Task.CompletedTask;
        }, ct);
    }

    public bool IsTheFileSkipped(string runId, string localFilePath)
    {
        if (!currentRunsCache.TryGetValue(runId, out var run)) return false;
        if (!run.Files.TryGetValue(localFilePath, out var meta)) return false;
        return meta.Status == FileStatus.Skipped;
    }

    public async Task TryRemove(string archiveRunId, CancellationToken cancellationToken)
    {
        currentRunsCache.TryRemove(archiveRunId, out _);
        currentRequests.TryRemove(archiveRunId, out _);
        await runMed.SaveCurrentArchiveRunRequests(currentRequests, cancellationToken);
    }

    public async Task ResetFileStatus(string runId, string localFilePath, CancellationToken ct)
    {
        await UpdateFileMetaData(runId, localFilePath, (run, meta, token) =>
        {
            logger.LogDebug("ResetFileStatus: {Run}/{File}", runId, localFilePath);
            foreach (var key in meta.ChunkStatus.Keys)
            {
                meta.ChunkStatus.TryUpdate(key, ChunkStatus.Added, ChunkStatus.Failed);
                meta.ChunkStatus.TryUpdate(key, ChunkStatus.Added, ChunkStatus.Uploaded);
            }

            run.Files[localFilePath] = meta with
            {
                Status = FileStatus.Added
            };
            run.SkipReason.TryRemove(localFilePath, out _);
            return Task.CompletedTask;
        }, ct);
    }

    public async Task RecordChunkUpload(string runId, string localFilePath, byte[] chunkHashKey,
        CancellationToken ct)
    {
        await UpdateFileMetaData(runId, localFilePath, (run, meta, _) =>
        {
            logger.LogDebug("RecordChunkUpload: {Run}/{File}", runId, localFilePath);
            var chunkKey = new ByteArrayKey(chunkHashKey);
            if (meta.ChunkStatus.TryGetValue(chunkKey, out var currentStatus))
                meta.ChunkStatus.TryUpdate(chunkKey, ChunkStatus.Uploaded, currentStatus);
            else
                meta.ChunkStatus.TryAdd(chunkKey, ChunkStatus.Uploaded);
            return Task.CompletedTask;
        }, ct);
    }

    public async Task AddChunkToFile(string runId, string localFilePath, byte[] chunkHasherHash,
        CancellationToken cancellationToken)
    {
        await UpdateFileMetaData(runId, localFilePath, (_, meta, _) =>
        {
            logger.LogDebug("AddChunkToFile: {Run}/{File}", runId, localFilePath);
            var chunkKey = new ByteArrayKey(chunkHasherHash);
            meta.ChunkStatus.TryAdd(chunkKey, ChunkStatus.Added);
            return Task.CompletedTask;
        }, cancellationToken);
    }

    public async Task ReportProcessingResult(string runId, FileProcessResult result,
        CancellationToken cancellationToken)
    {
        var localFilePath = result.LocalFilePath;
        await UpdateFileMetaData(runId, localFilePath, (run, meta, _) =>
        {
            logger.LogDebug("ReportProcessingResult: {Run}/{File}", runId, localFilePath);

            run.Files[localFilePath] = meta with
            {
                CompressedSize = result.Chunks.Sum(c => c.ChunkSize),
                OriginalSize = result.OriginalSize,
                HashKey = result.FullFileHash,
                Chunks = result.Chunks.ToArray()
            };

            return Task.CompletedTask;
        }, cancellationToken);
    }

    public void Dispose()
    {
        foreach (var sem in _runLocks.Values) sem.Dispose();
    }

    private async Task UpdateFileMetaData(
        string runId, string localFilePath, Func<ArchiveRun, FileMetaData, CancellationToken, Task> updateFunc,
        CancellationToken ct)
    {
        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            if (!currentRunsCache.TryGetValue(runId, out var run)) return;

            if (!run.Files.TryGetValue(localFilePath, out var meta))
            {
                meta = new FileMetaData(localFilePath) { Status = FileStatus.Added };
                run.Files.TryAdd(localFilePath, meta);
            }

            await updateFunc(run, meta, ct);
            await SaveAndFinalizeIfComplete(run, ct);
        }
        finally
        {
            lockSlim.Release();
        }
    }

    private static bool AreEqualSets<T>(IEnumerable<T> a, IEnumerable<T> b)
    {
        return new HashSet<T>(a).SetEquals(b);
    }

    private async Task SaveAndFinalizeIfComplete(ArchiveRun run, CancellationToken ct)
    {
        var currentFileData = run.Files.ToArray();
        foreach (var (key, meta) in currentFileData)
        {
            var fileStatus = FileStatus.Added;
            var fileReady =
                !meta.ChunkStatus.IsEmpty &&
                meta.Chunks.Length > 0 &&
                meta.ChunkStatus.Values.All(chunkStatus => chunkStatus is ChunkStatus.Uploaded or ChunkStatus.Failed) &&
                AreEqualSets(meta.ChunkStatus.Keys, meta.Chunks.Select(c => new ByteArrayKey(c.HashKey)));

            var hasFailedChunks = meta.ChunkStatus.Values.Any(chunkStatus => chunkStatus == ChunkStatus.Failed);
            if (fileReady) fileStatus = hasFailedChunks ? FileStatus.Skipped : FileStatus.Processed;
            if (fileStatus is FileStatus.Added ||
                meta.Status is FileStatus.Skipped ||
                fileStatus == meta.Status) continue;

            run.Files[key] = meta with { Status = fileStatus };
            if (fileStatus is FileStatus.Skipped && !run.SkipReason.ContainsKey(key))
            {
                run.SkipReason.TryAdd(key, "File skipped due to chunk failing to upload");
            }
            
            logger.LogDebug("File {File} status updated to {Status}", key, fileStatus);
        }

        // persist current state
        await runMed.SaveArchiveRun(run, ct);

        // check for completion
        if (run.Files.Values.All(f => f.Status is FileStatus.Processed or FileStatus.Skipped))
            await FinalizeRun(run, ct);
    }

    private async Task FinalizeRun(ArchiveRun run, CancellationToken cancellationToken)
    {
        var runId = run.RunId;
        logger.LogInformation("Finalizing runId {RunId}", runId);

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
                $"Archive runId {runId} completed with errors",
                "Some files were skipped.",
                run
            ), cancellationToken);
        else
            await snsMed.PublishMessage(new ArchiveCompleteMessage(
                $"Archive runId {runId} completed successfully",
                "All files processed.",
                run
            ), cancellationToken);

        // persist final
        await runMed.SaveArchiveRun(run, cancellationToken);

        // remove from in‐memory caches
        await TryRemove(runId, cancellationToken);

        // currentRequests.TryRemove(runId, out _);
        logger.LogInformation("Run {RunId} removed from in‐memory cache", runId);
    }
}