using System.Collections.Concurrent;
using aws_backup_common;
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
    bool IsTheFileSkipped(string archiveRunId, string parentFile);
    Task TryRemove(string archiveRunId, CancellationToken cancellationToken);
    Task ResetFileStatus(string runId, string inputPath, CancellationToken cancellationToken);

    Task RecordChunkUpload(string requestArchiveRunId, string parentFile, byte[] chunkHashKey,
        CancellationToken cancellationToken);

    Task AddChunkToFile(string runId, string localFilePath, DataChunkDetails chunkDetails,
        CancellationToken cancellationToken);

    Task ReportProcessingResult(string runId, FileProcessResult result, CancellationToken cancellationToken);
    Task ReportAllFilesListed(ArchiveRun archiveRun, CancellationToken cancellationToken);
}

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
            need = meta.Status is not FileStatus.UploadComplete;
            logger.LogDebug("DoesFileRequireProcessing({Run},{File}) => {Need}", runId, filePath, need);
            return Task.FromResult(false);
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
            return Task.FromResult(true);
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
            return Task.FromResult(true);
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
            return Task.FromResult(true);
        }, ct);
    }

    public async Task RecordFailedFile(
        string runId, string localFilePath, Exception exception,
        CancellationToken ct)
    {
        await UpdateFileMetaData(runId, localFilePath, async (run, meta, token) =>
        {
            logger.LogWarning(exception, "RecordFailedFile: {Run}/{File}", runId, localFilePath);

            meta = meta with
            {
                Status = FileStatus.Skipped,
                SkipReason = exception.Message
            };
            run.Files[localFilePath] = meta;

            // notify via SNS
            await snsMed.PublishMessage(new ExceptionMessage(
                $"File Skipped: {localFilePath} in run {runId}",
                $"Skipped due to: {exception.Message}"), token);

            return true;
        }, ct);
    }

    public async Task RecordFailedChunk(string archiveRunId, string filePath, byte[] chunkHash, Exception exception,
        CancellationToken cancellationToken)
    {
        await UpdateFileMetaData(archiveRunId, filePath, (run, meta, _) =>
        {
            var hashKey = new ByteArrayKey(chunkHash);

            meta.Chunks[hashKey] = meta.Chunks[hashKey] with
            {
                Status = ChunkStatus.Failed
            };

            run.Files[filePath] = meta with
            {
                Status = FileStatus.Skipped,
                SkipReason = exception.Message
            };
            return Task.FromResult(true);
        }, cancellationToken);
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
        await UpdateFileMetaData(runId, localFilePath, (run, meta, _) =>
        {
            logger.LogDebug("ResetFileStatus: {Run}/{File}", runId, localFilePath);
            meta.Chunks.Clear();

            run.Files[localFilePath] = meta with
            {
                Status = FileStatus.Added,
                SkipReason = string.Empty
            };
            return Task.FromResult(true);
        }, ct);
    }

    public async Task RecordChunkUpload(string runId, string localFilePath, byte[] chunkHashKey,
        CancellationToken ct)
    {
        await UpdateFileMetaData(runId, localFilePath, (_, meta, _) =>
        {
            logger.LogDebug("RecordChunkUpload: {Run}/{File}", runId, localFilePath);
            var chunkKey = new ByteArrayKey(chunkHashKey);
            if (!meta.Chunks.TryGetValue(chunkKey, out var existingChunk))
            {
                logger.LogWarning("Skipping {FilePath} Chunk {ChunkKey} not found for file in run {RunId}",
                    localFilePath, chunkKey, runId);
                meta.Status = FileStatus.Skipped;
                return Task.FromResult(true);
            }

            existingChunk.Status = ChunkStatus.Uploaded;
            return Task.FromResult(true);
        }, ct);
    }

    public async Task AddChunkToFile(string runId, string localFilePath, DataChunkDetails chunkDetails,
        CancellationToken cancellationToken)
    {
        await UpdateFileMetaData(runId, localFilePath, (_, meta, _) =>
        {
            logger.LogDebug("AddChunkToFile: {Run}/{File}", runId, localFilePath);
            var chunkKey = new ByteArrayKey(chunkDetails.HashKey);
            if (meta.Chunks.TryAdd(chunkKey, chunkDetails)) return Task.FromResult(true);
            logger.LogWarning("Chunk {ChunkKey} already exists for file {FilePath} in run {RunId}",
                chunkKey, localFilePath, runId);
            meta.Chunks[chunkKey] = meta.Chunks[chunkKey] with
            {
                ChunkIndex = chunkDetails.ChunkIndex,
                ChunkSize = chunkDetails.ChunkSize,
                Size = chunkDetails.Size,
                Status = ChunkStatus.Added,
                LocalFilePath = localFilePath
            };
            return Task.FromResult(true);
        }, cancellationToken);
    }

    public async Task ReportProcessingResult(string runId, FileProcessResult result,
        CancellationToken cancellationToken)
    {
        var localFilePath = result.LocalFilePath;
        await UpdateFileMetaData(runId, localFilePath, (run, meta, _) =>
        {
            logger.LogDebug("ReportProcessingResult: {Run}/{File}", runId, localFilePath);
            var status = FileStatus.ChunkingComplete;
            var skipReason = string.Empty;
            if (result.Error is not null)
            {
                status = FileStatus.Skipped;
                skipReason = result.Error.Message;
            }

            run.Files[localFilePath] = meta with
            {
                CompressedSize = result.CompressedSize,
                OriginalSize = result.OriginalSize,
                Status = status,
                HashKey = result.FullFileHash,
                SkipReason = skipReason
            };

            return Task.FromResult(true);
        }, cancellationToken);
    }

    public async Task ReportAllFilesListed(ArchiveRun archiveRun, CancellationToken cancellationToken)
    {
        archiveRun.Status = ArchiveRunStatus.AllFilesListed;
        await SaveAndFinalizeIfComplete(archiveRun, true, cancellationToken);
    }

    public void Dispose()
    {
        foreach (var sem in _runLocks.Values) sem.Dispose();
    }

    private async Task UpdateFileMetaData(
        string runId, string localFilePath, Func<ArchiveRun, FileMetaData, CancellationToken, Task<bool>> updateFunc,
        CancellationToken ct)
    {
        var changesMade = false;
        var lockSlim = _runLocks.GetOrAdd(runId, _ => new SemaphoreSlim(1, 1));
        await lockSlim.WaitAsync(ct);
        try
        {
            if (!currentRunsCache.TryGetValue(runId, out var run)) return;

            if (!run.Files.TryGetValue(localFilePath, out var meta))
            {
                changesMade = true;
                meta = new FileMetaData(localFilePath) { Status = FileStatus.Added };
                run.Files.TryAdd(localFilePath, meta);
            }

            changesMade |= await updateFunc(run, meta, ct);
            await SaveAndFinalizeIfComplete(run, changesMade, ct);
        }
        finally
        {
            lockSlim.Release();
        }
    }

    private async Task SaveAndFinalizeIfComplete(ArchiveRun run, bool changesMade, CancellationToken ct)
    {
        foreach (var (key, meta) in run.Files)
        {
            if (meta.Status is not FileStatus.ChunkingComplete) continue;

            var hasAnAdd = false;
            var hasAnError = false;
            var hasAnElement = false;

            foreach (var chunk in meta.Chunks.Values)
            {
                var chunkStatus = chunk.Status;
                hasAnElement = true;

                if (chunkStatus is ChunkStatus.Added)
                {
                    hasAnAdd = true;
                    break;
                }

                if (chunkStatus is not ChunkStatus.Failed) continue;

                hasAnError = true;
                break;
            }

            if (hasAnAdd || !hasAnElement) continue;

            changesMade = true;
            if (hasAnError && string.IsNullOrWhiteSpace(meta.SkipReason))
                meta.SkipReason = "File skipped due to chunk failing to upload";

            if (hasAnError)
            {
                meta.Status = FileStatus.Skipped;
                continue;
            }

            meta.Status = FileStatus.UploadComplete;
            logger.LogDebug("File {File} status updated to UploadComplete", key);
        }

        // persist current state
        if (changesMade)
            await runMed.SaveArchiveRun(run, ct);

        // check for completion
        if (run.Files.Values.All(f => f.Status is FileStatus.UploadComplete or FileStatus.Skipped))
            await FinalizeRun(run, ct);
    }

    private async Task FinalizeRun(ArchiveRun run, CancellationToken cancellationToken)
    {
        var runId = run.RunId;
        logger.LogInformation("Finalizing runId {RunId}", runId);

        // update summary fields
        run.Status = ArchiveRunStatus.Completed;
        run.CompletedAt = DateTimeOffset.UtcNow;
        run.CompressedSize = run.Files.Values.Where(f => f.Status == FileStatus.UploadComplete)
            .Sum(f => f.CompressedSize ?? 0);
        run.OriginalSize = run.Files.Values.Where(f => f.Status == FileStatus.UploadComplete)
            .Sum(f => f.OriginalSize ?? 0);
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