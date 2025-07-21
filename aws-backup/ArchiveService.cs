using System.Collections.Concurrent;
using aws_backup_common;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IArchiveService
{
    Task<ArchiveRun?> LookupArchiveRun(string runId, CancellationToken cancellationToken);

    Task<ArchiveRun> StartNewArchiveRun(RunRequest request, CancellationToken cancellationToken);

    Task<bool> DoesFileRequireProcessing(string archiveRunId, string filePath, CancellationToken cancellationToken);

    Task RecordFailedFile(string archiveRunId, string filePath, Exception exception,
        CancellationToken cancellationToken);

    Task RecordFailedChunk(string runId, string localFilePath, byte[] chunkHash, Exception exception,
        CancellationToken cancellationToken);

    Task<bool> IsTheFileSkipped(string archiveRunId, string parentFile, CancellationToken cancellationToken);
    Task ClearCache(string archiveRunId, CancellationToken cancellationToken);
    Task ResetFileStatus(string runId, string inputPath, CancellationToken cancellationToken);

    Task RecordChunkUpload(string requestArchiveRunId, string parentFile, byte[] chunkHashKey,
        CancellationToken cancellationToken);

    Task AddChunkToFile(string runId, string localFilePath, DataChunkDetails chunkDetails,
        CancellationToken cancellationToken);

    Task ReportProcessingResult(string runId, FileProcessResult result, CancellationToken cancellationToken);
    Task ReportAllFilesListed(ArchiveRun archiveRun, CancellationToken cancellationToken);
}

public sealed class ArchiveService(
    ISnsMessageMediator snsMed,
    ILogger<ArchiveService> logger,
    IArchiveDataStore archiveDataStore)
    : IArchiveService
{
    private readonly ConcurrentDictionary<string, ArchiveRun> _runCache = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _runLocks = new();

    public async Task<ArchiveRun?> LookupArchiveRun(string runId, CancellationToken cancellationToken)
    {
        if (_runCache.TryGetValue(runId, out var cached))
        {
            logger.LogDebug("LookupArchiveRun({RunId}) => cached", runId);
            return cached;
        }

        var archiveRun = await archiveDataStore.GetArchiveRun(runId, cancellationToken);
        if (archiveRun is null)
        {
            logger.LogInformation("LookupArchiveRun({RunId}) => not found in DataStore", runId);
            return null;
        }

        _runCache.TryAdd(runId, archiveRun);
        logger.LogInformation("LookupArchiveRun({RunId}) => loaded from DataStore", runId);
        return archiveRun;
    }

    public async Task<ArchiveRun> StartNewArchiveRun(RunRequest request, CancellationToken cancellationToken)
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

        await archiveDataStore.SaveRunRequest(request, cancellationToken);
        await archiveDataStore.SaveArchiveRun(run, cancellationToken);

        return run;
    }

    public async Task<bool> DoesFileRequireProcessing(
        string runId, string filePath, CancellationToken cancellationToken)
    {
        var fileStatusData = await GetFileMetaData(runId, filePath, cancellationToken);
        return fileStatusData.Status is not FileStatus.UploadComplete;
    }
    
    public async Task RecordFailedFile(
        string runId, string localFilePath, Exception exception,
        CancellationToken cancellationToken)
    {
        await archiveDataStore.UpdateFileStatus(runId, localFilePath, FileStatus.Skipped, exception.Message,
            cancellationToken);

        var run = await GetArchiveRun(runId);
        var fileMetaData = await GetFileMetaData(run, localFilePath, cancellationToken);
        fileMetaData.Status = FileStatus.Skipped;
        fileMetaData.SkipReason = exception.Message;

        // notify via SNS
        await snsMed.PublishMessage(new ExceptionMessage(
            $"File Skipped: {localFilePath} in run {runId}",
            $"Skipped due to: {exception.Message}"), cancellationToken);

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task RecordFailedChunk(string runId, string localFilePath, byte[] chunkHash, Exception exception,
        CancellationToken cancellationToken)
    {
        var chunkKey = new ByteArrayKey(chunkHash);

        await archiveDataStore.UpdateFileStatus(runId, localFilePath, FileStatus.Skipped, exception.Message,
            cancellationToken);
        await archiveDataStore.SaveChunkStatus(runId, localFilePath, chunkKey, ChunkStatus.Failed, cancellationToken);

        var run = await GetArchiveRun(runId);
        var fileStatusData = await GetFileMetaData(run, localFilePath, cancellationToken);
        if (fileStatusData.Chunks.TryGetValue(chunkKey, out var chunkDetails)) chunkDetails.Status = ChunkStatus.Failed;
        fileStatusData.Status = FileStatus.Skipped;
        fileStatusData.SkipReason = exception.Message;

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task<bool> IsTheFileSkipped(string runId, string localFilePath, CancellationToken cancellationToken)
    {
        var fileStatusData = await GetFileMetaData(runId, localFilePath, cancellationToken);
        return fileStatusData.Status is FileStatus.Skipped;
    }

    public async Task ClearCache(string archiveRunId, CancellationToken cancellationToken)
    {
        _runCache.TryRemove(archiveRunId, out _);
        _runLocks.TryRemove(archiveRunId, out _);
        await archiveDataStore.RemoveArchiveRequest(archiveRunId, cancellationToken);
    }

    public async Task ResetFileStatus(string runId, string localFilePath, CancellationToken cancellationToken)
    {
        await archiveDataStore.UpdateFileStatus(runId, localFilePath, FileStatus.Added, string.Empty,
            cancellationToken);
        await archiveDataStore.DeleteFileChunks(runId, localFilePath, cancellationToken);

        var fileStatusData = await GetFileMetaData(runId, localFilePath, cancellationToken);
        fileStatusData.Status = FileStatus.Added;
        fileStatusData.Chunks.Clear();
    }

    public async Task RecordChunkUpload(string runId, string localFilePath, byte[] chunkHashKey,
        CancellationToken cancellationToken)
    {
        var chunkKey = new ByteArrayKey(chunkHashKey);
        await archiveDataStore.SaveChunkStatus(runId, localFilePath, chunkKey, ChunkStatus.Uploaded, cancellationToken);

        var run = await GetArchiveRun(runId);
        var fileStatusData = await GetFileMetaData(runId, localFilePath, cancellationToken);
        if (fileStatusData.Chunks.TryGetValue(chunkKey, out var chunkDetails))
            chunkDetails.Status = ChunkStatus.Uploaded;

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task AddChunkToFile(string runId, string localFilePath, DataChunkDetails chunkDetails,
        CancellationToken cancellationToken)
    {
        var chunkKey = new ByteArrayKey(chunkDetails.HashKey);
        await archiveDataStore.SaveChunkDetails(runId, localFilePath, chunkDetails, cancellationToken);

        var run = await GetArchiveRun(runId);
        var fileMetaData = await GetFileMetaData(runId, localFilePath, cancellationToken);
        fileMetaData.Chunks.TryAdd(chunkKey, chunkDetails);

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task ReportProcessingResult(string runId, FileProcessResult result,
        CancellationToken cancellationToken)
    {
        var localFilePath = result.LocalFilePath;
        var status = FileStatus.ChunkingComplete;
        var skipReason = string.Empty;
        if (result.Error is not null)
        {
            status = FileStatus.Skipped;
            skipReason = result.Error.Message;
        }

        await archiveDataStore.UpdateFileStatus(runId, localFilePath, status, skipReason, cancellationToken);
        await archiveDataStore.SaveFileMetaData(runId, localFilePath, result.FullFileHash, result.OriginalSize,
            result.CompressedSize, cancellationToken);

        var run = await GetArchiveRun(runId);
        var fileStatusData = await GetFileMetaData(run, localFilePath, cancellationToken);
        fileStatusData.Status = status;
        fileStatusData.SkipReason = skipReason;

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task ReportAllFilesListed(ArchiveRun archiveRun, CancellationToken cancellationToken)
    {
        archiveRun.Status = ArchiveRunStatus.AllFilesListed;
        
        await archiveDataStore.UpdateArchiveStatus(archiveRun.RunId, archiveRun.Status, cancellationToken);
        await SaveAndFinalizeIfComplete(archiveRun, cancellationToken);
    }

    private async Task<ArchiveRun> GetArchiveRun(string runId)
    {
        if (_runCache.TryGetValue(runId, out var run)) return run;
        run =
            await archiveDataStore.GetArchiveRun(runId, CancellationToken.None);
        _runCache.TryAdd(runId, run!);
        return run!;
    }
    
    private async Task<FileMetaData> GetFileMetaData(string runId, string filePath,
        CancellationToken cancellationToken)
    {
        var run = await GetArchiveRun(runId);
        return await GetFileMetaData(run, filePath, cancellationToken);
    }

    private async Task<FileMetaData> GetFileMetaData(ArchiveRun run, string filePath,
        CancellationToken cancellationToken)
    {
        var runId = run.RunId;
        if (run.Files.TryGetValue(filePath, out var metaData))
            return metaData;

        metaData = await archiveDataStore.GetFileMetaData(runId, filePath, cancellationToken);
        run.Files.TryAdd(filePath, metaData!);
        return metaData!;
    }

    private async Task SaveAndFinalizeIfComplete(ArchiveRun run, CancellationToken cancellationToken)
    {
        var semaphore = _runLocks.GetOrAdd(run.RunId, _ => new SemaphoreSlim(1, 1));
        await semaphore.WaitAsync(cancellationToken);
        try
        {
            await SaveAndFinalizeInternal(run, cancellationToken);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task SaveAndFinalizeInternal(ArchiveRun run, CancellationToken cancellationToken)
    {
        foreach (var (filePath, fileMeta) in run.Files)
        {
            if (fileMeta.Status is not FileStatus.ChunkingComplete)
                continue;

            // Snapshot current chunk statuses
            var chunkStatuses = fileMeta.Chunks.Values.Select(c => c.Status).ToArray();

            if (chunkStatuses.Length == 0)
                continue;

            if (chunkStatuses.Any(s => s == ChunkStatus.Added))
                continue;

            if (chunkStatuses.Any(s => s == ChunkStatus.Failed))
            {
                if (string.IsNullOrWhiteSpace(fileMeta.SkipReason))
                    fileMeta.SkipReason = "File skipped due to chunk failing to upload";

                fileMeta.Status = FileStatus.Skipped;
                continue;
            }

            fileMeta.Status = FileStatus.UploadComplete;
            await archiveDataStore.UpdateFileStatus(
                run.RunId, filePath, FileStatus.UploadComplete, fileMeta.SkipReason, cancellationToken);

            logger.LogDebug("File {File} status updated to UploadComplete", filePath);
        }

        // Finalize if all files are accounted for
        if (run.Status is ArchiveRunStatus.AllFilesListed &&
            run.Files.Values.All(f => f.Status is FileStatus.UploadComplete or FileStatus.Skipped))
        {
            await FinalizeRun(run, cancellationToken);
        }
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
        await archiveDataStore.SaveArchiveRun(run, cancellationToken);

        // remove from in‐memory caches
        await ClearCache(runId, cancellationToken);

        // currentRequests.ClearCache(runId, out _);
        logger.LogInformation("Run {RunId} removed from in‐memory cache", runId);
    }
}