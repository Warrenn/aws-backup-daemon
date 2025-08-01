using System.Collections.Concurrent;
using aws_backup_common;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IArchiveService
{
    Task<ArchiveRun?> LookupArchiveRun(string runId, CancellationToken cancellationToken);

    Task<ArchiveRun> StartNewArchiveRun(RunRequest request, CancellationToken cancellationToken);

    Task<(bool, FileMetaData)> DoesFileRequireProcessing(ArchiveRun archiveRun, string filePath,
        CancellationToken cancellationToken);

    Task RecordFailedFile(ArchiveRun archiveRun, FileMetaData fileMetaData, Exception exception,
        CancellationToken cancellationToken);

    Task RecordFailedChunk(ArchiveRun archiveRun, FileMetaData fileMetaData, DataChunkDetails details,
        Exception exception,
        CancellationToken cancellationToken);

    Task ClearCache(ArchiveRun archiveRun, CancellationToken cancellationToken);

    Task RecordChunkUpload(ArchiveRun archiveRun, FileMetaData fileMetaData, DataChunkDetails details,
        CancellationToken cancellationToken);

    Task AddChunkToFile(ArchiveRun archiveRun, FileMetaData fileMetaData, DataChunkDetails chunkDetails,
        CancellationToken cancellationToken);

    Task ReportProcessingResult(ArchiveRun archiveRun, FileProcessResult result,
        CancellationToken cancellationToken);

    Task<bool> ReportAllFilesListed(ArchiveRun archiveRun, CancellationToken cancellationToken);
}

public sealed class ArchiveService(
    ISnsMessageMediator snsMed,
    ILogger<ArchiveService> logger,
    IDataStoreMediator dataStoreMediator,
    IArchiveDataStore archiveDataStore)
    : IArchiveService
{
    private readonly ConcurrentDictionary<string, ArchiveRun> _runCache = new();

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

        var saveRunRequestCommand = new SaveRunRequestCommand(request);
        await dataStoreMediator.ExecuteCommand(saveRunRequestCommand, cancellationToken);

        var saveArchiveRunCommand = new SaveArchiveRunCommand(run);
        await dataStoreMediator.ExecuteCommand(saveArchiveRunCommand, cancellationToken);

        return run;
    }

    public async Task RecordFailedFile(
        ArchiveRun run, FileMetaData fileMetaData, Exception exception,
        CancellationToken cancellationToken)
    {
        var updateFileStatusCommand = new UpdateFileStatusCommand(
            run.RunId, fileMetaData.LocalFilePath, FileStatus.Skipped, exception.Message);
        await dataStoreMediator.ExecuteCommand(updateFileStatusCommand, cancellationToken);

        fileMetaData.Status = FileStatus.Skipped;
        fileMetaData.SkipReason = exception.Message;

        // notify via SNS
        await snsMed.PublishMessage(new ExceptionMessage(
            $"File Skipped: {fileMetaData.LocalFilePath} in run {run.RunId}",
            $"Skipped due to: {exception.Message}"), cancellationToken);

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task RecordFailedChunk(ArchiveRun run, FileMetaData fileMetaData, DataChunkDetails details,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var updateFileStatusCommand = new UpdateFileStatusCommand(
            run.RunId, fileMetaData.LocalFilePath, FileStatus.Skipped, exception.Message);
        await dataStoreMediator.ExecuteCommand(updateFileStatusCommand, cancellationToken);

        var chunkKey = new ByteArrayKey(details.HashKey);
        var saveChunkStatusCommand = new SaveChunkStatusCommand(
            run.RunId, fileMetaData.LocalFilePath, chunkKey, ChunkStatus.Failed);
        await dataStoreMediator.ExecuteCommand(saveChunkStatusCommand, cancellationToken);

        details.Status = ChunkStatus.Failed;

        fileMetaData.Status = FileStatus.Skipped;
        fileMetaData.SkipReason = exception.Message;

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }


    public async Task ClearCache(ArchiveRun archiveRun, CancellationToken cancellationToken)
    {
        _runCache.TryRemove(archiveRun.RunId, out _);

        var removeRunRequestCommand = new RemoveArchiveRequestCommand(archiveRun.RunId);
        await dataStoreMediator.ExecuteCommand(removeRunRequestCommand, cancellationToken);
    }

    public async Task RecordChunkUpload(ArchiveRun run, FileMetaData fileStatusData, DataChunkDetails chunkDetails,
        CancellationToken cancellationToken)
    {
        var chunkKey = new ByteArrayKey(chunkDetails.HashKey);

        chunkDetails.Status = ChunkStatus.Uploaded;

        var saveChunkStatusCommand = new SaveChunkStatusCommand(
            run.RunId, fileStatusData.LocalFilePath, chunkKey, ChunkStatus.Uploaded);
        await dataStoreMediator.ExecuteCommand(saveChunkStatusCommand, cancellationToken);

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task<bool> ReportAllFilesListed(ArchiveRun archiveRun, CancellationToken cancellationToken)
    {
        var run = await archiveDataStore.GetArchiveRun(archiveRun.RunId, cancellationToken) ?? archiveRun;
        run.Status = ArchiveRunStatus.AllFilesListed;
        return await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task AddChunkToFile(ArchiveRun run, FileMetaData fileMetaData, DataChunkDetails chunkDetails,
        CancellationToken cancellationToken)
    {
        var chunkKey = new ByteArrayKey(chunkDetails.HashKey);
        fileMetaData.Chunks.TryAdd(chunkKey, chunkDetails);

        var saveChunkDetailsCommand = new SaveChunkDetailsCommand(
            run.RunId, fileMetaData.LocalFilePath, chunkDetails);
        await dataStoreMediator.ExecuteCommand(saveChunkDetailsCommand, cancellationToken);

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task ReportProcessingResult(ArchiveRun run, FileProcessResult result,
        CancellationToken cancellationToken)
    {
        var status = FileStatus.ChunkingComplete;
        var skipReason = string.Empty;
        if (result.Error is not null)
        {
            status = FileStatus.Skipped;
            skipReason = result.Error.Message;
        }

        var fileMetaData = result.FileMetaData;
        if (fileMetaData.Status is FileStatus.Added) fileMetaData.Status = status;

        if (string.IsNullOrWhiteSpace(fileMetaData.SkipReason)) fileMetaData.SkipReason = skipReason;

        var saveFileMetaDataCommand = new SaveFileMetaDataCommand(run.RunId, fileMetaData);
        await dataStoreMediator.ExecuteCommand(saveFileMetaDataCommand, cancellationToken);

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task<(bool, FileMetaData)> DoesFileRequireProcessing(ArchiveRun archiveRun, string filePath,
        CancellationToken cancellationToken)
    {
        var fileStatusData = await GetOrCreateFileMetaData(archiveRun, filePath, cancellationToken);
        return (fileStatusData.Status is not FileStatus.UploadComplete, fileStatusData);
    }

    private async Task<FileMetaData> GetOrCreateFileMetaData(ArchiveRun run, string filePath,
        CancellationToken cancellationToken)
    {
        var runId = run.RunId;
        if (run.Files.TryGetValue(filePath, out var metaData))
            return metaData;

        metaData = await archiveDataStore.GetFileMetaData(runId, filePath, cancellationToken) ??
                   new FileMetaData(filePath)
                   {
                       Status = FileStatus.Added,
                       Chunks = new ConcurrentDictionary<ByteArrayKey, DataChunkDetails>()
                   };

        run.Files.TryAdd(filePath, metaData);
        return metaData;
    }

    private async Task<bool> SaveAndFinalizeIfComplete(ArchiveRun run, CancellationToken cancellationToken)
    {
        var snapshot = run.Files.ToArray();
        var isIncomplete = false;
        var originalSize = 0L;
        var compressedSize = 0L;
        var skippedFiles = 0;
        var totalFiles = 0;

        foreach (var (filePath, fileMeta) in snapshot)
        {
            totalFiles++;

            switch (fileMeta.Status)
            {
                case FileStatus.Added:
                    isIncomplete = true;
                    continue;
                case FileStatus.Skipped:
                    skippedFiles++;
                    continue;
                case FileStatus.UploadComplete:
                    originalSize += fileMeta.OriginalSize ?? 0;
                    compressedSize += fileMeta.CompressedSize ?? 0;
                    continue;
            }

            // Snapshot current chunk statuses
            var chunkStatuses = fileMeta.Chunks.Values.Select(c => c.Status).ToArray();

            if (chunkStatuses.Length == 0 || chunkStatuses.Any(s => s == ChunkStatus.Added))
            {
                isIncomplete = true;
                continue;
            }

            if (chunkStatuses.Any(s => s == ChunkStatus.Failed))
            {
                if (string.IsNullOrWhiteSpace(fileMeta.SkipReason))
                    fileMeta.SkipReason = "File skipped due to chunk failing to upload";

                fileMeta.Status = FileStatus.Skipped;
                skippedFiles++;

                continue;
            }

            fileMeta.Status = FileStatus.UploadComplete;

            logger.LogDebug("File {File} status updated to UploadComplete", filePath);
        }

        logger.LogInformation("ArchiveService.SaveAndFinalizeIfComplete: totalFiles:{totalFiles} isIncomplete:{isIncomplete} Status:{status}",
                              totalFiles, isIncomplete, Enum.GetName(ArchiveRunStatus.AllFilesListed));
        if (totalFiles == 0) isIncomplete = true;
        if (run.Status is not ArchiveRunStatus.AllFilesListed || isIncomplete) return false;

        // Finalize if all files are accounted for
        await SummarizeAndSave(run, originalSize, compressedSize, skippedFiles, totalFiles, cancellationToken);
        return true;
    }

    private async Task SummarizeAndSave(
        ArchiveRun run,
        long originalSize,
        long compressedSize,
        int skippedFiles,
        int totalFiles,
        CancellationToken cancellationToken)
    {
        var runId = run.RunId;
        logger.LogInformation("Finalizing runId {RunId}", runId);

        // update summary fields
        run.Status = ArchiveRunStatus.Completed;
        run.CompletedAt = DateTimeOffset.UtcNow;
        run.CompressedSize = compressedSize;
        run.OriginalSize = originalSize;
        run.TotalFiles = totalFiles;
        run.TotalSkippedFiles = skippedFiles;

        // publish summary
        if (skippedFiles > 0)
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
        var saveRunCommand = new SaveArchiveRunCommand(run);
        await dataStoreMediator.ExecuteCommand(saveRunCommand, cancellationToken);

        // remove from in‐memory caches
        if (run.Status is ArchiveRunStatus.Completed)
            await ClearCache(run, cancellationToken);

        // currentRequests.ClearCache(runId, out _);
        logger.LogInformation("Run {RunId} removed from in‐memory cache", runId);
    }
}