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

    public async Task<bool> DoesFileRequireProcessing(
        string runId, string filePath, CancellationToken cancellationToken)
    {
        var fileStatusData = await GetOrCreateFileMetaData(runId, filePath, cancellationToken);
        return fileStatusData.Status is not FileStatus.UploadComplete &&
               fileStatusData.Status is not FileStatus.ChunkingComplete;
    }

    public async Task RecordFailedFile(
        string runId, string localFilePath, Exception exception,
        CancellationToken cancellationToken)
    {
        var updateFileStatusCommand = new UpdateFileStatusCommand(
            runId, localFilePath, FileStatus.Skipped, exception.Message);
        await dataStoreMediator.ExecuteCommand(updateFileStatusCommand, cancellationToken);

        var run = await GetArchiveRun(runId, cancellationToken);
        var fileMetaData = await GetOrCreateFileMetaData(run, localFilePath, cancellationToken);
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

        var updateFileStatusCommand = new UpdateFileStatusCommand(
            runId, localFilePath, FileStatus.Skipped, exception.Message);
        await dataStoreMediator.ExecuteCommand(updateFileStatusCommand, cancellationToken);

        var saveChunkStatusCommand = new SaveChunkStatusCommand(
            runId, localFilePath, chunkKey, ChunkStatus.Failed);
        await dataStoreMediator.ExecuteCommand(saveChunkStatusCommand, cancellationToken);

        var run = await GetArchiveRun(runId, cancellationToken);
        var fileStatusData = await GetOrCreateFileMetaData(run, localFilePath, cancellationToken);
        if (fileStatusData.Chunks.TryGetValue(chunkKey, out var chunkDetails)) chunkDetails.Status = ChunkStatus.Failed;
        fileStatusData.Status = FileStatus.Skipped;
        fileStatusData.SkipReason = exception.Message;

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task<bool> IsTheFileSkipped(string runId, string localFilePath, CancellationToken cancellationToken)
    {
        var fileStatusData = await GetOrCreateFileMetaData(runId, localFilePath, cancellationToken);
        return fileStatusData.Status is FileStatus.Skipped;
    }

    public async Task ClearCache(string archiveRunId, CancellationToken cancellationToken)
    {
        _runCache.TryRemove(archiveRunId, out _);

        var removeRunRequestCommand = new RemoveArchiveRequestCommand(archiveRunId);
        await dataStoreMediator.ExecuteCommand(removeRunRequestCommand, cancellationToken);
    }

    public async Task ResetFileStatus(string runId, string localFilePath, CancellationToken cancellationToken)
    {
        var updateFileStatusCommand = new UpdateFileStatusCommand(
            runId, localFilePath, FileStatus.Added, string.Empty);
        await dataStoreMediator.ExecuteCommand(updateFileStatusCommand, cancellationToken);

        var deleteFileChunksCommand = new DeleteFileChunksCommand(runId, localFilePath);
        await dataStoreMediator.ExecuteCommand(deleteFileChunksCommand, cancellationToken);

        var fileStatusData = await GetOrCreateFileMetaData(runId, localFilePath, cancellationToken);
        fileStatusData.Status = FileStatus.Added;
        fileStatusData.Chunks.Clear();
    }

    public async Task RecordChunkUpload(string runId, string localFilePath, byte[] chunkHashKey,
        CancellationToken cancellationToken)
    {
        var chunkKey = new ByteArrayKey(chunkHashKey);

        var saveChunkStatusCommand = new SaveChunkStatusCommand(
            runId, localFilePath, chunkKey, ChunkStatus.Uploaded);
        await dataStoreMediator.ExecuteCommand(saveChunkStatusCommand, cancellationToken);

        var run = await GetArchiveRun(runId, cancellationToken);
        var fileStatusData = await GetOrCreateFileMetaData(runId, localFilePath, cancellationToken);
        if (fileStatusData.Chunks.TryGetValue(chunkKey, out var chunkDetails))
            chunkDetails.Status = ChunkStatus.Uploaded;

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task AddChunkToFile(string runId, string localFilePath, DataChunkDetails chunkDetails,
        CancellationToken cancellationToken)
    {
        var chunkKey = new ByteArrayKey(chunkDetails.HashKey);

        var saveChunkDetailsCommand = new SaveChunkDetailsCommand(
            runId, localFilePath, chunkDetails);
        await dataStoreMediator.ExecuteCommand(saveChunkDetailsCommand, cancellationToken);

        var run = await GetArchiveRun(runId, cancellationToken);
        var fileMetaData = await GetOrCreateFileMetaData(runId, localFilePath, cancellationToken);
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

        var run = await GetArchiveRun(runId, cancellationToken);

        var fileMetaData = await GetOrCreateFileMetaData(run, localFilePath, cancellationToken);
        fileMetaData.Status = status;
        fileMetaData.SkipReason = skipReason;
        fileMetaData.HashKey = result.FullFileHash;
        fileMetaData.OriginalSize = result.OriginalSize;
        fileMetaData.CompressedSize = result.CompressedSize;

        run.Files[localFilePath] = fileMetaData;

        var saveFileMetaDataCommand = new SaveFileMetaDataCommand(
            runId, fileMetaData);
        await dataStoreMediator.ExecuteCommand(saveFileMetaDataCommand, cancellationToken);

        await SaveAndFinalizeIfComplete(run, cancellationToken);
    }

    public async Task ReportAllFilesListed(ArchiveRun archiveRun, CancellationToken cancellationToken)
    {
        archiveRun.Status = ArchiveRunStatus.AllFilesListed;

        var updateArchiveStatusCommand = new UpdateArchiveStatusCommand(
            archiveRun.RunId, archiveRun.Status);
        await dataStoreMediator.ExecuteCommand(updateArchiveStatusCommand, cancellationToken);

        await SaveAndFinalizeIfComplete(archiveRun, cancellationToken);
    }

    private async Task<ArchiveRun> GetArchiveRun(string runId, CancellationToken cancellationToken)
    {
        if (_runCache.TryGetValue(runId, out var run)) return run;
        run =
            await archiveDataStore.GetArchiveRun(runId, cancellationToken);
        _runCache.TryAdd(runId, run!);
        return run!;
    }

    private async Task<FileMetaData> GetOrCreateFileMetaData(string runId, string filePath,
        CancellationToken cancellationToken)
    {
        var run = await GetArchiveRun(runId, cancellationToken);
        return await GetOrCreateFileMetaData(run, filePath, cancellationToken);
    }

    private async Task<FileMetaData> GetOrCreateFileMetaData(ArchiveRun run, string filePath,
        CancellationToken cancellationToken)
    {
        var runId = run.RunId;
        if (run.Files.TryGetValue(filePath, out var metaData))
            return metaData;

        metaData = await archiveDataStore.GetFileMetaData(runId, filePath, cancellationToken);
        if (metaData is null)
        {
            metaData = new FileMetaData(filePath)
            {
                Status = FileStatus.Added,
                Chunks = new ConcurrentDictionary<ByteArrayKey, DataChunkDetails>()
            };

            var saveFileMetaDataCommand = new SaveFileMetaDataCommand(runId, metaData);
            await dataStoreMediator.ExecuteCommand(saveFileMetaDataCommand, cancellationToken);
        }

        run.Files.TryAdd(filePath, metaData);
        return metaData;
    }


    private async Task SaveAndFinalizeIfComplete(ArchiveRun run, CancellationToken cancellationToken)
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
                run.Files[filePath] = fileMeta;
                skippedFiles++;

                var updateFileStatusCommandSkipped = new UpdateFileStatusCommand(
                    run.RunId, filePath, FileStatus.Skipped, fileMeta.SkipReason);
                await dataStoreMediator.ExecuteCommand(updateFileStatusCommandSkipped, cancellationToken);

                continue;
            }

            fileMeta.Status = FileStatus.UploadComplete;
            run.Files[filePath] = fileMeta;

            var updateFileStatusCommandCompleted = new UpdateFileStatusCommand(
                run.RunId, filePath, FileStatus.UploadComplete, fileMeta.SkipReason);
            await dataStoreMediator.ExecuteCommand(updateFileStatusCommandCompleted, cancellationToken);

            logger.LogDebug("File {File} status updated to UploadComplete", filePath);
        }

        if (totalFiles == 0) isIncomplete = true;

        // Finalize if all files are accounted for
        if (run.Status is ArchiveRunStatus.AllFilesListed && !isIncomplete)
            await FinalizeRun(run, originalSize, compressedSize, skippedFiles, totalFiles, cancellationToken);
    }

    private async Task FinalizeRun(
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
            await ClearCache(runId, cancellationToken);

        // currentRequests.ClearCache(runId, out _);
        logger.LogInformation("Run {RunId} removed from in‐memory cache", runId);
    }
}