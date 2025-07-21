using System.Collections.Concurrent;
using Amazon.S3;
using aws_backup_common;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IRestoreService
{
    Task<RestoreRun?> LookupRestoreRun(string restoreId, CancellationToken cancellationToken);

    Task ReportS3Storage(string s3Key, S3StorageClass storageClass,
        CancellationToken cancellationToken);

    Task ReportDownloadComplete(DownloadFileFromS3Request request, CancellationToken cancellationToken);
    Task ReportDownloadFailed(DownloadFileFromS3Request request, Exception reason, CancellationToken cancellationToken);
    Task ClearCache(string restoreId, CancellationToken cancellationToken);

    Task<RestoreRun> StartNewRestoreRun(RestoreRequest restoreRequest, string restoreId,
        CancellationToken cancellationToken);

    Task ScheduleFileRecovery(RestoreRun restoreRun, RestoreRequest restoreRequest, FileMetaData fileMetaData,
        CancellationToken cancellationToken);
}

public sealed class RestoreService(
    IDownloadFileMediator downloadMediator,
    IS3StorageClassMediator s3StorageClassMediator,
    IS3Service s3Service,
    ISnsMessageMediator snsMed,
    IRestoreDataStore restoreDataStore,
    ICloudChunkStorage cloudChunkStorage,
    ILogger<RestoreService> logger)
    : IRestoreService
{
    private readonly ConcurrentDictionary<string, RestoreRun> _restoreRunsCache = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _runLocks = new();
    private readonly ConcurrentDictionary<string, ByteArrayKey[]> _s3KeysToChunks = new();

    public async Task<RestoreRun?> LookupRestoreRun(string restoreId, CancellationToken cancellationToken)
    {
        if (_restoreRunsCache.TryGetValue(restoreId, out var cached)) return cached;
        logger.LogDebug("Looking up restore run {RestoreId} in dataStore", restoreId);
        cached = await restoreDataStore.LookupRestoreRun(restoreId, cancellationToken);

        if (cached is null)
        {
            logger.LogDebug("No remote restore run found for {RestoreId}", restoreId);
            return null;
        }

        // cache it
        if (_restoreRunsCache.TryAdd(restoreId, cached)) return cached;

        logger.LogWarning("Failed to cache restore run {RestoreId}", restoreId);
        return null;
    }

    public async Task ReportS3Storage(
        string s3Key,
        S3StorageClass storageClass,
        CancellationToken cancellationToken)
    {
        try
        {
            if (storageClass == S3StorageClass.DeepArchive) return;
            if (!_s3KeysToChunks.TryRemove(s3Key, out var chunks))
            {
                logger.LogInformation("No s3 keys found for {S3Key}", s3Key);
                return;
            }

            var chunkStatuses =
                from ByteArrayKey in chunks
                from runKeyPair in _restoreRunsCache
                let run = runKeyPair.Value
                from requestedFilePair in run.RequestedFiles
                let requestedFile = requestedFilePair.Value
                where requestedFile.CloudChunkDetails.ContainsKey(ByteArrayKey) &&
                      requestedFile.Status is FileRestoreStatus.PendingDeepArchiveRestore
                from chunkDetailsKeyPair in requestedFile.CloudChunkDetails
                select new
                {
                    run.RestoreId,
                    ChunkKey = chunkDetailsKeyPair.Key,
                    ChunkRestore = chunkDetailsKeyPair.Value,
                    RequestedFile = requestedFile
                };

            foreach (var restoreChunkStatus in chunkStatuses)
            {
                var restoreRunId = restoreChunkStatus.RestoreId;
                var chunkRestore = restoreChunkStatus.ChunkRestore;
                var restoreFile = restoreChunkStatus.RequestedFile;
                var chunkHashKey = restoreChunkStatus.ChunkKey;

                chunkRestore.Status = S3ChunkRestoreStatus.ReadyToRestore;
                await restoreDataStore.SaveRestoreChunkStatus(
                    restoreRunId,
                    restoreFile.FilePath,
                    chunkHashKey,
                    S3ChunkRestoreStatus.ReadyToRestore, cancellationToken);

                var chunkStatusesSnapshot = restoreFile.CloudChunkDetails.Values.Select(d => d.Status).ToArray();
                if (chunkStatusesSnapshot.Any(s => s == S3ChunkRestoreStatus.PendingDeepArchiveRestore))
                    continue;

                // if all chunks are now ready, we can enqueue the download
                restoreFile.Status = FileRestoreStatus.PendingS3Download;

                var s3Request = new DownloadFileFromS3Request(
                    restoreRunId,
                    restoreFile.FilePath,
                    restoreFile.CloudChunkDetails.Values.ToArray(),
                    restoreFile.Size,
                    restoreFile.RestorePathStrategy,
                    restoreFile.RestoreFolder)
                {
                    LastModified = restoreFile.LastModified,
                    Created = restoreFile.Created,
                    AclEntries = restoreFile.AclEntries,
                    Owner = restoreFile.Owner,
                    Group = restoreFile.Group,
                    Sha256Checksum = restoreFile.Sha256Checksum
                };
                await downloadMediator.DownloadFileFromS3(s3Request, cancellationToken);

                await restoreDataStore.SaveRestoreFileMetaData(restoreRunId, restoreFile, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error in ReportS3Storage for {Key}", s3Key);
        }
    }

    public async Task ReportDownloadComplete(
        DownloadFileFromS3Request req,
        CancellationToken cancellationToken
    )
    {
        try
        {
            if (!_restoreRunsCache.TryGetValue(req.RestoreId, out var run)) return;
            if (!run.RequestedFiles.TryGetValue(req.FilePath, out var fileMeta)) return;

            fileMeta.Status = FileRestoreStatus.Completed;
            logger.LogInformation("File {File} in run {RunId} marked Completed",
                req.FilePath, req.RestoreId);

            await restoreDataStore.SaveRestoreFileStatus(req.RestoreId, fileMeta.FilePath, FileRestoreStatus.Completed,
                "", cancellationToken);

            // if *all* files done → finalize
            await SaveAndFinalizeIfComplete(run, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error in ReportDownloadComplete for {Run}/{File}",
                req.RestoreId, req.FilePath);
        }
    }

    public async Task ReportDownloadFailed(
        DownloadFileFromS3Request req,
        Exception reason,
        CancellationToken cancellationToken)
    {
        try
        {
            if (!_restoreRunsCache.TryGetValue(req.RestoreId, out var run)) return;
            if (!run.RequestedFiles.TryGetValue(req.FilePath, out var fileMeta)) return;

            fileMeta.Status = FileRestoreStatus.Failed;
            fileMeta.FailedMessage = reason.Message;
            logger.LogWarning(reason, "File {File} in run {Run} failed", req.FilePath, req.RestoreId);

            await snsMed.PublishMessage(
                new SnsMessage($"Download failed: {req}", reason.ToString()), cancellationToken);

            await restoreDataStore.SaveRestoreFileStatus(req.RestoreId, fileMeta.FilePath, FileRestoreStatus.Failed,
                reason.Message, cancellationToken);

            await SaveAndFinalizeIfComplete(run, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error in ReportDownloadFailed for {Run}/{File}",
                req.RestoreId, req.FilePath);
        }
    }

    public async Task ClearCache(string restoreId, CancellationToken cancellationToken)
    {
        await restoreDataStore.RemoveRestoreRequest(restoreId, cancellationToken);
        _restoreRunsCache.TryRemove(restoreId, out _);
    }

    public async Task<RestoreRun> StartNewRestoreRun(RestoreRequest restoreRequest, string restoreId,
        CancellationToken cancellationToken)
    {
        var restoreRun = new RestoreRun
        {
            RestoreId = restoreId,
            RestorePaths = restoreRequest.RestorePaths,
            ArchiveRunId = restoreRequest.ArchiveRunId,
            RequestedAt = restoreRequest.RequestedAt,
            Status = RestoreRunStatus.Processing
        };

        await restoreDataStore.SaveRestoreRequest(restoreRequest, cancellationToken);
        await restoreDataStore.SaveRestoreRun(restoreRun, cancellationToken);

        _restoreRunsCache.TryAdd(restoreId, restoreRun);
        return restoreRun;
    }

    public async Task ScheduleFileRecovery(RestoreRun restoreRun, RestoreRequest request, FileMetaData fileMetaData,
        CancellationToken cancellationToken)
    {
        try
        {
            if (fileMetaData.Status is not FileStatus.UploadComplete)
            {
                logger.LogWarning(
                    "File {File} in run {RunId} is not in UploadComplete status {UploadStatus}, skipping",
                    fileMetaData.LocalFilePath, restoreRun.RestoreId, fileMetaData.Status);
                return;
            }

            var filePath = fileMetaData.LocalFilePath;

            if (restoreRun.RequestedFiles.TryGetValue(filePath, out var restoreFileMeta) &&
                restoreFileMeta.Status is FileRestoreStatus.Completed or FileRestoreStatus.Failed)
            {
                logger.LogInformation("File {File} in run {RunId} already completed or failed, skipping",
                    filePath, restoreRun.RestoreId);
                return;
            }

            if (fileMetaData.Chunks.IsEmpty) return;

            var fileStatus = FileRestoreStatus.PendingDeepArchiveRestore;
            var deepArchiveFiles = new Dictionary<string, HashSet<ByteArrayKey>>();
            var chunkDetails = new ConcurrentDictionary<ByteArrayKey, RestoreChunkDetails>();

            foreach (var (key, dataChunk) in fileMetaData.Chunks)
            {
                var cloudChunk = await cloudChunkStorage.GetCloudChunkDetails(key, cancellationToken);
                if(cloudChunk is null)
                {
                    logger.LogWarning("No cloud chunk found for {Key} in file {File}", key, filePath);
                    continue;
                }
                
                var chunkRestoreStatus = _s3KeysToChunks.ContainsKey(cloudChunk.S3Key) ||
                                         deepArchiveFiles.ContainsKey(cloudChunk.S3Key)
                    ? S3ChunkRestoreStatus.PendingDeepArchiveRestore
                    : await s3Service.ScheduleDeepArchiveRecovery(cloudChunk.S3Key, cancellationToken);

                var restoreChunk = new RestoreChunkDetails(
                    cloudChunk.S3Key,
                    cloudChunk.BucketName,
                    cloudChunk.ChunkSize,
                    cloudChunk.Offset,
                    cloudChunk.Size,
                    cloudChunk.HashKey,
                    dataChunk.ChunkIndex)
                {
                    Status = chunkRestoreStatus
                };
                chunkDetails.TryAdd(new ByteArrayKey(dataChunk.HashKey), restoreChunk);

                if (chunkRestoreStatus is S3ChunkRestoreStatus.ReadyToRestore) continue;

                if (!deepArchiveFiles.TryGetValue(cloudChunk.S3Key, out var detailsList))
                {
                    detailsList = [];
                    deepArchiveFiles[cloudChunk.S3Key] = detailsList;
                }

                var hashKey = new ByteArrayKey(cloudChunk.HashKey);
                detailsList.Add(hashKey);

                logger.LogInformation("Chunk {Chunk} is in DeepArchive, scheduling restore", cloudChunk.S3Key);
            }

            foreach (var (s3Key, detailsList) in deepArchiveFiles)
            {
                if (_s3KeysToChunks.TryGetValue(s3Key, out var chunkDetailsList))
                {
                    var merged = chunkDetailsList.Concat(detailsList).Distinct().ToArray();
                    _s3KeysToChunks[s3Key] = merged;
                    continue;
                }

                _s3KeysToChunks.TryAdd(s3Key, detailsList.ToArray());
                logger.LogInformation("Added S3 key {S3Key} with chunks {Chunks} to local cache",
                    s3Key, string.Join(", ", detailsList));

                await s3StorageClassMediator.QueryStorageClass(s3Key, cancellationToken);
            }

            if (deepArchiveFiles.Count == 0)
            {
                fileStatus = FileRestoreStatus.PendingS3Download;
                var s3Request = new DownloadFileFromS3Request(
                    restoreRun.RestoreId,
                    filePath,
                    chunkDetails.Values.ToArray(),
                    fileMetaData.OriginalSize ?? 0,
                    request.RestorePathStrategy,
                    request.RestoreFolder)
                {
                    LastModified = fileMetaData.LastModified,
                    Created = fileMetaData.Created,
                    AclEntries = fileMetaData.AclEntries,
                    Owner = fileMetaData.Owner,
                    Group = fileMetaData.Group,
                    Sha256Checksum = fileMetaData.HashKey
                };
                await downloadMediator.DownloadFileFromS3(s3Request, cancellationToken);
            }

            restoreFileMeta ??= new RestoreFileMetaData(filePath);

            restoreFileMeta.Status = fileStatus;
            restoreFileMeta.CloudChunkDetails = chunkDetails;
            restoreFileMeta.Size = fileMetaData.OriginalSize ?? 0;
            restoreFileMeta.LastModified = fileMetaData.LastModified;
            restoreFileMeta.Created = fileMetaData.Created;
            restoreFileMeta.AclEntries = fileMetaData.AclEntries;
            restoreFileMeta.Owner = fileMetaData.Owner;
            restoreFileMeta.Group = fileMetaData.Group;
            restoreFileMeta.Sha256Checksum = fileMetaData.HashKey;
            restoreFileMeta.RestorePathStrategy = request.RestorePathStrategy;
            restoreFileMeta.RestoreFolder = request.RestoreFolder;

            restoreRun.RequestedFiles[filePath] = restoreFileMeta;

            await restoreDataStore.SaveRestoreFileMetaData(restoreRun.RestoreId, restoreFileMeta,
                cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error scheduling file recovery for {File} in run {RunId}",
                fileMetaData.LocalFilePath, restoreRun.RestoreId);
        }
    }

    private async Task SaveAndFinalizeIfComplete(RestoreRun run, CancellationToken cancellationToken)
    {
        var semaphore = _runLocks.GetOrAdd(run.RestoreId, _ => new SemaphoreSlim(1, 1));
        await semaphore.WaitAsync(cancellationToken);
        try
        {
            if (run.Status is RestoreRunStatus.AllFilesListed &&
                run.RequestedFiles.Values
                    .All(f => f.Status is FileRestoreStatus.Completed or FileRestoreStatus.Failed))
                await FinalizeRun(run, cancellationToken);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task FinalizeRun(RestoreRun run, CancellationToken cancellationToken)
    {
        if (run.Status is RestoreRunStatus.Completed) return;

        run.Status = RestoreRunStatus.Completed;
        run.CompletedAt = DateTimeOffset.UtcNow;
        logger.LogInformation("Finalizing restore run {RunId}, status {ChunkRestore}",
            run.RestoreId, run.Status);

        // pick the right message
        if (run.RequestedFiles.Values.Any(f => f.Status == FileRestoreStatus.Failed))
            await snsMed.PublishMessage(
                new RestoreCompleteErrorMessage(
                    run.RestoreId,
                    $"Run {run.RestoreId} completed WITH ERRORS",
                    "Some files failed", run),
                cancellationToken);
        else
            await snsMed.PublishMessage(
                new RestoreCompleteMessage(
                    $"Run {run.RestoreId} completed successfully",
                    "All files restored", run),
                cancellationToken);

        await restoreDataStore.SaveRestoreRun(run, cancellationToken);
        await ClearCache(run.RestoreId, cancellationToken);
    }
}