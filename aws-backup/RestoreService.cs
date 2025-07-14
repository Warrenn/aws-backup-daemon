using System.Collections.Concurrent;
using Amazon.S3;
using aws_backup_common;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IRestoreService
{
    Task<RestoreRun?> LookupRestoreRun(string restoreId, CancellationToken cancellationToken);
    Task InitiateRestoreRun(RestoreRequest request, RestoreRun restoreRun, CancellationToken cancellationToken);

    Task ReportS3Storage(string bucketId, string s3Key, S3StorageClass storageClass,
        CancellationToken cancellationToken);

    Task ReportDownloadComplete(DownloadFileFromS3Request request, CancellationToken cancellationToken);
    Task ReportDownloadFailed(DownloadFileFromS3Request request, Exception reason, CancellationToken cancellationToken);
}

public interface IRestoreManifestMediator
{
    Task SaveRestoreManifest(S3RestoreChunkManifest currentManifest, CancellationToken cancellationToken);

    IAsyncEnumerable<S3LocationAndValue<S3RestoreChunkManifest>> GetRestoreManifest(
        CancellationToken cancellationToken);
}

public interface IRestoreRunMediator
{
    IAsyncEnumerable<S3LocationAndValue<RestoreRun>> GetRestoreRuns(CancellationToken cancellationToken);
    Task SaveRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken);
}

public interface IDownloadFileMediator
{
    Task DownloadFileFromS3(DownloadFileFromS3Request downloadFileFromS3Request,
        CancellationToken cancellationToken);

    IAsyncEnumerable<DownloadFileFromS3Request> GetDownloadFileRequests(CancellationToken cancellationToken);
}


public sealed class RestoreService(
    IDownloadFileMediator mediator,
    IRestoreRunMediator runMed,
    IRestoreManifestMediator manifestMed,
    IRestoreRequestsMediator requestsMed,
    ISnsMessageMediator snsMed,
    IS3Service s3Service,
    S3RestoreChunkManifest restoreManifest,
    CurrentRestoreRequests restoreRequests,
    DataChunkManifest chunkManifest,
    ILogger<RestoreService> logger)
    : IRestoreService
{
    // instance‐scoped state
    private readonly ConcurrentDictionary<string, RestoreRun> _restoreRunsCache = new();

    // per‐run locks to serialize multi‐threaded updates
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _runLocks = new();

    public async Task<RestoreRun?> LookupRestoreRun(string restoreId, CancellationToken ct)
    {
        if (_restoreRunsCache.TryGetValue(restoreId, out var cached)) return cached;

        if (!await s3Service.RestoreExists(restoreId, ct))
        {
            logger.LogDebug("No remote restore run found for {RestoreId}", restoreId);
            return null;
        }

        var run = await s3Service.GetRestoreRun(restoreId, ct);
        _restoreRunsCache[restoreId] = run;
        logger.LogInformation("Loaded restore run {RestoreId} from S3", restoreId);
        return run;
    }

    public async Task ReportS3Storage(
        string bucketId,
        string s3Key,
        S3StorageClass storageClass,
        CancellationToken ct
    )
    {
        try
        {
            // locate our chunk
            var chunk = chunkManifest.Values
                .FirstOrDefault(d => d.S3Key == s3Key && d.BucketName == bucketId);
            if (chunk is null) return;

            var key = new ByteArrayKey(chunk.Hash);
            if (!restoreManifest.TryGetValue(key, out var status)) return;

            // dispatch based on current + incoming
            if (status == S3ChunkRestoreStatus.PendingDeepArchiveRestore &&
                storageClass != S3StorageClass.DeepArchive)
                await HandlePendingToReady(key, ct);
            else if (status == S3ChunkRestoreStatus.ReadyToRestore &&
                     storageClass == S3StorageClass.DeepArchive)
                await HandleReadyToPending(key, chunk, ct);
            // all other combinations are no-ops
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogInformation("ReportS3Storage canceled for key {Key}", s3Key);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error in ReportS3Storage for {Key}", s3Key);
            throw;
        }
    }

    public async Task ReportDownloadComplete(
        DownloadFileFromS3Request req,
        CancellationToken ct
    )
    {
        try
        {
            if (!_restoreRunsCache.TryGetValue(req.RestoreId, out var run)) return;
            if (!run.RequestedFiles.TryGetValue(req.FilePath, out var fileMeta)) return;

            fileMeta.Status = FileRestoreStatus.Completed;
            logger.LogInformation("File {File} in run {RunId} marked Completed",
                req.FilePath, req.RestoreId);
            await runMed.SaveRestoreRun(run, ct);

            // if *all* files done → finalize
            if (run.RequestedFiles.Values
                .All(f => f.Status is FileRestoreStatus.Completed or FileRestoreStatus.Failed))
                await FinalizeRun(run, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogInformation("ReportDownloadComplete canceled for {Run}/{File}",
                req.RestoreId, req.FilePath);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error in ReportDownloadComplete for {Run}/{File}",
                req.RestoreId, req.FilePath);
            throw;
        }
    }

    public async Task ReportDownloadFailed(
        DownloadFileFromS3Request req,
        Exception reason,
        CancellationToken ct
    )
    {
        try
        {
            if (!_restoreRunsCache.TryGetValue(req.RestoreId, out var run)) return;
            if (!run.RequestedFiles.TryGetValue(req.FilePath, out var fileMeta)) return;

            fileMeta.Status = FileRestoreStatus.Failed;
            run.FailedFiles[req.FilePath] = reason.Message;
            logger.LogWarning(reason, "File {File} in run {Run} failed", req.FilePath, req.RestoreId);

            await runMed.SaveRestoreRun(run, ct);
            await snsMed.PublishMessage(
                new SnsMessage($"Download failed: {req}", reason.ToString()), ct);

            // if all done → finalize
            if (run.RequestedFiles.Values
                .All(f => f.Status is FileRestoreStatus.Completed or FileRestoreStatus.Failed))
                await FinalizeRun(run, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogInformation("ReportDownloadFailed canceled for {Run}/{File}",
                req.RestoreId, req.FilePath);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error in ReportDownloadFailed for {Run}/{File}",
                req.RestoreId, req.FilePath);
            throw;
        }
    }

    public async Task InitiateRestoreRun(
        RestoreRequest request,
        RestoreRun run,
        CancellationToken ct
    )
    {
        try
        {
            restoreRequests[run.RestoreId] = request;
            await requestsMed.SaveRunningRequest(restoreRequests, ct);

            if (!_restoreRunsCache.TryAdd(run.RestoreId, run)) return;
            logger.LogInformation("Initiating restore run {RunId}", run.RestoreId);

            // schedule each chunk
            foreach (var meta in run.RequestedFiles.Values)
            {
                var readyToRestore = true;
                foreach (var key in meta.Chunks)
                {
                    var chunk = chunkManifest[key];
                    var status = await s3Service.ScheduleDeepArchiveRecovery(chunk.S3Key, ct);
                    restoreManifest[key] = status;
                    if (status != S3ChunkRestoreStatus.ReadyToRestore)
                        readyToRestore = false;
                    logger.LogDebug("Chunk {Key} initial state {Status}", key, status);
                }

                if (!readyToRestore) continue;
                // enqueue download
                logger.LogDebug("Enqueuing download of {File} in for restore Id {RestoreId}", meta.FilePath,
                    run.RestoreId);
                await mediator.DownloadFileFromS3(new DownloadFileFromS3Request(
                    run.RestoreId,
                    meta.FilePath,
                    run.RequestedFiles[meta.FilePath].Chunks
                        .Select(c => chunkManifest[c]).ToArray(),
                    meta.Size)
                {
                    LastModified = meta.LastModified,
                    Created = meta.Created,
                    AclEntries = meta.AclEntries,
                    Owner = meta.Owner,
                    Group = meta.Group,
                    Checksum = meta.Checksum
                }, ct);

                meta.Status = FileRestoreStatus.PendingS3Download;
            }

            await manifestMed.SaveRestoreManifest(restoreManifest, ct);
            await runMed.SaveRestoreRun(run, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogInformation("InitiateRestoreRun canceled for {RestoreId}", run.RestoreId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error initiating restore run {RestoreId}", run.RestoreId);
            throw;
        }
    }

    private async Task HandlePendingToReady(ByteArrayKey key,
        CancellationToken ct)
    {
        logger.LogInformation("Chunk {Key} moved Pending→Ready", key);
        restoreManifest[key] = S3ChunkRestoreStatus.ReadyToRestore;
        await manifestMed.SaveRestoreManifest(restoreManifest, ct);

        await ForEachRunLocking(key, async (run, runCts) =>
        {
            var affected = run.RequestedFiles.Values
                .Where(f => f.Chunks.Contains(key))
                .ToArray();

            foreach (var fileMeta in affected)
            {
                if (fileMeta.Status is FileRestoreStatus.Completed or FileRestoreStatus.Failed)
                    continue;

                // only if *all* its chunks are now ready
                var allReady = fileMeta.Chunks.All(c =>
                    restoreManifest[c] == S3ChunkRestoreStatus.ReadyToRestore);
                if (!allReady) continue;

                // enqueue download
                logger.LogDebug("Enqueuing download of {File} for restore Id {RestoreId}", fileMeta.FilePath,
                    run.RestoreId);
                await mediator.DownloadFileFromS3(new DownloadFileFromS3Request(
                    run.RestoreId,
                    fileMeta.FilePath,
                    run.RequestedFiles[fileMeta.FilePath].Chunks
                        .Select(c => chunkManifest[c]).ToArray(),
                    fileMeta.Size
                )
                {
                    LastModified = fileMeta.LastModified,
                    Created = fileMeta.Created,
                    AclEntries = fileMeta.AclEntries,
                    Owner = fileMeta.Owner,
                    Group = fileMeta.Group,
                    Checksum = fileMeta.Checksum
                }, runCts);

                fileMeta.Status = FileRestoreStatus.PendingS3Download;
            }

            await runMed.SaveRestoreRun(run, runCts);
        }, ct);
    }

    private async Task HandleReadyToPending(
        ByteArrayKey key,
        CloudChunkDetails chunk,
        CancellationToken ct
    )
    {
        logger.LogInformation("Chunk {Key} moved Ready→PendingDeepArchive", key);
        restoreManifest[key] = S3ChunkRestoreStatus.PendingDeepArchiveRestore;
        await manifestMed.SaveRestoreManifest(restoreManifest, ct);

        var anyScheduled = false;

        await ForEachRunLocking(key, async (run, runCts) =>
        {
            foreach (var fileMeta in run.RequestedFiles.Values
                         .Where(f => f.Chunks.Contains(key)))
            {
                if (fileMeta.Status is FileRestoreStatus.Completed or FileRestoreStatus.Failed)
                    continue;
                fileMeta.Status = FileRestoreStatus.PendingDeepArchiveRestore;
                anyScheduled = true;
            }

            await runMed.SaveRestoreRun(run, runCts);
        }, ct);

        if (anyScheduled)
        {
            logger.LogDebug("Scheduling deep‐archive restore for {Key}", chunk.S3Key);
            await s3Service.ScheduleDeepArchiveRecovery(chunk.S3Key, ct);
        }
    }

    /// <summary>
    ///     Called when *all* files are either Completed or Failed.
    ///     Sends final SNS, updates run status, and cleans up in‐memory state.
    /// </summary>
    private async Task FinalizeRun(RestoreRun run, CancellationToken ct)
    {
        run.Status = RestoreRunStatus.Completed;
        run.CompletedAt = DateTimeOffset.UtcNow;
        logger.LogInformation("Finalizing restore run {RunId}, status {Status}",
            run.RestoreId, run.Status);

        // pick the right message
        if (run.RequestedFiles.Values.Any(f => f.Status == FileRestoreStatus.Failed))
            await snsMed.PublishMessage(
                new RestoreCompleteErrorMessage(
                    run.RestoreId,
                    $"Run {run.RestoreId} completed WITH ERRORS",
                    "Some files failed", run),
                ct);
        else
            await snsMed.PublishMessage(
                new RestoreCompleteMessage(
                    $"Run {run.RestoreId} completed successfully",
                    "All files restored", run),
                ct);

        await runMed.SaveRestoreRun(run, ct);

        // clean up
        _restoreRunsCache.TryRemove(run.RestoreId, out _);
        restoreRequests.TryRemove(run.RestoreId, out _);
        await requestsMed.SaveRunningRequest(restoreRequests, ct);
    }

    /// <summary>
    ///     Helper: takes a chunk‐key, then for every run that references it,
    ///     acquires that run’s SemaphoreSlim to serialize updates.
    /// </summary>
    private async Task ForEachRunLocking(
        ByteArrayKey key,
        Func<RestoreRun, CancellationToken, Task> action,
        CancellationToken ct
    )
    {
        foreach (var run in _restoreRunsCache.Values
                     .Where(r => r.RequestedFiles.Values.Any(f => f.Chunks.Contains(key))))
        {
            // get or create the run‐lock
            var sem = _runLocks.GetOrAdd(run.RestoreId, _ => new SemaphoreSlim(1, 1));
            await sem.WaitAsync(ct);
            try
            {
                await action(run, ct);
            }
            finally
            {
                sem.Release();
            }
        }
    }
}