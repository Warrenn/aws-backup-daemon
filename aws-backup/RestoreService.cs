using System.Collections.Concurrent;
using Amazon.S3;

namespace aws_backup;

public enum RestoreRunStatus
{
    Processing,
    Completed
}

public enum S3RestoreStatus
{
    PendingDeepArchiveRestore,
    ReadyToRestore
}

public class S3RestoreChunkManifest : ConcurrentDictionary<ByteArrayKey, S3RestoreStatus>
{
    public static S3RestoreChunkManifest Current { get; } = new();
}

public enum FileRestoreStatus
{
    PendingDeepArchiveRestore,
    PendingS3Download,
    Completed,
    Failed
}

public record RestoreFileMetaData(
    ByteArrayKey[] Chunks,
    string FilePath,
    long Size
)
{
    public DateTimeOffset? LastModified { get; set; }
    public DateTimeOffset? Created { get; set; }
    public AclEntry[]? AclEntries { get; set; }
    public string? Owner { get; set; }
    public string? Group { get; set; }
    public byte[]? Checksum { get; set; }
    public FileRestoreStatus Status { get; set; } = FileRestoreStatus.PendingDeepArchiveRestore;
}

public record DownloadFileFromS3Request(
    string RestoreId,
    string FilePath,
    CloudChunkDetails[] CloudChunkDetails,
    long Size)
{
    public DateTimeOffset? LastModified { get; set; }
    public DateTimeOffset? Created { get; set; }
    public AclEntry[]? AclEntries { get; set; }
    public string? Owner { get; set; }
    public string? Group { get; set; }
    public byte[]? Checksum { get; set; }
}

public record S3ChunkData(
    ByteArrayKey Hash,
    string S3Key, // S3 key for the chunk
    string BucketName)
{
    public S3RestoreStatus Status { get; set; }
}

public record RestoreRequest(
    string ArchiveRunId,
    string RestorePaths,
    DateTimeOffset RequestedAt);

public class RestoreRun
{
    public required string RestoreId { get; init; }
    public required string RestorePaths { get; init; }
    public required string ArchiveRunId { get; init; }
    public required RestoreRunStatus Status { get; set; } = RestoreRunStatus.Processing;
    public required DateTimeOffset RequestedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? CompletedAt { get; set; }
    public Dictionary<string, RestoreFileMetaData> RequestedFiles { get; init; } = new();
    public Dictionary<string, string> FailedFiles { get; init; } = new();
}

public interface IRestoreService
{

    Task<RestoreRun?> LookupRestoreRun(string restoreId, CancellationToken cancellationToken);
    Task InitiateRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken);

    Task ReportS3Storage(string bucketId, string s3Key, S3StorageClass storageClass,
        CancellationToken cancellationToken);

    Task ReportDownloadComplete(DownloadFileFromS3Request request, CancellationToken cancellationToken);
    Task ReportDownloadFailed(DownloadFileFromS3Request request, Exception reason, CancellationToken cancellationToken);
}

public class RestoreService(
    IMediator mediator,
    IS3Service s3Service,
    S3RestoreChunkManifest restoreManifest,
    DataChunkManifest dataChunkManifest
) : IRestoreService
{
    //List of RestoreRun objects
    private readonly Dictionary<string, RestoreRun> _restoreRuns = [];

    public Task<RestoreRun?> LookupRestoreRun(string restoreId, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task InitiateRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken)
    {
        if (!_restoreRuns.TryAdd(restoreRun.RestoreId, restoreRun)) return;

        foreach (var metaData in restoreRun.RequestedFiles.Values)
        {
            foreach (var key in metaData.Chunks)
            {
                if (restoreManifest.TryGetValue(key, out var status)) continue;

                var chunkDetails = dataChunkManifest[key];
                await s3Service.ScheduleDeepArchiveRecovery(chunkDetails, cancellationToken);

                status = S3RestoreStatus.PendingDeepArchiveRestore;
                restoreManifest[key] = status;
            }

            var readyToRestore =
                metaData.Chunks.All(c => restoreManifest[c] is S3RestoreStatus.ReadyToRestore);
            if (readyToRestore) continue;

            var cloudChunkDetails = metaData.Chunks
                .Select(c => dataChunkManifest[c])
                .ToArray();

            await mediator.DownloadFileFromS3(new DownloadFileFromS3Request(
                restoreRun.RestoreId,
                metaData.FilePath,
                cloudChunkDetails,
                metaData.Size
            )
            {
                LastModified = metaData.LastModified,
                Created = metaData.Created,
                AclEntries = metaData.AclEntries,
                Owner = metaData.Owner,
                Group = metaData.Group,
                Checksum = metaData.Checksum
            }, cancellationToken);

            metaData.Status = FileRestoreStatus.PendingS3Download;
        }

        await mediator.SaveS3RestoreChunkManifest(restoreManifest, cancellationToken);
        await mediator.SaveRestoreRun(restoreRun, cancellationToken);
    }

    public async Task ReportS3Storage(string bucketId, string s3Key, S3StorageClass storageClass,
        CancellationToken cancellationToken)
    {
        var matchingDataChunk = dataChunkManifest.Values.FirstOrDefault(d => d.S3Key == s3Key && d.BucketName == bucketId);
        if (matchingDataChunk is null) return;

        var hashKey = new ByteArrayKey(matchingDataChunk.Hash);
        if (!restoreManifest.TryGetValue(hashKey, out var status))
            return;

        if (status == S3RestoreStatus.ReadyToRestore && storageClass != S3StorageClass.DeepArchive)
            return;
        if (status == S3RestoreStatus.PendingDeepArchiveRestore && storageClass == S3StorageClass.DeepArchive)
            return;

        if (status == S3RestoreStatus.PendingDeepArchiveRestore && storageClass != S3StorageClass.DeepArchive)
        {
            restoreManifest[hashKey] = S3RestoreStatus.ReadyToRestore;
            await mediator.SaveS3RestoreChunkManifest(restoreManifest, cancellationToken);

            foreach (var restoreRun in _restoreRuns.Values)
            {
                var runChanged = false;
                foreach (var fileMeta in restoreRun.RequestedFiles.Values.Where(f => f.Chunks.Contains(hashKey)))
                {
                    var allReadyToRestore = fileMeta.Chunks
                        .All(c => restoreManifest[c] == S3RestoreStatus.ReadyToRestore);
                    if (!allReadyToRestore ||
                        fileMeta.Status is FileRestoreStatus.Completed or FileRestoreStatus.Failed) continue;

                    runChanged = true;

                    var cloudChunkDetails = fileMeta.Chunks
                        .Select(c => dataChunkManifest[c])
                        .ToArray();

                    await mediator.DownloadFileFromS3(new DownloadFileFromS3Request(
                        restoreRun.RestoreId,
                        fileMeta.FilePath,
                        cloudChunkDetails,
                        fileMeta.Size
                    )
                    {
                        LastModified = fileMeta.LastModified,
                        Created = fileMeta.Created,
                        AclEntries = fileMeta.AclEntries,
                        Owner = fileMeta.Owner,
                        Group = fileMeta.Group,
                        Checksum = fileMeta.Checksum
                    }, cancellationToken);

                    fileMeta.Status = FileRestoreStatus.PendingS3Download;
                }

                if (!runChanged) continue;
                await mediator.SaveRestoreRun(restoreRun, cancellationToken);
            }
        }

        if (status == S3RestoreStatus.ReadyToRestore && storageClass == S3StorageClass.DeepArchive)
        {
            restoreManifest[hashKey] = S3RestoreStatus.PendingDeepArchiveRestore;
            await mediator.SaveS3RestoreChunkManifest(restoreManifest, cancellationToken);

            var scheduleDownload = false;
            foreach (var restoreRun in _restoreRuns.Values)
            {
                var runChanged = false;
                foreach (var fileMeta in restoreRun.RequestedFiles.Values.Where(f => f.Chunks.Contains(hashKey)))
                {
                    if (fileMeta.Status is FileRestoreStatus.Completed or FileRestoreStatus.Failed) continue;
                    scheduleDownload = true;
                    if (fileMeta.Status == FileRestoreStatus.PendingDeepArchiveRestore) continue;
                    fileMeta.Status = FileRestoreStatus.PendingDeepArchiveRestore;

                    runChanged = true;
                }

                if (!runChanged) continue;
                await mediator.SaveRestoreRun(restoreRun, cancellationToken);
            }

            if (!scheduleDownload) return;
            await s3Service.ScheduleDeepArchiveRecovery(dataChunkManifest[hashKey], cancellationToken);
        }
    }

    public async Task ReportDownloadComplete(DownloadFileFromS3Request request, CancellationToken cancellationToken)
    {
        if (!_restoreRuns.TryGetValue(request.RestoreId, out var restoreRun)) return;
        if (!restoreRun.RequestedFiles.TryGetValue(request.FilePath, out var fileMeta)) return;
        fileMeta.Status = FileRestoreStatus.Completed;
        await mediator.SaveRestoreRun(restoreRun, cancellationToken);
        var runComplete = restoreRun.RequestedFiles.Values.All(f =>
            f.Status is FileRestoreStatus.Completed or FileRestoreStatus.Failed);
        if (!runComplete) return;
        restoreRun.Status = RestoreRunStatus.Completed;
        restoreRun.CompletedAt = DateTimeOffset.UtcNow;
        await mediator.SaveRestoreRun(restoreRun, cancellationToken);
        _restoreRuns.Remove(restoreRun.RestoreId);
    }

    public async Task ReportDownloadFailed(DownloadFileFromS3Request request, Exception reason,
        CancellationToken cancellationToken)
    {
        if (!_restoreRuns.TryGetValue(request.RestoreId, out var restoreRun)) return;
        if (!restoreRun.RequestedFiles.TryGetValue(request.FilePath, out var fileMeta)) return;
        fileMeta.Status = FileRestoreStatus.Failed;
        restoreRun.FailedFiles[request.FilePath] = reason.Message;
        await mediator.SaveRestoreRun(restoreRun, cancellationToken);
        var runComplete = restoreRun.RequestedFiles.Values.All(f =>
            f.Status is FileRestoreStatus.Completed or FileRestoreStatus.Failed);
        if (!runComplete) return;
        restoreRun.Status = RestoreRunStatus.Completed;
        restoreRun.CompletedAt = DateTimeOffset.UtcNow;
        await mediator.SaveRestoreRun(restoreRun, cancellationToken);
        _restoreRuns.Remove(restoreRun.RestoreId);
    }
}