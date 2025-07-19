using System.Collections.Concurrent;
using aws_backup_common;
using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;


public interface IRestoreRequestsMediator
{
    Task RestoreBackup(RestoreRequest restoreRequest, CancellationToken cancellationToken);
    Task SaveRunningRequest(CurrentRestoreRequests currentRestoreRequests, CancellationToken cancellationToken);

    IAsyncEnumerable<S3LocationAndValue<CurrentRestoreRequests>> GetRunningRequests(
        CancellationToken cancellationToken);

    IAsyncEnumerable<RestoreRequest> GetRestoreRequests(CancellationToken cancellationToken);
}


public sealed class RestoreRunActor(
    IRestoreRequestsMediator mediator,
    IArchiveService archiveService,
    IRestoreService restoreService,
    ILogger<RestoreRunActor> logger,
    IContextResolver contextResolver,
    ISnsMessageMediator snsMessageMediator,
    CurrentRestoreRequests currentRestoreRequests
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("DownloadFileActor started");
        await LoadRestoreRunsFromCloud(cancellationToken);

        await foreach (var restoreRequest in mediator.GetRestoreRequests(cancellationToken))
            try
            {
                if (string.IsNullOrWhiteSpace(restoreRequest.ArchiveRunId) ||
                    string.IsNullOrWhiteSpace(restoreRequest.RestorePaths))
                {
                    logger.LogWarning("Received invalid restore request with null ArchiveRunId or RestorePaths");
                    continue;
                }

                logger.LogInformation(
                    "Processing restore request for ArchiveRunId {ArchiveRunId} with paths {RestorePaths}",
                    restoreRequest.ArchiveRunId, restoreRequest.RestorePaths);

                var restoreId = contextResolver.RestoreId(restoreRequest.ArchiveRunId, restoreRequest.RestorePaths,
                    restoreRequest.RequestedAt);
                var restoreRun = await restoreService.LookupRestoreRun(restoreId, cancellationToken);
                
                if (restoreRun?.Status is RestoreRunStatus.Completed)
                {
                    await restoreService.FinalizeRun(restoreRun, cancellationToken);
                    logger.LogInformation("Restore run with ID {RestoreId} already completed, skipping processing",
                        restoreId);
                    continue;
                }
                
                if(restoreRun?.Status is RestoreRunStatus.Processing)
                {
                    logger.LogInformation("Restore run with ID {RestoreId} didn't finish processing, re initializing it",
                        restoreId);
                    await restoreService.InitiateRestoreRun(restoreRequest, restoreRun, cancellationToken);
                    continue;
                }

                var matcher = restoreRequest
                    .RestorePaths.Split(':')
                    .Aggregate(new Matcher(), (m, filePath) => m.AddInclude(filePath));
                var archiveRun = await archiveService.LookupArchiveRun(restoreRequest.ArchiveRunId, cancellationToken);
                if (archiveRun is null)
                {
                    logger.LogWarning("No archive run found for ArchiveRunId {ArchiveRunId}",
                        restoreRequest.ArchiveRunId);
                    continue;
                }

                var matchingFiles = matcher.Match("/", archiveRun.Files.Keys);

                var requestedFilesArray = (
                    from file in matchingFiles.Files
                    let path = $"/{file.Path}"
                    where archiveRun.Files.ContainsKey(path)
                    let metadata = archiveRun.Files[path]
                    select new
                    {
                        path,
                        chunkIds = metadata
                            .Chunks.Values
                            .OrderBy(c => c.ChunkIndex)
                            .Select(c => new ByteArrayKey(c.HashKey))
                            .ToArray(),
                        metadata.OriginalSize,
                        metadata
                    }).ToArray();

                var requestedFiles = new ConcurrentDictionary<string, RestoreFileMetaData>();
                foreach (var file in requestedFilesArray)
                    requestedFiles[file.path] = new RestoreFileMetaData(
                        file.chunkIds,
                        file.path,
                        file.OriginalSize ?? 0
                    )
                    {
                        Status = FileRestoreStatus.PendingDeepArchiveRestore,
                        LastModified = file.metadata.LastModified,
                        Created = file.metadata.Created,
                        Owner = file.metadata.Owner,
                        Group = file.metadata.Group,
                        AclEntries = file.metadata.AclEntries,
                        Checksum = file.metadata.HashKey
                    };

                restoreRun = new RestoreRun
                {
                    RestoreId = restoreId,
                    RestorePaths = restoreRequest.RestorePaths,
                    ArchiveRunId = restoreRequest.ArchiveRunId,
                    RequestedAt = restoreRequest.RequestedAt,
                    Status = RestoreRunStatus.Processing,
                    RequestedFiles = requestedFiles
                };

                logger.LogInformation("Initiating restore run with ID {RestoreId} for ArchiveRunId {ArchiveRunId}",
                    restoreRun.RestoreId, restoreRequest.ArchiveRunId);

                await restoreService.InitiateRestoreRun(restoreRequest, restoreRun, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                await snsMessageMediator.PublishMessage(new ExceptionMessage(
                    $"Error processing restore request: {ex.Message}",
                    ex.ToString()), cancellationToken);
                logger.LogError(ex, "Error processing restore request: {Message}", ex.Message);
            }
    }

    private async Task LoadRestoreRunsFromCloud(CancellationToken cancellationToken)
    {
        foreach (var (_, request) in currentRestoreRequests) await mediator.RestoreBackup(request, cancellationToken);
    }
}