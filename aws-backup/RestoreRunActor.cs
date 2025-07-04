using System.Collections.Concurrent;
using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

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

                var restoreId = contextResolver.RestoreId(restoreRequest.ArchiveRunId, restoreRequest.RestorePaths,
                    restoreRequest.RequestedAt);
                var restoreRun = await restoreService.LookupRestoreRun(restoreId, cancellationToken);
                if (restoreRun != null) continue;

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
                            .Chunks
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