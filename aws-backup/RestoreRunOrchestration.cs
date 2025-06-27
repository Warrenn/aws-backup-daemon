using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class RestoreOrchestration(
    IRestoreRequestsMediator mediator,
    IArchiveService archiveService,
    IRestoreService restoreService,
    ILogger<ArchiveFilesOrchestration> logger,
    IContextResolver contextResolver
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        //await a IAsyncEnumerable<RestoreRequest> from the mediator
        await foreach (var restoreRequest in mediator.GetRestoreRequests(cancellationToken))
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
                logger.LogWarning("No archive run found for ArchiveRunId {ArchiveRunId}", restoreRequest.ArchiveRunId);
                continue;
            }

            var requestedFiles = (
                from file in archiveRun.Files.Keys
                where matcher.Match(file).HasMatches
                let metadata = archiveRun.Files[file]
                select new
                {
                    file,
                    chunkIds = metadata
                        .Chunks
                        .OrderBy(c => c.ChunkIndex)
                        .Select(c => new ByteArrayKey(c.HashKey))
                        .ToArray(),
                    metadata.OriginalSize,
                    metadata
                }).ToDictionary(
                i => i.file,
                i => new RestoreFileMetaData(
                    i.chunkIds,
                    i.file,
                    i.OriginalSize ?? 0
                )
                {
                    Status = FileRestoreStatus.PendingDeepArchiveRestore,
                    LastModified = i.metadata.LastModified,
                    Created = i.metadata.Created,
                    Owner = i.metadata.Owner,
                    Group = i.metadata.Group,
                    AclEntries = i.metadata.AclEntries,
                    Checksum = i.metadata.HashKey
                }
            );

            restoreRun = new RestoreRun
            {
                RestoreId = restoreId,
                RestorePaths = restoreRequest.RestorePaths,
                ArchiveRunId = restoreRequest.ArchiveRunId,
                RequestedAt = restoreRequest.RequestedAt,
                Status = RestoreRunStatus.Processing,
                RequestedFiles = requestedFiles
            };

            await restoreService.InitiateRestoreRun(restoreRun, cancellationToken);
        }
    }
}