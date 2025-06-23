using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class RestoreOrchestration(
    IMediator mediator,
    IArchiveService archiveService,
    IRestoreService restoreService,
    ILogger<ArchiveFilesOrchestration> logger
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        //await a IAsyncEnumerable<RestoreRequest> from the mediator
        await foreach (var restoreRequest in mediator.GetRestoreRequests(cancellationToken))
        {
            if (string.IsNullOrWhiteSpace(restoreRequest.RunId) ||
                string.IsNullOrWhiteSpace(restoreRequest.RestorePaths))
            {
                logger.LogWarning("Received invalid restore request with null RunId or RestorePaths");
                continue;
            }

            var restoreId = restoreService.ResolveId(restoreRequest);
            var restoreRun = await restoreService.LookupRestoreRun(restoreId, cancellationToken);
            if (restoreRun != null) continue;

            var matcher = restoreRequest
                .RestorePaths.Split(':')
                .Aggregate(new Matcher(), (m, filePath) => m.AddInclude(filePath));
            var archiveRun = await archiveService.LookupArchiveRun(restoreRequest.RunId, cancellationToken);
            if (archiveRun is null)
            {
                logger.LogWarning("No archive run found for RunId {RunId}", restoreRequest.RunId);
                continue;
            }

            var requestedFiles = (
                from file in archiveRun.Files.Keys
                where matcher.Match(file).HasMatches
                select new
                {
                    file,
                    chunkIds = archiveRun
                        .Files[file]
                        .Chunks
                        .OrderBy(c => c.ChunkIndex)
                        .Select(c => c.Key)
                        .ToArray()
                }).ToDictionary(
                i => i.file,
                i => new RestoreFileMetaData
                {
                    Chunks = i.chunkIds,
                    Status = FileRestoreStatus.PendingDeepArchiveRestore,
                    FilePath = i.file
                }
            );

            restoreRun = new RestoreRun
            {
                RestoreId = restoreId,
                RestorePaths = restoreRequest.RestorePaths,
                ArchiveRunId = restoreRequest.RunId,
                RequestedAt = restoreRequest.RequestedAt,
                Status = RestoreRunStatus.Processing,
                RequestedFiles = requestedFiles
            };

            await restoreService.InitiateRestoreRun(restoreRun, cancellationToken);
        }
    }
}