using aws_backup_common;
using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IRestoreRequestsMediator
{
    Task RestoreBackup(RestoreRequest restoreRequest, CancellationToken cancellationToken);

    IAsyncEnumerable<RestoreRequest> GetRestoreRequests(CancellationToken cancellationToken);
}

public sealed class RestoreRunActor(
    IRestoreRequestsMediator mediator,
    IArchiveDataStore archiveDataStore,
    IRestoreDataStore restoreDataStore,
    IRestoreService restoreService,
    ILogger<RestoreRunActor> logger,
    IContextResolver contextResolver,
    ISnsMessageMediator snsMessageMediator
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
                if (restoreRun is null)
                {
                    logger.Log(LogLevel.Information, "Creating new restore run for {RestoreRunId}", restoreId);
                    restoreRun = await restoreService.StartNewRestoreRun(restoreRequest, restoreId, cancellationToken);
                }

                if (restoreRun.Status is RestoreRunStatus.Completed)
                {
                    await restoreService.ClearCache(restoreRun.RestoreId, cancellationToken);
                    logger.LogInformation("Restore run with ID {RestoreId} already completed, skipping processing",
                        restoreId);
                    continue;
                }

                var matcher = restoreRequest
                    .RestorePaths.Split(':')
                    .Aggregate(new Matcher(), (m, filePath) => m.AddInclude(filePath));

                logger.LogInformation("Initiating restore run with ID {RestoreId} for ArchiveRunId {ArchiveRunId}",
                    restoreRun.RestoreId, restoreRequest.ArchiveRunId);

                await foreach (var fileMetaData in archiveDataStore.GetRestorableFileMetaData(
                                   restoreRequest.ArchiveRunId, cancellationToken))
                {
                    if (!matcher.Match("/", fileMetaData.LocalFilePath).HasMatches)
                        continue;

                    await restoreService.ScheduleFileRecovery(restoreRun, restoreRequest, fileMetaData,
                        cancellationToken);
                }
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
        await foreach (var request in restoreDataStore.GetRestoreRequests(cancellationToken))
            await mediator.RestoreBackup(request, cancellationToken);
    }
}