using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IRunRequestMediator
{
    IAsyncEnumerable<RunRequest> GetRunRequests(CancellationToken cancellationToken);
    Task ScheduleRunRequest(RunRequest runRequest, CancellationToken cancellationToken);
}

public sealed class ArchiveRunActor(
    IRunRequestMediator mediator,
    IArchiveFileMediator archiveFileMediator,
    IUploadChunksMediator uploadChunksMediator,
    IArchiveService archiveService,
    IArchiveDataStore archiveDataStore,
    IContextResolver contextResolver,
    IFileLister fileLister,
    ILogger<ArchiveRunActor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await LoadArchiveRunsFromCloud(cancellationToken);
        logger.LogInformation("ArchiveRunActor started");

        await foreach (var runRequest in mediator.GetRunRequests(cancellationToken))
            try
            {
                var ignoreFilePath = contextResolver.LocalIgnoreFile();
                var archiveRun = await archiveService.LookupArchiveRun(runRequest.RunId, cancellationToken);
                if (archiveRun is null)
                {
                    logger.Log(LogLevel.Information, "Creating new archive run for {ArchiveRunId}", runRequest.RunId);
                    archiveRun = await archiveService.StartNewArchiveRun(runRequest, cancellationToken);
                }

                if (archiveRun.Status == ArchiveRunStatus.Completed)
                {
                    logger.Log(LogLevel.Information, "Archive run {ArchiveRunId} is already completed",
                        runRequest.RunId);
                    await archiveService.ClearCache(runRequest.RunId, cancellationToken);
                    continue;
                }

                string[] ignorePatterns = [];
                if (File.Exists(ignoreFilePath))
                    try
                    {
                        ignorePatterns = (await File.ReadAllLinesAsync(ignoreFilePath, cancellationToken))
                            .Select(l => l.Trim())
                            .Where(l => !string.IsNullOrWhiteSpace(l) && !l.StartsWith('#'))
                            .ToArray();
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e, "Failed to read ignore file {LocalIgnoreFile}", ignoreFilePath);
                    }

                foreach (var filePath in fileLister.GetAllFiles(runRequest.PathsToArchive, ignorePatterns))
                {
                    var archiveFileRequest = new ArchiveFileRequest(archiveRun.RunId, filePath);
                    await archiveFileMediator.ProcessFile(archiveFileRequest, cancellationToken);
                }

                logger.LogInformation("All files processed for archive run {RunId}", runRequest.RunId);

                await archiveService.ReportAllFilesListed(archiveRun, cancellationToken);
                await uploadChunksMediator.WaitForAllChunksProcessed();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing archive run request {RunId}", runRequest.RunId);
            }
    }

    private async Task LoadArchiveRunsFromCloud(CancellationToken cancellationToken)
    {
        await foreach (var archiveRequest in archiveDataStore.GetRunRequests(cancellationToken))
            await mediator.ScheduleRunRequest(archiveRequest, cancellationToken);
    }
}