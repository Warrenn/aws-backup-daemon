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
    IArchiveService archiveService,
    IContextResolver contextResolver,
    IFileLister fileLister,
    ILogger<ArchiveRunActor> logger,
    CurrentArchiveRunRequests currentArchiveRequests) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await LoadArchiveRunsFromCloud(cancellationToken);
        // This service is responsible for processing archive runs.
        // It will read from the archiveQueue and process each archive.
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

                if (archiveRun.Status == ArchiveRunStatus.Completed) continue;

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
                    await archiveService.RecordLocalFile(archiveRun, filePath, cancellationToken);

                foreach (var filePath in archiveRun.Files.Keys)
                {
                    var archiveFileRequest = new ArchiveFileRequest(archiveRun.RunId, filePath);
                    await archiveFileMediator.ProcessFile(archiveFileRequest, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing archive run request {RunId}", runRequest.RunId);
                // Optionally, you can handle retries or logging here
            }
    }

    private async Task LoadArchiveRunsFromCloud(CancellationToken cancellationToken)
    {
        foreach (var (_, archiveRequest) in currentArchiveRequests)
            await mediator.ScheduleRunRequest(archiveRequest, cancellationToken);
    }
}