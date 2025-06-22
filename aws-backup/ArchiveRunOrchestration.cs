using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class ArchiveRunOrchestration(
    IMediator mediator,
    IArchiveService archiveService,
    Configuration configuration,
    IFileLister fileLister,
    ILogger<ArchiveRunOrchestration> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // This service is responsible for processing archive runs.
        // It will read from the archiveQueue and process each archive.
        await foreach (var runRequest in mediator.RunRequests(stoppingToken))
        {
            var archiveRun = await archiveService.LookupArchiveRun(runRequest.RunId, stoppingToken);
            if (archiveRun is null)
            {
                logger.Log(LogLevel.Information, "Creating new archive run for {RunId}", runRequest.RunId);
                archiveRun = new ArchiveRun(
                    runRequest.RunId,
                    runRequest.PathsToArchive,
                    runRequest.CronSchedule
                );
                await archiveService.StartNewArchiveRun(archiveRun, configuration, stoppingToken);
            }

            if (archiveRun.Status == ArchiveRunStatus.Completed) continue;

            string[] ignorePatterns = [];
            if (File.Exists(configuration.IgnoreFile))
                try
                {
                    ignorePatterns = (await File.ReadAllLinesAsync(configuration.IgnoreFile, stoppingToken))
                        .Select(l => l.Trim())
                        .Where(l => !string.IsNullOrWhiteSpace(l) && !l.StartsWith('#'))
                        .ToArray();
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Failed to read ignore file {IgnoreFile}", configuration.IgnoreFile);
                }

            foreach (var filePath in fileLister.GetAllFiles(runRequest.PathsToArchive, ignorePatterns))
            {
                await archiveService.RecordLocalFile(archiveRun.RunId, filePath, stoppingToken);
                await mediator.ProcessFile(archiveRun.RunId, filePath, stoppingToken);
            }
            
            await archiveService.CompleteArchiveRun(archiveRun.RunId, stoppingToken);
        }
    }
}