using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class ArchiveRequestProcessor(
    IMediator mediator,
    IArchiveService archiveService,
    Configuration configuration,
    ILogger<ArchiveRequestProcessor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // This service is responsible for processing archive runs.
        // It will read from the archiveQueue and process each archive.
        await foreach (var archiveRequest in mediator.GetRequests(stoppingToken))
        {
            var archiveRun = await archiveService.GetArchiveRun(archiveRequest.RunId, stoppingToken);
            if (archiveRun is null)
            {
                logger.Log(LogLevel.Information, "Creating new archive run for {RunId}", archiveRequest.RunId);
                archiveRun = new ArchiveRun(
                    archiveRequest.RunId,
                    archiveRequest.PathsToArchive,
                    archiveRequest.CronSchedule
                );
                await archiveService.SaveArchiveRun(archiveRun, stoppingToken);
            }

            if (archiveRun.Status == ArchiveRunStatus.Completed) return;

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

            foreach (var filePath in FileLister.GetAllFiles(archiveRequest.PathsToArchive, ignorePatterns))
                await mediator.ProcessFile(archiveRun.RunId, filePath, stoppingToken);
        }
    }
}