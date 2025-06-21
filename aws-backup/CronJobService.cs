using Cronos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class CronJobService(
    string cronSchedule,
    Func<CancellationToken, Task> job,
    ILogger<CronJobService> logger)
    : BackgroundService
{
    private readonly CronExpression _cronExpression = CronExpression.Parse(cronSchedule, CronFormat.Standard);

    // or "America/New_York", etc.

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("CronJobService started with schedule '{Schedule}' in zone '{Zone}'",
            _cronExpression, TimeZoneInfo.Utc.Id);

        while (!stoppingToken.IsCancellationRequested)
        {
            // 1) Compute next run time
            var nextUtc = _cronExpression.GetNextOccurrence(
                DateTimeOffset.UtcNow,
                TimeZoneInfo.Utc
            );

            if (!nextUtc.HasValue)
            {
                logger.LogWarning("Cron expression '{Schedule}' will not occur again.", _cronExpression);
                break;
            }

            var delay = nextUtc.Value - DateTimeOffset.UtcNow;
            if (delay.TotalMilliseconds <= 0)
                // If we're already past it (due to drift), skip ahead
                continue;

            logger.LogInformation("Next run at {NextRun}", nextUtc.Value);
            try
            {
                //new cancelation source where if original stoppingToken is cancelled, this will also cancel
                var source = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

                // 2) Wait until it's time (or until shutdown)
                await Task.Delay(delay, source.Token);

                //if (cronExpressionChanged) continue;
            }
            catch (TaskCanceledException)
            {
                break; // shutting down
            }

            // 3) Invoke your job
            try
            {
                logger.LogInformation("Cron job starting at {Now}", DateTimeOffset.UtcNow);
                await job(stoppingToken);
                logger.LogInformation("Cron job completed at {Now}", DateTimeOffset.UtcNow);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogError(ex, "Error running cron job");
            }

            // loop for the next occurrence...
        }

        logger.LogInformation("CronJobService is stopping.");
    }
}