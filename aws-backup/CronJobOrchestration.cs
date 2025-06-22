using Cronos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace aws_backup;

public class CronJobOrchestration(
    IOptionsMonitor<Configuration> configurationMonitor,
    Func<CancellationToken, Task> job,
    ILogger<CronJobOrchestration> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        CancellationTokenSource scheduleCts = new();
        var cronSchedule = configurationMonitor.CurrentValue.CronSchedule;
        var cronExpression = CronExpression.Parse(cronSchedule, CronFormat.Standard);

        configurationMonitor.OnChange((config, _) =>
        {
            if (config.CronSchedule == cronExpression.ToString()) return;
            scheduleCts.Cancel();
            cronExpression = CronExpression.Parse(config.CronSchedule, CronFormat.Standard);
            scheduleCts = new CancellationTokenSource();
        });

        logger.LogInformation("CronJobService started with schedule '{Schedule}' in zone '{Zone}'",
            cronExpression, TimeZoneInfo.Utc.Id);

        while (!stoppingToken.IsCancellationRequested)
        {
            // 1) Compute next run time
            var nextUtc = cronExpression.GetNextOccurrence(
                DateTimeOffset.UtcNow,
                TimeZoneInfo.Utc
            );

            if (!nextUtc.HasValue)
            {
                logger.LogWarning("Cron expression '{Schedule}' will not occur again.", cronExpression);
                break;
            }

            var delay = nextUtc.Value - DateTimeOffset.UtcNow;
            if (delay.TotalMilliseconds <= 0)
                // If we're already past it (due to drift), skip ahead
                continue;

            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken, scheduleCts.Token);
            logger.LogInformation("Next run at {NextRun}", nextUtc.Value);
            try
            {
                // 2) Wait until it's time (or until shutdown)
                await Task.Delay(delay, linked.Token);

                //if (cronExpressionChanged) continue;
            }
            catch (OperationCanceledException)
            {
                if (stoppingToken.IsCancellationRequested) break;
                continue;
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