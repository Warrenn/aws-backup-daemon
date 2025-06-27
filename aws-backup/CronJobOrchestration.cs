using Cronos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace aws_backup;

public interface ICronScheduler
{
    /// <summary>
    ///     Returns the next occurrence *after* the given time, or null if never.
    /// </summary>
    DateTimeOffset? GetNext(DateTimeOffset afterUtc);
}

public interface ICronSchedulerFactory
{
    ICronScheduler Create(string cron);
}

public class CronSchedulerFactory : ICronSchedulerFactory
{
    public ICronScheduler Create(string cron)
    {
        return new CronScheduler(cron);
    }
}

public class CronScheduler(string cron) : ICronScheduler
{
    private readonly CronExpression _expr = CronExpression.Parse(cron, CronFormat.Standard);

    public DateTimeOffset? GetNext(DateTimeOffset afterUtc)
    {
        return _expr.GetNextOccurrence(afterUtc, TimeZoneInfo.Utc);
    }
}

public class CronJobOrchestration(
    IOptionsMonitor<Configuration> configurationMonitor,
    IRunRequestMediator mediator,
    IContextResolver contextResolver,
    ICronSchedulerFactory cronSchedulerFactory,
    ILogger<CronJobOrchestration> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        CancellationTokenSource scheduleCts = new();
        var scheduler = cronSchedulerFactory.Create(configurationMonitor.CurrentValue.CronSchedule);

        configurationMonitor.OnChange((config, _) =>
        {
            scheduler = cronSchedulerFactory.Create(config.CronSchedule);
            scheduleCts.Cancel();
            scheduleCts = new CancellationTokenSource();
        });

        logger.LogInformation("CronJobService started with schedule '{Schedule}' in zone '{Zone}'",
            configurationMonitor.CurrentValue.CronSchedule, TimeZoneInfo.Utc.Id);

        while (!cancellationToken.IsCancellationRequested)
        {
            var now = TimeProvider.System.GetUtcNow();
            var next = scheduler.GetNext(now);
            if (next == null) break;

            var delay = next.Value - now;
            if (delay.TotalMilliseconds <= 0)
                // If we're already past it (due to drift), skip ahead
                continue;

            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, scheduleCts.Token);
            try
            {
                // 2) Wait until it's time (or until shutdown)
                await Task.Delay(delay, linked.Token);

                var runId = contextResolver.ArchiveRunId(TimeProvider.System.GetUtcNow());
                var cronSchedule = configurationMonitor.CurrentValue.CronSchedule;
                var pathsToArchive = configurationMonitor.CurrentValue.PathsToArchive;
                var runRequest = new RunRequest(runId, pathsToArchive, cronSchedule);
                
                logger.LogInformation("Starting backup {runId} for {pathsToArchive} at {Now}", runId, pathsToArchive,
                    TimeProvider.System.GetUtcNow());
                await mediator.ScheduleRunRequest(runRequest, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                if (cancellationToken.IsCancellationRequested) break;
                continue;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                logger.LogError(ex, "Error running cron job");
            }


            // loop for the next occurrence...
        }

        logger.LogInformation("CronJobService is stopping.");
    }
}