using Cronos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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

public sealed class CronJobActor(
    IRunRequestMediator mediator,
    IContextResolver contextResolver,
    ICronSchedulerFactory cronSchedulerFactory,
    ILogger<CronJobActor> logger,
    TimeProvider timeProvider,
    ISnsMessageMediator snsMessageMediator,
    ISignalHub<string> signalHub)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var cronSchedule = contextResolver.CronSchedule();
        var scheduler = cronSchedulerFactory.Create(cronSchedule);

        logger.LogInformation("CronJobService started with schedule '{Schedule}' in zone '{Zone}'",
            cronSchedule, TimeZoneInfo.Utc.Id);

        while (!cancellationToken.IsCancellationRequested)
            try
            {
                var now = timeProvider.GetUtcNow();
                var next = scheduler.GetNext(now);
                if (next == null) break;

                var delayTime = next.Value - now;
                if (delayTime.TotalMilliseconds <= 0)
                    continue;

                var waitForSignal = signalHub.WaitAsync(cancellationToken);
                var delay = Task.Delay(delayTime, cancellationToken);

                // Whichever completes first…
                var finished = await Task.WhenAny(waitForSignal, delay);

                if (finished == waitForSignal)
                {
                    cronSchedule = await waitForSignal;
                    scheduler = cronSchedulerFactory.Create(cronSchedule);
                    logger.LogInformation("Cron schedule changed {cronSchedule}.", cronSchedule); // Signal arrived
                    continue;
                }

                var runId = contextResolver.ArchiveRunId(timeProvider.GetUtcNow());
                var pathsToArchive = contextResolver.PathsToArchive();
                var runRequest = new RunRequest(runId, pathsToArchive, cronSchedule);

                logger.LogInformation("Starting backup {runId} for {pathsToArchive} at {Now}", runId, pathsToArchive,
                    timeProvider.GetUtcNow());
                await mediator.ScheduleRunRequest(runRequest, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                if (cancellationToken.IsCancellationRequested) break;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                await snsMessageMediator.PublishMessage(
                    new SnsMessage($"Error running cron job {cronSchedule}",
                        ex.ToString()),
                    cancellationToken);

                logger.LogError(ex, "Error running cron job {schedule}", cronSchedule);
            }
    }
}