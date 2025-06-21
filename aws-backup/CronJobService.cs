using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Cronos;

public class CronJobService : BackgroundService
{
    private readonly CronExpression _cronExpression;
    private readonly Func<CancellationToken, Task> _job;
    private readonly ILogger<CronJobService> _logger;

    public CronJobService(
        string cronSchedule,
        Func<CancellationToken, Task> job,
        ILogger<CronJobService> logger // or "America/New_York", etc.
    )
    {
        _cronExpression = CronExpression.Parse(cronSchedule, CronFormat.Standard);
        _job = job;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("CronJobService started with schedule '{Schedule}' in zone '{Zone}'",
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
                _logger.LogWarning("Cron expression '{Schedule}' will not occur again.", _cronExpression);
                break;
            }

            var delay = nextUtc.Value - DateTimeOffset.UtcNow;
            if (delay.TotalMilliseconds <= 0)
                // If we're already past it (due to drift), skip ahead
                continue;

            _logger.LogInformation("Next run at {NextRun}", nextUtc.Value);
            try
            {
                // 2) Wait until it's time (or until shutdown)
                await Task.Delay(delay, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break; // shutting down
            }

            // 3) Invoke your job
            try
            {
                _logger.LogInformation("Cron job starting at {Now}", DateTimeOffset.UtcNow);
                await _job(stoppingToken);
                _logger.LogInformation("Cron job completed at {Now}", DateTimeOffset.UtcNow);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogError(ex, "Error running cron job");
            }

            // loop for the next occurrence...
        }

        _logger.LogInformation("CronJobService is stopping.");
    }
}