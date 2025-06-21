using Microsoft.Extensions.Hosting;
using Timer = System.Timers.Timer;

namespace aws_backup;

public class RetryFileProcessor(
    IMediator mediator,
    IContextFactory contextFactory,
    Configuration configuration,
    IArchiveService archiveService) : BackgroundService
{
    private readonly Dictionary<string, Exception> _failedFiles = [];
    private readonly Dictionary<string, int> _retryAttempts = [];
    private readonly Dictionary<string, Timer> _timers = [];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var (archive, filePath, exception) in mediator.GetRetries(stoppingToken))
        {
            if (_failedFiles.ContainsKey(filePath)) continue;

            var attemptNo = 0;
            if (_retryAttempts.TryGetValue(filePath, out var attempts))
                attemptNo = attempts + 1;

            if (attemptNo > configuration.MaxRetryAttempts)
            {
                _failedFiles.Add(filePath, exception);
                await archiveService.RecordFailedFile(archive.RunId, filePath, exception, stoppingToken);
                continue;
            }

            var timeAlg = contextFactory.ResolveRetryTimeAlgorithm(configuration);
            _retryAttempts[filePath] = attemptNo;
            var retryDelay = timeAlg(attemptNo, filePath, exception);
            if (_timers.TryGetValue(filePath, out var existingTimer))
            {
                existingTimer.Stop();
                existingTimer.Dispose();
                _timers.Remove(filePath);
            }

            var timer = new Timer(retryDelay);
            timer.AutoReset = false;

            timer.Elapsed += async (_, _) =>
            {
                timer.Stop();
                await mediator.ProcessFile(archive.RunId, filePath, stoppingToken);
            };

            timer.Start();
            _timers[filePath] = timer;
        }
    }
}