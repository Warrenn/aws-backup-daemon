using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Timer = System.Timers.Timer;

namespace aws_backup;

public class RetryFileProcessor(
    ChannelReader<(string filePath, Exception exception)> retryQueue,
    ChannelWriter<string> fileWriter,
    int retryLimit,
    Func<int, string, Exception, TimeSpan> timeAlg) : BackgroundService, IRetryManager
{
    private readonly Dictionary<string, Exception> _failedFiles = [];
    private readonly Dictionary<string, int> _retryAttempts = [];

    public Task ClearPendingRetries(string archive, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<(string filePath, Exception exception)[]> GetFailedRetries(string archive,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var (filePath, exception) in retryQueue.ReadAllAsync(stoppingToken))
        {
            if (_failedFiles.ContainsKey(filePath)) continue;

            var attemptNo = 0;
            if (_retryAttempts.TryGetValue(filePath, out var attempts))
                attemptNo = attempts + 1;

            if (attemptNo > retryLimit)
            {
                _failedFiles[filePath] = exception;
                continue;
            }

            _retryAttempts[filePath] = attemptNo;
            var retryDelay = timeAlg(attemptNo, filePath, exception);
            var timer = new Timer(retryDelay);

            timer.Elapsed += async (_, _) =>
            {
                timer.Stop();
                await fileWriter.WriteAsync(filePath, stoppingToken);
            };

            timer.Start();
        }
    }
}