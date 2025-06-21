using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

public class FixedPoolFileProcessor(
    Channel<string> sharedChannel,
    IChunkedEncryptingFileProcessor processor,
    ILogger<FixedPoolFileProcessor> logger,
    int maxDegreeOfParallelism = 4,
    int shutdownTimeoutSeconds = 120)
    : BackgroundService
{
    private readonly ILogger<FixedPoolFileProcessor> _logger = logger;
    private readonly int _maxDegreeOfParallelism = maxDegreeOfParallelism;
    private readonly IChunkedEncryptingFileProcessor _processor = processor;
    private readonly ChannelReader<string> _queue = sharedChannel.Reader;
    private readonly int _shutdownTimeoutSeconds = shutdownTimeoutSeconds;
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Spin up N worker loops
        _workers = new Task[_maxDegreeOfParallelism];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(stoppingToken), stoppingToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken ct)
    {
        await foreach (var filePath in _queue.ReadAllAsync(ct))
            try
            {
                _logger.LogInformation("Processing {File}", filePath);
                await _processor.ProcessFileAsync(filePath, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing {File}", filePath);
            }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal no more items
        // (Producer side should call sharedChannel.Writer.Complete())
        await base.StopAsync(cancellationToken);

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(_shutdownTimeoutSeconds));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}