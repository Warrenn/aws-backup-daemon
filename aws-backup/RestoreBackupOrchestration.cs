using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class RestoreBackupOrchestration(
        IMediator mediator,
        IChunkedEncryptingFileProcessor processor,
        IArchiveService archiveService,
        ILogger<ArchiveFilesOrchestration> logger,
        Configuration configuration
    ) : BackgroundService
{
    private Task[] _workers = [];
    
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Spin up N worker loops
        _workers = new Task[configuration.RestoreConcurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(stoppingToken), stoppingToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }
    
    private async Task WorkerLoopAsync(CancellationToken ct)
    {
        await foreach (var request in mediator.GetRestoreRequests<string>(ct))
            try
            {
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
            }
    }
    
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal no more items
        // (Producer side should call sharedChannel.Writer.Complete())
        await base.StopAsync(cancellationToken);

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(configuration.ShutdownTimeoutSeconds));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}