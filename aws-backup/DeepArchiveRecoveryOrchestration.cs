using Microsoft.Extensions.Hosting;

namespace aws_backup;

public class DeepArchiveRecoveryOrchestration(
    IS3Service s3Service,
    IMediator mediator,
    Configuration configuration
) : BackgroundService
{
    private Dictionary<ByteArrayKey, CloudChunkDetails> _recoveryQueue = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var recoveryTask = Task.Run(async () =>
        {
            await foreach (var (restoreId, filePath, cloudChunkDetails) in mediator.GetRecoveryQueue(cancellationToken))
            {
                var key = new ByteArrayKey(chunkKey);
                if (_recoveryQueue.ContainsKey(key)) continue;

                _recoveryQueue[key] = cloudChunkDetails;
                await s3Service.RecoverChunk(runId, cloudChunkDetails, cancellationToken);
            }
        }, cancellationToken);

        var schedulingTask = Task.Run(async () => { }, cancellationToken);
        
        return Task.WhenAll(recoveryTask, schedulingTask);
    }
}