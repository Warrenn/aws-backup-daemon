using Amazon.S3.Transfer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class ChunkDataProcessor(
    IMediator mediator,
    Configuration configuration,
    ILogger<ChunkDataProcessor> logger,
    IAwsClientFactory awsClientFactory,
    IContextResolver contextResolver,
    IArchiveService archiveService,
    IDataChunkService dataChunkService)
    : BackgroundService
{
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Spin up N worker loops
        _workers = new Task[configuration.UploadS3Concurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(stoppingToken), stoppingToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken stoppingToken)
    {
        await foreach (var chunk in mediator.Chunks(stoppingToken))
            try
            {
                if (!await dataChunkService.ChunkRequiresUpload(chunk, stoppingToken))
                {
                    logger.LogInformation("Skipping chunk {ChunkIndex} for file {LocalFilePath} - already uploaded",
                        chunk.ChunkIndex, chunk.LocalFilePath);
                    if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
                    continue;
                }

                var s3Client = awsClientFactory.CreateS3Client(configuration);
                var bucketName = contextResolver.ResolveS3BucketName(configuration);
                var storageClass = contextResolver.ResolveColdStorage(configuration);
                var serverSideEncryptionMethod = contextResolver.ResolveServerSideEncryptionMethod(configuration);
                var key = contextResolver.ResolveS3Key(chunk, configuration);

                // upload the chunk file to S3
                var transferUtil = new TransferUtility(s3Client);
                var uploadReq = new TransferUtilityUploadRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    FilePath = chunk.LocalFilePath,
                    PartSize = configuration.S3PartSize,
                    StorageClass = storageClass,
                    ServerSideEncryptionMethod = serverSideEncryptionMethod
                };

                await transferUtil.UploadAsync(uploadReq, stoppingToken);
                await dataChunkService.MarkChunkAsUploaded(chunk, key, bucketName, stoppingToken);

                if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing chunk {ChunkIndex} for file {LocalFilePath}", chunk.ChunkIndex,
                    chunk.LocalFilePath);
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
