using System.Threading.Channels;
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

public class ChunkDataProcessor(
    ChannelReader<ChunkData> chunkQueue,
    ILogger<FixedPoolFileProcessor> logger,
    int maxDegreeOfParallelism = 4,
    int shutdownTimeoutSeconds = 120)
    : BackgroundService
{
    private readonly ChannelReader<ChunkData> _chunkQueue = chunkQueue;
    private readonly ILogger<FixedPoolFileProcessor> _logger = logger;
    private readonly int _maxDegreeOfParallelism = maxDegreeOfParallelism;
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
        await foreach (var chunk in _chunkQueue.ReadAllAsync(ct))
            try
            {
                // see if it not in dynamo db
                // add the chunk data to dynamo db but mark it as not processed
                // upload the chunk file to S3
                // update the chunk data in dynamo db to mark it as processed
                var transferUtil = new TransferUtility(new AmazonS3Client(new AmazonS3Config
                {
                    RegionEndpoint = RegionEndpoint.USEast1, // specify your region
                    UseAccelerateEndpoint = true
                    // Region = "us-west-2", // specify your region
                    // UseHttp = true, // if you want to use HTTP instead of HTTPS
                }));
                var uploadReq = new TransferUtilityUploadRequest
                {
                    // BucketName = bucketName,
                    // Key        = key,
                    // FilePath   = filePath,
                    PartSize = 5 * 1024 * 1024, // 5 MiB parts
                    StorageClass = S3StorageClass.Standard,
                    ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256
                };

                await transferUtil.UploadAsync(uploadReq, ct);

                if (File.Exists(chunk.FilePath)) File.Delete(chunk.FilePath);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
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
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(_shutdownTimeoutSeconds));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}