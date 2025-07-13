using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public sealed record UploadChunkRequest(
    string ArchiveRunId,
    string ParentFile,
    DataChunkDetails DataChunkDetails) : RetryState;

public interface IUploadChunksMediator
{
    IAsyncEnumerable<UploadChunkRequest> GetChunks(CancellationToken cancellationToken);
    Task ProcessChunk(UploadChunkRequest request, CancellationToken cancellationToken);
}

public sealed class UploadChunkDataActor(
    IUploadChunksMediator mediator,
    ILogger<UploadChunkDataActor> logger,
    IAwsClientFactory awsClientFactory,
    IContextResolver contextResolver,
    IDataChunkService dataChunkService,
    IArchiveService archiveService,
    IRetryMediator retryMediator,
    AwsConfiguration awsConfiguration)
    : BackgroundService
{
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Upload Chunk Actor started");
        var concurrency = contextResolver.NoOfS3FilesToUploadConcurrently();
        // Spin up N worker loops
        _workers = new Task[concurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        //this should block the producer side due to bounded channel
        await foreach (var request in mediator.GetChunks(cancellationToken))
        {
            var (_, parentFile, chunk) = request;
            request.RetryLimit = contextResolver.UploadAttemptLimit();
            request.Retry ??= (state, token) =>
                mediator.ProcessChunk((UploadChunkRequest)state, token);
            request.LimitExceeded ??= (state, token) =>
                archiveService.RecordFailedChunk(
                    ((UploadChunkRequest)state).ArchiveRunId,
                    ((UploadChunkRequest)state).ParentFile,
                    ((UploadChunkRequest)state).DataChunkDetails.HashKey,
                    state.Exception ?? new Exception("Exceeded limit"),
                    token);

            if (dataChunkService.ChunkAlreadyUploaded(chunk) ||
                archiveService.IsTheFileSkipped(request.ArchiveRunId, parentFile))
            {
                logger.LogInformation("Skipping chunk {ChunkIndex} for file {LocalFilePath} - already uploaded",
                    chunk.ChunkIndex, chunk.LocalFilePath);
                await archiveService.RecordChunkUpload(
                    request.ArchiveRunId,
                    parentFile,
                    chunk.HashKey,
                    cancellationToken);
                if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
                continue;
            }

            try
            {
                logger.LogInformation("Uploading chunk {ChunkIndex} for file {LocalFilePath} parent {ParentFile}",
                    chunk.ChunkIndex, chunk.LocalFilePath, parentFile);

                var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
                var bucketName = awsConfiguration.BucketName;
                var storageClass = contextResolver.ColdStorage();
                var serverSideEncryptionMethod = contextResolver.ServerSideEncryption();
                var s3PartSize = contextResolver.S3PartSize();
                var key = contextResolver.ChunkS3Key(chunk.HashKey);

                var localCheckSum = Convert.ToBase64String(chunk.CompressedHashKey);

                // upload the chunk file to S3
                var transferUtil = new TransferUtility(s3Client);
                var uploadReq = new TransferUtilityUploadRequest
                {
                    ChecksumSHA256 = localCheckSum,
                    BucketName = bucketName,
                    Key = key,
                    FilePath = chunk.LocalFilePath,
                    PartSize = s3PartSize,
                    StorageClass = storageClass,
                    ServerSideEncryptionMethod = serverSideEncryptionMethod,
                    ChecksumAlgorithm = ChecksumAlgorithm.SHA256,
                    TagSet =
                    [
                        new Tag { Key = "storage-class", Value = "cold" },
                        new Tag { Key = "archive-run-id", Value = S3Service.ScrubTagValue(request.ArchiveRunId) },
                        new Tag { Key = "parent-file", Value = S3Service.ScrubTagValue(parentFile) },
                        new Tag { Key = "chunk-index", Value = S3Service.ScrubTagValue(chunk.ChunkIndex.ToString()) },
                        new Tag { Key = "size", Value = S3Service.ScrubTagValue(chunk.Size.ToString()) },
                        new Tag { Key = "chunk-hash", Value = S3Service.ScrubTagValue(key) },
                        new Tag
                        {
                            Key = "chunk-size-setting",
                            Value = S3Service.ScrubTagValue(awsConfiguration.ChunkSizeBytes.ToString())
                        }
                    ]
                };

                await transferUtil.UploadAsync(uploadReq, cancellationToken);

                logger.LogInformation("Upload complete for chunk {ChunkIndex} for file {ParentFile} chunk file {LocalFilePath} chunk key {Key}",
                    chunk.ChunkIndex, parentFile, chunk.LocalFilePath, key);

                logger.LogInformation("Marking chunk {ChunkIndex} for file {ParentFile} as uploaded. Key: {Key}, Bucket: {BucketName}, LocalFilePath: {LocalFilePath}",
                    chunk.ChunkIndex, parentFile, key, bucketName, chunk.LocalFilePath);
                await dataChunkService.MarkChunkAsUploaded(chunk, key, bucketName, cancellationToken);

                await archiveService.RecordChunkUpload(
                    request.ArchiveRunId,
                    parentFile,
                    chunk.HashKey,
                    cancellationToken);
                if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Upload failed");
                request.Exception = ex;
                await retryMediator.RetryAttempt(request, cancellationToken);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal no more items
        // (Producer side should call sharedChannel.Writer.Complete())
        await base.StopAsync(cancellationToken);

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts =
            new CancellationTokenSource(TimeSpan.FromSeconds(contextResolver.ShutdownTimeoutSeconds()));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}