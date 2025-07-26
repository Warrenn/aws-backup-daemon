using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IUploadBatchMediator
{
    IAsyncEnumerable<UploadBatch> GetUploadBatches(CancellationToken cancellationToken);
    Task ProcessBatch(UploadBatch batch, CancellationToken cancellationToken);
}

public sealed class UploadBatchActor(
    ILogger<UploadBatchActor> logger,
    IUploadBatchMediator mediator,
    IContextResolver contextResolver,
    IRetryMediator retryMediator,
    IAwsClientFactory awsClientFactory,
    AwsConfiguration awsConfiguration,
    IDataChunkService dataChunkService,
    IArchiveService archiveService
) : BackgroundService
{
    private Task[] _workers = [];

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("UploadBatchActor started");
        var concurrency = contextResolver.NoOfConcurrentS3Uploads();

        _workers = new Task[concurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        await Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var batch in mediator.GetUploadBatches(cancellationToken))
            try
            {
                logger.LogInformation("Uploading batch {BatchFileName}", batch.LocalFilePath);

                var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
                var bucketName = awsConfiguration.BucketName;
                var storageClass = contextResolver.ColdStorage();
                var serverSideEncryptionMethod = contextResolver.ServerSideEncryption();
                var s3PartSize = contextResolver.S3PartSize();
                var key = contextResolver.BatchS3Key(batch.LocalFilePath);

                // upload the batch file to S3
                var transferUtil = new TransferUtility(s3Client);
                var uploadReq = new TransferUtilityUploadRequest
                {
                    ChecksumAlgorithm = ChecksumAlgorithm.CRC32C,
                    BucketName = bucketName,
                    Key = key,
                    FilePath = batch.LocalFilePath,
                    PartSize = s3PartSize,
                    StorageClass = storageClass,
                    ServerSideEncryptionMethod = serverSideEncryptionMethod,
                    TagSet =
                    [
                        new Tag { Key = "storage-class", Value = "cold" },
                        new Tag { Key = "archive-run-id", Value = S3Service.ScrubTagValue(batch.ArchiveRunId) },
                        new Tag { Key = "compression", Value = "brotli" },
                        new Tag
                        {
                            Key = "chunk-size",
                            Value = S3Service.ScrubTagValue(awsConfiguration.ChunkSizeBytes.ToString())
                        }
                    ]
                };
                await transferUtil.UploadAsync(uploadReq, cancellationToken);
                logger.LogInformation("Upload complete for batch {BatchFileName} for backup {ArchiveRunId}",
                    batch.LocalFilePath, batch.ArchiveRunId);

                var offset = 0L;
                foreach (var (archiveRunId, parentFile, chunk) in batch.Requests)
                {
                    logger.LogInformation(
                        "Marking chunk {ChunkIndex} for file {ParentFile} as uploaded. Key: {Key}, Bucket: {BucketName}, LocalFilePath: {LocalFilePath}",
                        chunk.ChunkIndex, parentFile, key, bucketName, chunk.LocalFilePath);

                    await dataChunkService.MarkChunkAsUploaded(
                        chunk,
                        offset,
                        key,
                        bucketName,
                        cancellationToken);

                    await archiveService.RecordChunkUpload(
                        archiveRunId,
                        parentFile,
                        chunk.HashKey,
                        cancellationToken);

                    offset += chunk.Size;
                }

                if (File.Exists(batch.LocalFilePath)) File.Delete(batch.LocalFilePath);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing upload batch {FileName}: {Message}", batch.LocalFilePath,
                    ex.Message);

                await RetryBatch(batch, cancellationToken);
            }
    }

    private async Task RetryBatch(UploadBatch batch, CancellationToken cancellationToken)
    {
        try
        {
            var offset = 0;
            var sourcePath = batch.LocalFilePath;
            await using var source = new FileStream(
                sourcePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                1,
                FileOptions.Asynchronous);

            foreach (var request in batch.Requests)
            {
                var (_, _, (destPath, _, _, _, size)) = request;
                var length = (int)size;
                var buffer = new byte[length];
                var read = await source.ReadAsync(buffer.AsMemory(offset, length), cancellationToken);
                if (read < length)
                    Array.Resize(ref buffer, read);

                logger.LogInformation("Re-writing to disk chunk {DestPath} with size {Size}", destPath, size);
                await File.WriteAllBytesAsync(destPath, buffer, cancellationToken);

                await retryMediator.RetryAttempt(request, cancellationToken);
                offset += read;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to retry batch {BatchFileName}: {Message}", batch.LocalFilePath, ex.Message);
            foreach (var (_, parentFile, (_, _, _, key, _)) in batch.Requests)
                await archiveService.RecordFailedChunk(batch.ArchiveRunId, parentFile, key, ex, cancellationToken);
        }
        finally
        {
            if (File.Exists(batch.LocalFilePath)) File.Delete(batch.LocalFilePath);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        using var timeoutCts =
            new CancellationTokenSource(TimeSpan.FromSeconds(contextResolver.ShutdownTimeoutSeconds()));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}