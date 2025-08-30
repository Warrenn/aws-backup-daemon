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
                        new Tag { Key = "archive-run-id", Value = S3Service.ScrubTagValue(batch.ArchiveRun.RunId) },
                        new Tag { Key = "client-id", Value = S3Service.ScrubTagValue(contextResolver.ClientId()) },
                        new Tag { Key = "compression", Value = "zstd" }
                    ]
                };
                await transferUtil.UploadAsync(uploadReq, cancellationToken);
                logger.LogInformation("Upload complete for batch {BatchFileName} for backup {ArchiveRunId}",
                    batch.LocalFilePath, batch.ArchiveRun.RunId);

                var offsetInS3Batch = 0L;
                foreach (var (archiveRun, fileMetaData, chunk) in batch.Requests)
                {
                    logger.LogInformation(
                        "Marking chunk {ChunkIndex} for file {ParentFile} as uploaded. Key: {Key}, Bucket: {BucketName}, OffsetInS3BatchFile: {OffsetInS3BatchFile} Size: {Size}",
                        chunk.Offset, fileMetaData.LocalFilePath, key, bucketName, offsetInS3Batch, chunk.Size);

                    await dataChunkService.MarkChunkAsUploaded(
                        chunk,
                        offsetInS3Batch,
                        key,
                        bucketName,
                        cancellationToken);

                    await archiveService.RecordChunkUpload(
                        archiveRun,
                        chunk,
                        cancellationToken);

                    offsetInS3Batch += chunk.CompressedSize;
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
            var bytesToWrite = 0;
            var sourcePath = batch.LocalFilePath;
            var bufferSize = contextResolver.ReadBufferSize();
            var buffer = new byte[bufferSize];
            var read = 0;

            await using var source = new FileStream(
                sourcePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize,
                FileOptions.SequentialScan);

            foreach (var request in batch.Requests)
            {
                var destPath = request.DataChunkDetails.LocalFilePath;
                var bytesRemaining = request.DataChunkDetails.CompressedSize;

                logger.LogInformation("Re-writing to disk chunk {DestPath} with size {Size}", destPath, bytesRemaining);

                await using var destFs = new FileStream(
                    destPath,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize,
                    FileOptions.WriteThrough);

                if (read > bytesToWrite)
                {
                    await destFs.WriteAsync(buffer.AsMemory(bytesToWrite), cancellationToken);
                    bytesRemaining -= read - bytesToWrite;
                }

                while ((read = await source.ReadAsync(buffer.AsMemory(), cancellationToken)) > 0)
                {
                    bytesToWrite = Math.Min(read, (int)bytesRemaining);
                    await destFs.WriteAsync(buffer.AsMemory(0, bytesToWrite), cancellationToken);
                    bytesRemaining -= bytesToWrite;
                    if (bytesRemaining <= 0) break;
                }

                logger.LogInformation("Re-write complete for chunk {DestPath}", destPath);
                await retryMediator.RetryAttempt(request, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to retry batch {BatchFileName}: {Message}", batch.LocalFilePath, ex.Message);
            foreach (var (_, _, chunk) in batch.Requests)
                await archiveService.RecordFailedChunk(batch.ArchiveRun, chunk, ex, cancellationToken);
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