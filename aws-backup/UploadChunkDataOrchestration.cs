using System.Security.Cryptography;
using Amazon.S3;
using Amazon.S3.Transfer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class UploadChunkDataOrchestration(
    IMediator mediator,
    ILogger<UploadChunkDataOrchestration> logger,
    IAwsClientFactory awsClientFactory,
    IContextResolver contextResolver,
    IDataChunkService dataChunkService,
    IArchiveService archiveService)
    : BackgroundService
{
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var uploadConcurrency = contextResolver.ResolveUploadConcurrency();
        // Spin up N worker loops
        _workers = new Task[uploadConcurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var (runId, parentFile, chunk) in mediator.GetChunks(cancellationToken))
        {
            if (!dataChunkService.ChunkRequiresUpload(chunk) &&
                !archiveService.FileIsSkipped(parentFile))
            {
                logger.LogInformation("Skipping chunk {ChunkIndex} for file {LocalFilePath} - already uploaded",
                    chunk.ChunkIndex, chunk.LocalFilePath);
                if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
                continue;
            }

            var uploadAttempts = 0;
            var maxUploadAttempts = contextResolver.ResolveUploadAttemptLimit();
            var uploadRetryDelay = contextResolver.ResolveUploadRetryDelay();

            retryUpload:
            try
            {
                if (uploadAttempts >= maxUploadAttempts)
                {
                    logger.LogWarning(
                        "Max upload attempts reached for chunk {ChunkIndex} for file {LocalFilePath}. Skipping upload.",
                        chunk.ChunkIndex, chunk.LocalFilePath);
                    if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
                    await archiveService.RecordFailedFile(runId, parentFile,
                        new Exception("Max upload attempts reached"),
                        cancellationToken);
                    continue;
                }

                var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
                var bucketName = contextResolver.ResolveS3BucketName();
                var storageClass = contextResolver.ResolveColdStorage();
                var serverSideEncryptionMethod = contextResolver.ResolveServerSideEncryptionMethod();
                var s3PartSize = contextResolver.ResolveS3PartSize();
                var key = contextResolver.ResolveS3Key(chunk);

                // upload the chunk file to S3
                var transferUtil = new TransferUtility(s3Client);
                var uploadReq = new TransferUtilityUploadRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    FilePath = chunk.LocalFilePath,
                    PartSize = s3PartSize,
                    StorageClass = storageClass,
                    ServerSideEncryptionMethod = serverSideEncryptionMethod,
                    ChecksumAlgorithm = ChecksumAlgorithm.SHA256
                };

                await transferUtil.UploadAsync(uploadReq, cancellationToken);

                var head = await s3Client.GetObjectMetadataAsync(bucketName, key, cancellationToken);

                // This property is non-null if S3 computed a SHA-256 checksum on the object
                var s3CheckSum = head.ChecksumSHA256;
                var localCheckSum = ComputeLocalSha256Base64(chunk.LocalFilePath);

                if (s3CheckSum != localCheckSum)
                {
                    logger.LogWarning("Checksum mismatch for chunk {ChunkIndex} for file {LocalFilePath}. " +
                                      "S3: {S3Checksum}, Local: {LocalChecksum}. Retrying upload...",
                        chunk.ChunkIndex, chunk.LocalFilePath, s3CheckSum, localCheckSum);

                    // Wait before retrying
                    await Task.Delay(TimeSpan.FromSeconds(uploadRetryDelay), cancellationToken);

                    // Increment the attempt counter
                    uploadAttempts++;

                    // Retry the upload
                    goto retryUpload;
                }

                await dataChunkService.MarkChunkAsUploaded(chunk, key, bucketName, cancellationToken);
                if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing chunk {ChunkIndex} for file {LocalFilePath}", chunk.ChunkIndex,
                    chunk.LocalFilePath);
                // Wait before retrying
                await Task.Delay(TimeSpan.FromSeconds(uploadRetryDelay), cancellationToken);

                // Increment the attempt counter
                uploadAttempts++;

                // Retry the upload
                goto retryUpload;
            }
        }
    }

    private static string ComputeLocalSha256Base64(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return Convert.ToBase64String(hash);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal no more items
        // (Producer side should call sharedChannel.Writer.Complete())
        await base.StopAsync(cancellationToken);

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts =
            new CancellationTokenSource(TimeSpan.FromSeconds(contextResolver.ResolveShutdownTimeoutSeconds()));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}