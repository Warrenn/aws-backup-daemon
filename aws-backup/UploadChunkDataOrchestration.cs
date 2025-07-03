using System.Security.Cryptography;
using Amazon.S3;
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

public sealed class UploadChunkDataOrchestration(
    IUploadChunksMediator mediator,
    ILogger<UploadChunkDataOrchestration> logger,
    IAwsClientFactory awsClientFactory,
    IContextResolver contextResolver,
    IDataChunkService dataChunkService,
    IArchiveService archiveService,
    IRetryMediator retryMediator)
    : BackgroundService
{
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var uploadConcurrency = contextResolver.NoOfS3FilesToUploadConcurrently();
        // Spin up N worker loops
        _workers = new Task[uploadConcurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var request in mediator.GetChunks(cancellationToken))
        {
            var (_, parentFile, chunk) = request;
            request.RetryLimit = contextResolver.UploadAttemptLimit();
            request.Retry ??= (state, token) =>
                mediator.ProcessChunk((UploadChunkRequest)state, token);
            request.LimitExceeded ??= (state, token) =>
                archiveService.RecordFailedFile(
                    ((UploadChunkRequest)state).ArchiveRunId,
                    ((UploadChunkRequest)state).ParentFile,
                    state.Exception ?? new Exception("Exceeded limit"),
                    token);

            if (!dataChunkService.ChunkRequiresUpload(chunk) &&
                !archiveService.IsTheFileSkipped(request.ArchiveRunId, parentFile))
            {
                logger.LogInformation("Skipping chunk {ChunkIndex} for file {LocalFilePath} - already uploaded",
                    chunk.ChunkIndex, chunk.LocalFilePath);
                if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
                continue;
            }

            try
            {
                var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
                var bucketName = contextResolver.S3BucketId();
                var storageClass = contextResolver.ColdStorage();
                var serverSideEncryptionMethod = contextResolver.ServerSideEncryption();
                var s3PartSize = contextResolver.S3PartSize();
                var key = contextResolver.ChunkS3Key(
                    chunk.LocalFilePath,
                    chunk.ChunkIndex,
                    chunk.ChunkSize,
                    chunk.HashKey,
                    chunk.ChunkSize);

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
                    logger.LogWarning(
                        "Checksum mismatch for chunk {ChunkIndex} for file {LocalFilePath}. S3: {S3Checksum}, Local: {LocalChecksum}. Retrying upload...",
                        chunk.ChunkIndex, chunk.LocalFilePath, s3CheckSum, localCheckSum);

                    request.Exception = new InvalidOperationException(
                        $"Checksum mismatch for chunk {chunk.ChunkIndex} for file {chunk.LocalFilePath}.S3: {s3CheckSum}, Local: {localCheckSum}");
                    await retryMediator.RetryAttempt(request, cancellationToken);
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
                request.Exception = ex;
                await retryMediator.RetryAttempt(request, cancellationToken);
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
            new CancellationTokenSource(TimeSpan.FromSeconds(contextResolver.ShutdownTimeoutSeconds()));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}