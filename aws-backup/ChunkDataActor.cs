using System.Collections.Concurrent;
using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// ReSharper disable AccessToModifiedClosure

namespace aws_backup;

public sealed record UploadChunkRequest(
    string ArchiveRunId,
    string ParentFile,
    DataChunkDetails DataChunkDetails) : RetryState;

public interface IUploadChunksMediator
{
    void SignalReaderCompleted();
    void RegisterReader();
    IAsyncEnumerable<UploadChunkRequest> GetChunks(CancellationToken cancellationToken);
    Task ProcessChunk(UploadChunkRequest request, CancellationToken cancellationToken);
    Task WaitForAllChunksProcessed();
}

public sealed class ChunkDataActor(
    IUploadChunksMediator mediator,
    IUploadBatchMediator batchMediator,
    ILogger<ChunkDataActor> logger,
    IContextResolver contextResolver,
    IDataChunkService dataChunkService,
    IArchiveService archiveService,
    IRetryMediator retryMediator,
    AwsConfiguration awsConfiguration)
    : BackgroundService
{
    private readonly ConcurrentBag<Func<bool, CancellationToken, Task>> _flushTasks = [];
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Upload Chunk Actor started");
        var concurrency = contextResolver.NoOfFilesToBackupConcurrently();
        // Spin up N worker loops

        _workers = new Task[concurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        var bufferSize = contextResolver.ReadBufferSize();
        var chunkSize = awsConfiguration.ChunkSizeBytes;

        FileStream? batchFileStream = null;
        UploadBatch? batch = null;

        _flushTasks.Add(FlushToS3);

        while (!cancellationToken.IsCancellationRequested)
        {
            mediator.RegisterReader();

            //this should block the producer side due to bounded channel
            await foreach (var request in mediator.GetChunks(cancellationToken))
            {
                var (archiveRunId, parentFile, chunk) = request;

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

                if (await dataChunkService.ChunkAlreadyUploaded(chunk, cancellationToken) ||
                    await archiveService.IsTheFileSkipped(request.ArchiveRunId, parentFile, cancellationToken))
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
                    logger.LogInformation("Processing chunk {ChunkIndex} for file {LocalFilePath} parent {ParentFile}",
                        chunk.ChunkIndex, chunk.LocalFilePath, parentFile);

                    var dataSize = chunk.Size;

                    if (batch is null || batchFileStream is null || batch.FileSize + dataSize > chunkSize)
                        // If the batch is null or the file size exceeds the chunk size, flush the current batch
                        await FlushToS3(true, cancellationToken);

                    batch ??= new UploadBatch(batchFileStream!.Name, archiveRunId);

                    logger.LogInformation("Adding chunk {ChunkIndex} of {ParentFile} to batch {BatchFileName}",
                        chunk.ChunkIndex, request.ParentFile, batch.LocalFilePath);

                    await using var src = new FileStream(
                        chunk.LocalFilePath,
                        FileMode.Open,
                        FileAccess.Read,
                        FileShare.Read,
                        bufferSize,
                        FileOptions.Asynchronous | FileOptions.SequentialScan);

                    await src.CopyToAsync(batchFileStream!, bufferSize, cancellationToken);

                    batch.Requests.Add(request);
                    batch.FileSize += dataSize;

                    if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    if (batchFileStream is not null)
                    {
                        await batchFileStream.FlushAsync(cancellationToken);
                        await batchFileStream.DisposeAsync();
                        batchFileStream = null;
                    }

                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Upload failed");
                    request.Exception = ex;
                    await retryMediator.RetryAttempt(request, cancellationToken);
                }
            }

            mediator.SignalReaderCompleted();
            if (batch is null) continue;

            logger.LogInformation("Flushing remaining batch data to S3");
            // Flush any remaining data to S3
            await FlushToS3(false, cancellationToken);
        }

        return;

        async Task FlushToS3(bool renewStream, CancellationToken token)
        {
            if (batchFileStream is not null)
            {
                await batchFileStream.FlushAsync(token);
                await batchFileStream.DisposeAsync();
                batchFileStream = null;
            }

            if (batch is not null)
            {
                logger.LogInformation("Processing batch {BatchFileName} for upload", batch.LocalFilePath);
                await batchMediator.ProcessBatch(batch, token);
                batch = null;
            }

            if (!renewStream) return;

            var cacheFolder = contextResolver.LocalCacheFolder();

            if (!Directory.Exists(cacheFolder))
                Directory.CreateDirectory(cacheFolder);

            var batchFileName = Guid.NewGuid().ToString("N");
            var outPath = Path.Combine(cacheFolder, batchFileName);

            batchFileStream = new FileStream(
                outPath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        var flushTasks = _flushTasks.Select(t => Task.Run(() => t(false, cancellationToken), cancellationToken));
        Task[] allTasks = [..flushTasks, .._workers];

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts =
            new CancellationTokenSource(TimeSpan.FromSeconds(contextResolver.ShutdownTimeoutSeconds()));
        await Task.WhenAny(Task.WhenAll(allTasks), Task.Delay(-1, timeoutCts.Token));
    }
}