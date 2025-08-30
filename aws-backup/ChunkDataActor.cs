using System.Collections.Concurrent;
using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IChunkRequest;

public sealed record UploadChunkRequest(
    ArchiveRun ArchiveRun,
    FileMetaData FileMetaData,
    DataChunkDetails DataChunkDetails) : RetryState, IChunkRequest;

public sealed record FlushS3ToS3Request(ArchiveRun ArchiveRun, TaskCompletionSource TaskCompletionSource)
    : IChunkRequest;

public interface IUploadChunksMediator
{
    IAsyncEnumerable<IChunkRequest> GetChunkRequests(CancellationToken cancellationToken);
    Task ProcessChunk(UploadChunkRequest request, CancellationToken cancellationToken);

    Task FlushPendingBatchesToS3(ArchiveRun archiveRun, TaskCompletionSource taskCompletion,
        CancellationToken cancellationToken);
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
    private readonly ConcurrentDictionary<Guid, UploadBatch?> _batches = [];
    private readonly ConcurrentDictionary<Guid, FileStream?> _streams = [];
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

    private async Task FlushPendingBatchesAsync(ArchiveRun archiveRun, TaskCompletionSource taskCompletion,
        CancellationToken cancellationToken)
    {
        var bufferSize = contextResolver.ReadBufferSize();
        var cacheFolder = contextResolver.LocalCacheFolder();
        var batchSizeLimit = awsConfiguration.S3BatchSizeBytes;

        var batchFileName = Guid.NewGuid().ToString("N");
        var outPath = Path.Combine(cacheFolder, batchFileName);

        var finalBatchStream = new FileStream(
            outPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        
        var finalBatch = new UploadBatch(finalBatchStream.Name, archiveRun);
        foreach (var threadId in _batches
                     .Where(kp => kp.Value?.ArchiveRun.RunId == archiveRun.RunId)
                     .Select(kp => kp.Key))
            if (_streams.TryGetValue(threadId, out var stream) && stream is not null &&
                _batches.TryGetValue(threadId, out var batch) && batch is not null)
            {
                logger.LogInformation("Flushing stream for thread {ThreadId} in archive run {ArchiveRunId}",
                    threadId, archiveRun.RunId);
                
                await _streams[threadId]!.FlushAsync(cancellationToken);
                await _streams[threadId]!.DisposeAsync();
                _streams[threadId] = null;

                var buffer = new byte[bufferSize];
                var bytesToWrite = 0L;
                var read = 0;

                await using var src = new FileStream(
                    batch.LocalFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    bufferSize,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);

                foreach (var uploadChunkRequest in batch.Requests)
                {
                    var details = uploadChunkRequest.DataChunkDetails;
                    var bytesRemainingInChunk = details.CompressedSize;

                    if (bytesToWrite < read)
                    {
                        await finalBatchStream.WriteAsync(buffer.AsMemory((int)bytesToWrite), cancellationToken);
                        bytesRemainingInChunk -= read - bytesToWrite;
                    }

                    while ((read = await src.ReadAsync(buffer, cancellationToken)) > 0)
                    {
                        bytesToWrite = Math.Min(read, bytesRemainingInChunk);
                        await finalBatchStream.WriteAsync(buffer.AsMemory(0, (int)bytesToWrite), cancellationToken);
                        bytesRemainingInChunk -= bytesToWrite;

                        if (bytesRemainingInChunk <= 0) // read the entire chunk
                            break;
                    }

                    finalBatch.Requests.Add(uploadChunkRequest);
                    finalBatch.FileSize += details.CompressedSize;

                    if (finalBatch.FileSize < batchSizeLimit) continue;

                    await finalBatchStream.FlushAsync(cancellationToken);
                    await finalBatchStream.DisposeAsync();

                    await batchMediator.ProcessBatch(finalBatch, cancellationToken);

                    batchFileName = Guid.NewGuid().ToString("N");
                    outPath = Path.Combine(cacheFolder, batchFileName);

                    finalBatchStream = new FileStream(
                        outPath,
                        FileMode.Create,
                        FileAccess.Write,
                        FileShare.None,
                        bufferSize,
                        FileOptions.Asynchronous | FileOptions.SequentialScan);
                    finalBatch = new UploadBatch(finalBatchStream.Name, archiveRun);
                }

                File.Delete(batch.LocalFilePath);
                _batches[threadId] = null;
            }
            else
            {
                logger.LogWarning("No stream or batch found for thread {ThreadId} in archive run {ArchiveRunId}",
                    threadId, archiveRun.RunId);
            }

        await finalBatchStream.FlushAsync(cancellationToken);
        await finalBatchStream.DisposeAsync();

        if (finalBatch.Requests.Count > 0)
        {
            await batchMediator.ProcessBatch(finalBatch, cancellationToken);
        }
        else
        {
            if (File.Exists(finalBatch.LocalFilePath)) File.Delete(finalBatch.LocalFilePath);
        }

        taskCompletion.SetResult();
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        var bufferSize = contextResolver.ReadBufferSize();
        var chunkSize = awsConfiguration.S3BatchSizeBytes;
        var index = Guid.NewGuid();

        _streams[index] = null;
        _batches[index] = null;

        //this should block the producer side due to bounded channel
        await foreach (var chunkRequest in mediator.GetChunkRequests(cancellationToken))
        {
            if (chunkRequest is FlushS3ToS3Request flushRequest)
            {
                logger.LogInformation("Flushing pending batches for archive run {ArchiveRunId}",
                    flushRequest.ArchiveRun.RunId);
                await FlushPendingBatchesAsync(flushRequest.ArchiveRun, flushRequest.TaskCompletionSource,
                    cancellationToken);
                continue;
            }

            if (chunkRequest is not UploadChunkRequest uploadRequest) continue;
            var (archiveRun, fileMetaData, chunk) = uploadRequest;

            uploadRequest.RetryLimit = contextResolver.UploadAttemptLimit();
            uploadRequest.Retry ??= (state, token) =>
                mediator.ProcessChunk((UploadChunkRequest)state, token);
            uploadRequest.LimitExceeded ??= (state, token) =>
                archiveService.RecordFailedChunk(
                    ((UploadChunkRequest)state).ArchiveRun,
                    ((UploadChunkRequest)state).DataChunkDetails,
                    state.Exception ?? new Exception("Exceeded limit"),
                    token);

            if (await dataChunkService.ChunkAlreadyUploaded(chunk, cancellationToken) ||
                fileMetaData.Status is FileStatus.Skipped)
            {
                logger.LogInformation(
                    "Skipping chunk {ChunkHash} {ChunkIndex} for file {LocalFilePath} - already uploaded",
                    Base64Url.Encode(chunk.HashId), chunk.Offset, fileMetaData.LocalFilePath);

                await archiveService.RecordChunkUpload(
                    archiveRun,
                    chunk,
                    cancellationToken);
                if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
                continue;
            }

            try
            {
                logger.LogInformation("Processing chunk {ChunkIndex} for file {LocalFilePath} parent {ParentFile}",
                    chunk.Offset, chunk.LocalFilePath, fileMetaData);

                var dataSize = chunk.CompressedSize;

                if (_batches[index] is null || _streams[index] is null ||
                    _batches[index]!.FileSize + dataSize > chunkSize)
                    // If the batch is null or the file size exceeds the chunk size, flush the current batch
                    await FlushToS3(index, true, cancellationToken);

                _batches[index] ??= new UploadBatch(_streams[index]!.Name, archiveRun);

                logger.LogInformation("Adding chunk {ChunkIndex} of {ParentFile} to batch {BatchFileName}",
                    chunk.Offset, uploadRequest.FileMetaData.LocalFilePath, _batches[index]!.LocalFilePath);

                await using var src = new FileStream(
                    chunk.LocalFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    bufferSize,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);

                await src.CopyToAsync(_streams[index]!, bufferSize, cancellationToken);

                _batches[index]!.Requests.Add(uploadRequest);
                _batches[index]!.FileSize += dataSize;

                if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                if (_streams[index] is not null)
                {
                    await _streams[index]!.FlushAsync(cancellationToken);
                    await _streams[index]!.DisposeAsync();
                    _streams[index] = null;
                }

                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Upload failed");
                uploadRequest.Exception = ex;
                await retryMediator.RetryAttempt(uploadRequest, cancellationToken);
            }
        }
    }

    private async Task FlushToS3(Guid index, bool renewStream, CancellationToken token)
    {
        var bufferSize = contextResolver.ReadBufferSize();
        if (_streams[index] is not null)
        {
            await _streams[index]!.FlushAsync(token);
            await _streams[index]!.DisposeAsync();
            _streams[index] = null;
        }

        if (_batches[index] is not null)
        {
            logger.LogInformation("Processing batch {BatchFileName} for upload", _batches[index]!.LocalFilePath);
            await batchMediator.ProcessBatch(_batches[index]!, token);
            _batches[index] = null;
        }

        if (!renewStream) return;

        var cacheFolder = contextResolver.LocalCacheFolder();

        if (!Directory.Exists(cacheFolder))
            Directory.CreateDirectory(cacheFolder);

        var batchFileName = Guid.NewGuid().ToString("N");
        var outPath = Path.Combine(cacheFolder, batchFileName);

        _streams[index] = new FileStream(
            outPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        var flushTasks =
            _streams.Keys.Select(k => Task.Run(() => FlushToS3(k, false, cancellationToken), cancellationToken));
        Task[] allTasks = [..flushTasks, .._workers];

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts =
            new CancellationTokenSource(TimeSpan.FromSeconds(contextResolver.ShutdownTimeoutSeconds()));
        await Task.WhenAny(Task.WhenAll(allTasks), Task.Delay(-1, timeoutCts.Token));
    }
}