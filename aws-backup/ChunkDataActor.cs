using System.Collections.Concurrent;
using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// ReSharper disable AccessToModifiedClosure

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

    Task FlushPendingBatchesToS3(ArchiveRun archiveRun, TaskCompletionSource taskCompletionSource,
        CancellationToken cancellationToken);
}

public sealed class ChunkDataActor(
    CountdownEvent countdown,
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
    private static readonly ConcurrentDictionary<Guid, UploadBatch?> Batches = [];
    private static readonly ConcurrentDictionary<Guid, FileStream?> Streams = [];
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

    private async Task FlushPendingBatchesAsync(ArchiveRun archiveRun, TaskCompletionSource taskCompletionSource,
        CancellationToken cancellationToken)
    {
        var bufferSize = contextResolver.ReadBufferSize();
        var cacheFolder = contextResolver.LocalCacheFolder();

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

        foreach (var threadId in Batches
                     .Where(kp => kp.Value?.ArchiveRun.RunId == archiveRun.RunId)
                     .Select(kp => kp.Key))
            if (Streams.TryGetValue(threadId, out var stream) && stream is not null &&
                Batches.TryGetValue(threadId, out var batch) && batch is not null)
            {
                logger.LogInformation("Flushing stream for thread {ThreadId} in archive run {ArchiveRunId}",
                    threadId, archiveRun.RunId);


                // Flush the stream to the final batch stream
                await Streams[threadId]!.FlushAsync(cancellationToken);
                await Streams[threadId]!.DisposeAsync();
                Streams[threadId] = null;

                // Write the batch to the final stream
                await using var src = new FileStream(
                    batch.LocalFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    bufferSize,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);

                await src.CopyToAsync(finalBatchStream, bufferSize, cancellationToken);
                File.Delete(batch.LocalFilePath);

                finalBatch.Requests.AddRange(batch.Requests);
                finalBatch.FileSize += batch.FileSize;
                Batches[threadId] = null;
            }
            else
            {
                logger.LogWarning("No stream or batch found for thread {ThreadId} in archive run {ArchiveRunId}",
                    threadId, archiveRun.RunId);
            }

        await finalBatchStream.FlushAsync(cancellationToken);
        await finalBatchStream.DisposeAsync();

        await batchMediator.ProcessBatch(finalBatch, cancellationToken);

        taskCompletionSource.SetResult();
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        var bufferSize = contextResolver.ReadBufferSize();
        var chunkSize = awsConfiguration.ChunkSizeBytes;
        var index = Guid.NewGuid();

        Streams[index] = null;
        Batches[index] = null;

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
                    ((UploadChunkRequest)state).FileMetaData,
                    ((UploadChunkRequest)state).DataChunkDetails,
                    state.Exception ?? new Exception("Exceeded limit"),
                    token);

            if (await dataChunkService.ChunkAlreadyUploaded(chunk, cancellationToken) ||
                fileMetaData.Status is FileStatus.Skipped)
            {
                logger.LogInformation(
                    "Skipping chunk {ChunkHash} {ChunkIndex} for file {LocalFilePath} - already uploaded",
                    Base64Url.Encode(chunk.HashKey), chunk.ChunkIndex, fileMetaData.LocalFilePath);

                await archiveService.RecordChunkUpload(
                    archiveRun,
                    fileMetaData,
                    chunk,
                    cancellationToken);
                if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
                continue;
            }

            try
            {
                countdown.AddCount();
                logger.LogInformation("Processing chunk {ChunkIndex} for file {LocalFilePath} parent {ParentFile}",
                    chunk.ChunkIndex, chunk.LocalFilePath, fileMetaData);

                var dataSize = chunk.Size;

                if (Batches[index] is null || Streams[index] is null ||
                    Batches[index]!.FileSize > chunkSize)
                    // If the batch is null or the file size exceeds the chunk size, flush the current batch
                    await FlushToS3(index, true, cancellationToken);

                Batches[index] ??= new UploadBatch(Streams[index]!.Name, archiveRun);

                logger.LogInformation("Adding chunk {ChunkIndex} of {ParentFile} to batch {BatchFileName}",
                    chunk.ChunkIndex, uploadRequest.FileMetaData.LocalFilePath, Batches[index]!.LocalFilePath);

                await using var src = new FileStream(
                    chunk.LocalFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    bufferSize,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);

                await src.CopyToAsync(Streams[index]!, bufferSize, cancellationToken);

                Batches[index]!.Requests.Add(uploadRequest);
                Batches[index]!.FileSize += dataSize;

                if (File.Exists(chunk.LocalFilePath)) File.Delete(chunk.LocalFilePath);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                if (Streams[index] is not null)
                {
                    await Streams[index]!.FlushAsync(cancellationToken);
                    await Streams[index]!.DisposeAsync();
                    Streams[index] = null;
                }

                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Upload failed");
                uploadRequest.Exception = ex;
                await retryMediator.RetryAttempt(uploadRequest, cancellationToken);
            }
            finally
            {
                countdown.Signal();
            }
        }
    }

    private async Task FlushToS3(Guid index, bool renewStream, CancellationToken token)
    {
        var bufferSize = contextResolver.ReadBufferSize();
        if (Streams[index] is not null)
        {
            await Streams[index]!.FlushAsync(token);
            await Streams[index]!.DisposeAsync();
            Streams[index] = null;
        }

        if (Batches[index] is not null)
        {
            logger.LogInformation("Processing batch {BatchFileName} for upload", Batches[index]!.LocalFilePath);
            await batchMediator.ProcessBatch(Batches[index]!, token);
            Batches[index] = null;
        }

        if (!renewStream) return;

        var cacheFolder = contextResolver.LocalCacheFolder();

        if (!Directory.Exists(cacheFolder))
            Directory.CreateDirectory(cacheFolder);

        var batchFileName = Guid.NewGuid().ToString("N");
        var outPath = Path.Combine(cacheFolder, batchFileName);

        Streams[index] = new FileStream(
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
            Streams.Keys.Select(k => Task.Run(() => FlushToS3(k, false, cancellationToken), cancellationToken));
        Task[] allTasks = [..flushTasks, .._workers];

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts =
            new CancellationTokenSource(TimeSpan.FromSeconds(contextResolver.ShutdownTimeoutSeconds()));
        await Task.WhenAny(Task.WhenAll(allTasks), Task.Delay(-1, timeoutCts.Token));
    }
}