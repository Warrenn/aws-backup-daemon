using System.Threading.Channels;
using aws_backup_common;

namespace aws_backup;

public sealed class Mediator(IContextResolver resolver) :
    IArchiveFileMediator,
    IRunRequestMediator,
    IDownloadFileMediator,
    IRetryMediator,
    IRestoreRequestsMediator,
    ISnsMessageMediator,
    IUploadBatchMediator,
    IS3StorageClassMediator,
    IDataStoreMediator
{
    private readonly Channel<ArchiveFileRequest> _archiveFileRequestChannel =
        Channel.CreateUnbounded<ArchiveFileRequest>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false
            });

    private readonly Channel<DataStoreCommand> _dataStoreCommandChannel =
        Channel.CreateUnbounded<DataStoreCommand>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false
            });

    private readonly Channel<DownloadFileFromS3Request> _downloadFileFromS3Channel =
        Channel.CreateUnbounded<DownloadFileFromS3Request>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false
            });

    private readonly Channel<RestoreRequest> _restoreRequestsChannel =
        Channel.CreateUnbounded<RestoreRequest>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

    private readonly Channel<RetryState> _retryStateChannel =
        Channel.CreateUnbounded<RetryState>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

    private readonly Channel<RunRequest> _runRequestChannel =
        Channel.CreateUnbounded<RunRequest>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

    private readonly Channel<string> _s3StorageClassManager =
        Channel.CreateUnbounded<string>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

    private readonly Channel<SnsMessage> _snsMessageChannel =
        Channel.CreateUnbounded<SnsMessage>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

    private readonly Channel<IUploadBatch> _uploadBatchChannel =
        Channel.CreateBounded<IUploadBatch>(
            new BoundedChannelOptions(resolver.NoOfConcurrentS3Uploads())
            {
                SingleReader = false,
                SingleWriter = false
            });

    IAsyncEnumerable<ArchiveFileRequest> IArchiveFileMediator.GetArchiveFiles(CancellationToken cancellationToken)
    {
        return _archiveFileRequestChannel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IArchiveFileMediator.ProcessFile(ArchiveFileRequest request, CancellationToken cancellationToken)
    {
        await _archiveFileRequestChannel.Writer.WriteAsync(request, cancellationToken);
    }

    public IAsyncEnumerable<DataStoreCommand> GetDataStoreCommands(CancellationToken cancellationToken)
    {
        return _dataStoreCommandChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public async Task ExecuteCommand(DataStoreCommand request, CancellationToken cancellationToken)
    {
        await _dataStoreCommandChannel.Writer.WriteAsync(request, cancellationToken);
    }

    async Task IDownloadFileMediator.DownloadFileFromS3(DownloadFileFromS3Request downloadFileFromS3Request,
        CancellationToken cancellationToken)
    {
        await _downloadFileFromS3Channel.Writer.WriteAsync(downloadFileFromS3Request, cancellationToken);
    }

    IAsyncEnumerable<DownloadFileFromS3Request> IDownloadFileMediator.GetDownloadFileRequests(
        CancellationToken cancellationToken)
    {
        return _downloadFileFromS3Channel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IRestoreRequestsMediator.RestoreBackup(RestoreRequest restoreRequest,
        CancellationToken cancellationToken)
    {
        await _restoreRequestsChannel.Writer.WriteAsync(restoreRequest, cancellationToken);
    }

    IAsyncEnumerable<RestoreRequest> IRestoreRequestsMediator.GetRestoreRequests(CancellationToken cancellationToken)
    {
        return _restoreRequestsChannel.Reader.ReadAllAsync(cancellationToken);
    }

    IAsyncEnumerable<RetryState> IRetryMediator.GetRetries(CancellationToken cancellationToken)
    {
        return _retryStateChannel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IRetryMediator.RetryAttempt(RetryState attempt, CancellationToken cancellationToken)
    {
        await _retryStateChannel.Writer.WriteAsync(attempt, cancellationToken);
    }

    IAsyncEnumerable<RunRequest> IRunRequestMediator.GetRunRequests(CancellationToken cancellationToken)
    {
        return _runRequestChannel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IRunRequestMediator.ScheduleRunRequest(RunRequest runRequest, CancellationToken cancellationToken)
    {
        await _runRequestChannel.Writer.WriteAsync(runRequest, cancellationToken);
    }

    IAsyncEnumerable<string> IS3StorageClassMediator.GetStorageClassesRequests(CancellationToken cancellationToken)
    {
        return _s3StorageClassManager.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IS3StorageClassMediator.QueryStorageClass(string key, CancellationToken cancellationToken)
    {
        await _s3StorageClassManager.Writer.WriteAsync(key, cancellationToken);
    }

    IAsyncEnumerable<SnsMessage> ISnsMessageMediator.GetMessages(CancellationToken cancellationToken)
    {
        return _snsMessageChannel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task ISnsMessageMediator.PublishMessage(SnsMessage message, CancellationToken cancellationToken)
    {
        await _snsMessageChannel.Writer.WriteAsync(message, cancellationToken);
    }

    IAsyncEnumerable<IUploadBatch> IUploadBatchMediator.GetUploadBatches(CancellationToken cancellationToken)
    {
        return _uploadBatchChannel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IUploadBatchMediator.ProcessBatch(UploadBatch batch, CancellationToken cancellationToken)
    {
        await _uploadBatchChannel.Writer.WriteAsync(batch, cancellationToken);
    }

    public async Task FinalizeBatch(ArchiveRun run, TaskCompletionSource taskCompletion,
        CancellationToken cancellationToken)
    {
        var lastBatchRequest = new FinalUploadBatch(run, taskCompletion);
        await _uploadBatchChannel.Writer.WaitToWriteAsync(cancellationToken);
        await _uploadBatchChannel.Writer.WriteAsync(lastBatchRequest, cancellationToken);
    }
}

public sealed class UploadChunksMediator(IContextResolver resolver) : IUploadChunksMediator
{
    private readonly Channel<IChunkRequest> _uploadChunksChannel = Channel.CreateBounded<IChunkRequest>(
        new BoundedChannelOptions(resolver.NoOfConcurrentS3Uploads())
        {
            SingleReader = false,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

    public IAsyncEnumerable<IChunkRequest> GetChunkRequests(CancellationToken cancellationToken)
    {
        return _uploadChunksChannel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IUploadChunksMediator.ProcessChunk(UploadChunkRequest request, CancellationToken cancellationToken)
    {
        await _uploadChunksChannel.Writer.WriteAsync(request, cancellationToken);
    }

    public async Task FlushPendingBatchesToS3(ArchiveRun archiveRun, TaskCompletionSource taskCompletionSource,
        CancellationToken cancellationToken)
    {
        var flushRequest = new FlushS3ToS3Request(archiveRun, taskCompletionSource);
        await _uploadChunksChannel.Writer.WaitToWriteAsync(cancellationToken);
        await _uploadChunksChannel.Writer.WriteAsync(flushRequest, cancellationToken);
    }
}

public sealed class CronScheduleMediator : ICronScheduleMediator
{
    private readonly Channel<string> _cronScheduleChannel =
        Channel.CreateUnbounded<string>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });

    public ValueTask<string> WaitForCronScheduleChangeAsync(CancellationToken cancellationToken = default)
    {
        return _cronScheduleChannel.Reader.ReadAsync(cancellationToken);
    }

    public ValueTask SignalCronScheduleChangeAsync(string value, CancellationToken cancellationToken = default)
    {
        return _cronScheduleChannel.Writer.WriteAsync(value, cancellationToken);
    }

    public bool SignalCronScheduleChange(string value)
    {
        return _cronScheduleChannel.Writer.TryWrite(value);
    }
}