using System.Runtime.CompilerServices;
using System.Threading.Channels;
using aws_backup_common;

namespace aws_backup;

public sealed class Mediator(
    IContextResolver resolver) :
    IArchiveFileMediator,
    IRunRequestMediator,
    IDownloadFileMediator,
    IRetryMediator,
    IRestoreRequestsMediator,
    IUploadChunksMediator,
    ISnsMessageMediator,
    IUploadBatchMediator,
    ICronScheduleMediator
{
    private readonly Channel<ArchiveFileRequest> _archiveFileRequestChannel =
        Channel.CreateUnbounded<ArchiveFileRequest>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false
            });

    private readonly Channel<ArchiveRun> _archiveRunChannel =
        Channel.CreateBounded<ArchiveRun>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            });

    private readonly Channel<string> _cronScheduleChannel =
        Channel.CreateUnbounded<string>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
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

    private readonly Channel<RestoreRun> _restoreRunChannel =
        Channel.CreateBounded<RestoreRun>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
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

    private readonly Channel<SnsMessage> _snsMessageChannel =
        Channel.CreateUnbounded<SnsMessage>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

    private readonly Channel<UploadBatch> _uploadBatchChannel =
        Channel.CreateBounded<UploadBatch>(
            new BoundedChannelOptions(resolver.NoOfConcurrentS3Uploads())
            {
                SingleReader = false,
                SingleWriter = false
            });

    private readonly ChannelManager<UploadChunkRequest> _uploadChunksChannelManager =
        new(new BoundedChannelOptions(resolver.NoOfConcurrentS3Uploads())
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

    IAsyncEnumerable<SnsMessage> ISnsMessageMediator.GetMessages(CancellationToken cancellationToken)
    {
        return _snsMessageChannel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task ISnsMessageMediator.PublishMessage(SnsMessage message, CancellationToken cancellationToken)
    {
        await _snsMessageChannel.Writer.WriteAsync(message, cancellationToken);
    }

    IAsyncEnumerable<UploadBatch> IUploadBatchMediator.GetUploadBatches(CancellationToken cancellationToken)
    {
        return _uploadBatchChannel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IUploadBatchMediator.ProcessBatch(UploadBatch batch, CancellationToken cancellationToken)
    {
        await _uploadBatchChannel.Writer.WriteAsync(batch, cancellationToken);
    }

    void IUploadChunksMediator.SignalReaderCompleted()
    {
        _uploadChunksChannelManager.Current.SignalReaderCompleted();
    }

    void IUploadChunksMediator.RegisterReader()
    {
        _uploadChunksChannelManager.Current.RegisterReader();
    }

    IAsyncEnumerable<UploadChunkRequest> IUploadChunksMediator.GetChunks(CancellationToken cancellationToken)
    {
        return _uploadChunksChannelManager.Current.Channel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IUploadChunksMediator.ProcessChunk(UploadChunkRequest request, CancellationToken cancellationToken)
    {
        await _uploadChunksChannelManager.Current.Channel.Writer.WriteAsync(request, cancellationToken);
    }

    async Task IUploadChunksMediator.WaitForAllChunksProcessed()
    {
        _uploadChunksChannelManager.Current.Channel.Writer.TryComplete();
        await _uploadChunksChannelManager.Current.WaitForAllReadersAsync();
        _uploadChunksChannelManager.Reset();
    }
}