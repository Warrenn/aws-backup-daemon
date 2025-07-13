using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace aws_backup;

public sealed class Mediator(
    IContextResolver resolver) :
    IArchiveFileMediator,
    IRunRequestMediator,
    IArchiveRunMediator,
    IChunkManifestMediator,
    IDownloadFileMediator,
    IRetryMediator,
    IRestoreRequestsMediator,
    IRestoreManifestMediator,
    IRestoreRunMediator,
    IUploadChunksMediator,
    ISnsMessageMediator
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

    private readonly Channel<CurrentArchiveRunRequests> _currentArchiveRunRequestsChannel =
        Channel.CreateBounded<CurrentArchiveRunRequests>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            });

    private readonly Channel<CurrentRestoreRequests> _currentRestoreRequestsChannel =
        Channel.CreateBounded<CurrentRestoreRequests>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            });

    private readonly Channel<DataChunkManifest> _dataChunksManifestChannel =
        Channel.CreateBounded<DataChunkManifest>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            });

    private readonly Channel<DownloadFileFromS3Request> _downloadFileFromS3Channel =
        Channel.CreateBounded<DownloadFileFromS3Request>(
            new BoundedChannelOptions(resolver.NoOfS3FilesToDownloadConcurrently())
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            });

    private readonly Channel<S3RestoreChunkManifest> _restoreManifestChannel =
        Channel.CreateBounded<S3RestoreChunkManifest>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
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

    private readonly Channel<UploadChunkRequest> _uploadChunksChannel =
        Channel.CreateUnbounded<UploadChunkRequest>(
            new UnboundedChannelOptions
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

    async IAsyncEnumerable<S3LocationAndValue<ArchiveRun>> IArchiveRunMediator.GetArchiveRuns(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var run in _archiveRunChannel.Reader.ReadAllAsync(cancellationToken))
        {
            var key = resolver.RunIdBucketKey(run.RunId);
            yield return new S3LocationAndValue<ArchiveRun>(key, run);
        }
    }

    async IAsyncEnumerable<S3LocationAndValue<CurrentArchiveRunRequests>> IArchiveRunMediator.
        GetCurrentArchiveRunRequests(
            [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var key = resolver.CurrentArchiveRunsBucketKey();
        await foreach (var request in _currentArchiveRunRequestsChannel
                           .Reader.ReadAllAsync(cancellationToken))
            yield return new S3LocationAndValue<CurrentArchiveRunRequests>(key, request);
    }

    async Task IArchiveRunMediator.SaveArchiveRun(ArchiveRun currentArchiveRun, CancellationToken cancellationToken)
    {
        await _archiveRunChannel.Writer.WriteAsync(currentArchiveRun, cancellationToken);
    }

    async Task IArchiveRunMediator.SaveCurrentArchiveRunRequests(CurrentArchiveRunRequests currentArchiveRuns,
        CancellationToken cancellationToken)
    {
        await _currentArchiveRunRequestsChannel.Writer.WriteAsync(currentArchiveRuns, cancellationToken);
    }

    async IAsyncEnumerable<S3LocationAndValue<DataChunkManifest>> IChunkManifestMediator.GetDataChunksManifest(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var key = resolver.ChunkManifestBucketKey();
        await foreach (var manifest in _dataChunksManifestChannel.Reader.ReadAllAsync(cancellationToken))
            yield return new S3LocationAndValue<DataChunkManifest>(key, manifest);
    }

    async Task IChunkManifestMediator.SaveChunkManifest(DataChunkManifest manifest,
        CancellationToken cancellationToken)
    {
        await _dataChunksManifestChannel.Writer.WriteAsync(manifest, cancellationToken);
    }

    async Task IDownloadFileMediator.DownloadFileFromS3(DownloadFileFromS3Request downloadFileFromS3Request,
        CancellationToken cancellationToken)
    {
        await _downloadFileFromS3Channel.Writer.WriteAsync(downloadFileFromS3Request, cancellationToken);
    }

    IAsyncEnumerable<DownloadFileFromS3Request> IDownloadFileMediator.GetDownloadRequests(
        CancellationToken cancellationToken)
    {
        return _downloadFileFromS3Channel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IRestoreManifestMediator.SaveRestoreManifest(S3RestoreChunkManifest currentManifest,
        CancellationToken cancellationToken)
    {
        await _restoreManifestChannel.Writer.WriteAsync(currentManifest, cancellationToken);
    }

    async IAsyncEnumerable<S3LocationAndValue<S3RestoreChunkManifest>> IRestoreManifestMediator.GetRestoreManifest(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var key = resolver.RestoreManifestBucketKey();
        await foreach (var manifest in _restoreManifestChannel.Reader.ReadAllAsync(cancellationToken))
            yield return new S3LocationAndValue<S3RestoreChunkManifest>(key, manifest);
    }

    async Task IRestoreRequestsMediator.RestoreBackup(RestoreRequest restoreRequest,
        CancellationToken cancellationToken)
    {
        await _restoreRequestsChannel.Writer.WriteAsync(restoreRequest, cancellationToken);
    }

    async Task IRestoreRequestsMediator.SaveRunningRequest(CurrentRestoreRequests currentRestoreRequests,
        CancellationToken cancellationToken)
    {
        await _currentRestoreRequestsChannel.Writer.WriteAsync(currentRestoreRequests, cancellationToken);
    }

    async IAsyncEnumerable<S3LocationAndValue<CurrentRestoreRequests>> IRestoreRequestsMediator.GetRunningRequests(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var key = resolver.CurrentRestoreBucketKey();
        await foreach (var request in _currentRestoreRequestsChannel.Reader.ReadAllAsync(cancellationToken))
            yield return new S3LocationAndValue<CurrentRestoreRequests>(key, request);
    }

    IAsyncEnumerable<RestoreRequest> IRestoreRequestsMediator.GetRestoreRequests(CancellationToken cancellationToken)
    {
        return _restoreRequestsChannel.Reader.ReadAllAsync(cancellationToken);
    }

    async IAsyncEnumerable<S3LocationAndValue<RestoreRun>> IRestoreRunMediator.GetRestoreRuns(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var run in _restoreRunChannel.Reader.ReadAllAsync(cancellationToken))
        {
            var key = resolver.RestoreIdBucketKey(run.RestoreId);
            yield return new S3LocationAndValue<RestoreRun>(key, run);
        }
    }

    async Task IRestoreRunMediator.SaveRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken)
    {
        await _restoreRunChannel.Writer.WriteAsync(restoreRun, cancellationToken);
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

    IAsyncEnumerable<UploadChunkRequest> IUploadChunksMediator.GetChunks(CancellationToken cancellationToken)
    {
        return _uploadChunksChannel.Reader.ReadAllAsync(cancellationToken);
    }

    async Task IUploadChunksMediator.ProcessChunk(UploadChunkRequest request, CancellationToken cancellationToken)
    {
        await _uploadChunksChannel.Writer.WriteAsync(request, cancellationToken);
    }
}