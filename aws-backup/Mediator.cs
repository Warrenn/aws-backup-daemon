namespace aws_backup;

public class Mediator(
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
    IUploadChunksMediator

{
    // IArchiveFileMediator
    public IAsyncEnumerable<ArchiveFileRequest> GetArchiveFiles(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task ProcessFile(ArchiveFileRequest request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<RunRequest> GetRunRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task ScheduleRunRequest(RunRequest runRequest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<KeyValuePair<string, ArchiveRun>> GetArchiveRuns(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<KeyValuePair<string, CurrentArchiveRuns>> GetCurrentArchiveRuns(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task SaveArchiveRun(ArchiveRun currentArchiveRun, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task SaveCurrentArchiveRuns(CurrentArchiveRuns currentArchiveRuns, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<KeyValuePair<string, DataChunkManifest>> GetDataChunksManifest(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask SaveChunkManifest(DataChunkManifest manifest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task DownloadFileFromS3(DownloadFileFromS3Request downloadFileFromS3Request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<DownloadFileFromS3Request> GetDownloadRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<RetryState> GetRetries(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task RetryAttempt(RetryState attempt, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task RestoreBackup(RestoreRequest restoreRequest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task SaveRunningRequest(CurrentRestoreRequests currentRestoreRequests, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<KeyValuePair<string, CurrentRestoreRequests>> GetRunningRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<RestoreRequest> GetRestoreRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task SaveRestoreManifest(S3RestoreChunkManifest currentManifest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<KeyValuePair<string, S3RestoreChunkManifest>> GetRestoreManifest(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<KeyValuePair<string, RestoreRun>> GetRestoreRuns(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task SaveRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<UploadChunkRequest> GetChunks(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task ProcessChunk(UploadChunkRequest request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}