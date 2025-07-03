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
    ISnsOrchestrationMediator

{
    IAsyncEnumerable<ArchiveFileRequest> IArchiveFileMediator.GetArchiveFiles(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IArchiveFileMediator.ProcessFile(ArchiveFileRequest request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<RunRequest> IRunRequestMediator.GetRunRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IRunRequestMediator.ScheduleRunRequest(RunRequest runRequest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<KeyValuePair<string, ArchiveRun>> IArchiveRunMediator.GetArchiveRuns(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<KeyValuePair<string, CurrentArchiveRunRequests>> IArchiveRunMediator.GetCurrentArchiveRunRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IArchiveRunMediator.SaveArchiveRun(ArchiveRun currentArchiveRun, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IArchiveRunMediator.SaveCurrentArchiveRunRequests(CurrentArchiveRunRequests currentArchiveRuns, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<KeyValuePair<string, DataChunkManifest>> IChunkManifestMediator.GetDataChunksManifest(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async ValueTask IChunkManifestMediator.SaveChunkManifest(DataChunkManifest manifest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IDownloadFileMediator.DownloadFileFromS3(DownloadFileFromS3Request downloadFileFromS3Request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<DownloadFileFromS3Request> IDownloadFileMediator.GetDownloadRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<RetryState> IRetryMediator.GetRetries(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IRetryMediator.RetryAttempt(RetryState attempt, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IRestoreRequestsMediator.RestoreBackup(RestoreRequest restoreRequest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IRestoreRequestsMediator.SaveRunningRequest(CurrentRestoreRequests currentRestoreRequests, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<KeyValuePair<string, CurrentRestoreRequests>> IRestoreRequestsMediator.GetRunningRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<RestoreRequest> IRestoreRequestsMediator.GetRestoreRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IRestoreManifestMediator.SaveRestoreManifest(S3RestoreChunkManifest currentManifest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<KeyValuePair<string, S3RestoreChunkManifest>> IRestoreManifestMediator.GetRestoreManifest(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<KeyValuePair<string, RestoreRun>> IRestoreRunMediator.GetRestoreRuns(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IRestoreRunMediator.SaveRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<UploadChunkRequest> IUploadChunksMediator.GetChunks(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task IUploadChunksMediator.ProcessChunk(UploadChunkRequest request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    IAsyncEnumerable<SnsMessage> ISnsOrchestrationMediator.GetMessages(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    async Task ISnsOrchestrationMediator.PublishMessage(SnsMessage message, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}