using System.Threading.Channels;

namespace aws_backup;

public interface IMediator
{
    //retries
    IAsyncEnumerable<(string runId, string filePath, Exception exception)> GetRetries(
        CancellationToken cancellationToken);

    ValueTask RetryFile(string archiveRunId, string filePath, Exception exception, CancellationToken cancellationToken);

    //archive files
    IAsyncEnumerable<(string runId, string fullFilePath)> GetArchiveFiles(CancellationToken cancellationToken);
    ValueTask ProcessFile(string archiveRunId, string filePath, CancellationToken cancellationToken);

    //data chunks
    IAsyncEnumerable<(string runId, string parentFile, DataChunkDetails dataChunkDetails)> GetChunks(
        CancellationToken cancellationToken);

    ValueTask ProcessChunk((string runId, string parentFile, DataChunkDetails dataChunkDetails) details,
        CancellationToken cancellationToken);

    //archive state
    IAsyncEnumerable<(ArchiveRun archiveRun, string key)> GetArchiveState(CancellationToken cancellationToken);
    ValueTask SaveArchiveRun(ArchiveRun currentArchiveRun, CancellationToken cancellationToken);

    //data chunks manifest
    IAsyncEnumerable<(string key, DataChunkManifest manifest)> GetDataChunksManifest(
        CancellationToken cancellationToken);

    ValueTask SaveChunkManifest(DataChunkManifest manifest, CancellationToken cancellationToken);

    //restore requests
    IAsyncEnumerable<RestoreRequest> GetRestoreRequests(CancellationToken cancellationToken);
    ValueTask RestoreBackup(RestoreRequest restoreRequest, CancellationToken cancellationToken);

    //run requests
    IAsyncEnumerable<RunRequest> GetRunRequests(CancellationToken cancellationToken);
    ValueTask ScheduleRunRequest(RunRequest runRequest, CancellationToken cancellationToken);

    //restore runs
    ValueTask SaveRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken);

    //download files from S3
    ValueTask DownloadFileFromS3(DownloadFileFromS3Request downloadFileFromS3Request,
        CancellationToken cancellationToken);

    IAsyncEnumerable<DownloadFileFromS3Request> GetDownloadRequests(CancellationToken cancellationToken);

    //save S3 restore chunk manifest
    ValueTask SaveS3RestoreChunkManifest(S3RestoreChunkManifest current, CancellationToken cancellationToken);
}

public class Mediator(
    IContextResolver resolver) : IMediator
{
    public IAsyncEnumerable<(string runId, string filePath, Exception exception)> GetRetries(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask RetryFile(string archiveRunId, string filePath, Exception exception, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<(string runId, string fullFilePath)> GetArchiveFiles(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask ProcessFile(string archiveRunId, string filePath, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<(string runId, string parentFile, DataChunkDetails dataChunkDetails)> GetChunks(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask ProcessChunk((string runId, string parentFile, DataChunkDetails dataChunkDetails) details,
        CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<(ArchiveRun archiveRun, string key)> GetArchiveState(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask SaveArchiveRun(ArchiveRun currentArchiveRun, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<(string key, DataChunkManifest manifest)> GetDataChunksManifest(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask SaveChunkManifest(DataChunkManifest manifest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<RestoreRequest> GetRestoreRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask RestoreBackup(RestoreRequest restoreRequest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<RunRequest> GetRunRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask ScheduleRunRequest(RunRequest runRequest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask SaveRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask DownloadFileFromS3(DownloadFileFromS3Request downloadFileFromS3Request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<DownloadFileFromS3Request> GetDownloadRequests(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask SaveS3RestoreChunkManifest(S3RestoreChunkManifest current, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}