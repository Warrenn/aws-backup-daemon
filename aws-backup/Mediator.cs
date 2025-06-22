using System.Threading.Channels;

namespace aws_backup;

public interface IMediator
{
    IAsyncEnumerable<(string runId, string fullFilePath)> GetArchiveFiles(CancellationToken cancellationToken);
    IAsyncEnumerable<RunRequest> RunRequests(CancellationToken cancellationToken);

    IAsyncEnumerable<(string runId, string filePath, Exception exception)> Retries(CancellationToken cancellationToken);

    IAsyncEnumerable<DataChunkDetails> Chunks(CancellationToken cancellationToken);

    Task CreateBackup(RunRequest request, CancellationToken cancellationToken);
    Task RetryFile(string archiveRunId, string filePath, Exception exception, CancellationToken cancellationToken);
    Task ProcessFile(string archiveRunId, string filePath, CancellationToken cancellationToken);
    Task ProcessChunk(DataChunkDetails dataChunkDetails, CancellationToken cancellationToken);
    IAsyncEnumerable<(ArchiveRun archive, string key)> GetArchiveState(CancellationToken cancellationToken);
    IAsyncEnumerable<(string key, DataChunkManifest manifest)> DataChunksManifest(CancellationToken cancellationToken);
}

public class Mediator : IMediator
{
    private readonly Channel<(ArchiveRun archive, string fullFilePath)> _archiveFilesChannel =
        Channel.CreateUnbounded<(ArchiveRun archive, string fullFilePath)>();

    private readonly Channel<RunRequest> _requestsChannel = Channel.CreateUnbounded<RunRequest>();

    private readonly Channel<(ArchiveRun archive, string filePath, Exception exception)> _retriesChannel =
        Channel.CreateUnbounded<(ArchiveRun archive, string filePath, Exception exception)>();

    private readonly Channel<DataChunkDetails> _uploadChunksChannel = Channel.CreateUnbounded<DataChunkDetails>();

    public IAsyncEnumerable<(string runId, string fullFilePath)> GetArchiveFiles(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<RunRequest> RunRequests(CancellationToken cancellationToken)
    {
        return _requestsChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public IAsyncEnumerable<(string runId, string filePath, Exception exception)> Retries(
        CancellationToken cancellationToken)
    {
        throw new Exception();
    }

    public IAsyncEnumerable<DataChunkDetails> Chunks(CancellationToken cancellationToken)
    {
        return _uploadChunksChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public Task CreateBackup(RunRequest request, CancellationToken cancellationToken)
    {
        return _requestsChannel.Writer.WriteAsync(request, cancellationToken).AsTask();
    }

    public Task RetryFile(string archiveRunId, string filePath, Exception exception,
        CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task ProcessFile(string archiveRunId, string filePath, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task ProcessChunk(DataChunkDetails dataChunkDetails, CancellationToken cancellationToken)
    {
        return _uploadChunksChannel.Writer.WriteAsync(dataChunkDetails, cancellationToken).AsTask();
    }

    public IAsyncEnumerable<(ArchiveRun archive, string key)> GetArchiveState(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<(string key, DataChunkManifest manifest)> DataChunksManifest(
        CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }
}