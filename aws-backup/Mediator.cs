using System.Threading.Channels;

namespace aws_backup;

public interface IMediator
{
    IAsyncEnumerable<(ArchiveRun archive, string fullFilePath)> GetArchiveFiles(CancellationToken cancellationToken);
    IAsyncEnumerable<RunRequest> GetRequests(CancellationToken cancellationToken);

    IAsyncEnumerable<(ArchiveRun archive, string filePath, Exception exception)> GetRetries(
        CancellationToken cancellationToken);

    IAsyncEnumerable<ChunkData> GetUploadChunks(CancellationToken cancellationToken);

    Task CreateBackup(RunRequest request, CancellationToken cancellationToken);
    Task RetryFile(string archiveRunId, string filePath, Exception exception, CancellationToken cancellationToken);
    Task ProcessFile(string archiveRunId, string filePath, CancellationToken cancellationToken);
    Task ProcessChunk(ChunkData chunkData, CancellationToken cancellationToken);
    IAsyncEnumerable<(ArchiveRun archive, string key, Func<Task<Stream>> getDataStream)> GetHotStorageUploads(
        CancellationToken cancellationToken);
}

public class Mediator : IMediator
{
    private readonly Channel<(ArchiveRun archive, string fullFilePath)> _archiveFilesChannel = Channel.CreateUnbounded<(ArchiveRun archive, string fullFilePath)>();
    private readonly Channel<RunRequest> _requestsChannel = Channel.CreateUnbounded<RunRequest>();
    private readonly Channel<(ArchiveRun archive, string filePath, Exception exception)> _retriesChannel = Channel.CreateUnbounded<(ArchiveRun archive, string filePath, Exception exception)>();
    private readonly Channel<ChunkData> _uploadChunksChannel = Channel.CreateUnbounded<ChunkData>();

    public IAsyncEnumerable<(ArchiveRun archive, string fullFilePath)> GetArchiveFiles(
        CancellationToken cancellationToken)
    {
        return _archiveFilesChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public IAsyncEnumerable<RunRequest> GetRequests(CancellationToken cancellationToken)
    {
        return _requestsChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public IAsyncEnumerable<(ArchiveRun archive, string filePath, Exception exception)> GetRetries(
        CancellationToken cancellationToken)
    {
        return _retriesChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public IAsyncEnumerable<ChunkData> GetUploadChunks(CancellationToken cancellationToken)
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

    public Task ProcessChunk(ChunkData chunkData, CancellationToken cancellationToken)
    {
        return _uploadChunksChannel.Writer.WriteAsync(chunkData, cancellationToken).AsTask();
    }

    public IAsyncEnumerable<(ArchiveRun archive, string key, Func<Task<Stream>> getDataStream)> GetHotStorageUploads(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}