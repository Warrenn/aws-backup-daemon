using System.Threading.Channels;

namespace aws_backup;

public interface IMediator
{
    IAsyncEnumerable<(string runId, string fullFilePath)> GetArchiveFiles(CancellationToken cancellationToken);

    IAsyncEnumerable<RunRequest> RunRequests(CancellationToken cancellationToken);

    IAsyncEnumerable<(string runId, string filePath, Exception exception)> GetRetries(
        CancellationToken cancellationToken);

    IAsyncEnumerable<DataChunkDetails> GetChunks(CancellationToken cancellationToken);

    IAsyncEnumerable<(ArchiveRun archive, string key)> GetArchiveState(CancellationToken cancellationToken);

    IAsyncEnumerable<(string key, DataChunkManifest manifest)> GetDataChunksManifest(
        CancellationToken cancellationToken);

    ValueTask RetryFile(string archiveRunId, string filePath, Exception exception, CancellationToken cancellationToken);
    ValueTask ProcessFile(string archiveRunId, string filePath, CancellationToken cancellationToken);
    ValueTask ProcessChunk(DataChunkDetails dataChunkDetails, CancellationToken cancellationToken);
    ValueTask SaveArchiveRun(ArchiveRun currentArchiveRun, CancellationToken stoppingToken);
    ValueTask SaveChunkManifest(DataChunkManifest manifest, CancellationToken stoppingToken);
}

public class Mediator(
    IContextResolver resolver) : IMediator
{
    private readonly Channel<(string runId, string fullFilePath)> _archiveFilesChannel =
        Channel.CreateUnbounded<(string runId, string fullFilePath)>();

    private readonly Channel<(ArchiveRun archive, string key)> _archiveStateChannel =
        Channel.CreateUnbounded<(ArchiveRun archive, string key)>();

    private readonly Channel<(string key, DataChunkManifest manifest)> _chunkManifestChannel =
        Channel.CreateUnbounded<(string key, DataChunkManifest manifest)>();

    private readonly Channel<RunRequest> _requestsChannel = Channel.CreateUnbounded<RunRequest>();

    private readonly Channel<(string runId, string filePath, Exception exception)> _retriesChannel =
        Channel.CreateUnbounded<(string runId, string filePath, Exception exception)>();

    private readonly Channel<DataChunkDetails> _uploadChunksChannel =
        Channel.CreateUnbounded<DataChunkDetails>();

    public IAsyncEnumerable<(string runId, string fullFilePath)> GetArchiveFiles(CancellationToken cancellationToken)
    {
        return _archiveFilesChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public IAsyncEnumerable<RunRequest> RunRequests(CancellationToken cancellationToken)
    {
        return _requestsChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public IAsyncEnumerable<(string runId, string filePath, Exception exception)> GetRetries(
        CancellationToken cancellationToken)
    {
        return _retriesChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public IAsyncEnumerable<DataChunkDetails> GetChunks(CancellationToken cancellationToken)
    {
        return _uploadChunksChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public IAsyncEnumerable<(ArchiveRun archive, string key)> GetArchiveState(CancellationToken cancellationToken)
    {
        return _archiveStateChannel.Reader.ReadAllAsync(cancellationToken);
    }

    public IAsyncEnumerable<(string key, DataChunkManifest manifest)> GetDataChunksManifest(
        CancellationToken stoppingToken)
    {
        return _chunkManifestChannel.Reader.ReadAllAsync(stoppingToken);
    }

    public ValueTask RetryFile(string archiveRunId, string filePath, Exception exception,
        CancellationToken cancellationToken)
    {
        return _retriesChannel.Writer.WriteAsync((runId: archiveRunId, filePath, exception), cancellationToken);
    }

    public ValueTask ProcessFile(string archiveRunId, string filePath, CancellationToken cancellationToken)
    {
        return _archiveFilesChannel.Writer.WriteAsync((runId: archiveRunId, filePath), cancellationToken);
    }

    public ValueTask ProcessChunk(DataChunkDetails dataChunkDetails, CancellationToken cancellationToken)
    {
        return _uploadChunksChannel.Writer.WriteAsync(dataChunkDetails, cancellationToken);
    }

    public ValueTask SaveArchiveRun(ArchiveRun currentArchiveRun, CancellationToken stoppingToken)
    {
        var key = resolver.ResolveArchiveKey(currentArchiveRun.RunId);
        return _archiveStateChannel.Writer.WriteAsync((archive: currentArchiveRun, key), stoppingToken);
    }

    public ValueTask SaveChunkManifest(DataChunkManifest manifest, CancellationToken stoppingToken)
    {
        var key = resolver.ResolveChunkManifestKey();
        return _chunkManifestChannel.Writer.WriteAsync((key, manifest), stoppingToken);
    }
}