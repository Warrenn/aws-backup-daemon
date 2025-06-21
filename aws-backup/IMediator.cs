using System.Threading.Channels;

namespace aws_backup;

public interface IMediator
{
    IAsyncEnumerable<(ArchiveRun archive, string fullFilePath)> GetArchiveFiles(CancellationToken cancellationToken);
    IAsyncEnumerable<RunRequest> GetRequests(CancellationToken cancellationToken);
    IAsyncEnumerable<(ArchiveRun archive, string filePath, Exception exception)> GetRetries(CancellationToken cancellationToken);
    IAsyncEnumerable<ChunkData> GetUploadChunks(CancellationToken cancellationToken);
    
    Task CreateBackup(RunRequest request, CancellationToken cancellationToken);
    Task RetryFile(string archiveRunId, string filePath, Exception exception, CancellationToken cancellationToken);
    Task ProcessFile(string archiveRunId, string filePath, CancellationToken cancellationToken);
    Task ProcessChunk(ChunkData chunkData, CancellationToken cancellationToken);
}