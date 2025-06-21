
public struct FileProcessResult
{
    public long Size { get; init; } // size in bytes
    public string FilePath { get; init; }
    public byte[] FullFileHash { get; init; } // SHA-256 hash of the full file
    public ChunkData[] Chunks { get; init; } // list of processed chunks
}

public struct ChunkData()
{
    public string FilePath { get; init; }
    public int ChunkIndex { get; init; }
    public byte[] Hash { get; init; } = []; // SHA-256 hash of the chunk
    public long Size { get; init; }   // size in bytes
}

public interface IChunkedEncryptingFileProcessor
{
    Task<FileProcessResult> ProcessFileAsync(string inputPath, CancellationToken cancellationToken = default);
}