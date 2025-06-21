namespace aws_backup;

public record FileProcessResult(
    string LocalFilePath,
    long OriginalSize, // Size before compression
    byte[] FullFileHash, // SHA-256 hash of the full file
    ChunkData[] Chunks
);

public record ChunkData(
    string LocalFilePath,
    int ChunkIndex,
    byte[] Hash, // SHA-256 hash of the chunk
    long Size
);

public interface IChunkedEncryptingFileProcessor
{
    Task<FileProcessResult> ProcessFileAsync(string inputPath, CancellationToken cancellationToken = default);
}