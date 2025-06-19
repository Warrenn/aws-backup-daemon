using System.IO.Compression;
using System.Security.Cryptography;
using System.Threading.Channels;

public class ChunkedEncryptingFileProcessor : IChunkedEncryptingFileProcessor
{
    private readonly byte[] _aesKey; // 32 bytes for AES-256
    private readonly int _bufferSize;
    private readonly int _chunkSizeBytes;
    private readonly ChannelWriter<ChunkData> _chunkWriter;
    private readonly IChunkedEncryptingFileProcessor _processor;

    public ChunkedEncryptingFileProcessor(
        byte[] aesKey,
        int chunkSizeBytes = 5 * 1024 * 1024,
        int bufferSize = 80 * 1024,
        IChunkedEncryptingFileProcessor processor = null,
        ChannelWriter<ChunkData> chunkWriter = null)
    {
        if (aesKey is null || aesKey.Length != 32)
            throw new ArgumentException("AES key must be 32 bytes for AES-256", nameof(aesKey));

        _aesKey = aesKey;
        _chunkSizeBytes = chunkSizeBytes;
        _bufferSize = bufferSize;
        _processor = processor;
        _chunkWriter = chunkWriter;
    }

    public async Task<FileProcessResult> ProcessFileAsync(string inputPath,
        CancellationToken cancellationToken = default)
    {
        // full-file hasher
        using var fullHasher = SHA256.Create();
        var chunks = new List<ChunkData>();

        // open for read, disallow writers
        await using var fs = new FileStream(
            inputPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            _bufferSize,
            FileOptions.SequentialScan);

        var chunkIndex = 0;
        long bytesInChunk = 0;

        // will be initialized at first write
        SHA256? chunkHasher = null!;
        GZipStream gzipStream = null!;
        CryptoStream cryptoStream = null!;
        FileStream chunkFileFs = null!;

        var buffer = new byte[_bufferSize];
        int read;

        while ((read = await fs.ReadAsync(buffer, cancellationToken)) > 0)
        {
            // feed the full-file hash
            fullHasher.TransformBlock(buffer, 0, read, null, 0);

            // ensure chunk pipeline is ready
            if (chunkHasher is null)
                InitializeChunkPipeline();
            if (chunkHasher is null) break; // safety check

            // feed chunk hash + compression + encryption
            chunkHasher.TransformBlock(buffer, 0, read, null, 0);
            await gzipStream.WriteAsync(buffer.AsMemory(0, read), cancellationToken);

            bytesInChunk += read;

            if (bytesInChunk >= _chunkSizeBytes)
                await FinalizeChunkAsync();
        }

        // finish last partial chunk
        if (bytesInChunk > 0)
            await FinalizeChunkAsync();

        // finish full-file hash
        fullHasher.TransformFinalBlock([], 0, 0);
        return new FileProcessResult
        {
            Size = fs.Length,
            FilePath = inputPath,
            FullFileHash = fullHasher.Hash ?? [],
            Chunks = [..chunks]
        };

        // local helpers:

        void InitializeChunkPipeline()
        {
            bytesInChunk = 0;

            // 1) chunk hasher
            chunkHasher = SHA256.Create();

            // 2) output file for this chunk
            var outPath = $"{Path.GetFileName(inputPath)}.chunk{chunkIndex:D4}.gz.aes";
            chunkFileFs = new FileStream(outPath, FileMode.CreateNew, FileAccess.Write, FileShare.None);

            // 3) AES setup
            using var aes = Aes.Create();
            aes.KeySize = 256;
            aes.Mode = CipherMode.CBC;
            aes.Key = _aesKey;
            aes.GenerateIV(); // unique per chunk

            // write the IV at the file start
            chunkFileFs.Write(aes.IV, 0, aes.IV.Length);

            // 4) crypto + gzip stream
            cryptoStream = new CryptoStream(chunkFileFs, aes.CreateEncryptor(), CryptoStreamMode.Write);
            gzipStream = new GZipStream(cryptoStream, CompressionLevel.Optimal, true);
        }

        async Task FinalizeChunkAsync()
        {
            // finalize chunk hash
            chunkHasher.TransformFinalBlock([], 0, 0);

            // finish compression & encryption
            await gzipStream.FlushAsync(cancellationToken);
            await gzipStream.DisposeAsync(); // closes GZip
            await cryptoStream.FlushFinalBlockAsync(cancellationToken);
            await cryptoStream.DisposeAsync(); // closes CryptoStream
            await chunkFileFs.DisposeAsync(); // closes file stream

            var chunkData = new ChunkData
            {
                FilePath = chunkFileFs.Name,
                ChunkIndex = chunkIndex,
                Hash = chunkHasher.Hash ?? [],
                Size = bytesInChunk
            };
            chunks.Add(chunkData);
            // optionally write chunk data to shared channel
            await _chunkWriter.WriteAsync(chunkData, cancellationToken);

            // prepare for next chunk
            chunkIndex++;
            chunkHasher = null!;
            gzipStream = null!;
            cryptoStream = null!;
            chunkFileFs = null!;
        }
    }
}