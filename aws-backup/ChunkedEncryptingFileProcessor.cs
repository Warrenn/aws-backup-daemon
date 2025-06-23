using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;

namespace aws_backup;

public record FileProcessResult(
    string LocalFilePath,
    long OriginalSize, // Size before compression
    byte[] FullFileHash, // SHA-256 hash of the full file
    DataChunkDetails[] Chunks)
{
    public ByteArrayKey Key { get; init; } = new(FullFileHash);
}

public interface IChunkedEncryptingFileProcessor
{
    Task<FileProcessResult> ProcessFileAsync(string inputPath, CancellationToken cancellationToken);
}

public class ChunkedEncryptingFileProcessor(
    Configuration configuration,
    IContextResolver contextResolver,
    IMediator mediator) : IChunkedEncryptingFileProcessor
{
    public async Task<FileProcessResult> ProcessFileAsync(string inputPath,
        CancellationToken cancellationToken = default)
    {
        // full-file hasher
        using var fullHasher = SHA256.Create();
        var chunks = new List<DataChunkDetails>();
        var bufferSize = configuration.ReadBufferSize;
        var chunkSize = configuration.ChunkSizeBytes;
        var cacheFolder = contextResolver.ResolveCacheFolder(configuration);
        var aesKey = await contextResolver.ResolveAesKey(configuration, cancellationToken);

        // open for read, disallow writers
        await using var fs = new FileStream(
            inputPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize,
            FileOptions.SequentialScan);

        var chunkIndex = 0;
        long bytesInChunk = 0;

        // will be initialized at first write
        SHA256? chunkHasher = null!;
        GZipStream gzipStream = null!;
        CryptoStream cryptoStream = null!;
        FileStream chunkFileFs = null!;

        var buffer = new byte[bufferSize];
        int read;

        while ((read = await fs.ReadAsync(buffer, cancellationToken)) > 0)
        {
            // feed the full-file hash
            fullHasher.TransformBlock(buffer, 0, read, null, 0);

            // ensure chunk pipeline is ready
            if (chunkHasher is null)
                InitializeChunkPipeline();
            if (chunkHasher is null) continue; // safety check

            // feed chunk hash + compression + encryption
            chunkHasher.TransformBlock(buffer, 0, read, null, 0);
            await gzipStream.WriteAsync(buffer.AsMemory(0, read), cancellationToken);

            bytesInChunk += read;

            if (bytesInChunk >= chunkSize)
                await FinalizeChunkAsync();
        }

        // finish last partial chunk
        if (bytesInChunk > 0)
            await FinalizeChunkAsync();

        // finish full-file hash
        fullHasher.TransformFinalBlock([], 0, 0);
        return new FileProcessResult(
            OriginalSize: fs.Length,
            LocalFilePath: inputPath,
            FullFileHash: fullHasher.Hash ?? [],
            Chunks: [..chunks]
        );

        // local helpers:
        void InitializeChunkPipeline()
        {
            bytesInChunk = 0;

            // 1) chunk hasher
            chunkHasher = SHA256.Create();

            // 2) output file for this chunk
            if (!Directory.Exists(cacheFolder))
                Directory.CreateDirectory(cacheFolder);

            var fileNameHash = Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(inputPath)));
            var outPath = $"{Path.Combine(cacheFolder, fileNameHash)}.chunk{chunkIndex:D4}.gz.aes";
            if (File.Exists(outPath)) File.Delete(outPath);
            chunkFileFs = new FileStream(outPath, FileMode.CreateNew, FileAccess.Write, FileShare.None);

            // 3) AES setup
            using var aes = Aes.Create();
            aes.KeySize = 256;
            aes.Mode = CipherMode.CBC;
            aes.Key = aesKey;
            aes.Padding = PaddingMode.PKCS7;
            aes.GenerateIV(); // unique per chunk

            // write the IV at the file start
            chunkFileFs.Write(aes.IV, 0, 16);

            // 4) crypto + gzip stream
            cryptoStream = new CryptoStream(chunkFileFs, aes.CreateEncryptor(), CryptoStreamMode.Write);
            gzipStream = new GZipStream(cryptoStream, CompressionLevel.SmallestSize, true);
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

            var chunkData = new DataChunkDetails(
                chunkFileFs.Name,
                chunkIndex,
                new ByteArrayKey(chunkHasher.Hash ?? []),
                bytesInChunk
            );
            chunks.Add(chunkData);
            await mediator.ProcessChunk(chunkData, cancellationToken);

            // prepare for next chunk
            chunkIndex++;
            chunkHasher = null!;
            gzipStream = null!;
            cryptoStream = null!;
            chunkFileFs = null!;
        }
    }
}