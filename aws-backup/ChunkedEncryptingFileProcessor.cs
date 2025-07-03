using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;

namespace aws_backup;

public sealed record FileProcessResult(
    string LocalFilePath,
    long OriginalSize, // Size before compression
    byte[] FullFileHash, // SHA-256 hash of the full file
    DataChunkDetails[] Chunks);

public interface IChunkedEncryptingFileProcessor
{
    Task<FileProcessResult> ProcessFileAsync(string runId, string inputPath, CancellationToken cancellationToken);
}

public sealed class ChunkedEncryptingFileProcessor(
    IContextResolver contextResolver,
    IUploadChunksMediator mediator) : IChunkedEncryptingFileProcessor
{
    public async Task<FileProcessResult> ProcessFileAsync(string runId, string inputPath,
        CancellationToken cancellationToken = default)
    {
        // full-file hasher
        using var fullHasher = SHA256.Create();
        var chunks = new List<DataChunkDetails>();
        var bufferSize = contextResolver.ReadBufferSize();
        var chunkSize = contextResolver.ChunkSizeBytes();
        var cacheFolder = contextResolver.LocalCacheFolder();
        var aesKey = await contextResolver.AesFileEncryptionKey(cancellationToken);

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

            var offset = 0;
            while (offset < read)
            {
                // ensure chunk pipeline is ready
                if (chunkHasher is null)
                    InitializeChunkPipeline();
                if (chunkHasher is null) continue; // safety check

                // how many bytes can we write into this chunk before we hit chunkSize?
                var spaceLeft = chunkSize - bytesInChunk;
                var toWrite = Math.Min(spaceLeft, read - offset);

                // feed chunk hash + compression + encryption
                chunkHasher.TransformBlock(buffer, offset, (int)toWrite, null, 0);
                await gzipStream.WriteAsync(buffer.AsMemory(offset, (int)toWrite), cancellationToken);

                bytesInChunk += toWrite;
                offset += (int)toWrite;

                if (bytesInChunk >= chunkSize)
                    await FinalizeChunkAsync();
            }
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

            var fileNameHash = Base64Url.Encode(SHA256.HashData(Encoding.UTF8.GetBytes(inputPath)));
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
            if (chunkHasher is null) return;

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
                chunkSize,
                chunkHasher.Hash,
                bytesInChunk
            );
            chunks.Add(chunkData);
            var request = new UploadChunkRequest(runId, inputPath, chunkData);
            await mediator.ProcessChunk(request, cancellationToken);

            // prepare for next chunk
            chunkIndex++;
            chunkHasher = null!;
            gzipStream = null!;
            cryptoStream = null!;
            chunkFileFs = null!;
        }
    }
}