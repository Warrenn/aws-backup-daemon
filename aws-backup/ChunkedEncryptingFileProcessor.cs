using System.IO.Compression;
using System.Security.Cryptography;
using aws_backup_common;
using Serilog;
using ZstdSharp;

// ReSharper disable AccessToModifiedClosure

namespace aws_backup;

public sealed record FileProcessResult(
    FileMetaData FileMetaData,
    Exception? Error = null);

public interface IChunkedEncryptingFileProcessor
{
    Task<FileProcessResult> ProcessFileAsync(ArchiveRun run, FileMetaData fileMetaData,
        CancellationToken cancellationToken);
}

public sealed class ChunkedEncryptingFileProcessor(
    IContextResolver contextResolver,
    IAesContextResolver aesContextResolver,
    IUploadChunksMediator mediator,
    IArchiveService archiveService) : IChunkedEncryptingFileProcessor
{
    private const int _defaultMinSize = 2 * 1024 * 1024; // ~2 MiB
    private const int _defaultMaxSize = 8 * 1024 * 1024; // ~8 MiB
    private const int _defaultMaskBits = 22; // 2^22 = 4 MiB expected

    public async Task<FileProcessResult> ProcessFileAsync(ArchiveRun run, FileMetaData fileMetaData,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(fileMetaData.LocalFilePath))
            return new FileProcessResult(
                fileMetaData,
                new FileNotFoundException("Input file does not exist", fileMetaData.LocalFilePath)
            );

        var removableFiles = new List<string>();
        try
        {
            // full-file hasher
            using var fullHasher = SHA256.Create();
            var chunks = new List<DataChunkDetails>();
            var bufferSize = contextResolver.ReadBufferSize();
            var cacheFolder = contextResolver.LocalCacheFolder();
            var compressionLevel = contextResolver.ZipCompressionLevel();
            var aesKey = await aesContextResolver.FileEncryptionKey(cancellationToken);
            var offsetInFile = 0L;

            var zstdCompressionLevel = compressionLevel switch
            {
                CompressionLevel.NoCompression => 1,
                CompressionLevel.Fastest => 7,
                CompressionLevel.Optimal => 14,
                CompressionLevel.SmallestSize => 22,
                _ => 14
            };

            // open for read, disallow writers
            await using var fs = new FileStream(
                fileMetaData.LocalFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize,
                FileOptions.SequentialScan);

            var chunkIndex = 0;
            long bytesInChunk = 0;

            // will be initialized at first write
            SHA256? chunkHasher = null!;
            CompressionStream zstdStream = null!;
            CryptoStream cryptoStream = null!;
            FileStream chunkFileFs = null!;

            var buffer = new byte[bufferSize];
            var rollingHasher = new RollingBuzhash32();
            int read;

            while ((read = await fs.ReadAsync(buffer, cancellationToken)) > 0)
            {
                // feed the full-file hash

                var inputOffset = 0;
                while (inputOffset < read)
                {
                    var b = buffer[inputOffset];
                    rollingHasher.Push(b);

                    fullHasher.TransformBlock(buffer, inputOffset, 1, null, 0);
                    // ensure chunk pipeline is ready
                    if (chunkHasher is null)
                        InitializeChunkPipeline();
                    if (chunkHasher is null) continue; // safety check

                    // feed chunk hash + compression + encryption
                    chunkHasher.TransformBlock(buffer, inputOffset, 1, null, 0);
                    await zstdStream.WriteAsync(buffer.AsMemory(inputOffset, 1), cancellationToken);

                    bytesInChunk++;
                    inputOffset++;
                    offsetInFile++;

                    if (rollingHasher.Filled != RollingBuzhash32.WindowSize || bytesInChunk < _defaultMinSize)
                        continue;

                    var maskHit = (rollingHasher.Value & ((1 << _defaultMaskBits) - 1)) == 0;
                    if (!maskHit && bytesInChunk < _defaultMaxSize)
                        continue;

                    // cut chunk here
                    await FinalizeChunkAsync();
                }
            }

            // finish last partial chunk
            if (bytesInChunk > 0)
                await FinalizeChunkAsync();

            // finish full-file hash
            fullHasher.TransformFinalBlock([], 0, 0);
            fileMetaData.OriginalSize = fs.Length;
            fileMetaData.HashKey = fullHasher.Hash ?? [];
            fileMetaData.CompressedSize = chunks.Sum(c => c.CompressedSize);

            return new FileProcessResult(fileMetaData);

            // local helpers:
            void InitializeChunkPipeline()
            {
                bytesInChunk = 0;

                // 1) chunk hasher
                chunkHasher = SHA256.Create();

                // 2) output file for this chunk
                if (!Directory.Exists(cacheFolder))
                    Directory.CreateDirectory(cacheFolder);

                var fileNameHash = Base64Url.ComputeSimpleHash(fileMetaData.LocalFilePath);
                var outPath = $"{Path.Combine(cacheFolder, fileNameHash)}.chunk{chunkIndex:D8}.zstd.aes";
                if (File.Exists(outPath)) File.Delete(outPath);
                removableFiles.Add(outPath);

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
                zstdStream = new CompressionStream(cryptoStream, zstdCompressionLevel);
            }

            async Task FinalizeChunkAsync()
            {
                if (chunkHasher is null) return;

                // finalize chunk hash
                chunkHasher.TransformFinalBlock([], 0, 0);

                // finish compression & encryption
                await zstdStream.FlushAsync(cancellationToken);
                await zstdStream.DisposeAsync(); // closes Zstd
                await cryptoStream.FlushFinalBlockAsync(cancellationToken);
                await cryptoStream.DisposeAsync(); // closes CryptoStream
                await chunkFileFs.DisposeAsync(); // closes file stream

                var fileInfo = new FileInfo(chunkFileFs.Name);

                var chunkData = new DataChunkDetails(
                    chunkFileFs.Name,
                    fileInfo.Length,
                    offsetInFile - bytesInChunk,
                    bytesInChunk,
                    chunkHasher.Hash ?? [])
                {
                    Status =
                        ChunkStatus.Added
                };

                chunks.Add(chunkData);
                await archiveService.AddChunkToFile(
                    run,
                    fileMetaData,
                    chunkData,
                    cancellationToken);
                var request = new UploadChunkRequest(run, fileMetaData, chunkData);

                //this should block due to bounded channel
                await mediator.ProcessChunk(request, cancellationToken);

                // prepare for next chunk
                chunkIndex++;
                chunkHasher = null!;
                zstdStream = null!;
                cryptoStream = null!;
                chunkFileFs = null!;
                rollingHasher.Reset();
            }
        }
        catch (Exception ex)
        {
            foreach (var removableFile in removableFiles.Where(File.Exists))
                try
                {
                    File.Delete(removableFile);
                }
                catch (Exception inner)
                {
                    // ignore errors during cleanup
                    Log.Error(inner, "Failed to delete temporary file {FilePath}", removableFile);
                }

            return new FileProcessResult(fileMetaData, ex);
        }
    }
}