using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using Serilog;

namespace aws_backup;

public sealed record FileProcessResult(
    string LocalFilePath,
    long OriginalSize, // Size before compression
    byte[] FullFileHash, // SHA-256 hash of the full file
    long CompressedSize, // Size after compression (optional)
    Exception? Error = null);

public interface IChunkedEncryptingFileProcessor
{
    Task<FileProcessResult> ProcessFileAsync(string runId, string inputPath, CancellationToken cancellationToken);
}

public sealed class ChunkedEncryptingFileProcessor(
    IContextResolver contextResolver,
    AwsConfiguration awsConfiguration,
    IAesContextResolver aesContextResolver,
    IUploadChunksMediator mediator,
    IArchiveService archiveService) : IChunkedEncryptingFileProcessor
{
    public async Task<FileProcessResult> ProcessFileAsync(string runId, string inputPath,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(inputPath))
            return new FileProcessResult(
                inputPath,
                0,
                [],
                0,
                new FileNotFoundException("Input file does not exist", inputPath)
            );

        var removableFiles = new List<string>();
        try
        {
            // full-file hasher
            using var fullHasher = SHA256.Create();
            var chunks = new List<DataChunkDetails>();
            var bufferSize = contextResolver.ReadBufferSize();
            var chunkSize = awsConfiguration.ChunkSizeBytes;
            var cacheFolder = contextResolver.LocalCacheFolder();
            var compressionLevel = contextResolver.ZipCompressionLevel();
            var aesKey = await aesContextResolver.FileEncryptionKey(cancellationToken);

            await archiveService.ResetFileStatus(runId, inputPath, cancellationToken);

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
            BrotliStream gzipStream = null!;
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
                CompressedSize: chunks.Sum(c => c.Size)
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
                var outPath = $"{Path.Combine(cacheFolder, fileNameHash)}.chunk{chunkIndex:D4}.br.aes";
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
                gzipStream = new BrotliStream(cryptoStream, compressionLevel, true);
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

                var compressedHash = await SHA256.HashDataAsync(File.OpenRead(chunkFileFs.Name), cancellationToken);
                var fileInfo = new FileInfo(chunkFileFs.Name);

                var chunkData = new DataChunkDetails(
                    chunkFileFs.Name,
                    chunkIndex,
                    chunkSize,
                    chunkHasher.Hash,
                    fileInfo.Length,
                    compressedHash
                )
                {
                    Status =
                        ChunkStatus.Added
                };

                chunks.Add(chunkData);
                await archiveService.AddChunkToFile(
                    runId,
                    inputPath,
                    chunkData,
                    cancellationToken);
                var request = new UploadChunkRequest(runId, inputPath, chunkData);
                
                //this should block due to bounded channel
                await mediator.ProcessChunk(request, cancellationToken);

                // prepare for next chunk
                chunkIndex++;
                chunkHasher = null!;
                gzipStream = null!;
                cryptoStream = null!;
                chunkFileFs = null!;
            }
        }
        catch (Exception ex)
        {
            foreach (var removableFile in removableFiles.Where(File.Exists))
                try
                {
                    File.Delete(removableFile);
                }
                catch
                {
                    // ignore errors during cleanup
                }

            return new FileProcessResult(
                LocalFilePath: inputPath,
                OriginalSize: 0,
                FullFileHash: [],
                CompressedSize: 0,
                Error: ex
            );
        }
    }
}