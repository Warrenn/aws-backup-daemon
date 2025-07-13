using System.IO.Compression;
using System.Security.Cryptography;
using Amazon.S3.Model;

namespace aws_backup;

public record ReconstructResult(
    string? LocalFilePath,
    Exception? Exception
);

public interface IS3ChunkedFileReconstructor
{
    Task<ReconstructResult> ReconstructAsync(
        DownloadFileFromS3Request request,
        CancellationToken cancellationToken);

    Task<bool> VerifyDownloadHashAsync(DownloadFileFromS3Request downloadRequest, string localFilePath,
        CancellationToken cancellationToken);
}

public sealed class S3ChunkedFileReconstructor(
    IContextResolver contextResolver,
    IAwsClientFactory awsClientFactory,
    IAesContextResolver aesContextResolver
) : IS3ChunkedFileReconstructor
{
    public async Task<ReconstructResult> ReconstructAsync(
        DownloadFileFromS3Request request,
        CancellationToken cancellationToken)
    {
        var outputFilePath = "";
        try
        {
            var s3 = await awsClientFactory.CreateS3Client(cancellationToken);
            var destinationFolder = contextResolver.LocalRestoreFolder(request.RestoreId);
            outputFilePath = Path.Combine(destinationFolder, request.FilePath);
            var bufferSize = contextResolver.ReadBufferSize();
            var maxDownloadConcurrency = contextResolver.NoOfConcurrentDownloadsPerFile();
            var originalFileSize = request.Size;
            var aesKey = await aesContextResolver.FileEncryptionKey(cancellationToken);

            // Ensure output file exists and is sized (optional)
            await using (var pre = new FileStream(
                             outputFilePath,
                             FileMode.Create,
                             FileAccess.Write,
                             FileShare.Write,
                             bufferSize, FileOptions.None))
            {
                // set length to chunkSize * totalChunks (last chunk may write less)
                pre.SetLength(originalFileSize);
            }

            var sem = new SemaphoreSlim(maxDownloadConcurrency);

            var tasks = Enumerable.Range(0, request.CloudChunkDetails.Length).Select(async idx =>
            {
                var (key, bucketName, chunkSize, _) = request.CloudChunkDetails[idx];
                await sem.WaitAsync(cancellationToken);
                try
                {
                    var resp = await s3.GetObjectAsync(new GetObjectRequest
                    {
                        BucketName = bucketName,
                        Key = key
                    }, cancellationToken);

                    await using var respStream = resp.ResponseStream;

                    // 1) read IV
                    var iv = new byte[16];
                    await respStream.ReadExactlyAsync(iv, cancellationToken);

                    // 2) decrypt + decompress
                    using var aes = Aes.Create();
                    aes.KeySize = 256;
                    aes.Key = aesKey;
                    aes.IV = iv;
                    aes.Mode = CipherMode.CBC;

                    await using var decryptStream = new CryptoStream(
                        respStream,
                        aes.CreateDecryptor(),
                        CryptoStreamMode.Read);

                    await using var gzipStream = new BrotliStream(
                        decryptStream,
                        CompressionMode.Decompress);

                    // 3) write into output at correct offset
                    await using var outFs = new FileStream(
                        outputFilePath,
                        FileMode.Open,
                        FileAccess.Write,
                        FileShare.Write,
                        bufferSize, FileOptions.None);

                    outFs.Seek(idx * chunkSize, SeekOrigin.Begin);

                    var buf = new byte[bufferSize];
                    int read;
                    while ((read = await gzipStream.ReadAsync(buf, cancellationToken)) > 0)
                        await outFs.WriteAsync(buf.AsMemory(0, read), cancellationToken);
                }
                finally
                {
                    sem.Release();
                }
            }).ToArray();

            await Task.WhenAll(tasks);

            await using var fs = new FileStream(
                outputFilePath,
                FileMode.Open,
                FileAccess.Write,
                FileShare.None);

            fs.SetLength(originalFileSize);

            return new ReconstructResult(outputFilePath, null);
        }
        catch (Exception ex)
        {
            try
            {
                if (!string.IsNullOrWhiteSpace(outputFilePath) && File.Exists(outputFilePath))
                    File.Delete(outputFilePath);
            }
            catch
            {
                // ignored
            }

            return new ReconstructResult(null, ex);
        }
    }

    public async Task<bool> VerifyDownloadHashAsync(DownloadFileFromS3Request downloadRequest, string localFilePath,
        CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(localFilePath);
        var hash = await SHA256.HashDataAsync(stream, cancellationToken);
        return hash.AsSpan().SequenceEqual(downloadRequest.Checksum ?? []);
    }
}