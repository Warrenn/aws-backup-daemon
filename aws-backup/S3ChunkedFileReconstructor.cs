using System.IO.Compression;
using System.Security.Cryptography;
using Amazon.S3.Model;

namespace aws_backup;

public interface IS3ChunkedFileReconstructor
{
    Task ReconstructAsync(
        DownloadFileFromS3Request request,
        CancellationToken cancellationToken);
}

public class S3ChunkedFileReconstructor(
    Configuration configuration,
    IContextResolver contextResolver,
    IAwsClientFactory awsClientFactory
) : IS3ChunkedFileReconstructor
{
    public async Task ReconstructAsync(
        DownloadFileFromS3Request request,
        CancellationToken cancellationToken)
    {
        var s3 = await awsClientFactory.CreateS3Client(configuration, cancellationToken);
        var destinationFolder = contextResolver.ResolveRestoreFolder(configuration, request.RestoreId);
        var outputFilePath = Path.Combine(destinationFolder, request.FilePath);
        var bufferSize = configuration.ReadBufferSize;
        var chunkSize = configuration.ChunkSizeBytes;
        var maxDownloadConcurrency = configuration.MaxDownloadConcurrency;
        var originalFileSize = request.Size;
        var aesKey = await contextResolver.ResolveAesKey(configuration, cancellationToken);

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
            var (key, bucketName, _) = request.CloudChunkDetails[idx];
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

                await using var gzipStream = new GZipStream(
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
    }
}