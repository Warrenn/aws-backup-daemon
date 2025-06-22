using System.IO.Compression;
using System.Security.Cryptography;
using Amazon.S3;
using Amazon.S3.Model;

namespace aws_backup;

public class S3ChunkedFileReconstructor
{
    private readonly byte[] _aesKey;
    private readonly long _originalFileSize;
    private readonly int _bufferSize;
    private readonly int _chunkSize;
    private readonly int _maxConcurrency;
    private readonly IAmazonS3 _s3;

    /// <param name="aesKey">32-byte AES-256 key</param>
    /// <param name="originalFileSize"></param>
    /// <param name="chunkSize">original chunk size (e.g. 5 MiB)</param>
    /// <param name="maxConcurrency">how many chunks to process in parallel</param>
    /// <param name="bufferSize">read/write buffer (~80 KiB default)</param>
    public S3ChunkedFileReconstructor(
        IAmazonS3 s3Client,
        byte[] aesKey,
        long originalFileSize,
        int chunkSize = 5 * 1024 * 1024,
        int maxConcurrency = 4,
        int bufferSize = 80 * 1024)
    {
        _s3 = s3Client;
        _aesKey = aesKey ?? throw new ArgumentNullException(nameof(aesKey));
        _originalFileSize = originalFileSize;
        _chunkSize = chunkSize;
        _maxConcurrency = maxConcurrency;
        _bufferSize = bufferSize;
    }

    public async Task ReconstructAsync(
        string bucketName,
        string keyPrefix, // e.g. "myfile.bin"
        int totalChunks, // e.g. 10
        string outputFilePath,
        CancellationToken ct = default)
    {
        // Ensure output file exists and is sized (optional)
        await using (var pre = new FileStream(
                         outputFilePath,
                         FileMode.Create,
                         FileAccess.Write,
                         FileShare.Write,
                         _bufferSize, FileOptions.None))
        {
            // set length to chunkSize * totalChunks (last chunk may write less)
            pre.SetLength(_originalFileSize);
        }

        var sem = new SemaphoreSlim(_maxConcurrency);

        var tasks = Enumerable.Range(0, totalChunks).Select(async idx =>
        {
            await sem.WaitAsync(ct);
            try
            {
                var chunkKey = $"{keyPrefix}.chunk{idx:D4}.gz.aes";
                var resp = await _s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = chunkKey
                }, ct);

                await using var respStream = resp.ResponseStream;

                // 1) read IV
                var iv = new byte[16];
                await respStream.ReadExactlyAsync(iv, ct);

                // 2) decrypt + decompress
                using var aes = Aes.Create();
                aes.Key = _aesKey;
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
                    _bufferSize, FileOptions.None);

                outFs.Seek((long)idx * _chunkSize, SeekOrigin.Begin);

                var buf = new byte[_bufferSize];
                int read;
                while ((read = await gzipStream.ReadAsync(buf, ct)) > 0)
                    await outFs.WriteAsync(buf.AsMemory(0, read), ct);
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
        
        fs.SetLength(_originalFileSize);
    }
}