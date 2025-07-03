using System.IO.Compression;
using System.IO.Pipelines;
using System.Net;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace aws_backup;

public interface IHotStorageService
{
    Task UploadAsync<T>(string key, T obj, CancellationToken cancellationToken);

    Task<T?> DownloadAsync<T>(string key, CancellationToken cancellationToken);

    Task<bool> S3ObjectExistsAsync(
        IAmazonS3 s3,
        string bucket,
        string key,
        CancellationToken ct);
}

public sealed class HotStorageService(
    IAwsClientFactory awsClientFactory,
    IContextResolver contextResolver) : IHotStorageService
{
    public async Task UploadAsync<T>(string key, T obj, CancellationToken cancellationToken)
    {
        // 1) Create the pipe
        var pipe = new Pipe();
        var s3 = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = contextResolver.S3BucketId();
        var partSizeBytes = contextResolver.S3PartSize();
        var storageClass = contextResolver.HotStorage();

        // 2) Kick off the upload task, reading from the pipe's reader as a Stream
        var uploadTask = Task.Run(async () =>
        {
            await using var readerStream = pipe.Reader.AsStream();
            var transfer = new TransferUtility(s3);
            var req = new TransferUtilityUploadRequest
            {
                BucketName = bucketName,
                Key = key,
                InputStream = readerStream,
                PartSize = partSizeBytes,
                ContentType = "application/json",
                StorageClass = storageClass,
                AutoCloseStream = true
            };
            await transfer.UploadAsync(req, cancellationToken);
        }, cancellationToken);

        // 3) In this thread, serialize → gzip → pipe.Writer
        await using (var writerStream = pipe.Writer.AsStream())
        await using (var gzip = new GZipStream(writerStream, CompressionLevel.SmallestSize, false))
        {
            await JsonSerializer.SerializeAsync(gzip, obj, Json.Options, cancellationToken);
        }
        // disposing gzip and writerStream will complete the pipe for the reader

        // 4) Wait for upload to finish
        await uploadTask;
    }

    public async Task<T?> DownloadAsync<T>(string key, CancellationToken cancellationToken)
    {
        var s3 = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = contextResolver.S3BucketId();

        if (!await S3ObjectExistsAsync(s3, bucketName, key, cancellationToken)) return default;

        using var resp = await s3.GetObjectAsync(
            new GetObjectRequest { BucketName = bucketName, Key = key },
            cancellationToken);

        await using var gzip = new GZipStream(resp.ResponseStream, CompressionMode.Decompress, false);
        return await JsonSerializer.DeserializeAsync<T>(gzip, Json.Options, cancellationToken);
    }

    public async Task<bool> S3ObjectExistsAsync(
        IAmazonS3 s3,
        string bucket,
        string key,
        CancellationToken ct)
    {
        try
        {
            await s3.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = bucket,
                Key = key
            }, ct);
            return true;
        }
        catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }
}