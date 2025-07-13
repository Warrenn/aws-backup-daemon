using System.IO.Compression;
using System.IO.Pipelines;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace aws_backup;

public sealed record S3StorageInfo(
    string BucketName,
    string Key,
    S3StorageClass StorageClass
);

public enum StorageTemperature
{
    Hot,
    Cold,
    LowCost
}

public interface IS3Service
{
    Task<bool> RunExists(string runId, CancellationToken cancellationToken);
    Task<ArchiveRun> GetArchive(string runId, CancellationToken cancellationToken);
    Task<bool> RestoreExists(string restoreId, CancellationToken cancellationToken);
    Task<RestoreRun> GetRestoreRun(string restoreId, CancellationToken cancellationToken);

    Task<S3ChunkRestoreStatus> ScheduleDeepArchiveRecovery(string chunkS3Key,
        CancellationToken cancellationToken);

    IAsyncEnumerable<S3StorageInfo> GetStorageClasses(CancellationToken cancellationToken);

    Task UploadCompressedObject<T>(string key, T obj, StorageTemperature temp, CancellationToken cancellationToken);

    Task UploadCompressedFile(string key, string localFilePath, StorageTemperature temp,
        CancellationToken cancellationToken);

    Task<T?> DownloadCompressedObject<T>(string key, CancellationToken cancellationToken);

    Task<bool> S3ObjectExistsAsync(IAmazonS3 s3, string bucket, string key, CancellationToken ct);
}

public sealed class S3Service(
    IAwsClientFactory awsClientFactory,
    IContextResolver contextResolver,
    AwsConfiguration awsConfiguration
) : IS3Service
{
    private const int _maxTagValueLength = 256;

    public async Task<bool> RunExists(string runId, CancellationToken cancellationToken)
    {
        var bucketId = awsConfiguration.BucketName;
        var key = contextResolver.RunIdBucketKey(runId);
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        return await S3ObjectExistsAsync(s3Client, bucketId, key, cancellationToken);
    }

    public async Task<ArchiveRun> GetArchive(string runId, CancellationToken cancellationToken)
    {
        var key = contextResolver.RunIdBucketKey(runId);
        return (await DownloadCompressedObject<ArchiveRun>(key, cancellationToken))!;
    }

    public async Task<bool> RestoreExists(string restoreId, CancellationToken cancellationToken)
    {
        var bucketId = awsConfiguration.BucketName;
        var key = contextResolver.RestoreIdBucketKey(restoreId);
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        return await S3ObjectExistsAsync(s3Client, bucketId, key, cancellationToken);
    }

    public async Task<RestoreRun> GetRestoreRun(string restoreId, CancellationToken cancellationToken)
    {
        var key = contextResolver.RestoreIdBucketKey(restoreId);
        return (await DownloadCompressedObject<RestoreRun>(key, cancellationToken))!;
    }

    public async Task<S3ChunkRestoreStatus> ScheduleDeepArchiveRecovery(string chunkS3Key,
        CancellationToken cancellationToken)
    {
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = awsConfiguration.BucketName;

        var (storageClass, restoreInProgress) =
            await GetStorageClass(s3Client, bucketName, chunkS3Key, cancellationToken);
        if (storageClass != S3StorageClass.DeepArchive) return S3ChunkRestoreStatus.ReadyToRestore;
        if (restoreInProgress) return S3ChunkRestoreStatus.PendingDeepArchiveRestore;

        var daysToKeepRestoredCopy = contextResolver.DaysToKeepRestoredCopy();
        var restoreRequest = new RestoreObjectRequest
        {
            BucketName = bucketName,
            Key = chunkS3Key,
            Days = daysToKeepRestoredCopy,
            RetrievalTier = GlacierJobTier.Standard
        };
        await s3Client.RestoreObjectAsync(restoreRequest, cancellationToken);

        return S3ChunkRestoreStatus.PendingDeepArchiveRestore;
    }

    public async IAsyncEnumerable<S3StorageInfo> GetStorageClasses(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = awsConfiguration.BucketName;
        var prefix = contextResolver.S3DataPrefix();
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = prefix
            // You can also set MaxKeys if you want smaller pages
        };

        ListObjectsV2Response response;
        do
        {
            response = await s3Client.ListObjectsV2Async(request, cancellationToken);

            if ((response.KeyCount ?? 0) <= 0)
                yield break; // No objects found

            foreach (var s3Object in response.S3Objects)
                yield return new S3StorageInfo(
                    s3Object.BucketName,
                    s3Object.Key,
                    s3Object.StorageClass
                );

            // If the response is truncated, set the token to get the next page
            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated ?? false);
    }

    public async Task UploadCompressedFile(string key, string localFilePath, StorageTemperature temp,
        CancellationToken cancellationToken)
    {
        if (!File.Exists(localFilePath)) return;

        var pipe = new Pipe();
        var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = awsConfiguration.BucketName;
        var partSizeBytes = contextResolver.S3PartSize();
        var encryptionMethod = contextResolver.ServerSideEncryption();
        var (storageClass, tag) = temp switch
        {
            StorageTemperature.Hot => (contextResolver.HotStorage(), "hot"),
            StorageTemperature.Cold => (contextResolver.ColdStorage(), "cold"),
            StorageTemperature.LowCost => (contextResolver.LowCostStorage(), "low-cost"),
            _ => (contextResolver.HotStorage(), "hot")
        };

        // Kick off the upload: reads from pipe.Reader.AsStream()
        var uploadTask = Task.Run(async () =>
        {
            await using var requestStream = pipe.Reader.AsStream();
            var transferUtil = new TransferUtility(s3Client);
            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = bucketName,
                Key = key,
                InputStream = requestStream,
                ContentType = "application/octet-stream",
                PartSize = partSizeBytes,
                StorageClass = storageClass,
                AutoCloseStream = true,
                ServerSideEncryptionMethod = encryptionMethod,
                TagSet = [new Tag { Key = "storage-class", Value = tag }]
            };

            await transferUtil.UploadAsync(uploadRequest, cancellationToken);
        }, cancellationToken);

        // In this thread, read the file → compress → write into pipe.Writer
        await using (var fileStream = File.OpenRead(localFilePath))
        await using (var gzip = new BrotliStream(pipe.Writer.AsStream(), CompressionLevel.SmallestSize, true))
        {
            await fileStream.CopyToAsync(gzip, cancellationToken);
        }

        // Completing the writer side signals end-of-stream for the reader/upload
        await pipe.Writer.CompleteAsync();

        await uploadTask;
    }

    public async Task<T?> DownloadCompressedObject<T>(string key, CancellationToken cancellationToken)
    {
        var s3 = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = awsConfiguration.BucketName;

        if (!await S3ObjectExistsAsync(s3, bucketName, key, cancellationToken)) return default;

        using var resp = await s3.GetObjectAsync(
            new GetObjectRequest { BucketName = bucketName, Key = key },
            cancellationToken);

        await using var gzip = new BrotliStream(resp.ResponseStream, CompressionMode.Decompress, false);
        return await JsonSerializer.DeserializeAsync(gzip, (JsonTypeInfo<T>)GetTypeInfo<T>(),
            cancellationToken);
    }

    public async Task UploadCompressedObject<T>(string key, T obj, StorageTemperature temp,
        CancellationToken cancellationToken)
    {
        // 1) Create the pipe
        var pipe = new Pipe();
        var s3 = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = awsConfiguration.BucketName;
        var partSizeBytes = contextResolver.S3PartSize();
        var encryptionMethod = contextResolver.ServerSideEncryption();
        var (storageClass, tag) = temp switch
        {
            StorageTemperature.Hot => (contextResolver.HotStorage(), "hot"),
            StorageTemperature.Cold => (contextResolver.ColdStorage(), "cold"),
            StorageTemperature.LowCost => (contextResolver.LowCostStorage(), "low-cost"),
            _ => (contextResolver.HotStorage(), "hot")
        };

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
                AutoCloseStream = true,
                ServerSideEncryptionMethod = encryptionMethod,
                TagSet = [new Tag { Key = "storage-class", Value = tag }]
            };
            await transfer.UploadAsync(req, cancellationToken);
        }, cancellationToken);

        // 3) In this thread, serialize → gzip → pipe.Writer
        await using (var writerStream = pipe.Writer.AsStream())
        await using (var gzip = new BrotliStream(writerStream, CompressionLevel.SmallestSize, false))
        {
            await JsonSerializer.SerializeAsync(gzip, obj, SourceGenerationContext.Default.GetTypeInfo(typeof(T))!,
                cancellationToken);
        }
        // disposing gzip and writerStream will complete the pipe for the reader

        // 4) Wait for upload to finish
        await uploadTask;
    }

    public async Task<bool> S3ObjectExistsAsync(IAmazonS3 s3, string bucket, string key, CancellationToken ct)
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

    private static JsonTypeInfo GetTypeInfo<T>()
    {
        // Use the source generation context to get the type info for T
        return SourceGenerationContext.Default.GetTypeInfo(typeof(T))!;
    }

    private static async Task<(S3StorageClass storageClass, bool restoreInProgress)> GetStorageClass(IAmazonS3 s3Client,
        string bucketName, string s3Key,
        CancellationToken cancellationToken)
    {
        var request = new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = s3Key
        };

        var metadata = await s3Client.GetObjectMetadataAsync(request, cancellationToken);

        // StorageClass is a nullable enum; if AWS returned no header it will be null
        return (metadata.StorageClass, metadata.RestoreInProgress ?? false);
    }

    public static string ScrubTagValue(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return "";
        input = input.Trim().Replace("\0", "");
        input = RegexHelper.InValidTagValuesRegex().Replace(input, "");

        var bytes = Encoding.UTF8.GetBytes(input);
        if (bytes.Length <= _maxTagValueLength)
            return input;

        // Truncate while preserving valid UTF-8
        var len = _maxTagValueLength;
        while (len > 0 && (bytes[len - 1] & 0xC0) == 0x80) len--; // avoid cutting mid-sequence
        return Encoding.UTF8.GetString(bytes, 0, len);
    }
}