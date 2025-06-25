using System.Diagnostics.CodeAnalysis;
using System.IO.Compression;
using System.IO.Pipelines;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using ProtoBuf;

namespace aws_backup;

public interface IHotStorageService
{
    Task UploadAsync<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors |
                                    DynamicallyAccessedMemberTypes.NonPublicFields |
                                    DynamicallyAccessedMemberTypes.NonPublicMethods |
                                    DynamicallyAccessedMemberTypes.NonPublicNestedTypes |
                                    DynamicallyAccessedMemberTypes.NonPublicProperties |
                                    DynamicallyAccessedMemberTypes.None |
                                    DynamicallyAccessedMemberTypes.PublicConstructors |
                                    DynamicallyAccessedMemberTypes.PublicFields |
                                    DynamicallyAccessedMemberTypes.PublicMethods |
                                    DynamicallyAccessedMemberTypes.PublicNestedTypes |
                                    DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
                                    DynamicallyAccessedMemberTypes.PublicProperties)]
        T>(string key, T obj, CancellationToken cancellationToken);

    Task<T> DownloadAsync<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors |
                                    DynamicallyAccessedMemberTypes.NonPublicFields |
                                    DynamicallyAccessedMemberTypes.NonPublicMethods |
                                    DynamicallyAccessedMemberTypes.NonPublicNestedTypes |
                                    DynamicallyAccessedMemberTypes.NonPublicProperties |
                                    DynamicallyAccessedMemberTypes.None |
                                    DynamicallyAccessedMemberTypes.PublicConstructors |
                                    DynamicallyAccessedMemberTypes.PublicFields |
                                    DynamicallyAccessedMemberTypes.PublicMethods |
                                    DynamicallyAccessedMemberTypes.PublicNestedTypes |
                                    DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
                                    DynamicallyAccessedMemberTypes.PublicProperties)]
        T>(string key, CancellationToken cancellationToken);
}

public class HotStorageService(
    IAwsClientFactory awsClientFactory,
    IContextResolver contextResolver) : IHotStorageService
{
    public async Task UploadAsync<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors |
                                    DynamicallyAccessedMemberTypes.NonPublicFields |
                                    DynamicallyAccessedMemberTypes.NonPublicMethods |
                                    DynamicallyAccessedMemberTypes.NonPublicNestedTypes |
                                    DynamicallyAccessedMemberTypes.NonPublicProperties |
                                    DynamicallyAccessedMemberTypes.None |
                                    DynamicallyAccessedMemberTypes.PublicConstructors |
                                    DynamicallyAccessedMemberTypes.PublicFields |
                                    DynamicallyAccessedMemberTypes.PublicMethods |
                                    DynamicallyAccessedMemberTypes.PublicNestedTypes |
                                    DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
                                    DynamicallyAccessedMemberTypes.PublicProperties)]
        T>(string key, T obj, CancellationToken cancellationToken)
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
                ContentType = "application/x-protobuf",
                StorageClass = storageClass,
                AutoCloseStream = true
            };
            await transfer.UploadAsync(req, cancellationToken);
        }, cancellationToken);

        // 3) In this thread, serialize → gzip → pipe.Writer
        await using (var writerStream = pipe.Writer.AsStream())
        await using (var gzip = new GZipStream(writerStream, CompressionLevel.Optimal, false))
        {
            Serializer.Serialize(gzip, obj);
        }
        // disposing gzip and writerStream will complete the pipe for the reader

        // 4) Wait for upload to finish
        await uploadTask;
    }

    public async Task<T> DownloadAsync<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors |
                                    DynamicallyAccessedMemberTypes.NonPublicFields |
                                    DynamicallyAccessedMemberTypes.NonPublicMethods |
                                    DynamicallyAccessedMemberTypes.NonPublicNestedTypes |
                                    DynamicallyAccessedMemberTypes.NonPublicProperties |
                                    DynamicallyAccessedMemberTypes.None |
                                    DynamicallyAccessedMemberTypes.PublicConstructors |
                                    DynamicallyAccessedMemberTypes.PublicFields |
                                    DynamicallyAccessedMemberTypes.PublicMethods |
                                    DynamicallyAccessedMemberTypes.PublicNestedTypes |
                                    DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
                                    DynamicallyAccessedMemberTypes.PublicProperties)]
        T>(string key, CancellationToken cancellationToken)
    {
        var s3 = await awsClientFactory.CreateS3Client(cancellationToken);
        var bucketName = contextResolver.S3BucketId();
        using var resp = await s3.GetObjectAsync(
            new GetObjectRequest { BucketName = bucketName, Key = key },
            cancellationToken);

        await using var gzip = new GZipStream(resp.ResponseStream, CompressionMode.Decompress, false);
        return Serializer.Deserialize<T>(gzip)!;
    }
}