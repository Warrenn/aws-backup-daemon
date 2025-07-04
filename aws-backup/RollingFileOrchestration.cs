using System.IO.Compression;
using System.IO.Pipelines;
using System.Threading.Channels;
using Amazon.S3.Transfer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog.Sinks.File;

namespace aws_backup;

public interface IRollingFileMediator
{
    ChannelWriter<string> Writer { get; }
    IAsyncEnumerable<string> GetNextLoggingFile(CancellationToken cancellationToken);
}

public class UploadToS3Hooks(IRollingFileMediator rollingFileMediator) : FileLifecycleHooks
{
    public override void OnFileDeleting(string path)
    {
        File.Move(path, $"{path}.gz");
        rollingFileMediator.Writer.TryWrite($"{path}.gz");
    }
}

public class RollingFileOrchestration(
    IRollingFileMediator rollingFileMediator,
    IContextResolver contextResolver,
    IAwsClientFactory factory,
    ILogger<RollingFileOrchestration> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await foreach (var filePath in rollingFileMediator.GetNextLoggingFile(cancellationToken))
            try
            {
                var pipe = new Pipe();
                var s3 = await factory.CreateS3Client(cancellationToken);
                var logFolder = contextResolver.S3LogFolder();
                var fileName = Path.GetFileName(filePath);
                var key = $"{logFolder}/{fileName}";
                var bucketName = contextResolver.S3BucketId();
                var partSizeBytes = contextResolver.S3PartSize();
                var storageClass = contextResolver.HotStorage();

                // Kick off the upload: reads from pipe.Reader.AsStream()
                var uploadTask = Task.Run(async () =>
                {
                    await using var requestStream = pipe.Reader.AsStream();
                    var transferUtil = new TransferUtility(s3);
                    var uploadRequest = new TransferUtilityUploadRequest
                    {
                        BucketName = bucketName,
                        Key = key,
                        InputStream = requestStream,
                        ContentType = "application/octet-stream",
                        PartSize = partSizeBytes,
                        StorageClass = storageClass,
                        AutoCloseStream = true
                    };

                    await transferUtil.UploadAsync(uploadRequest, cancellationToken);
                }, cancellationToken);

                // In this thread, read the file → compress → write into pipe.Writer
                await using (var fileStream = File.OpenRead(filePath))
                await using (var gzip = new GZipStream(pipe.Writer.AsStream(), CompressionLevel.SmallestSize, true))
                {
                    await fileStream.CopyToAsync(gzip, cancellationToken);
                }

                // Completing the writer side signals end-of-stream for the reader/upload
                await pipe.Writer.CompleteAsync();

                await uploadTask;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Failed to upload file {FilePath} to S3", filePath);
            }
    }
}