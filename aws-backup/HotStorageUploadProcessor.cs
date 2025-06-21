using System.IO.Compression;
using Amazon.S3.Transfer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class HotStorageUploadProcessor(
    Configuration configuration,
    IAwsClientFactory awsClientFactory,
    IContextFactory contextFactory,
    IMediator mediator,
    ILogger<HotStorageUploadProcessor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var (archive, key, getDataStream) in mediator.GetHotStorageUploads(stoppingToken))
            try
            {
                var s3Client = awsClientFactory.CreateS3Client(configuration);
                var bucketName = contextFactory.ResolveS3BucketName(configuration);
                var storageClass = contextFactory.ResolveHotStorage(configuration);
                var serverSideEncryptionMethod = contextFactory.ResolveServerSideEncryptionMethod(configuration);
                var transferUtil = new TransferUtility(s3Client);

                await using var stream = await getDataStream();
                var gzipStream = new GZipStream(stream,
                    CompressionLevel.Optimal,
                    leaveOpen: false);
                
                var uploadReq = new TransferUtilityUploadRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    InputStream = gzipStream,
                    PartSize = configuration.S3PartSize,
                    StorageClass = storageClass,
                    ServerSideEncryptionMethod = serverSideEncryptionMethod,
                    ContentType = "application/gzip"
                };

                await transferUtil.UploadAsync(uploadReq, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Handle cancellation gracefully
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing hot storage upload for archive {ArchiveId} and key {Key}",
                    archive.RunId, key);
                // Optionally, you can rethrow or handle the exception as needed
            }
    }
}