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
        var uploadPath = $"{path}.gz";
        if (File.Exists(uploadPath))
            File.Delete(uploadPath);
        File.Move(path, uploadPath);
        File.WriteAllText(path,"");
        rollingFileMediator.Writer.TryWrite(uploadPath);
    }
}

public sealed class RollingFileActor(
    IRollingFileMediator rollingFileMediator,
    IContextResolver contextResolver,
    IS3Service s3Service,
    ILogger<RollingFileActor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await foreach (var filePath in rollingFileMediator.GetNextLoggingFile(cancellationToken))
            try
            {
                if (!File.Exists(filePath)) continue;

                var logFolder = contextResolver.S3LogFolder();
                var fileName = Path.GetFileName(filePath);
                var key = $"{logFolder}/{fileName}";
                
                await s3Service.UploadCompressedFile(
                    key,
                    filePath,
                    StorageTemperature.LowCost,
                    cancellationToken);

                File.Delete(filePath); // Delete the original file after upload
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