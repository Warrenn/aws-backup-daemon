using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

//todo:update to use the rolling size
public sealed class RollingFileActor(
    IContextResolver contextResolver,
    IS3Service s3Service,
    ILogger<RollingFileActor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting rolling file actor");
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromDays(1), cancellationToken);
            var s3LogFolder = contextResolver.S3LogFolder();
            var logFolder = contextResolver.RollingLogFolder();
            
            if(string.IsNullOrEmpty(logFolder) || !Directory.Exists(logFolder)) continue;
            
            logger.LogInformation("Rolling file actor started, checking for files in {LogFolder}", logFolder);
            var (thisYear, thisMonth, thisDay) = DateTime.UtcNow;

            foreach (var filePath in Directory.GetFiles(logFolder))
            {
                var fileName = Path.GetFileName(filePath);
                var match = RegexHelper.DateRegex().Match(fileName);
                if (match.Success &&
                    int.Parse(match.Groups[1].Value) == thisYear &&
                    int.Parse(match.Groups[2].Value) == thisMonth &&
                    int.Parse(match.Groups[3].Value) == thisDay) continue;
                try
                {
                    logger.LogInformation("Processing old log file: {FileName}", fileName);
                    var key = $"{s3LogFolder}/{fileName}";
                    await s3Service.UploadCompressedFile(
                        key,
                        filePath,
                        StorageTemperature.LowCost,
                        cancellationToken);
                    
                    File.Delete(filePath); // Delete files older than today
                    logger.LogInformation("Deleted old log file: {FileName}", fileName);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to delete old log file: {FileName}", fileName);
                }
            }
        }
    }
}