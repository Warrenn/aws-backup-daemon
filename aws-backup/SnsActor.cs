using System.Collections.Concurrent;
using System.Text;
using Amazon.SimpleNotificationService.Model;
using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface ISnsMessageMediator
{
    IAsyncEnumerable<SnsMessage> GetMessages(CancellationToken cancellationToken);
    Task PublishMessage(SnsMessage message, CancellationToken cancellationToken);
}

public record SnsMessage(
    string Subject,
    string Message);

public sealed record ArchiveCompleteMessage(
    string Subject,
    string Message,
    ArchiveRun ArchiveRun) : SnsMessage(Subject, Message);

public sealed record ArchiveCompleteErrorMessage(
    string RunId,
    string Subject,
    string Message,
    ArchiveRun ArchiveRun) : SnsMessage(Subject, Message);

public sealed record RestoreCompleteMessage(
    string Subject,
    string Message,
    RestoreRun RestoreRun) : SnsMessage(Subject, Message);

public sealed record RestoreCompleteErrorMessage(
    string RestoreId,
    string Subject,
    string Message,
    RestoreRun RestoreRun) : SnsMessage(Subject, Message);

public sealed record ExceptionMessage(
    string Subject,
    string Message) : SnsMessage(Subject, Message);

public sealed class SnsActor(
    ISnsMessageMediator snsMessageMediator,
    ILogger<SnsActor> logger,
    IContextResolver contextResolver,
    IAwsClientFactory clientFactory,
    AwsConfiguration awsConfiguration) : BackgroundService
{
    private readonly ConcurrentDictionary<Type, (bool notify, string arn, Func<SnsMessage, string> getMessage)>
        _messageTypeToSnsArn = new()
        {
            [typeof(ArchiveCompleteMessage)] =
                (contextResolver.NotifyOnArchiveComplete(), awsConfiguration.ArchiveCompleteTopicArn,
                    m => GetArchiveCompleteMessage(m, am => ((ArchiveCompleteMessage)am).ArchiveRun)),
            [typeof(ArchiveCompleteErrorMessage)] = (contextResolver.NotifyOnArchiveCompleteErrors(),
                awsConfiguration.ArchiveCompleteErrorsTopicArn,
                m => GetArchiveCompleteMessage(m, am => ((ArchiveCompleteErrorMessage)am).ArchiveRun)),
            [typeof(RestoreCompleteMessage)] =
                (contextResolver.NotifyOnRestoreComplete(), awsConfiguration.RestoreCompleteTopicArn,
                    m => GetRestoreCompleteMessage(m, rm => ((RestoreCompleteMessage)rm).RestoreRun)),
            [typeof(RestoreCompleteErrorMessage)] = (contextResolver.NotifyOnRestoreCompleteErrors(),
                awsConfiguration.RestoreCompleteErrorsTopicArn, m => GetRestoreCompleteMessage(m,
                    rm => ((RestoreCompleteErrorMessage)rm).RestoreRun)),
            [typeof(ExceptionMessage)] = (contextResolver.NotifyOnException(), awsConfiguration.ExceptionTopicArn,
                GetExceptionMessage)
        };

    private static string GetExceptionMessage(SnsMessage message)
    {
        var exceptionMessage = message as ExceptionMessage;
        return $"Exception: {exceptionMessage?.Message}";
    }

    private static string GetArchiveCompleteMessage(SnsMessage message, Func<SnsMessage, ArchiveRun> getRun)
    {
        var builder = new StringBuilder();
        var archiveRun = getRun(message);

        foreach (var (filePath, metaData) in archiveRun.Files)
        {
            builder.Append(
                $"File: {filePath} Status: {metaData.Status} Size: {metaData.OriginalSize} LastModified: {metaData.LastModified} Skip Reason: {metaData.SkipReason} ");
            builder.AppendLine();
        }

        return $"""
                {message.Message}
                RunId: {archiveRun.RunId}
                CronSchedule: {archiveRun.CronSchedule}
                StartTime: {archiveRun.CreatedAt}
                EndTime: {archiveRun.CompletedAt}
                Original Size: {archiveRun.OriginalSize}
                Compressed Size: {archiveRun.CompressedSize}

                Archived Files {archiveRun.TotalFiles}:
                Skipped Files {archiveRun.TotalSkippedFiles}
                {builder}
                """;
    }

    private static string GetRestoreCompleteMessage(SnsMessage message, Func<SnsMessage, RestoreRun> getRun)
    {
        var builder = new StringBuilder();
        var restoreRun = getRun(message);

        foreach (var (filePath, metaData) in restoreRun.RequestedFiles)
        {
            builder.Append(
                $"File: {filePath} Status: {metaData.Status} LastModified: {metaData.LastModified} ");
            if (restoreRun.FailedFiles.TryGetValue(filePath, out var value))
                builder.Append($"Failed Reason: {value} ");
            builder.AppendLine();
        }

        return $"""
                {message.Message}
                Restore Id: {restoreRun.RestoreId}
                Archive Run Id: {restoreRun.ArchiveRunId}
                StartTime: {restoreRun.RequestedAt}
                EndTime: {restoreRun.CompletedAt}
                Restore Paths:{restoreRun.RestorePaths}
                Restore Files {restoreRun.RequestedFiles.Count}:
                {builder}
                """;
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("SnsActor started");
        await foreach (var message in snsMessageMediator.GetMessages(cancellationToken))
            try
            {
                if (!_messageTypeToSnsArn.TryGetValue(message.GetType(), out var snsNotification))
                {
                    logger.LogWarning("Received unhandled SNS message type: {MessageType}", message.GetType());
                    continue;
                }

                if (!snsNotification.notify) continue;
                var messageContent = snsNotification.getMessage(message);

                var sns = await clientFactory.CreateSnsClient(cancellationToken);
                var request = new PublishRequest
                {
                    TargetArn = snsNotification.arn,
                    Subject = message.Subject,
                    Message = messageContent
                };

                var response = await sns.PublishAsync(request, cancellationToken);

                logger.LogInformation("Sns published successfully {response}", response);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to process SNS message: {Message}", message);
            }
    }
}