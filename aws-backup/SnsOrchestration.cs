using System.Collections.Concurrent;
using Amazon.SimpleNotificationService.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface ISnsOrchestrationMediator
{
    IAsyncEnumerable<SnsMessage> GetMessages(CancellationToken cancellationToken);
    Task PublishMessage(SnsMessage message, CancellationToken cancellationToken);
}

public record SnsMessage(
    string Subject,
    string Message);

public record ArchiveCompleteMessage(
    string RunId,
    string Subject,
    string Message,
    ArchiveRun ArchiveRun) : SnsMessage(Subject, Message);

public record ArchiveCompleteErrorMessage(
    string RunId,
    string Subject,
    string Message,
    ArchiveRun ArchiveRun) : SnsMessage(Subject, Message);

public record RestoreCompleteMessage(
    string RestoreId,
    string Subject,
    string Message,
    RestoreRun RestoreRun) : SnsMessage(Subject, Message);

public record RestoreCompleteErrorMessage(
    string RestoreId,
    string Subject,
    string Message,
    RestoreRun RestoreRun) : SnsMessage(Subject, Message);

public record ExceptionMessage(
    string Subject,
    string Message) : SnsMessage(Subject, Message);

public class SnsOrchestration(
    ISnsOrchestrationMediator snsOrchestrationMediator,
    ILogger<SnsOrchestration> logger,
    IContextResolver contextResolver,
    IAwsClientFactory clientFactory) : BackgroundService
{
    private readonly ConcurrentDictionary<Type, (bool notify, string arn)> _messageTypeToSnsArn = new()
    {
        [typeof(ArchiveCompleteMessage)] =
            (contextResolver.NotifyOnArchiveComplete(), contextResolver.ArchiveCompleteSnsArn()),
        [typeof(ArchiveCompleteErrorMessage)] = (contextResolver.NotifyOnArchiveCompleteErrors(),
            contextResolver.ArchiveCompleteErrorSnsArn()),
        [typeof(RestoreCompleteMessage)] =
            (contextResolver.NotifyOnRestoreComplete(), contextResolver.RestoreCompleteSnsArn()),
        [typeof(RestoreCompleteErrorMessage)] = (contextResolver.NotifyOnRestoreCompleteErrors(),
            contextResolver.RestoreCompleteErrorSnsArn()),
        [typeof(ExceptionMessage)] = (contextResolver.NotifyOnException(), contextResolver.ExceptionSnsArn())
    };

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var sns = await clientFactory.CreateSnsClient(cancellationToken);
        await foreach (var message in snsOrchestrationMediator.GetMessages(cancellationToken))
            try
            {
                if (!_messageTypeToSnsArn.TryGetValue(message.GetType(), out var snsNotification))
                {
                    logger.LogWarning("Received unhandled SNS message type: {MessageType}", message.GetType());
                    continue;
                }

                if (!snsNotification.notify) continue;

                var request = new PublishRequest
                {
                    TargetArn = snsNotification.arn,
                    Subject = message.Subject,
                    Message = $"""
                               Message:{message.Message}
                               Data:{message}
                               """
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