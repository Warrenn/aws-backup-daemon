using System.Text;
using System.Text.Json;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class SqsPollingOrchestration(
    IAwsClientFactory clientFactory,
    ILogger<SqsPollingOrchestration> logger,
    IRestoreRequestsMediator mediator,
    IContextResolver contextResolver
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var sqs = await clientFactory.CreateSqsClient(cancellationToken);

        var queueUrl = contextResolver.SqsQueueUrl();
        var waitTimeSeconds = contextResolver.SqsWaitTimeSeconds();
        var maxNumberOfMessages = contextResolver.SqsMaxNumberOfMessages();
        var visibilityTimeout = contextResolver.SqsVisibilityTimeout();
        var retryDelay = contextResolver.SqsRetryDelaySeconds();
        var sqsDecryptionKey = await contextResolver.SqsEncryptionKey(cancellationToken);

        logger.LogInformation("Starting SQS polling on {Url}", queueUrl);

        while (!cancellationToken.IsCancellationRequested)
        {
            ReceiveMessageResponse resp;
            try
            {
                resp = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = queueUrl,
                    WaitTimeSeconds = waitTimeSeconds, // long poll
                    MaxNumberOfMessages = maxNumberOfMessages, // batch up to 10
                    VisibilityTimeout = visibilityTimeout // allow 60s to process
                }, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error receiving messages, retrying in {retryDelay} seconds", retryDelay);

                await Task.Delay(TimeSpan.FromSeconds(retryDelay), cancellationToken);
                continue;
            }

            if (resp.Messages.Count == 0)
                continue; // no messages this cycle, long-poll will loop

            foreach (var msg in resp.Messages)
                try
                {
                    logger.LogInformation("Received message {Id}", msg.MessageId);
                    var messageString = msg.Body;
                    if (string.IsNullOrWhiteSpace(messageString)) continue;
                    if (contextResolver.EncryptSqs()) messageString = AesHelper.DecryptString(msg.Body, sqsDecryptionKey);

                    var utf8 = Encoding.UTF8.GetBytes(messageString);
                    var reader = new Utf8JsonReader(utf8, true, default);
                    if (!JsonDocument.TryParseValue(ref reader, out var jsonDocument))
                    {
                        logger.LogError("Received message {Id} but no json document", msg.MessageId);
                        continue;
                    }

                    var rootElement = jsonDocument.RootElement;

                    if (!rootElement.TryGetProperty("command", out var commandElement))
                    {
                        logger.LogWarning("Message {Id} does not contain a 'command' property, skipping",
                            msg.MessageId);
                        continue;
                    }

                    var command = commandElement.GetString();
                    switch (command)
                    {
                        case "restore-backup":
                            var restoreRequest = rootElement.GetProperty("body")
                                .Deserialize<RestoreRequest>(
                                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                            if (restoreRequest is null) continue;
                            
                            await mediator.RestoreBackup(restoreRequest, cancellationToken);
                            break;
                        default:
                            logger.LogWarning("Unknown command '{Command}' in message {Id}, skipping",
                                command, msg.MessageId);
                            break;
                    }


                    await sqs.DeleteMessageAsync(queueUrl, msg.ReceiptHandle, cancellationToken);
                    logger.LogInformation("Deleted message {Id} from SQS", msg.MessageId);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to process message {Id}, it will become visible again", msg.MessageId);
                }
        }

        logger.LogInformation("SQS polling service is stopping.");
    }
}