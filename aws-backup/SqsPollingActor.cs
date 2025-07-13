using System.Text;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public sealed class SqsPollingActor(
    IAwsClientFactory clientFactory,
    ILogger<SqsPollingActor> logger,
    IRestoreRequestsMediator mediator,
    IContextResolver contextResolver,
    ISnsMessageMediator snsMessageMediator,
    AwsConfiguration awsConfiguration,
    IAesContextResolver aesContextResolver
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting SQS polling");
        var logQueueUrl = "";

        while (!cancellationToken.IsCancellationRequested)
        {
            var sqs = await clientFactory.CreateSqsClient(cancellationToken);

            var queueUrl = awsConfiguration.SqsInboxQueueUrl;
            var waitTimeSeconds = contextResolver.SqsWaitTimeSeconds();
            var maxNumberOfMessages = contextResolver.SqsMaxNumberOfMessages();
            var visibilityTimeout = contextResolver.SqsVisibilityTimeout();
            var retryDelay = contextResolver.SqsRetryDelaySeconds();
            var sqsDecryptionKey = await aesContextResolver.SqsEncryptionKey(cancellationToken);

            if (logQueueUrl != queueUrl)
            {
                logQueueUrl = queueUrl;
                logger.LogInformation("SQS queue URL: {QueueUrl}", queueUrl);
            }

            ReceiveMessageResponse resp = null!;
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
            catch (AmazonSQSException ex) when (ex.Message.Contains("Signature expired"))
            {
                logger.LogError(ex, "Signature expired. System clock or credentials may be invalid.");
                clientFactory.ResetCachedCredentials();
                
                await Task.Delay(TimeSpan.FromSeconds(retryDelay), cancellationToken);
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
            
            if(resp?.Messages is not { Count: > 0 }) continue;

            foreach (var msg in resp.Messages!)
                try
                {
                    logger.LogInformation("Received message {Id}", msg.MessageId);
                    var messageString = msg.Body;
                    if (string.IsNullOrWhiteSpace(messageString)) continue;
                    if (contextResolver.EncryptSqs())
                        messageString = AesHelper.DecryptString(msg.Body, sqsDecryptionKey);

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
                                .Deserialize<RestoreRequest>(SourceGenerationContext.Default.RestoreRequest);
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
                    await snsMessageMediator.PublishMessage(new SnsMessage(
                        $"Failed to process SQS message {msg.MessageId}, it will become visible again",
                        ex.ToString()), cancellationToken);
                    logger.LogError(ex, "Failed to process message {Id}, it will become visible again", msg.MessageId);
                }
        }

        logger.LogInformation("SQS polling service is stopping.");
    }
}