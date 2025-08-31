using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

//todo: implement retry write to read Q if message gone
//todo: read confirmation receipt from sender
//todo: implement write to read Q
// - use message encrypt key of sender
// - use message id of sender
public sealed class SqsPollingActor(
    IAwsClientFactory clientFactory,
    ILogger<SqsPollingActor> logger,
    IRestoreRequestsMediator mediator,
    IContextResolver contextResolver,
    ISnsMessageMediator snsMessageMediator,
    AwsConfiguration awsConfiguration,
    IAesContextResolver aesContextResolver,
    TimeProvider timeProvider
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting SQS polling");
        var logInboxQueueUrl = "";
        var retryDelay = contextResolver.SqsRetryDelaySeconds();
        var timer = new PeriodicTimer(TimeSpan.FromSeconds(retryDelay), timeProvider);

        while (!cancellationToken.IsCancellationRequested)
        {
            var sqs = await clientFactory.CreateSqsClient(cancellationToken);

            var sqsInboxQueueUrl = awsConfiguration.SqsInboxQueueUrl;
            var waitTimeSeconds = contextResolver.SqsWaitTimeSeconds();
            var maxNumberOfMessages = contextResolver.SqsMaxNumberOfMessages();
            var visibilityTimeout = contextResolver.SqsVisibilityTimeout();
            var sqsDecryptionKey = await aesContextResolver.SqsEncryptionKey(cancellationToken);

            if (logInboxQueueUrl != sqsInboxQueueUrl)
            {
                logInboxQueueUrl = sqsInboxQueueUrl;
                logger.LogInformation("SQS queue URL: {QueueUrl}", sqsInboxQueueUrl);
            }

            ReceiveMessageResponse resp;
            try
            {
                resp = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = sqsInboxQueueUrl,
                    WaitTimeSeconds = waitTimeSeconds, // long poll
                    MaxNumberOfMessages = maxNumberOfMessages, // batch up to 10
                    VisibilityTimeout = visibilityTimeout,
                    MessageAttributeNames = ["command", "encrypted"]
                }, cancellationToken);
            }
            catch (AmazonSQSException ex) when (ex.Message.Contains("Signature expired"))
            {
                logger.LogError(ex, "Signature expired. System clock or credentials may be invalid.");
                await timer.WaitForNextTickAsync(cancellationToken);

                clientFactory.ResetCachedCredentials();
                continue;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error receiving messages, retrying in {retryDelay} seconds", retryDelay);

                await timer.WaitForNextTickAsync(cancellationToken);
                continue;
            }

            if (resp?.Messages is not { Count: > 0 }) continue;

            foreach (var msg in resp.Messages!)
                try
                {
                    logger.LogInformation("Received message {Id}", msg.MessageId);
                    if (msg.MessageAttributes is null ||
                        !msg.MessageAttributes.TryGetValue("command", out var commandAttribute) ||
                        commandAttribute is null) continue;

                    var isEncrypted =
                        msg.MessageAttributes.TryGetValue("encrypted", out var encryptedAttribute) &&
                        encryptedAttribute is not null &&
                        bool.TryParse(encryptedAttribute.StringValue, out var encrypted) &&
                        encrypted;

                    var messageString = msg.Body;
                    var command = commandAttribute.StringValue;
                    if (string.IsNullOrWhiteSpace(messageString) ||
                        string.IsNullOrWhiteSpace(command))
                    {
                        await sqs.DeleteMessageAsync(sqsInboxQueueUrl, msg.ReceiptHandle, cancellationToken);
                        continue;
                    }

                    if (contextResolver.EncryptSqs() && !isEncrypted)
                    {
                        logger.LogWarning("Message encryption expected but message {Id} was not encrypted", msg.MessageId);
                        await sqs.DeleteMessageAsync(sqsInboxQueueUrl, msg.ReceiptHandle, cancellationToken);
                        continue;
                    }
                    
                    if (isEncrypted)
                        messageString = AesHelper.DecryptString(msg.Body, sqsDecryptionKey);

                    switch (command)
                    {
                        case "restore-backup":
                            var restoreRequest = JsonSerializer.Deserialize<RestoreRequest>(messageString,
                                SourceGenerationContext.Default.RestoreRequest);
                            if (restoreRequest is null) continue;

                            await mediator.RestoreBackup(restoreRequest, cancellationToken);
                            break;
                        default:
                            logger.LogWarning("Unknown command '{Command}' in message {Id}, skipping",
                                command, msg.MessageId);
                            break;
                    }

                    await sqs.DeleteMessageAsync(sqsInboxQueueUrl, msg.ReceiptHandle, cancellationToken);
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