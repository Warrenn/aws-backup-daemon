using System.Collections.Concurrent;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using aws_backup_common;
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
    IAesContextResolver aesContextResolver,
    TimeProvider timeProvider
) : BackgroundService
{
    private ConcurrentDictionary<string, SendMessageRequest> ResponseMessages { get; } = new();

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting SQS polling service");

        var workers = new Task[2];
        workers[0] = Task.Run(() => ResendResponsesAsync(cancellationToken), cancellationToken);
        workers[1] = Task.Run(() => InboxPollingAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        await Task.WhenAll(workers);

        logger.LogInformation("SQS polling service is stopping.");
    }

    private async Task ResendResponsesAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting ResendResponsesAsync loop");
        var retryDelay = contextResolver.SqsRetryResponseDelaySeconds();
        var timer = new PeriodicTimer(TimeSpan.FromSeconds(retryDelay), timeProvider);

        while (!cancellationToken.IsCancellationRequested)
            try
            {
                await timer.WaitForNextTickAsync(cancellationToken);

                if (ResponseMessages.IsEmpty) continue;

                var sqs = await clientFactory.CreateSqsClient(cancellationToken);
                var sqsOutboxQueueUrl = awsConfiguration.SqsOutboxQueueUrl;
                if (string.IsNullOrWhiteSpace(sqsOutboxQueueUrl)) continue;

                foreach (var (sessionId, message) in ResponseMessages)
                    try
                    {
                        await sqs.SendMessageAsync(message, cancellationToken);
                        logger.LogInformation("Resent response message for session-id {SessionId}", sessionId);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to resend response message for session-id {SessionId}", sessionId);
                    }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in ResendResponsesAsync loop, retrying in {retryDelay} seconds",
                    retryDelay);
            }
    }

    private async Task InboxPollingAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting Inbox polling");

        var logInboxQueueUrl = "";
        var retryDelay = contextResolver.SqsRetryDelaySeconds();
        var timer = new PeriodicTimer(TimeSpan.FromSeconds(retryDelay), timeProvider);

        while (!cancellationToken.IsCancellationRequested)
        {
            var sqs = await clientFactory.CreateSqsClient(cancellationToken);

            var sqsInboxQueueUrl = awsConfiguration.SqsInboxQueueUrl;
            var sqsOutboxQueueUrl = awsConfiguration.SqsOutboxQueueUrl;
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
                    MessageAttributeNames = ["command", "encrypted", "response-id", "response-enc-key"]
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

                    var sessionId =
                        msg.MessageAttributes.TryGetValue("session-id", out var sessionIdAttribute) &&
                        sessionIdAttribute is not null
                            ? sessionIdAttribute.StringValue
                            : null;

                    if (contextResolver.EncryptSqs() && !isEncrypted)
                        logger.LogWarning("Message encryption expected but message {Id} was not encrypted",
                            msg.MessageId);

                    if (isEncrypted && sqsDecryptionKey is not null)
                        messageString = AesHelper.DecryptString(msg.Body, sqsDecryptionKey);

                    switch (command)
                    {
                        case "confirmation":
                            logger.LogInformation("Received confirmation message {Id}", msg.MessageId);
                            if (string.IsNullOrWhiteSpace(sessionId) ||
                                !ResponseMessages.TryRemove(sessionId, out _))
                                logger.LogWarning(
                                    "Received confirmation for unknown session-id {SessionId} in message {Id}",
                                    sessionId, msg.MessageId);
                            // no action needed, just a confirmation
                            break;
                        case "ping":
                            logger.LogInformation("Received ping message {Id}", msg.MessageId);

                            await SendResponse("pong");

                            break;
                        case "restore-backup":
                            logger.LogInformation("Received restore backup message {Id}", msg.MessageId);
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

                    async Task SendResponse(string messageBody, bool encryptResponse = false)
                    {
                        if (string.IsNullOrWhiteSpace(sessionId) || string.IsNullOrWhiteSpace(sqsOutboxQueueUrl))
                            return;

                        if (encryptResponse && sqsDecryptionKey is not null && sqsDecryptionKey.Length > 0)
                            messageBody = AesHelper.EncryptString(messageBody, sqsDecryptionKey);

                        var responseMessage = new SendMessageRequest
                        {
                            QueueUrl = sqsOutboxQueueUrl,
                            MessageBody = messageBody,
                            MessageAttributes =
                            {
                                ["session-id"] = new MessageAttributeValue
                                {
                                    DataType = "String",
                                    StringValue = sessionId
                                }
                            }
                        };

                        if (encryptResponse && sqsDecryptionKey is not null && sqsDecryptionKey.Length > 0)
                            responseMessage.MessageAttributes["encrypted"] = new MessageAttributeValue
                            {
                                DataType = "String",
                                StringValue = bool.TrueString
                            };

                        if (!ResponseMessages.TryAdd(sessionId, responseMessage))
                            logger.LogWarning("Response message for session-id {SessionId} already exists", sessionId);

                        await sqs.SendMessageAsync(responseMessage, cancellationToken);
                        logger.LogInformation(
                            "Sent response for message {Id} with session-id {SessionId}",
                            msg.MessageId, sessionId);
                    }
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
    }
}