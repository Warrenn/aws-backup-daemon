using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Security.Cryptography;

namespace aws_backup;

public class SqsPollingOrchestration(
    Configuration configuration,
    IAwsClientFactory clientFactory,
    ILogger<SqsPollingOrchestration> logger,
    IMediator mediator,
    IContextResolver contextResolver
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var sqs = await clientFactory.CreateSqsClient(configuration, stoppingToken);

        var queueUrl = configuration.QueueUrl;
        var waitTimeSeconds = configuration.SqsWaitTimeSeconds;
        var maxNumberOfMessages = configuration.SqsMaxNumberOfMessages;
        var visibilityTimeout = configuration.SqsVisibilityTimeout;
        var retryDelay = configuration.SqsRetryDelaySeconds;
        var sqsDecryptionKey = await contextResolver.ResolveSqsDecryptionKey(configuration);

        logger.LogInformation("Starting SQS polling on {Url}", queueUrl);

        while (!stoppingToken.IsCancellationRequested)
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
                }, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error receiving messages, retrying in {retryDelay} seconds", retryDelay);
                
                await Task.Delay(TimeSpan.FromSeconds(retryDelay), stoppingToken);
                continue;
            }

            if (resp.Messages.Count == 0)
                continue; // no messages this cycle, long-poll will loop

            foreach (var msg in resp.Messages)
                try
                {
                    logger.LogInformation("Received message {Id}", msg.MessageId);
                    var body = msg.Body;
                    if (string.IsNullOrWhiteSpace(body)) continue;

                    if (configuration.EncryptSQS)
                    {
                        var encryptedData = Convert.FromBase64String(body);
                        var iv = encryptedData[..16]; // first 16 bytes are IV
                        using var aes = Aes.Create();
                        aes.Key = sqsDecryptionKey;
                        aes.IV = iv;
                        aes.Mode = CipherMode.CBC;
                        
                        await using var decryptStream = new CryptoStream(
                            respStream,
                            aes.CreateDecryptor(),
                            CryptoStreamMode.Read);
                    }

                    await mediator.ProcessSqsMessage(body, stoppingToken);

                    await sqs.DeleteMessageAsync(queueUrl, msg.ReceiptHandle, stoppingToken);
                    logger.LogInformation("Deleted message {Id} from SQS", msg.MessageId);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
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