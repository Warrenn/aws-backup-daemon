using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public class SqsPollingService(
    Configuration configuration,
    IAwsClientFactory clientFactory,
    ILogger<SqsPollingService> logger,
    IMediator mediator
    ) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var sqs = await clientFactory.CreateSqsClient(configuration, stoppingToken);
        
        var queueUrl = configuration.QueueUrl;
        var waitTimeSeconds = configuration.SqsWaitTimeSeconds;
        var maxNumberOfMessages = configuration.SqsMaxNumberOfMessages;
        var visibilityTimeout = configuration.SqsVisibilityTimeout;
        
        logger.LogInformation("Starting SQS polling on {Url}", queueUrl);

        while (!stoppingToken.IsCancellationRequested)
        {
            ReceiveMessageResponse resp;
            try
            {
                resp = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl            = queueUrl,
                    WaitTimeSeconds     = waitTimeSeconds,      // long poll
                    MaxNumberOfMessages = maxNumberOfMessages,      // batch up to 10
                    VisibilityTimeout   = visibilityTimeout       // allow 60s to process
                }, stoppingToken);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error receiving messages, retrying in 10s");
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                continue;
            }

            if (resp.Messages.Count == 0)
                continue;  // no messages this cycle, long-poll will loop

            foreach (var msg in resp.Messages)
            {
                try
                {
                    logger.LogInformation("Received message {Id}", msg.MessageId);

                    logger.LogInformation("Wrote message {Id} to DynamoDB", msg.MessageId);
                    var body = msg.Body;
                    
                    await mediator.ProcessSqsMessage(body, stoppingToken);

                    // 2) Delete from SQS
                    await sqs.DeleteMessageAsync(queueUrl, msg.ReceiptHandle, stoppingToken);
                    logger.LogInformation("Deleted message {Id} from SQS", msg.MessageId);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    logger.LogInformation("Cancellation requested during processing");
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to process message {Id}, it will become visible again", msg.MessageId);
                    // do not delete: message will reappear
                }
            }
        }

        logger.LogInformation("SQS polling service is stopping.");
    }
}