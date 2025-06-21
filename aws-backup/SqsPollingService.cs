using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

public class SqsPollingService : BackgroundService
{
    private readonly IAmazonSQS   _sqs;
    private readonly ILogger<SqsPollingService> _logger;
    private readonly string      _queueUrl;
    private readonly string      _tableName;

    public SqsPollingService(
        IAmazonSQS sqs,
        ILogger<SqsPollingService> logger,
        IConfiguration config)
    {
        _sqs      = sqs;
        _logger   = logger;
        _queueUrl = config["SQS:QueueUrl"] 
                    ?? throw new ArgumentException("Missing SQS:QueueUrl");
        _tableName = config["DynamoDB:TableName"] 
                     ?? throw new ArgumentException("Missing DynamoDB:TableName");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting SQS polling on {Url}", _queueUrl);

        while (!stoppingToken.IsCancellationRequested)
        {
            ReceiveMessageResponse resp;
            try
            {
                resp = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl            = _queueUrl,
                    WaitTimeSeconds     = 20,      // long poll
                    MaxNumberOfMessages = 10,      // batch up to 10
                    VisibilityTimeout   = 60       // allow 60s to process
                }, stoppingToken);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error receiving messages, retrying in 10s");
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                continue;
            }

            if (resp.Messages.Count == 0)
                continue;  // no messages this cycle, long-poll will loop

            foreach (var msg in resp.Messages)
            {
                try
                {
                    _logger.LogInformation("Received message {Id}", msg.MessageId);

                    _logger.LogInformation("Wrote message {Id} to DynamoDB", msg.MessageId);

                    // 2) Delete from SQS
                    await _sqs.DeleteMessageAsync(_queueUrl, msg.ReceiptHandle, stoppingToken);
                    _logger.LogInformation("Deleted message {Id} from SQS", msg.MessageId);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Cancellation requested during processing");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process message {Id}, it will become visible again", msg.MessageId);
                    // do not delete: message will reappear
                }
            }
        }

        _logger.LogInformation("SQS polling service is stopping.");
    }
}
