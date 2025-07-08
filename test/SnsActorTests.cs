using System.Collections;
using System.Runtime.CompilerServices;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class SnsActorTests
{
    private readonly SnsActor _actor;
    private readonly Mock<IAwsClientFactory> _clientFactory;
    private readonly AwsConfiguration _config = new(
        16,
        "sqs-enc", "file-enc",
    "bucket-name", "region",
    "queue-in", "queue-out",
    "arn:aws:sns:us-east-1:123456789012:archive-complete", "arn:aws:sns:us-east-1:123456789012:archive-error",
    "arn:aws:sns:us-east-1:123456789012:restore-complete", "arn:aws:sns:us-east-1:123456789012:restore-error", "arn:aws:sns:us-east-1:123456789012:exception");
    private readonly Mock<IContextResolver> _contextResolver;
    private readonly TestLoggerClass<SnsActor> _logger;
    private readonly Mock<IAmazonSimpleNotificationService> _snsClient;
    private readonly Mock<ISnsMessageMediator> _snsOrchestrationMediator;

    public SnsActorTests()
    {
        _snsOrchestrationMediator = new Mock<ISnsMessageMediator>();
        _contextResolver = new Mock<IContextResolver>();
        _clientFactory = new Mock<IAwsClientFactory>();
        _snsClient = new Mock<IAmazonSimpleNotificationService>();
        _logger = new TestLoggerClass<SnsActor>();

        // Setup default context resolver responses
        SetupDefaultContextResolver();

        _clientFactory.Setup(x => x.CreateSnsClient(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_snsClient.Object);

        _actor = new SnsActor(
            _snsOrchestrationMediator.Object,
            _logger,
            _contextResolver.Object,
            _clientFactory.Object,
            _config);
    }

    [Fact]
    public async Task ExecuteAsync_WithArchiveCompleteMessage_PublishesToCorrectArn()
    {
        // Arrange
        var archiveRun = CreateTestRun("test-run");
        var message = new ArchiveCompleteMessage(
            "Archive Complete",
            "Archive operation completed successfully",
            archiveRun);

        var messages = CreateAsyncEnumerable([message]);
        _snsOrchestrationMediator.Setup(x => x.GetMessages(It.IsAny<CancellationToken>()))
            .Returns(messages);

        var publishResponse = new PublishResponse { MessageId = "msg-123" };
        _snsClient.Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(publishResponse);

        // Act
        await _actor.StartAsync(CancellationToken.None);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(CancellationToken.None);

        // Assert
        _snsClient.Verify(x => x.PublishAsync(It.Is<PublishRequest>(req =>
            req.TargetArn == "arn:aws:sns:us-east-1:123456789012:archive-complete" &&
            req.Subject == "Archive Complete" &&
            req.Message.Contains("Archive operation completed successfully")
        ), It.IsAny<CancellationToken>()), Times.Once);

        var infoLogs = _logger.LogRecords.Where(x => x.LogLevel == LogLevel.Information).ToList();
        Assert.Single((IEnumerable)infoLogs);
        Assert.Contains("Sns published successfully", infoLogs[0].Message);
    }

    [Fact]
    public async Task ExecuteAsync_WithArchiveCompleteErrorMessage_PublishesToErrorArn()
    {
        // Arrange
        var archiveRun = CreateTestRun("failed-run");
        var message = new ArchiveCompleteErrorMessage(
            "run-456",
            "Archive Failed",
            "Archive operation failed with errors",
            archiveRun);

        var messages = CreateAsyncEnumerable([message]);
        _snsOrchestrationMediator.Setup(x => x.GetMessages(It.IsAny<CancellationToken>()))
            .Returns(messages);

        var publishResponse = new PublishResponse { MessageId = "msg-456" };
        _snsClient.Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(publishResponse);

        // Act
        await _actor.StartAsync(CancellationToken.None);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(CancellationToken.None);

        // Assert
        _snsClient.Verify(x => x.PublishAsync(It.Is<PublishRequest>(req =>
            req.TargetArn == "arn:aws:sns:us-east-1:123456789012:archive-error" &&
            req.Subject == "Archive Failed"
        ), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_WithRestoreCompleteMessage_PublishesToRestoreArn()
    {
        // Arrange
        var restoreRun = CreateRestoreRun("restore-123");
        var message = new RestoreCompleteMessage(
            "Restore Complete",
            "Restore Complete",
            restoreRun);

        var messages = CreateAsyncEnumerable([message]);
        _snsOrchestrationMediator.Setup(x => x.GetMessages(It.IsAny<CancellationToken>()))
            .Returns(messages);

        var publishResponse = new PublishResponse { MessageId = "msg-789" };
        _snsClient.Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(publishResponse);

        // Act
        await _actor.StartAsync(CancellationToken.None);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(CancellationToken.None);

        // Assert
        _snsClient.Verify(x => x.PublishAsync(It.Is<PublishRequest>(req =>
            req.TargetArn == "arn:aws:sns:us-east-1:123456789012:restore-complete" &&
            req.Subject == "Restore Complete"
        ), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_WithExceptionMessage_PublishesToExceptionArn()
    {
        // Arrange
        var message = new ExceptionMessage(
            "System Exception",
            "An unexpected error occurred in the system");

        var messages = CreateAsyncEnumerable([message]);
        _snsOrchestrationMediator.Setup(x => x.GetMessages(It.IsAny<CancellationToken>()))
            .Returns(messages);

        var publishResponse = new PublishResponse { MessageId = "msg-exception" };
        _snsClient.Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(publishResponse);

        // Act
        await _actor.StartAsync(CancellationToken.None);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(CancellationToken.None);

        // Assert
        _snsClient.Verify(x => x.PublishAsync(It.Is<PublishRequest>(req =>
            req.TargetArn == "arn:aws:sns:us-east-1:123456789012:exception" &&
            req.Subject == "System Exception"
        ), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_WithNotificationDisabled_SkipsPublishing()
    {
        // Arrange

        // Disable archive complete notifications
        _contextResolver.Setup(x => x.NotifyOnArchiveComplete()).Returns(false);

        var archiveRun = CreateTestRun("test-run");
        var message = new ArchiveCompleteMessage(
            "run-123",
            "Archive Complete",
            archiveRun);

        var messages = CreateAsyncEnumerable([message]);
        _snsOrchestrationMediator.Setup(x => x.GetMessages(It.IsAny<CancellationToken>()))
            .Returns(messages);

        // Act
        var orchestration = new SnsActor(
            _snsOrchestrationMediator.Object,
            _logger,
            _contextResolver.Object,
            _clientFactory.Object,
            _config);
        await orchestration.StartAsync(CancellationToken.None);
        await (orchestration.ExecuteTask ?? Task.CompletedTask);
        await orchestration.StopAsync(CancellationToken.None);

        // Assert
        _snsClient.Verify(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()),
            Times.Never);

        Assert.Empty(_logger.LogRecords.Where(x => x.LogLevel == LogLevel.Information));
    }

    [Fact]
    public async Task ExecuteAsync_WithUnknownMessageType_LogsWarning()
    {
        // Arrange
        var unknownMessage = new UnknownSnsMessage("Unknown", "Unknown message type");

        var messages = CreateAsyncEnumerable([unknownMessage]);
        _snsOrchestrationMediator.Setup(x => x.GetMessages(It.IsAny<CancellationToken>()))
            .Returns(messages);

        // Act
        await _actor.StartAsync(CancellationToken.None);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(CancellationToken.None);

        // Assert
        var warningLogs = _logger.LogRecords.Where(x => x.LogLevel == LogLevel.Warning).ToList();
        Assert.Single((IEnumerable)warningLogs);
        Assert.Contains("Received unhandled SNS message type", warningLogs[0].Message);
        Assert.Contains(typeof(UnknownSnsMessage).ToString(), warningLogs[0].Message);

        _snsClient.Verify(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_WithSnsPublishException_LogsErrorAndContinues()
    {
        // Arrange
        var testException = new AmazonSimpleNotificationServiceException("SNS publish failed");

        var archiveRun = CreateTestRun("test-run");
        var message1 = new ArchiveCompleteMessage("run-1", "Archive 1", archiveRun);
        var message2 = new ArchiveCompleteMessage("run-2", "Archive 2", archiveRun);

        var messages = CreateAsyncEnumerable([message1, message2]);
        _snsOrchestrationMediator.Setup(x => x.GetMessages(It.IsAny<CancellationToken>()))
            .Returns(messages);

        _snsClient.SetupSequence(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(testException)
            .ReturnsAsync(new PublishResponse { MessageId = "msg-2" });

        // Act
        await _actor.StartAsync(CancellationToken.None);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(CancellationToken.None);

        // Assert
        var errorLogs = _logger.LogRecords.Where(x => x.LogLevel == LogLevel.Error).ToList();
        Assert.Single((IEnumerable)errorLogs);
        Assert.Contains("Failed to process SNS message", errorLogs[0].Message);
        Assert.Equal(testException, errorLogs[0].Exception);

        // Verify second message was still processed
        var infoLogs = _logger.LogRecords.Where(x => x.LogLevel == LogLevel.Information).ToList();
        Assert.Single((IEnumerable)infoLogs);
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellation_StopsProcessingGracefully()
    {
        // Arrange
        using var cts = new CancellationTokenSource();

        var archiveRun = CreateTestRun("test-run");
        var messages = CreateLongRunningAsyncEnumerable(cts.Token);

        _snsOrchestrationMediator.Setup(x => x.GetMessages(It.IsAny<CancellationToken>()))
            .Returns(messages);

        _snsClient.Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                await Task.Delay(100, cts.Token);
                return new PublishResponse { MessageId = "msg-123" };
            });

        // Act
        await _actor.StartAsync(cts.Token);
        await Task.Delay(50); // Let it start processing
        cts.Cancel();
        await Task.Delay(200); // Allow cancellation to propagate
        await (_actor.ExecuteTask ?? Task.CompletedTask);

        // Assert - Should not throw and should handle cancellation gracefully
        var errorLogs = _logger.LogRecords.Where(x => x.LogLevel == LogLevel.Error).ToList();
        Assert.DoesNotContain(errorLogs, log => log.Exception is OperationCanceledException);
    }

    [Fact]
    public async Task ExecuteAsync_WithMultipleMessageTypes_ProcessesAllCorrectly()
    {
        // Arrange
        var archiveRun = CreateTestRun("test-run");
        var restoreRun = CreateRestoreRun("restore-run");

        var messages = CreateAsyncEnumerable([
            new ArchiveCompleteMessage("run-1", "Archive 1",  archiveRun),
            new ExceptionMessage("Exception", "System error"),
            new RestoreCompleteMessage("restore-1", "Restore 1",  restoreRun)
        ]);

        _snsOrchestrationMediator.Setup(x => x.GetMessages(It.IsAny<CancellationToken>()))
            .Returns(messages);

        _snsClient.Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PublishResponse { MessageId = "msg-123" });

        // Act
        await _actor.StartAsync(CancellationToken.None);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(CancellationToken.None);

        // Assert
        _snsClient.Verify(x => x.PublishAsync(It.Is<PublishRequest>(req =>
            req.TargetArn == "arn:aws:sns:us-east-1:123456789012:archive-complete"
        ), It.IsAny<CancellationToken>()), Times.Once);

        _snsClient.Verify(x => x.PublishAsync(It.Is<PublishRequest>(req =>
            req.TargetArn == "arn:aws:sns:us-east-1:123456789012:exception"
        ), It.IsAny<CancellationToken>()), Times.Once);

        _snsClient.Verify(x => x.PublishAsync(It.Is<PublishRequest>(req =>
            req.TargetArn == "arn:aws:sns:us-east-1:123456789012:restore-complete"
        ), It.IsAny<CancellationToken>()), Times.Once);

        var infoLogs = _logger.LogRecords.Where(x => x.LogLevel == LogLevel.Information).ToList();
        Assert.Equal(3, infoLogs.Count);
    }

    private void SetupDefaultContextResolver()
    {
        _contextResolver.Setup(x => x.NotifyOnArchiveComplete()).Returns(true);
        _contextResolver.Setup(x => x.NotifyOnArchiveCompleteErrors()).Returns(true);
        _contextResolver.Setup(x => x.NotifyOnRestoreComplete()).Returns(true);
        _contextResolver.Setup(x => x.NotifyOnRestoreCompleteErrors()).Returns(true);
        _contextResolver.Setup(x => x.NotifyOnException()).Returns(true);
    }

    private static async IAsyncEnumerable<SnsMessage> CreateAsyncEnumerable(IEnumerable<SnsMessage> messages)
    {
        foreach (var message in messages)
        {
            yield return message;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<SnsMessage> CreateLongRunningAsyncEnumerable(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var count = 0;
        while (!cancellationToken.IsCancellationRequested && count < 100)
        {
            var archiveRun = CreateTestRun($"run-{count}");
            yield return new ArchiveCompleteMessage($"Archive {count}", $"Message {count}", archiveRun);
            count++;
            await Task.Delay(50, cancellationToken);
        }
    }

    private static ArchiveRun CreateTestRun(string id, ArchiveRunStatus status = ArchiveRunStatus.Completed)
    {
        return new ArchiveRun
        {
            RunId = id,
            CreatedAt = DateTimeOffset.UtcNow,
            Status = status,
            CronSchedule = "",
            PathsToArchive = ""
        };
    }

    private static RestoreRun CreateRestoreRun(string id, RestoreRunStatus status = RestoreRunStatus.Completed)
    {
        return new RestoreRun
        {
            RestoreId = id,
            RequestedAt = DateTimeOffset.UtcNow,
            Status = status,
            ArchiveRunId = "",
            RestorePaths = ""
        };
    }

    // Helper class for testing unknown message types
    private record UnknownSnsMessage(string Subject, string Message) : SnsMessage(Subject, Message);
}