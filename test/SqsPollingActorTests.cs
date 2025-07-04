using System.Security.Cryptography;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class SqsPollingActorTests
{
    private readonly byte[] _aesKey = new byte[32]; // dummy key for AES
    private readonly Mock<IContextResolver> _ctx = new();
    private readonly Mock<IAwsClientFactory> _factory = new();
    private readonly TestLoggerClass<SqsPollingActor> _logger = new();
    private readonly Mock<IRestoreRequestsMediator> _mediator = new();
    private readonly Mock<IAmazonSQS> _sqs = new();

    private SqsPollingActor CreateOrch()
    {
        RandomNumberGenerator.Fill(_aesKey);
        _factory.Setup(f => f.CreateSqsClient(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_sqs.Object);

        // common context stubs
        _ctx.Setup(c => c.SqsQueueUrl()).Returns("https://queue");
        _ctx.Setup(c => c.SqsWaitTimeSeconds()).Returns(1);
        _ctx.Setup(c => c.SqsMaxNumberOfMessages()).Returns(1);
        _ctx.Setup(c => c.SqsVisibilityTimeout()).Returns(10);
        _ctx.Setup(c => c.SqsRetryDelaySeconds()).Returns(0);
        _ctx.Setup(c => c.EncryptSqs()).Returns(false);
        _ctx.Setup(c => c.SqsEncryptionKey(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_aesKey); // no encryption key

        return new SqsPollingActor(
            _factory.Object,
            _logger,
            _mediator.Object,
            _ctx.Object,
            Mock.Of<ISnsMessageMediator>()
        );
    }

    [Fact]
    public async Task ValidRestoreBackupMessage_ProcessesAndDeletes()
    {
        var orch = CreateOrch();
        var cts = new CancellationTokenSource();

        // Prepare JSON body
        var body = JsonSerializer.Serialize(new
        {
            command = "restore-backup",
            body = new RestoreRequest("runX", "/p", DateTimeOffset.UtcNow)
        });

        // Setup SQS Receive: first a message, then cancel
        var seq = _sqs.SetupSequence(s => s.ReceiveMessageAsync(
            It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()));
        seq.ReturnsAsync(new ReceiveMessageResponse
        {
            Messages = [new Message { MessageId = "m1", Body = body, ReceiptHandle = "rh1" }]
        });
        seq.ThrowsAsync(new OperationCanceledException());

        // Spy DeleteMessageAsync
        _sqs.Setup(s => s.DeleteMessageAsync("https://queue", "rh1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteMessageResponse());

        // Act
        await orch.StartAsync(cts.Token);
        await (orch.ExecuteTask ?? Task.CompletedTask);

        // Assert mediator called
        _mediator.Verify(m => m.RestoreBackup(
            It.Is<RestoreRequest>(r => r.ArchiveRunId == "runX" && r.RestorePaths == "/p"),
            It.IsAny<CancellationToken>()), Times.Once);

        // Assert delete
        _sqs.Verify(s => s.DeleteMessageAsync("https://queue", "rh1", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task EmptyBody_SkipsProcessing()
    {
        var orch = CreateOrch();
        var cts = new CancellationTokenSource();

        var seq = _sqs.SetupSequence(s => s.ReceiveMessageAsync(
            It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()));
        seq.ReturnsAsync(new ReceiveMessageResponse
        {
            Messages = [new Message { MessageId = "m2", Body = "   ", ReceiptHandle = "rh2" }]
        });
        seq.ThrowsAsync(new OperationCanceledException());

        await orch.StartAsync(cts.Token);
        await (orch.ExecuteTask ?? Task.CompletedTask);

        _mediator.Verify(m => m.RestoreBackup(It.IsAny<RestoreRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        _sqs.Verify(s => s.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task InvalidJson_SkipsProcessing()
    {
        var orch = CreateOrch();
        var cts = new CancellationTokenSource();

        var seq = _sqs.SetupSequence(s => s.ReceiveMessageAsync(
            It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()));
        seq.ReturnsAsync(new ReceiveMessageResponse
        {
            Messages = [new Message { MessageId = "m3", Body = "not-json", ReceiptHandle = "rh3" }]
        });
        seq.ThrowsAsync(new OperationCanceledException());

        await orch.StartAsync(cts.Token);
        await (orch.ExecuteTask ?? Task.CompletedTask);

        _mediator.Verify(m => m.RestoreBackup(It.IsAny<RestoreRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        _sqs.Verify(s => s.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task MissingCommand_SkipsProcessing()
    {
        var orch = CreateOrch();
        var cts = new CancellationTokenSource();

        var payload = JsonSerializer.Serialize(new { foo = "bar" });
        var seq = _sqs.SetupSequence(s => s.ReceiveMessageAsync(
            It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()));
        seq.ReturnsAsync(new ReceiveMessageResponse
        {
            Messages = [new Message { MessageId = "m4", Body = payload, ReceiptHandle = "rh4" }]
        });
        seq.ThrowsAsync(new OperationCanceledException());

        await orch.StartAsync(cts.Token);
        await (orch.ExecuteTask ?? Task.CompletedTask);

        _mediator.Verify(m => m.RestoreBackup(It.IsAny<RestoreRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        _sqs.Verify(s => s.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task EncryptionEnabled_DecryptsBeforeProcessing()
    {
        // Arrange encryption
        var orch = CreateOrch();
        _ctx.Setup(c => c.EncryptSqs()).Returns(true);
        // Fake AES decrypt: just reverse the string
        var raw = JsonSerializer.Serialize(new
        {
            command = "restore-backup",
            body = new RestoreRequest("r5", "/p5", DateTimeOffset.UtcNow)
        });
        var enc = AesHelper.EncryptString(raw, _aesKey);

        var cts = new CancellationTokenSource();
        var seq = _sqs.SetupSequence(s => s.ReceiveMessageAsync(
            It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()));
        seq.ReturnsAsync(new ReceiveMessageResponse
        {
            Messages = [new Message { MessageId = "m5", Body = enc, ReceiptHandle = "rh5" }]
        });
        seq.ThrowsAsync(new OperationCanceledException());

        _sqs.Setup(s => s.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteMessageResponse());

        // Act
        await orch.StartAsync(cts.Token);
        await (orch.ExecuteTask ?? Task.CompletedTask);

        _mediator.Verify(m => m.RestoreBackup(
            It.Is<RestoreRequest>(r => r.ArchiveRunId == "r5" && r.RestorePaths == "/p5"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ReceiveException_LogsAndRetries()
    {
        var orch = CreateOrch();
        var cts = new CancellationTokenSource();

        // First call throws generic
        var seq = _sqs.SetupSequence(s => s.ReceiveMessageAsync(
            It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()));
        seq.ThrowsAsync(new Exception("fail"));
        // Then break
        seq.ThrowsAsync(new OperationCanceledException());

        // Spy on logger
        _logger.LogRecords.Clear();

        await orch.StartAsync(cts.Token);
        await (orch.ExecuteTask ?? Task.CompletedTask);

        var logMessages = _logger.LogRecords.Where(r =>
            r.LogLevel == LogLevel.Error &&
            r.Message.Contains("retrying")).ToList();

        // One LogError invocation for receive exception
        Assert.Single(logMessages);
    }
}