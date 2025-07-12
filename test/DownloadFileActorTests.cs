using System.Reflection;
using System.Threading.Channels;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class DownloadFileActorTests
{
    private readonly Mock<IContextResolver> _ctx = new();
    private readonly Mock<ILogger<DownloadFileActor>> _logger = new();
    private readonly Mock<IDownloadFileMediator> _mediator = new();
    private readonly Mock<IS3ChunkedFileReconstructor> _reconstructor = new();
    private readonly Mock<IRestoreService> _restoreService = new();
    private readonly Mock<IRetryMediator> _retryMediator = new();

    private DownloadFileActor CreateOrch(Channel<DownloadFileFromS3Request> chan)
    {
        _mediator.Setup(m => m.GetDownloadRequests(It.IsAny<CancellationToken>()))
            .Returns(chan.Reader.ReadAllAsync());
        _ctx.Setup(c => c.NoOfS3FilesToDownloadConcurrently()).Returns(1);
        _ctx.Setup(c => c.DownloadAttemptLimit()).Returns(2);
        _ctx.Setup(c => c.CheckDownloadHash()).Returns(true);
        _ctx.Setup(c => c.KeepTimeStamps()).Returns(false);
        _ctx.Setup(c => c.KeepOwnerGroup()).Returns(false);
        _ctx.Setup(c => c.KeepAclEntries()).Returns(false);

        return new DownloadFileActor(
            _retryMediator.Object,
            _mediator.Object,
            _reconstructor.Object,
            _restoreService.Object,
            _logger.Object,
            _ctx.Object);
    }

    private MethodInfo GetWorker()
    {
        return typeof(DownloadFileActor)
            .GetMethod("WorkerLoopAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
    }

    [Fact]
    public async Task DefaultDelegates_AreInitialized()
    {
        // Arrange a channel with one stub request lacking delegates
        var chan = Channel.CreateUnbounded<DownloadFileFromS3Request>();
        var req = new DownloadFileFromS3Request("r", "f", Array.Empty<CloudChunkDetails>(), 0);
        chan.Writer.TryWrite(req);
        chan.Writer.Complete();

        var orch = CreateOrch(chan);
        var worker = GetWorker();

        // Stub reconstructor to skip hash check
        _reconstructor.Setup(r => r.ReconstructAsync(req, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReconstructResult("/tmp", null));
        _reconstructor.Setup(r => r.VerifyDownloadHashAsync(req, "/tmp", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        // Act
        await (Task)worker.Invoke(orch, new object[] { CancellationToken.None });

        // Assert default Retry and LimitExceeded
        Assert.NotNull(req.Retry);
        Assert.NotNull(req.LimitExceeded);

        // Invoking Retry should call mediator.DownloadFileFromS3
        _mediator.Setup(m => m.DownloadFileFromS3(req, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask).Verifiable();
        await req.Retry(req, CancellationToken.None);
        _mediator.Verify();

        // Invoking LimitExceeded should call restoreService.ReportDownloadFailed
        _restoreService.Setup(r => r.ReportDownloadFailed(req, It.IsAny<Exception>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask).Verifiable();
        await req.LimitExceeded(req, CancellationToken.None);
        _restoreService.Verify();
    }

    [Fact]
    public async Task SuccessfulDownload_CallsReportComplete()
    {
        var chan = Channel.CreateUnbounded<DownloadFileFromS3Request>();
        var req = new DownloadFileFromS3Request("r", "f", Array.Empty<CloudChunkDetails>(), 0);
        chan.Writer.TryWrite(req);
        chan.Writer.Complete();

        var orch = CreateOrch(chan);
        var worker = GetWorker();

        _reconstructor.Setup(r => r.ReconstructAsync(req, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReconstructResult("file1", null));
        _reconstructor.Setup(r => r.VerifyDownloadHashAsync(req, "file1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        // Act
        await (Task)worker.Invoke(orch, new object[] { CancellationToken.None });

        // Assert
        _restoreService.Verify(r => r.ReportDownloadComplete(req, It.IsAny<CancellationToken>()), Times.Once);
        _retryMediator.Verify(r => r.RetryAttempt(It.IsAny<DownloadFileFromS3Request>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task HashFailure_TriggersRetryAttemptAndSetsException()
    {
        var chan = Channel.CreateUnbounded<DownloadFileFromS3Request>();
        var req = new DownloadFileFromS3Request("r", "f", Array.Empty<CloudChunkDetails>(), 0);
        chan.Writer.TryWrite(req);
        chan.Writer.Complete();

        var orch = CreateOrch(chan);
        var worker = GetWorker();

        _reconstructor.Setup(r => r.ReconstructAsync(req, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReconstructResult("file2", null));
        _reconstructor.Setup(r => r.VerifyDownloadHashAsync(req, "file2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _retryMediator.Setup(r => r.RetryAttempt(req, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask).Verifiable();

        // Act
        await (Task)worker.Invoke(orch, new object[] { CancellationToken.None });

        // Assert
        _retryMediator.Verify();
        Assert.IsType<InvalidOperationException>(req.Exception);
        _restoreService.Verify(
            r => r.ReportDownloadComplete(It.IsAny<DownloadFileFromS3Request>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ExceptionDuringReconstruct_TriggersRetryAttemptAndSetsException()
    {
        var chan = Channel.CreateUnbounded<DownloadFileFromS3Request>();
        var req = new DownloadFileFromS3Request("r", "f", [], 0);
        chan.Writer.TryWrite(req);
        chan.Writer.Complete();

        var orch = CreateOrch(chan);
        var worker = GetWorker();

        _reconstructor.Setup(r => r.ReconstructAsync(req, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("fail"));

        _retryMediator.Setup(r => r.RetryAttempt(req, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask).Verifiable();

        // Act
        await (Task)worker.Invoke(orch, new object[] { CancellationToken.None });

        // Assert
        _retryMediator.Verify();
        Assert.IsType<IOException>(req.Exception);
    }
}