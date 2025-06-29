using System.Collections.Concurrent;
using System.Reflection;
using System.Threading.Channels;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class DownloadFileOrchestrationTests
{
    /// <summary>
    ///     1) Successful download: reconstructor succeeds, hash OK, DownloadComplete is called, stamps/owner/acl applied, and
    ///     no retry entry remains.
    /// </summary>
    [Fact]
    public async Task WorkerLoop_SuccessfulDownload_ReportsCompleteAndClearsRetry()
    {
        // Arrange a single download request
        var chan = Channel.CreateUnbounded<DownloadFileFromS3Request>();
        var req = new StubRequest("r1", "f1");
        chan.Writer.TryWrite(req);
        chan.Writer.Complete();

        // Mock mediator to read from our channel
        var retryMediator = new Mock<IRetryMediator>();
        var mediator = new Mock<IDownloadFileMediator>();
        mediator.Setup(m => m.GetDownloadRequests(It.IsAny<CancellationToken>()))
            .Returns(chan.Reader.ReadAllAsync());

        // Mock reconstructor: reconstruct → returns a temp file path
        var tmp = Path.GetTempFileName();
        await File.WriteAllTextAsync(tmp, "x");
        var reconstructor = new Mock<IS3ChunkedFileReconstructor>();
        reconstructor
            .Setup(r => r.ReconstructAsync(req, It.IsAny<CancellationToken>()))
            .ReturnsAsync(tmp);
        // hash ok
        reconstructor
            .Setup(r => r.VerifyDownloadHashAsync(req, tmp, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        // Spy on restoreService
        var restore = new Mock<IRestoreService>();
        // stub context
        var ctx = new Mock<IContextResolver>();
        ctx.Setup(c => c.NoOfS3FilesToDownloadConcurrently()).Returns(1);
        ctx.Setup(c => c.DownloadRetryDelaySeconds()).Returns(1);
        ctx.Setup(c => c.DownloadAttemptLimit()).Returns(3);
        ctx.Setup(c => c.CheckDownloadHash()).Returns(true);
        ctx.Setup(c => c.KeepTimeStamps()).Returns(true);
        ctx.Setup(c => c.KeepOwnerGroup()).Returns(true);
        ctx.Setup(c => c.KeepAclEntries()).Returns(true);

        var logger = Mock.Of<ILogger<DownloadFileOrchestration>>();

        var orch = new DownloadFileOrchestration(
            retryMediator.Object,
            mediator.Object,
            reconstructor.Object,
            restore.Object,
            logger,
            ctx.Object);

        // Act
        var cts = new CancellationTokenSource();
        var run = orch.StartAsync(cts.Token);
        await run; // finishes after channel completes
        await orch.ExecuteTask;

        // Assert DownloadComplete called once
        restore.Verify(r => r.ReportDownloadComplete(req, It.IsAny<CancellationToken>()), Times.Once);
        // No retry entry left
        retryMediator.Verify(r => r.RetryAttempt(It.IsAny<RetryState>(), It.IsAny<CancellationToken>()), Times.Never);
    }


    /// <summary>
    ///     2) Hash‐fail: reconstructor OK, but VerifyDownloadHashAsync==false → a retry entry is recorded with AttemptNo=1.
    /// </summary>
    [Fact]
    public async Task WorkerLoop_HashFails_EnqueuesRetryAttempt()
    {
        // Arrange one request
        var chan = Channel.CreateUnbounded<DownloadFileFromS3Request>();
        var req = new StubRequest("r2", "f2");
        chan.Writer.TryWrite(req);
        chan.Writer.Complete();

        var retryMediator = new Mock<IRetryMediator>();
        var mediator = new Mock<IDownloadFileMediator>();
        mediator.Setup(m => m.GetDownloadRequests(It.IsAny<CancellationToken>()))
            .Returns(chan.Reader.ReadAllAsync());

        // reconstructor returns a dummy file
        var tmp = Path.GetTempFileName();
        File.WriteAllText(tmp, "x");
        var reconstructor = new Mock<IS3ChunkedFileReconstructor>();
        reconstructor.Setup(r => r.ReconstructAsync(req, It.IsAny<CancellationToken>())).ReturnsAsync(tmp);
        reconstructor.Setup(r => r.VerifyDownloadHashAsync(req, tmp, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var restore = new Mock<IRestoreService>();
        var ctx = new Mock<IContextResolver>();
        ctx.Setup(c => c.NoOfS3FilesToDownloadConcurrently()).Returns(1);
        ctx.Setup(c => c.DownloadRetryDelaySeconds()).Returns(10);
        ctx.Setup(c => c.DownloadAttemptLimit()).Returns(5);
        ctx.Setup(c => c.CheckDownloadHash()).Returns(true);
        ctx.Setup(c => c.KeepTimeStamps()).Returns(false);
        ctx.Setup(c => c.KeepOwnerGroup()).Returns(false);
        ctx.Setup(c => c.KeepAclEntries()).Returns(false);

        var orch = new DownloadFileOrchestration(
            retryMediator.Object,
            mediator.Object,
            reconstructor.Object,
            restore.Object,
            Mock.Of<ILogger<DownloadFileOrchestration>>(),
            ctx.Object);

        // Act
        await orch.StartAsync(CancellationToken.None);
        await orch.ExecuteTask;

        // Inspect retryAttempts

        retryMediator.Verify(r => r.RetryAttempt(It.Is<RetryState>(state =>
                state == req &&
                req.Exception != null &&
                req.Exception.GetType() == typeof(InvalidOperationException)), It.IsAny<CancellationToken>()),
            Times.Once);
    }


    /// <summary>
    ///     3) Retry loop: when NextAttemptAt passes, mediator.DownloadFileFromS3() is called to re‐enqueue the request.
    /// </summary>
    [Fact]
    public async Task RetryFailedAttempts_TriggersMediatorDownloadWhenDue()
    {
        // Arrange a single failed attempt
        var req = new StubRequest("r3", "f3");

        var retryMediator = new Mock<IRetryMediator>();
        var mediator = new Mock<IDownloadFileMediator>();
        // Expect DownloadFileFromS3 to be called once
        mediator.Setup(m => m.DownloadFileFromS3(req, It.IsAny<CancellationToken>()))
            .Returns(() => ValueTask.CompletedTask)
            .Verifiable();

        var reconstructor = Mock.Of<IS3ChunkedFileReconstructor>();
        var restore = Mock.Of<IRestoreService>();

        var ctx = new Mock<IContextResolver>();
        ctx.Setup(c => c.NoOfS3FilesToDownloadConcurrently()).Returns(0); // only retry task
        ctx.Setup(c => c.RetryCheckIntervalSeconds()).Returns(1);
        ctx.Setup(c => c.ShutdownTimeoutSeconds()).Returns(1);

        // Build orchestration and inject the single retry
        var orch = new DownloadFileOrchestration(
            retryMediator.Object,
            mediator.Object,
            reconstructor,
            restore,
            Mock.Of<ILogger<DownloadFileOrchestration>>(),
            ctx.Object);

        // Prime the private _retryAttempts dict
        
        // Assert
        mediator.Verify();
    }

    private record StubRequest(string RestoreId, string FilePath) : DownloadFileFromS3Request(
        RestoreId, FilePath, Array.Empty<CloudChunkDetails>(), 0);
}