using System.Collections.Concurrent;
using System.Reflection;
using System.Threading.Channels;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class RestoreRunOrchestrationTests
{
    private readonly Mock<IArchiveService> _archiveService = new();
    private readonly Mock<IContextResolver> _ctx = new();
    private readonly TestLoggerClass<RestoreRunOrchestration> _logger = new();
    private readonly Mock<IRestoreRequestsMediator> _mediator = new();
    private readonly Mock<IRestoreService> _restoreService = new();

    private RestoreRunOrchestration CreateOrchestration(Channel<RestoreRequest> chan)
    {
        _mediator.Setup(m => m.GetRestoreRequests(It.IsAny<CancellationToken>()))
            .Returns(chan.Reader.ReadAllAsync());
        _ctx.Setup(c => c.RestoreId(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTimeOffset>()))
            .Returns((string runId, string paths, DateTimeOffset _) => $"rid-{runId}-{paths}");
        return new RestoreRunOrchestration(
            _mediator.Object,
            _archiveService.Object,
            _restoreService.Object,
            _logger,
            _ctx.Object,
            Mock.Of<ISnsOrchestrationMediator>(),
            Mock.Of<CurrentRestoreRequests>());
    }

    private MethodInfo GetExecute()
    {
        return typeof(RestoreRunOrchestration)
            .GetMethod("ExecuteAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
    }

    [Fact]
    public async Task InvalidRequest_LoggedAndSkipped()
    {
        var chan = Channel.CreateUnbounded<RestoreRequest>();
        chan.Writer.TryWrite(new RestoreRequest(
            "",
            "",
            TimeProvider.System.GetUtcNow()));
        chan.Writer.Complete();

        var orch = CreateOrchestration(chan);
        var exec = GetExecute();

        await (Task)exec.Invoke(orch, new object[] { CancellationToken.None });

        _archiveService.Verify(a => a.LookupArchiveRun(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _restoreService.Verify(
            r => r.InitiateRestoreRun(It.IsAny<RestoreRequest>(), It.IsAny<RestoreRun>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
        var logMatches = _logger.LogRecords
            .Where(l => l.LogLevel == LogLevel.Warning && l.Message.Contains("Received invalid restore request"));
        Assert.Single(logMatches);
    }

    [Fact]
    public async Task ExistingRestoreRun_Skipped()
    {
        // Arrange
        var req = new RestoreRequest(
            "ar1",
            "/p",
            DateTimeOffset.UtcNow
        );

        var chan = Channel.CreateUnbounded<RestoreRequest>();
        chan.Writer.TryWrite(req);
        chan.Writer.Complete();

        // Stub the mediator to return our single request
        _mediator
            .Setup(m => m.GetRestoreRequests(It.IsAny<CancellationToken>()))
            .Returns(chan.Reader.ReadAllAsync());

        // Stub the context to generate a known restoreId
        var expectedRestoreId = $"rid-{req.ArchiveRunId}-{req.RestorePaths}";
        _ctx
            .Setup(c => c.RestoreId(req.ArchiveRunId, req.RestorePaths, req.RequestedAt))
            .Returns(expectedRestoreId);

        // Simulate that a RestoreRun already exists => should skip
        _restoreService
            .Setup(r => r.LookupRestoreRun(expectedRestoreId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RestoreRun
            {
                RestoreId = expectedRestoreId,
                ArchiveRunId = req.ArchiveRunId,
                RestorePaths = req.RestorePaths,
                RequestedAt = req.RequestedAt,
                Status = RestoreRunStatus.Processing,
                RequestedFiles = new ConcurrentDictionary<string, RestoreFileMetaData>()
            });

        var orch = CreateOrchestration(chan);
        var exec = GetExecute();

        // Act
        await (Task)exec.Invoke(orch, new object[] { CancellationToken.None });

        // Assert

        // 1) We should have checked LookupRestoreRun exactly once with our generated key
        _restoreService.Verify(r =>
            r.LookupRestoreRun(expectedRestoreId, It.IsAny<CancellationToken>()), Times.Once);

        // 2) Since we skipped, we never even called into the archive service
        _archiveService.Verify(a =>
            a.LookupArchiveRun(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);

        // 3) And we never initiated a new restore
        _restoreService.Verify(r =>
            r.InitiateRestoreRun(It.IsAny<RestoreRequest>(), It.IsAny<RestoreRun>(),
                It.IsAny<CancellationToken>()), Times.Never);
    }


    [Fact]
    public async Task MissingArchiveRun_LogsWarningAndSkips()
    {
        var req = new RestoreRequest(
            "ar2",
            "/p",
            TimeProvider.System.GetUtcNow());
        var chan = Channel.CreateUnbounded<RestoreRequest>();
        chan.Writer.TryWrite(req);
        chan.Writer.Complete();

        _restoreService.Setup(r => r.LookupRestoreRun(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((RestoreRun)null);
        _archiveService.Setup(a => a.LookupArchiveRun(req.ArchiveRunId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((ArchiveRun)null);

        var orch = CreateOrchestration(chan);
        var exec = GetExecute();
        await (Task)exec.Invoke(orch, new object[] { CancellationToken.None });

        var loggerMatches = _logger.LogRecords
            .Where(l => l.LogLevel == LogLevel.Warning && l.Message.Contains("No archive run found"));
        Assert.Single(loggerMatches);
        _restoreService.Verify(
            r => r.InitiateRestoreRun(It.IsAny<RestoreRequest>(), It.IsAny<RestoreRun>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ValidRequest_InitiatesRestoreRunWithCorrectFiles()
    {
        // Arrange the incoming RestoreRequest
        var req = new RestoreRequest
        (
            "ar3",
            "/a/**:/b/**", // Match all files under /a and /b
            DateTimeOffset.UtcNow
        );
        var chan = Channel.CreateUnbounded<RestoreRequest>();
        chan.Writer.TryWrite(req);
        chan.Writer.Complete();

        // No existing RestoreRun
        _restoreService
            .Setup(r => r.LookupRestoreRun(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((RestoreRun)null);

        // Build an ArchiveRun with a ConcurrentDictionary<string,FileMetaData>
        var arc = new ArchiveRun
        {
            PathsToArchive = "/some/path",
            RunId = "ar3",
            CronSchedule = "* * * * *",
            Status = ArchiveRunStatus.Processing,
            CreatedAt = TimeProvider.System.GetUtcNow()
        };

        // Helper to add a file with one chunk
        void AddFile(string path)
        {
            var hashKey = new byte[] { 1, 2, 3, 4 };
            var chunk = new DataChunkDetails(
                path + ".chunk0",
                0,
                1234,
                hashKey,
                1234
            );

            var meta = new FileMetaData(path)
            {
                OriginalSize = 9999,
                LastModified = DateTimeOffset.UtcNow.AddDays(-1),
                Created = DateTimeOffset.UtcNow.AddDays(-2),
                Owner = "ownerX",
                Group = "groupX",
                AclEntries = new[] { new AclEntry("id", "rwx", "POSIX") },
                HashKey = hashKey,
                Status = FileStatus.Added,
                CompressedSize = 5678,
                Chunks = new[] { chunk }
            };

            // Note: ArchiveRun.Files is a ConcurrentDictionary
            arc.Files[path] = meta;
        }

        AddFile("/a/x.txt");
        AddFile("/b/y.txt");
        AddFile("/c/z.txt"); // should be excluded by the matcher

        _archiveService
            .Setup(a => a.LookupArchiveRun(req.ArchiveRunId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(arc);

        RestoreRun captured = null!;
        _restoreService
            .Setup(r => r.InitiateRestoreRun(
                It.IsAny<RestoreRequest>(),
                It.IsAny<RestoreRun>(),
                It.IsAny<CancellationToken>()))
            .Callback<RestoreRequest, RestoreRun, CancellationToken>((_, rr, _) => captured = rr)
            .Returns(Task.CompletedTask);

        var orch = CreateOrchestration(chan);
        var exec = GetExecute();
        await (Task)exec.Invoke(orch, new object[] { CancellationToken.None });

        // Assert the RestoreRun passed to InitiateRestoreRun is correct
        Assert.Equal($"rid-{req.ArchiveRunId}-{req.RestorePaths}", captured.RestoreId);
        Assert.Equal(req.RestorePaths, captured.RestorePaths);
        Assert.Equal(req.ArchiveRunId, captured.ArchiveRunId);
        Assert.Equal(req.RequestedAt, captured.RequestedAt);

        // Only the files matching /a and /b
        Assert.Contains("/a/x.txt", captured.RequestedFiles.Keys);
        Assert.Contains("/b/y.txt", captured.RequestedFiles.Keys);
        Assert.DoesNotContain("/c/z.txt", captured.RequestedFiles.Keys);

        // Verify one of the entries has exactly the metadata we set
        var rf = captured.RequestedFiles["/a/x.txt"];
        Assert.Equal("/a/x.txt", rf.FilePath);
        Assert.Equal(9999, rf.Size);
        Assert.Equal(FileRestoreStatus.PendingDeepArchiveRestore, rf.Status);
        Assert.Equal(1, rf.Chunks.Length);
        Assert.True(rf.Chunks[0].ToArray().SequenceEqual(new byte[] { 1, 2, 3, 4 }));
        Assert.Equal(rf.LastModified, arc.Files["/a/x.txt"].LastModified);
        Assert.Equal(rf.Created, arc.Files["/a/x.txt"].Created);
        Assert.Equal(rf.Owner, arc.Files["/a/x.txt"].Owner);
        Assert.Equal(rf.Group, arc.Files["/a/x.txt"].Group);
        Assert.Equal(rf.AclEntries.Length, arc.Files["/a/x.txt"].AclEntries.Length);
    }


    [Fact]
    public async Task ExceptionInHandler_LogsErrorAndContinues()
    {
        var req = new RestoreRequest("ar4", "/p", DateTimeOffset.UtcNow);
        var chan = Channel.CreateUnbounded<RestoreRequest>();
        chan.Writer.TryWrite(req);
        chan.Writer.Complete();

        _restoreService.Setup(r => r.LookupRestoreRun(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("boom"));
        var orch = CreateOrchestration(chan);
        var exec = GetExecute();
        await (Task)exec.Invoke(orch, new object[] { CancellationToken.None });

        var loggerMatches = _logger.LogRecords
            .Where(l => l.LogLevel == LogLevel.Error && l.Message.Contains("Error processing restore request"));
        Assert.Single(loggerMatches);
    }
}