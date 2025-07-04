using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class ArchiveServiceTests
{
    private readonly CurrentArchiveRunRequests _currentReqs = new();
    private readonly CurrentArchiveRuns _currentRuns = new();
    private readonly Mock<ILogger<ArchiveService>> _loggerMock = new();
    private readonly Mock<IArchiveRunMediator> _runMed = new();
    private readonly Mock<IS3Service> _s3Service = new();
    private readonly Mock<ISnsOrchestrationMediator> _snsMed = new();

    private ArchiveService CreateSut()
    {
        return new ArchiveService(
            _s3Service.Object,
            _runMed.Object,
            _snsMed.Object,
            _currentRuns,
            _currentReqs,
            _loggerMock.Object
        );
    }

    [Fact]
    public async Task LookupArchiveRun_CachesAndPersistsRequests()
    {
        var sut = CreateSut();
        var runId = "r1";
        var run = new ArchiveRun
        {
            RunId = runId, PathsToArchive = "/p", CronSchedule = "* * * * *", Status = ArchiveRunStatus.Processing,
            CreatedAt = DateTimeOffset.Now
        };

        _s3Service.Setup(s => s.RunExists(runId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        _s3Service.Setup(s => s.GetArchive(runId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(run);

        // first lookup: hits S3
        var got1 = await sut.LookupArchiveRun(runId, CancellationToken.None);
        Assert.Equal(run, got1);

        // should have saved requests
        _runMed.Verify(m => m.SaveCurrentArchiveRunRequests(_currentReqs, It.IsAny<CancellationToken>()), Times.Once);

        // second lookup: uses cache, no new S3 calls
        _s3Service.Invocations.Clear();
        var got2 = await sut.LookupArchiveRun(runId, CancellationToken.None);
        Assert.Equal(run, got2);
        _s3Service.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task StartNewArchiveRun_InitializesStateAndTcs()
    {
        var sut = CreateSut();
        var req = new RunRequest("r2", "/p2", "0 0 * * *");

        var run = await sut.StartNewArchiveRun(req, CancellationToken.None);
        Assert.Equal("r2", run.RunId);
        Assert.Equal(ArchiveRunStatus.Processing, run.Status);

        // Should have persisted both requests and the new run
        _runMed.Verify(m => m.SaveCurrentArchiveRunRequests(_currentReqs, It.IsAny<CancellationToken>()), Times.Once);
        _runMed.Verify(m => m.SaveArchiveRun(run, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task DoesFileRequireProcessing_NewAndProcessedBehaviour()
    {
        var sut = CreateSut();
        var req = new RunRequest("r3", "/p", "* * * * *");
        await sut.StartNewArchiveRun(req, CancellationToken.None);

        // New file → true
        var need1 = await sut.DoesFileRequireProcessing("r3", "f.txt", CancellationToken.None);
        Assert.True(need1);
        Assert.Contains("f.txt", (await sut.LookupArchiveRun("r3", CancellationToken.None))!.Files.Keys);
        _runMed.Verify(m => m.SaveArchiveRun(It.IsAny<ArchiveRun>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);

        // Mark file processed
        var run = await sut.LookupArchiveRun("r3", CancellationToken.None);
        run!.Files["f.txt"].Status = FileStatus.Processed;

        // Now it should return false
        var need2 = await sut.DoesFileRequireProcessing("r3", "f.txt", CancellationToken.None);
        Assert.False(need2);
    }

    [Fact]
    public async Task ReportProcessingResult_FinalizesRunAndPublishes()
    {
        var sut = CreateSut();
        var req = new RunRequest("r4", "/p", "* * * * *");
        await sut.StartNewArchiveRun(req, CancellationToken.None);

        // Add two files
        await sut.DoesFileRequireProcessing("r4", "a", CancellationToken.None);
        await sut.DoesFileRequireProcessing("r4", "b", CancellationToken.None);

        // Process first file
        await sut.ReportProcessingResult("r4", new FileProcessResult("a", 10, new byte[] { }, new DataChunkDetails[0]),
            CancellationToken.None);
        // Should not yet finalize
        _snsMed.VerifyNoOtherCalls();

        // Process second file → this is last
        await sut.ReportProcessingResult("r4", new FileProcessResult("b", 20, new byte[] { }, new DataChunkDetails[0]),
            CancellationToken.None);

        // Now run is completed and SNS message sent
        _snsMed.Verify(s =>
            s.PublishMessage(It.IsAny<ArchiveCompleteMessage>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RecordFailedFile_SkipsAndFinalizesIfLast()
    {
        var sut = CreateSut();
        var req = new RunRequest("r5", "/p", "* * * * *");
        await sut.StartNewArchiveRun(req, CancellationToken.None);
        ArchiveRun? savedRun = null;
        _runMed
            .Setup(m => m.SaveArchiveRun(It.IsAny<ArchiveRun>(), It.IsAny<CancellationToken>()))
            .Callback<ArchiveRun, CancellationToken>((m, _) =>
            {
                // Ensure we save the run after adding files
                savedRun = m;
            });
        _s3Service
            .Setup(s => s.RunExists("r5", It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => savedRun is not null);

        _s3Service
            .Setup(s => s.GetArchive("r5", It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => savedRun!);

        // Add one file
        await sut.DoesFileRequireProcessing("r5", "x.txt", CancellationToken.None);

        // Fail that file
        var ex = new Exception("whoops");
        await sut.RecordFailedFile("r5", "x.txt", ex, CancellationToken.None);

        // Status should be Skipped
        var run = await sut.LookupArchiveRun("r5", CancellationToken.None);
        Assert.Equal(FileStatus.Skipped, run!.Files["x.txt"].Status);

        // Should publish skip‐exception and final error message
        _snsMed.Verify(
            s => s.PublishMessage(It.Is<ExceptionMessage>(m => m.Subject.Contains("Skipped")),
                It.IsAny<CancellationToken>()), Times.Once);
        _snsMed.Verify(
            s => s.PublishMessage(It.Is<ArchiveCompleteErrorMessage>(m => m.RunId == "r5"),
                It.IsAny<CancellationToken>()), Times.Once);
    }
}