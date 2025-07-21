using aws_backup_common;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class ArchiveServiceTests
{
    private readonly Mock<IArchiveDataStore> _dataStore = new();
    private readonly Mock<ILogger<ArchiveService>> _loggerMock = new();
    private readonly Mock<ISnsMessageMediator> _snsMed = new();

    private ArchiveService CreateSut()
    {
        return new ArchiveService(
            _snsMed.Object,
            _loggerMock.Object,
            _dataStore.Object);
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

        _dataStore.Setup(s => s.GetArchiveRun(runId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(run);
        // first lookup: hits S3
        var got1 = await sut.LookupArchiveRun(runId, CancellationToken.None);
        Assert.Equal(run, got1);

        // should have saved requests
        _dataStore.Verify(m => m.SaveArchiveRun(run, It.IsAny<CancellationToken>()), Times.Once);

        // second lookup: uses cache, no new S3 calls
        _dataStore.Invocations.Clear();
        var got2 = await sut.LookupArchiveRun(runId, CancellationToken.None);
        Assert.Equal(run, got2);
        _dataStore.VerifyNoOtherCalls();
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
        _dataStore.Verify(m => m.SaveArchiveRun(run, It.IsAny<CancellationToken>()), Times.Once);
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
        _dataStore.Verify(m => m.SaveArchiveRun(It.IsAny<ArchiveRun>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);

        // Mark file processed
        var run = await sut.LookupArchiveRun("r3", CancellationToken.None);
        run!.Files["f.txt"].Status = FileStatus.UploadComplete;

        // Now it should return false
        var need2 = await sut.DoesFileRequireProcessing("r3", "f.txt", CancellationToken.None);
        Assert.False(need2);
    }

    [Fact]
    public async Task ReportProcessingResult_FinalizesRunAndPublishes()
    {
        var runId = "r4";
        var run = new ArchiveRun
        {
            RunId = runId, PathsToArchive = "/p", CronSchedule = "* * * * *", Status = ArchiveRunStatus.Processing,
            CreatedAt = DateTimeOffset.Now
        };

        _dataStore.Setup(s => s.GetArchiveRun(runId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(run);

        var sut = CreateSut();
        var req = new RunRequest(runId, "/p", "* * * * *");
        await sut.StartNewArchiveRun(req, CancellationToken.None);

        // Add two files
        await sut.DoesFileRequireProcessing(runId, "a", CancellationToken.None);
        await sut.DoesFileRequireProcessing(runId, "b", CancellationToken.None);

        var d1 = new DataChunkDetails("a", 0, 10, [1, 2, 3], 10);
        var d2 = new DataChunkDetails("a", 1, 10, [4, 5, 6], 10);
        var d3 = new DataChunkDetails("b", 0, 10, [7, 8, 9], 10);
        var d4 = new DataChunkDetails("b", 1, 10, [10, 11, 12], 10);

        // Process first file
        await sut.ReportProcessingResult(runId, new FileProcessResult("a", 10, [1, 2, 3], 1),
            CancellationToken.None);
        // Process second file → this is last
        await sut.ReportProcessingResult(runId, new FileProcessResult("b", 20, [4, 5, 6], 1),
            CancellationToken.None);

        // Should not yet finalize
        _snsMed.VerifyNoOtherCalls();

        //add the chunks from processor
        await sut.AddChunkToFile("r4", "a", d1, CancellationToken.None);
        await sut.AddChunkToFile("r4", "a", d2, CancellationToken.None);
        await sut.AddChunkToFile("r4", "b", d3, CancellationToken.None);
        await sut.AddChunkToFile("r4", "b", d4, CancellationToken.None);

        // Should not yet finalize
        _snsMed.VerifyNoOtherCalls();

        await sut.RecordChunkUpload("r4", "a", d1.HashKey, CancellationToken.None);
        await sut.RecordChunkUpload("r4", "b", d3.HashKey, CancellationToken.None);

        // Should not yet finalize
        _snsMed.VerifyNoOtherCalls();
        await sut.RecordChunkUpload("r4", "a", d2.HashKey, CancellationToken.None);
        await sut.RecordChunkUpload("r4", "b", d4.HashKey, CancellationToken.None);

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
        _dataStore
            .Setup(m => m.SaveArchiveRun(It.IsAny<ArchiveRun>(), It.IsAny<CancellationToken>()))
            .Callback<ArchiveRun, CancellationToken>((m, _) =>
            {
                // Ensure we save the run after adding files
                savedRun = m;
            });

        _dataStore
            .Setup(s => s.GetArchiveRun("r5", It.IsAny<CancellationToken>()))
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