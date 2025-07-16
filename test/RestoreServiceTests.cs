using Amazon.S3;
using aws_backup_common;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class RestoreServiceTests
{
    private readonly DataChunkManifest _chunkMan = new();
    private readonly CurrentRestoreRequests _currentReqs = new();
    private readonly Mock<IDownloadFileMediator> _downloadMed = new();
    private readonly Mock<ILogger<RestoreService>> _loggerMock = new();
    private readonly Mock<IRestoreManifestMediator> _manifestMed = new();
    private readonly Mock<IRestoreRequestsMediator> _requestsMed = new();
    private readonly S3RestoreChunkManifest _restoreMan = new();
    private readonly Mock<IRestoreRunMediator> _runMed = new();
    private readonly Mock<IS3Service> _s3Service = new();
    private readonly Mock<ISnsMessageMediator> _snsMed = new();

    private RestoreService CreateSut()
    {
        return new RestoreService(
            _downloadMed.Object,
            _runMed.Object,
            _manifestMed.Object,
            _requestsMed.Object,
            _snsMed.Object,
            _s3Service.Object,
            _restoreMan,
            _currentReqs,
            _chunkMan,
            _loggerMock.Object
        );
    }

    private void ResetManifests()
    {
        _restoreMan.Clear();
        _chunkMan.Clear();
        _currentReqs.Clear();
    }

    [Fact]
    public async Task LookupRestoreRun_CachesRemoteRun()
    {
        ResetManifests();
        var sut = CreateSut();
        var id = "runX";
        var fake = new RestoreRun
        {
            RestoreId = id, RestorePaths = "/p", ArchiveRunId = "ar", Status = RestoreRunStatus.Processing,
            RequestedAt = DateTimeOffset.Now
        };

        _s3Service.Setup(s => s.RestoreExists(id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        _s3Service.Setup(s => s.GetRestoreRun(id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(fake);

        // first call hits S3
        var r1 = await sut.LookupRestoreRun(id, CancellationToken.None);
        Assert.Same(fake, r1);

        // remove setups, second call is cached
        _s3Service.Reset();
        var r2 = await sut.LookupRestoreRun(id, CancellationToken.None);
        Assert.Same(fake, r2);
    }

    [Fact]
    public async Task InitiateRestoreRun_SchedulesAllChunksAndPersists()
    {
        ResetManifests();
        var sut = CreateSut();

        // create a run with two chunks
        var run = new RestoreRun
        {
            RestoreId = "R1",
            RestorePaths = "/p",
            ArchiveRunId = "A1",
            Status = RestoreRunStatus.Processing,
            RequestedAt = DateTimeOffset.Now
        };
        var chunk1 = new ByteArrayKey(new byte[] { 1, 2 });
        var chunk2 = new ByteArrayKey(new byte[] { 3, 4 });
        run.RequestedFiles["f1"] = new RestoreFileMetaData(new[] { chunk1 }, "f1", 100);
        run.RequestedFiles["f2"] = new RestoreFileMetaData(new[] { chunk2 }, "f2", 200);

        // populate chunk manifest
        _chunkMan[chunk1] = new CloudChunkDetails("k1", "b", chunk1.ToArray().Length, chunk1.ToArray());
        _chunkMan[chunk2] = new CloudChunkDetails("k2", "b", chunk2.ToArray().Length, chunk2.ToArray());

        // make S3Service return initial statuses
        _s3Service.Setup(s => s.ScheduleDeepArchiveRecovery("k1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(S3ChunkRestoreStatus.ReadyToRestore);
        _s3Service.Setup(s => s.ScheduleDeepArchiveRecovery("k2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(S3ChunkRestoreStatus.PendingDeepArchiveRestore);

        await sut.InitiateRestoreRun(
            new RestoreRequest("A1", "/p", DateTimeOffset.UtcNow),
            run,
            CancellationToken.None
        );

        // manifest should contain both statuses
        Assert.Equal(S3ChunkRestoreStatus.ReadyToRestore, _restoreMan[chunk1]);
        Assert.Equal(S3ChunkRestoreStatus.PendingDeepArchiveRestore, _restoreMan[chunk2]);

        // SaveRestoreManifest and SaveRestoreRun must be called
        _manifestMed.Verify(m => m.SaveRestoreManifest(_restoreMan, It.IsAny<CancellationToken>()), Times.Once);
        _runMed.Verify(m => m.SaveRestoreRun(run, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ReportS3Storage_PendingToReady_EnqueuesDownloadAndPersists()
    {
        ResetManifests();
        var sut = CreateSut();

        // prepare chunk manifest + initial restoreManifest
        var hash = new byte[] { 9 };
        var key = new ByteArrayKey(hash);
        var s3Key = Base64Url.Encode(hash);
        _chunkMan[key] = new CloudChunkDetails(s3Key, "b", hash.Length, hash);
        _restoreMan[key] = S3ChunkRestoreStatus.PendingDeepArchiveRestore;

        // seed run
        var run = new RestoreRun
        {
            ArchiveRunId = "",
            RestoreId = "R2", RestorePaths = "/p", Status = RestoreRunStatus.Processing,
            RequestedAt = DateTimeOffset.Now
        };
        var fileMeta = new RestoreFileMetaData(new[] { key }, "f", 1);
        run.RequestedFiles["f"] = fileMeta;
        // put into private dictionary via public Lookup
        _s3Service.Setup(s => s.RestoreExists("R2", It.IsAny<CancellationToken>())).ReturnsAsync(true);
        _s3Service.Setup(s => s.GetRestoreRun("R2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(run);
        await sut.LookupRestoreRun("R2", CancellationToken.None);

        // now simulate object moving to STANDARD
        await sut.ReportS3Storage("b", s3Key, S3StorageClass.Standard, CancellationToken.None);

        // manifest flipped
        Assert.Equal(S3ChunkRestoreStatus.ReadyToRestore, _restoreMan[key]);
        // mediator called to download
        _downloadMed.Verify(d => d.DownloadFileFromS3(
            It.Is<DownloadFileFromS3Request>(r => r.RestoreId == "R2" && r.FilePath == "f"),
            It.IsAny<CancellationToken>()), Times.Once);
        // fileMeta status updated
        Assert.Equal(FileRestoreStatus.PendingS3Download, fileMeta.Status);
        // run persisted
        _runMed.Verify(r => r.SaveRestoreRun(run, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ReportS3Storage_ReadyToPending_SchedulesRecoveryAndPersists()
    {
        ResetManifests();
        var sut = CreateSut();

        // prepare chunk manifest + initial restoreManifest
        var hash = new byte[] { 8 };
        var key = new ByteArrayKey(hash);
        var s3Key = Base64Url.Encode(hash);
        _chunkMan[key] = new CloudChunkDetails(s3Key, "bx", hash.Length, hash);
        _restoreMan[key] = S3ChunkRestoreStatus.ReadyToRestore;

        // seed run
        var run = new RestoreRun
        {
            RestoreId = "R3", RestorePaths = "/p", Status = RestoreRunStatus.Processing,
            RequestedAt = DateTimeOffset.Now, ArchiveRunId = "ar",
            RequestedFiles =
            {
                [s3Key] = new RestoreFileMetaData([key], s3Key, 1)
            }
        };
        _s3Service.Setup(s => s.RestoreExists("R3", It.IsAny<CancellationToken>())).ReturnsAsync(true);
        _s3Service.Setup(s => s.GetRestoreRun("R3", It.IsAny<CancellationToken>()))
            .ReturnsAsync(run);
        await sut.LookupRestoreRun("R3", CancellationToken.None);

        // stub ScheduleDeepArchiveRecovery
        _s3Service.Setup(s => s.ScheduleDeepArchiveRecovery(s3Key, It.IsAny<CancellationToken>()))
            .ReturnsAsync(S3ChunkRestoreStatus.PendingDeepArchiveRestore);

        await sut.ReportS3Storage("bx", s3Key, S3StorageClass.DeepArchive, CancellationToken.None);

        // manifest flipped
        Assert.Equal(S3ChunkRestoreStatus.PendingDeepArchiveRestore, _restoreMan[key]);
        // recovery scheduled
        _s3Service.Verify(s => s.ScheduleDeepArchiveRecovery(s3Key, It.IsAny<CancellationToken>()), Times.Once);
        // run persisted
        _runMed.Verify(r => r.SaveRestoreRun(run, It.IsAny<CancellationToken>()), Times.Once);
        // fileMeta status updated
        Assert.Equal(FileRestoreStatus.PendingDeepArchiveRestore, run.RequestedFiles[s3Key].Status);
    }

    [Fact]
    public async Task ReportDownloadComplete_FinishesRun_PublishesSuccess()
    {
        ResetManifests();
        var sut = CreateSut();

        // seed run with 2 files
        var run = new RestoreRun
        {
            RestoreId = "R4", RestorePaths = "/p", Status = RestoreRunStatus.Processing, ArchiveRunId = "",
            RequestedAt = DateTimeOffset.Now
        };
        run.RequestedFiles["a"] = new RestoreFileMetaData(Array.Empty<ByteArrayKey>(), "a", 0)
            { Status = FileRestoreStatus.PendingS3Download };
        run.RequestedFiles["b"] = new RestoreFileMetaData(Array.Empty<ByteArrayKey>(), "b", 0)
            { Status = FileRestoreStatus.PendingS3Download };
        _s3Service.Setup(s => s.RestoreExists("R4", It.IsAny<CancellationToken>())).ReturnsAsync(true);
        _s3Service.Setup(s => s.GetRestoreRun("R4", It.IsAny<CancellationToken>())).ReturnsAsync(run);
        await sut.LookupRestoreRun("R4", CancellationToken.None);

        // complete first
        await sut.ReportDownloadComplete(
            new DownloadFileFromS3Request("R4", "a", Array.Empty<CloudChunkDetails>(), 0),
            CancellationToken.None);
        // should not finalize yet
        _snsMed.VerifyNoOtherCalls();

        // complete second
        await sut.ReportDownloadComplete(
            new DownloadFileFromS3Request("R4", "b", Array.Empty<CloudChunkDetails>(), 0),
            CancellationToken.None);
        // run complete, persisted & SNS
        Assert.Equal(RestoreRunStatus.Completed, run.Status);
        _runMed.Verify(r => r.SaveRestoreRun(run, It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        _snsMed.Verify(s => s.PublishMessage(It.IsAny<RestoreCompleteMessage>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ReportDownloadFailed_PublishesErrorAndFinalizes()
    {
        ResetManifests();
        var sut = CreateSut();

        // seed run with 1 file
        var run = new RestoreRun
        {
            RestoreId = "R5", RestorePaths = "/p", Status = RestoreRunStatus.Processing, ArchiveRunId = "",
            RequestedAt = DateTimeOffset.Now
        };
        run.RequestedFiles["x"] = new RestoreFileMetaData(Array.Empty<ByteArrayKey>(), "x", 0)
            { Status = FileRestoreStatus.PendingS3Download };
        _s3Service.Setup(s => s.RestoreExists("R5", It.IsAny<CancellationToken>())).ReturnsAsync(true);
        _s3Service.Setup(s => s.GetRestoreRun("R5", It.IsAny<CancellationToken>())).ReturnsAsync(run);
        await sut.LookupRestoreRun("R5", CancellationToken.None);

        // fail the only file
        var ex = new InvalidOperationException("boom");
        await sut.ReportDownloadFailed(
            new DownloadFileFromS3Request("R5", "x", Array.Empty<CloudChunkDetails>(), 0),
            ex,
            CancellationToken.None);

        // file marked Failed
        Assert.Equal(FileRestoreStatus.Failed, run.RequestedFiles["x"].Status);
        // single SNS for download‐failure
        _snsMed.Verify(
            s => s.PublishMessage(It.Is<SnsMessage>(m => m.Subject.Contains("failed")), It.IsAny<CancellationToken>()),
            Times.Once);
        // final SNS for run complete error
        _snsMed.Verify(
            s => s.PublishMessage(It.Is<RestoreCompleteErrorMessage>(m => m.RestoreId == "R5"),
                It.IsAny<CancellationToken>()), Times.Once);
        // run persisted twice (once on fail, once on finalize)
        _runMed.Verify(r => r.SaveRestoreRun(run, It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task ReportS3Storage_Cancellation_RespectsToken()
    {
        ResetManifests();
        var sut = CreateSut();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // should just return, no exceptions
        await sut.ReportS3Storage("any", "any", S3StorageClass.Standard, cts.Token);
    }
}