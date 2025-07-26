using System.Collections.Concurrent;
using System.Reflection;
using Amazon.S3;
using aws_backup_common;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class RestoreServiceTests
{
    private readonly Mock<ICloudChunkStorage> _cloudChunkStorage = new();
    private readonly Mock<IDownloadFileMediator> _downloadMediator = new();
    private readonly Mock<ILogger<RestoreService>> _logger = new();
    private readonly Mock<IRestoreDataStore> _restoreDataStore = new();
    private readonly Mock<IS3Service> _s3Service = new();
    private readonly Mock<ISnsMessageMediator> _snsMediator = new();
    private readonly Mock<IS3StorageClassMediator> _storageMediator = new();

    private readonly CancellationToken _token = CancellationToken.None;

    private RestoreService CreateService()
    {
        return new RestoreService(
            _downloadMediator.Object,
            _storageMediator.Object,
            _s3Service.Object,
            _snsMediator.Object,
            _restoreDataStore.Object,
            _cloudChunkStorage.Object,
            _logger.Object);
    }

    [Fact]
    public async Task ReportS3Storage_ChunkMarkedReadyAndTriggersDownload()
    {
        var service = CreateService();
        var s3Key = "key-001";
        var byteKey = new ByteArrayKey([1, 2, 3]);
        var chunk = new RestoreChunkDetails(s3Key, "bucket", 100, 0, 100, [1, 2, 3], 0);
        var fileMeta = new RestoreFileMetaData("file.txt")
        {
            Status = FileRestoreStatus.PendingDeepArchiveRestore,
            CloudChunkDetails = new ConcurrentDictionary<ByteArrayKey, RestoreChunkDetails>(
                [new KeyValuePair<ByteArrayKey, RestoreChunkDetails>(byteKey, chunk)])
        };

        var run = new RestoreRun
        {
            Status = RestoreRunStatus.Processing,
            RestoreId = "restore-1",
            ArchiveRunId = "arch",
            RestorePaths = "/tmp",
            RequestedAt = DateTimeOffset.UtcNow,
            RequestedFiles = new ConcurrentDictionary<string, RestoreFileMetaData>(
                [new KeyValuePair<string, RestoreFileMetaData>("file.txt", fileMeta)])
        };

        await service.StartNewRestoreRun(new RestoreRequest("arch", "/tmp", DateTimeOffset.UtcNow), run.RestoreId,
            _token);

        // Manually insert into cache
        var restoreRunsCache = typeof(RestoreService)
            .GetField("_restoreRunsCache", BindingFlags.NonPublic | BindingFlags.Instance)
            ?.GetValue(service) as ConcurrentDictionary<string, RestoreRun>;

        restoreRunsCache?.TryAdd(run.RestoreId, run);

        // Manually simulate chunk registration in _s3KeysToChunks
        var chunkCache = typeof(RestoreService)
            .GetField("_s3KeysToChunks", BindingFlags.NonPublic | BindingFlags.Instance)
            ?.GetValue(service) as ConcurrentDictionary<string, ByteArrayKey[]>;

        chunkCache![s3Key] = [byteKey];

        await service.ReportS3Storage(s3Key, S3StorageClass.Standard, _token);

        Assert.Equal(FileRestoreStatus.PendingS3Download, fileMeta.Status);
        Assert.Equal(S3ChunkRestoreStatus.ReadyToRestore, chunk.Status);

        _downloadMediator.Verify(x => x.DownloadFileFromS3(
            It.Is<DownloadFileFromS3Request>(r => r.FilePath == "file.txt"),
            _token), Times.Once);
    }

    [Fact]
    public async Task ReportDownloadComplete_MarksFileCompleted_AndPersistsStatus()
    {
        var service = CreateService();

        var fileMeta = new RestoreFileMetaData("file.txt")
        {
            Status = FileRestoreStatus.PendingS3Download
        };

        var run = new RestoreRun
        {
            Status = RestoreRunStatus.Processing,
            RestoreId = "restore-1",
            ArchiveRunId = "arch",
            RestorePaths = "/tmp",
            RequestedAt = DateTimeOffset.UtcNow,
            RequestedFiles = new ConcurrentDictionary<string, RestoreFileMetaData>
            {
                ["file.txt"] = fileMeta
            }
        };

        var req = new DownloadFileFromS3Request("restore-1", "file.txt", [], 0);

        // manually inject run into cache
        var cache = typeof(RestoreService)
            .GetField("_restoreRunsCache", BindingFlags.NonPublic | BindingFlags.Instance)
            ?.GetValue(service) as ConcurrentDictionary<string, RestoreRun>;

        cache![run.RestoreId] = run;

        await service.ReportDownloadComplete(req, _token);

        Assert.Equal(FileRestoreStatus.Completed, fileMeta.Status);

        _restoreDataStore.Verify(x =>
            x.SaveRestoreFileStatus("restore-1", "file.txt", FileRestoreStatus.Completed, "", _token), Times.Once);
    }

    [Fact]
    public async Task ConcurrentClearCache_ShouldNotThrowAndCorrectlyRemoveEntry()
    {
        var service = CreateService();
        var restoreId = "restore-race";
        var request = new RestoreRequest("arch", "/tmp", DateTimeOffset.UtcNow);

        await service.StartNewRestoreRun(request, restoreId, CancellationToken.None);

        var tasks = Enumerable.Range(0, 10).Select(_ =>
            Task.Run(() => service.ClearCache(restoreId, CancellationToken.None), _token));

        await Task.WhenAll(tasks);

        var result = await service.LookupRestoreRun(restoreId, CancellationToken.None);
        Assert.Null(result);
    }

    [Fact]
    public async Task SaveAndFinalizeIfComplete_ShouldBeThreadSafe()
    {
        var service = CreateService();
        var restoreId = "restore-finalize";

        var fileMeta = new RestoreFileMetaData("file.txt")
        {
            Status = FileRestoreStatus.Completed
        };

        var run = new RestoreRun
        {
            Status = RestoreRunStatus.AllFilesListed,
            RestoreId = restoreId,
            ArchiveRunId = "arch",
            RestorePaths = "/tmp",
            RequestedAt = DateTimeOffset.UtcNow,
            RequestedFiles = new ConcurrentDictionary<string, RestoreFileMetaData>
            {
                ["file.txt"] = fileMeta
            }
        };

        if (typeof(RestoreService)
                .GetField("_restoreRunsCache", BindingFlags.NonPublic | BindingFlags.Instance)
                ?.GetValue(service) is ConcurrentDictionary<string, RestoreRun> cache) cache[restoreId] = run;

        var tasks = Enumerable.Range(0, 5).Select(_ =>
            
            Task.Run(() =>
                service.ReportDownloadComplete(
                    new DownloadFileFromS3Request(restoreId, "file.txt", [], 0),
                    CancellationToken.None), _token));

        await Task.WhenAll(tasks);

        _restoreDataStore.Verify(x =>
            x.SaveRestoreRun(It.Is<RestoreRun>(r => r.RestoreId == restoreId && r.Status == RestoreRunStatus.Completed),
                It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ReportDownloadFailed_MarksFileFailed_AndPublishesError()
    {
        var service = CreateService();
        var exception = new Exception("Disk full");

        var fileMeta = new RestoreFileMetaData("file.txt")
        {
            Status = FileRestoreStatus.PendingS3Download
        };

        var run = new RestoreRun
        {
            Status = RestoreRunStatus.Processing,
            RestoreId = "restore-2",
            ArchiveRunId = "arch",
            RestorePaths = "/tmp",
            RequestedAt = DateTimeOffset.UtcNow,
            RequestedFiles = new ConcurrentDictionary<string, RestoreFileMetaData>
            {
                ["file.txt"] = fileMeta
            }
        };

        var req = new DownloadFileFromS3Request("restore-2", "file.txt", [], 0);

        // manually inject run into cache

        if (typeof(RestoreService)
                .GetField("_restoreRunsCache", BindingFlags.NonPublic | BindingFlags.Instance)
                ?.GetValue(service) is ConcurrentDictionary<string, RestoreRun> cache) cache[run.RestoreId] = run;

        await service.ReportDownloadFailed(req, exception, _token);

        Assert.Equal(FileRestoreStatus.Failed, fileMeta.Status);
        Assert.Equal("Disk full", fileMeta.FailedMessage);

        _snsMediator.Verify(x =>
            x.PublishMessage(It.Is<SnsMessage>(m => m.Message.Contains("Download failed")), _token), Times.Once);

        _restoreDataStore.Verify(x =>
                x.SaveRestoreFileStatus("restore-2", "file.txt", FileRestoreStatus.Failed, "Disk full", _token),
            Times.Once);
    }

    [Fact]
    public async Task ScheduleFileRecovery_ShouldSkip_WhenFileAlreadyCompleted()
    {
        var service = CreateService();
        var run = new RestoreRun
        {
            Status = RestoreRunStatus.Processing,
            RestoreId = "run-1",
            ArchiveRunId = "arch",
            RestorePaths = "/tmp",
            RequestedAt = DateTimeOffset.UtcNow,
            RequestedFiles = new ConcurrentDictionary<string, RestoreFileMetaData>()
        };

        var request = new RestoreRequest("arch", "/tmp", DateTimeOffset.UtcNow);

        var meta = new FileMetaData("file-1")
        {
            Status = FileStatus.UploadComplete,
            Chunks = new ConcurrentDictionary<ByteArrayKey, DataChunkDetails>()
        };

        var completed = new RestoreFileMetaData("file-1")
        {
            Status = FileRestoreStatus.Completed
        };
        run.RequestedFiles["file-1"] = completed;

        await service.ScheduleFileRecovery(run, request, meta, _token);

        // Should not call mediator or change existing completed file
        _downloadMediator.Verify(
            x => x.DownloadFileFromS3(It.IsAny<DownloadFileFromS3Request>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task LookupRestoreRun_ReturnsFromCache()
    {
        var service = CreateService();
        await service.StartNewRestoreRun(new RestoreRequest("arch", "/some/path", DateTimeOffset.UtcNow), "abc",
            CancellationToken.None);

        var result = await service.LookupRestoreRun("abc", CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal("abc", result.RestoreId);
    }

    [Fact]
    public async Task LookupRestoreRun_ReturnsFromStore_WhenNotCached()
    {
        var service = CreateService();
        var run = new RestoreRun
        {
            RestoreId = "run-001",
            ArchiveRunId = "arch",
            RestorePaths = "/some/path",
            RequestedAt = DateTimeOffset.UtcNow,
            RequestedFiles = new ConcurrentDictionary<string, RestoreFileMetaData>(),
            Status = RestoreRunStatus.Processing
        };

        _restoreDataStore
            .Setup(x => x.LookupRestoreRun("run-001", It.IsAny<CancellationToken>()))
            .ReturnsAsync(run);

        var result = await service.LookupRestoreRun("run-001", CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal("run-001", result.RestoreId);
        _restoreDataStore.Verify(x => x.LookupRestoreRun("run-001", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task LookupRestoreRun_ReturnsNull_WhenMissingFromStore()
    {
        var service = CreateService();
        _restoreDataStore.Setup(x => x.LookupRestoreRun("not-found", It.IsAny<CancellationToken>()))
            .ReturnsAsync((RestoreRun?)null);

        var result = await service.LookupRestoreRun("not-found", CancellationToken.None);

        Assert.Null(result);
    }

    [Fact]
    public async Task StartNewRestoreRun_CachesRun_AndPersistsToStore()
    {
        var service = CreateService();
        var request = new RestoreRequest(
            "arch-001",
            "/tmp/test",
            DateTimeOffset.UtcNow
        );

        var result = await service.StartNewRestoreRun(request, "restore-123", CancellationToken.None);

        Assert.Equal("restore-123", result.RestoreId);
        _restoreDataStore.Verify(x => x.SaveRestoreRequest(request, It.IsAny<CancellationToken>()), Times.Once);
        _restoreDataStore.Verify(
            x => x.SaveRestoreRun(It.Is<RestoreRun>(r => r.RestoreId == "restore-123"), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ClearCache_RemovesRunAndCallsDataStore()
    {
        var service = CreateService();
        await service.StartNewRestoreRun(new RestoreRequest("arch", "/some/path", DateTimeOffset.UtcNow), "abc",
            CancellationToken.None);

        await service.ClearCache("abc", CancellationToken.None);

        var result = await service.LookupRestoreRun("abc", CancellationToken.None);
        Assert.Null(result);
        _restoreDataStore.Verify(x => x.RemoveRestoreRequest("abc", It.IsAny<CancellationToken>()), Times.Once);
    }
}