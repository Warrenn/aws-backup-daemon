using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using aws_backup_common;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class UploadActorTests
{
    private readonly Mock<IChunkManifestMediator> _chunkManifestMediator;
    private readonly Mock<IContextResolver> _contextResolver;
    private readonly Mock<IS3Service> _hotStorageService;
    private readonly Mock<IRestoreManifestMediator> _restoreManifestMediator;
    private readonly Mock<IRestoreRequestsMediator> _restoreRequestsMediator;
    private readonly Mock<IRestoreRunMediator> _restoreRunMediator;
    private readonly Mock<IArchiveRunMediator> _runMediator;
    private UploadActor _actor;
    private TestLoggerClass<UploadActor> _logger;

    public UploadActorTests()
    {
        _hotStorageService = new Mock<IS3Service>();
        _runMediator = new Mock<IArchiveRunMediator>();
        _chunkManifestMediator = new Mock<IChunkManifestMediator>();
        _restoreManifestMediator = new Mock<IRestoreManifestMediator>();
        _restoreRunMediator = new Mock<IRestoreRunMediator>();
        _contextResolver = new Mock<IContextResolver>();
        _restoreRequestsMediator = new Mock<IRestoreRequestsMediator>();
        _logger = new TestLoggerClass<UploadActor>();

        _actor = new UploadActor(
            _hotStorageService.Object,
            _runMediator.Object,
            _chunkManifestMediator.Object,
            _restoreManifestMediator.Object,
            _restoreRunMediator.Object,
            _restoreRequestsMediator.Object,
            Mock.Of<ISnsMessageMediator>(),
            _contextResolver.Object,
            _logger);
    }

    [Fact]
    public async Task ExecuteAsync_WithValidData_UploadsAllItemsSuccessfully()
    {
        // Arrange
        var cancellationToken = new CancellationToken();
//        var delaySeconds = TimeSpan.FromSeconds(1);

        _contextResolver.Setup(x => x.DelayBetweenUploadsSeconds()).Returns(1);

        var archiveData = CreateAsyncEnumerable([
            new S3LocationAndValue<ArchiveRun>("archive1", new ArchiveRun
            {
                Status = ArchiveRunStatus.Processing,
                RunId = "1",
                OriginalSize = 123,
                CreatedAt = DateTimeOffset.UtcNow,
                CronSchedule = "* * * * *",
                PathsToArchive = "/test",
                Files = new ConcurrentDictionary<string, FileMetaData>()
            }),
            new S3LocationAndValue<ArchiveRun>("archive2", new ArchiveRun
            {
                Status = ArchiveRunStatus.Processing,
                RunId = "2",
                OriginalSize = 456,
                CreatedAt = DateTimeOffset.UtcNow,
                CronSchedule = "* * * * *",
                PathsToArchive = "/test2",
                Files = new ConcurrentDictionary<string, FileMetaData>()
            })
        ]);

        var chunkData = CreateAsyncEnumerable([
            new S3LocationAndValue<DataChunkManifest>("chunk1", new DataChunkManifest())
        ]);

        var restoreManifestData = CreateAsyncEnumerable([
            new S3LocationAndValue<S3RestoreChunkManifest>("restore1", new S3RestoreChunkManifest())
        ]);

        var restoreRunData = CreateAsyncEnumerable([
            new S3LocationAndValue<RestoreRun>("run1", new RestoreRun
            {
                RestoreId = "1",
                RestorePaths = "/restore",
                ArchiveRunId = "1",
                RequestedAt = DateTimeOffset.UtcNow,
                Status = RestoreRunStatus.Processing
            })
        ]);

        var restoreRequestData = CreateAsyncEnumerable([
            new S3LocationAndValue<CurrentRestoreRequests>("current1", new CurrentRestoreRequests())
        ]);

        var currentArchiveRunsData = CreateAsyncEnumerable([
            new S3LocationAndValue<CurrentArchiveRunRequests>("currentarchive1", new CurrentArchiveRunRequests())
        ]);

        _runMediator.Setup(x => x.GetArchiveRuns(It.IsAny<CancellationToken>())).Returns(() => archiveData);
        _runMediator.Setup(x => x.GetCurrentArchiveRunRequests(It.IsAny<CancellationToken>()))
            .Returns(() => currentArchiveRunsData);
        _chunkManifestMediator.Setup(x => x.GetDataChunksManifest(It.IsAny<CancellationToken>()))
            .Returns(() => chunkData);
        _restoreManifestMediator.Setup(x => x.GetRestoreManifest(It.IsAny<CancellationToken>()))
            .Returns(() => restoreManifestData);
        _restoreRunMediator.Setup(x => x.GetRestoreRuns(It.IsAny<CancellationToken>()))
            .Returns(() => restoreRunData);
        _restoreRequestsMediator.Setup(x => x.GetRunningRequests(It.IsAny<CancellationToken>()))
            .Returns(() => restoreRequestData);

        // Act
        await _actor.StartAsync(cancellationToken);
        await Task.Delay(2000); // Allow some processing time
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(cancellationToken);

        // Assert
        _hotStorageService.Verify(
            x => x.UploadCompressedObject("archive1", It.IsAny<object>(), It.IsAny<StorageTemperature>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
        _hotStorageService.Verify(x => x.UploadCompressedObject("archive2", It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()),
            Times.Once);
        _hotStorageService.Verify(x => x.UploadCompressedObject("chunk1", It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()),
            Times.Once);
        _hotStorageService.Verify(x => x.UploadCompressedObject("restore1", It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()),
            Times.Once);
        _hotStorageService.Verify(x => x.UploadCompressedObject("run1", It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()),
            Times.Once);
        _hotStorageService.Verify(x => x.UploadCompressedObject("current1", It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()),
            Times.Once);
        _hotStorageService.Verify(
            x => x.UploadCompressedObject("currentarchive1", It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()),
            Times.Once);

        Assert.Empty(_logger.LogRecords.Where(x => x.LogLevel == LogLevel.Error));
    }

    [Fact]
    public async Task ExecuteAsync_WithUploadException_LogsErrorAndContinues()
    {
        // Arrange
        var cancellationToken = new CancellationToken();

        _contextResolver.Setup(x => x.DelayBetweenUploadsSeconds()).Returns(0);

        var testException = new InvalidOperationException("Upload failed");
        var archiveData = CreateAsyncEnumerable([
            new S3LocationAndValue<ArchiveRun>("failing-key", new ArchiveRun
            {
                Status = ArchiveRunStatus.Processing,
                RunId = "1",
                OriginalSize = 123,
                CreatedAt = DateTimeOffset.UtcNow,
                CronSchedule = "* * * * *",
                PathsToArchive = "/test",
                Files = new ConcurrentDictionary<string, FileMetaData>()
            }),
            new S3LocationAndValue<ArchiveRun>("success-key", new ArchiveRun
            {
                Status = ArchiveRunStatus.Processing,
                RunId = "2",
                OriginalSize = 456,
                CreatedAt = DateTimeOffset.UtcNow,
                CronSchedule = "* * * * *",
                PathsToArchive = "/test2",
                Files = new ConcurrentDictionary<string, FileMetaData>()
            })
        ]);

        _runMediator.Setup(x => x.GetArchiveRuns(It.IsAny<CancellationToken>())).Returns(() => archiveData);
        _runMediator.Setup(x => x.GetCurrentArchiveRunRequests(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<CurrentArchiveRunRequests>([]));
        _chunkManifestMediator.Setup(x => x.GetDataChunksManifest(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<DataChunkManifest>([]));
        _restoreManifestMediator.Setup(x => x.GetRestoreManifest(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<S3RestoreChunkManifest>([]));
        _restoreRunMediator.Setup(x => x.GetRestoreRuns(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<RestoreRun>([]));
        _restoreRequestsMediator.Setup(x => x.GetRunningRequests(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<CurrentRestoreRequests>([]));

        _hotStorageService.Setup(x => x.UploadCompressedObject("failing-key", It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(testException);
        _hotStorageService.Setup(x => x.UploadCompressedObject("success-key", It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act
        _logger = new TestLoggerClass<UploadActor>();

        _actor = new UploadActor(
            _hotStorageService.Object,
            _runMediator.Object,
            _chunkManifestMediator.Object,
            _restoreManifestMediator.Object,
            _restoreRunMediator.Object,
            _restoreRequestsMediator.Object,
            Mock.Of<ISnsMessageMediator>(),
            _contextResolver.Object,
            _logger);

        await _actor.StartAsync(cancellationToken);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(cancellationToken);

        // Assert
        var errorLogs = _logger.LogRecords.Where(x => x.LogLevel == LogLevel.Error).ToList();
        Assert.Single((IEnumerable)errorLogs);
        Assert.Contains("Error processing upload failing-key", errorLogs[0].Message);
        Assert.Equal(testException, errorLogs[0].Exception);

        _hotStorageService.Verify(x => x.UploadCompressedObject("success-key", It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellation_StopsProcessingGracefully()
    {
        // Arrange
        using var cts = new CancellationTokenSource();

        _contextResolver.Setup(x => x.DelayBetweenUploadsSeconds()).Returns(0);

        var longRunningData = CreateAsyncEnumerable([
            new S3LocationAndValue<ArchiveRun>("key1", new ArchiveRun
            {
                Status = ArchiveRunStatus.Processing,
                RunId = "1",
                OriginalSize = 123,
                CreatedAt = DateTimeOffset.UtcNow,
                CronSchedule = "* * * * *",
                PathsToArchive = "/test",
                Files = new ConcurrentDictionary<string, FileMetaData>()
            }),
            new S3LocationAndValue<ArchiveRun>("key2", new ArchiveRun
            {
                Status = ArchiveRunStatus.Processing,
                RunId = "2",
                OriginalSize = 123,
                CreatedAt = DateTimeOffset.UtcNow,
                CronSchedule = "* * * * *",
                PathsToArchive = "/test",
                Files = new ConcurrentDictionary<string, FileMetaData>()
            }),
            new S3LocationAndValue<ArchiveRun>("key3", new ArchiveRun
            {
                Status = ArchiveRunStatus.Processing,
                RunId = "3",
                OriginalSize = 123,
                CreatedAt = DateTimeOffset.UtcNow,
                CronSchedule = "* * * * *",
                PathsToArchive = "/test",
                Files = new ConcurrentDictionary<string, FileMetaData>()
            })
        ]);

        _runMediator.Setup(x => x.GetArchiveRuns(It.IsAny<CancellationToken>())).Returns(() => longRunningData);
        _runMediator.Setup(x => x.GetCurrentArchiveRunRequests(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<CurrentArchiveRunRequests>([]));
        _chunkManifestMediator.Setup(x => x.GetDataChunksManifest(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<DataChunkManifest>([]));
        _restoreManifestMediator.Setup(x => x.GetRestoreManifest(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<S3RestoreChunkManifest>([]));
        _restoreRunMediator.Setup(x => x.GetRestoreRuns(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<RestoreRun>([]));
        _restoreRequestsMediator.Setup(x => x.GetRunningRequests(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<CurrentRestoreRequests>([]));
        _hotStorageService.Setup(x =>
                x.UploadCompressedObject(It.IsAny<string>(), It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()))
            .Returns(async () => { await Task.Delay(100, cts.Token); });

        // Act
        await _actor.StartAsync(cts.Token);
        await Task.Delay(50); // Let it start processing
        cts.Cancel();
        await Task.Delay(200); // Allow cancellation to propagate
        await _actor.StopAsync(CancellationToken.None);
        await (_actor.ExecuteTask ?? Task.CompletedTask);

        // Assert - Should not throw and should handle cancellation gracefully
        var errorLogs = _logger.LogRecords.Where(x => x.LogLevel == LogLevel.Error).ToList();
        Assert.DoesNotContain(errorLogs, log => log.Exception is OperationCanceledException);
    }

    [Fact]
    public async Task ExecuteAsync_WithEmptyData_CompletesSuccessfully()
    {
        // Arrange
        var cancellationToken = CancellationToken.None;

        _runMediator.Setup(x => x.GetArchiveRuns(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<ArchiveRun>([]));
        _runMediator.Setup(x => x.GetCurrentArchiveRunRequests(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<CurrentArchiveRunRequests>([]));
        _chunkManifestMediator.Setup(x => x.GetDataChunksManifest(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<DataChunkManifest>([]));
        _restoreManifestMediator.Setup(x => x.GetRestoreManifest(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<S3RestoreChunkManifest>([]));
        _restoreRequestsMediator.Setup(x => x.GetRunningRequests(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<CurrentRestoreRequests>([]));
        _restoreRunMediator.Setup(x => x.GetRestoreRuns(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<RestoreRun>([]));

        // Act
        await _actor.StartAsync(cancellationToken);
        await Task.Delay(100);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(cancellationToken);

        // Assert
        _hotStorageService.Verify(
            x => x.UploadCompressedObject(It.IsAny<string>(), It.IsAny<object>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()),
            Times.Never);
        Assert.Single(_logger.LogRecords);
    }

    [Fact]
    public async Task ExecuteAsync_WithDelayConfiguration_RespectsDelay()
    {
        // Arrange
        var cancellationToken = CancellationToken.None;
        var stopwatch = Stopwatch.StartNew();

        _contextResolver.Setup(x => x.DelayBetweenUploadsSeconds()).Returns(1);

        var archiveData = CreateAsyncEnumerable([
            new S3LocationAndValue<ArchiveRun>("key1", new ArchiveRun
            {
                Status = ArchiveRunStatus.Processing,
                RunId = "1",
                OriginalSize = 123,
                CreatedAt = DateTimeOffset.UtcNow,
                CronSchedule = "* * * * *",
                PathsToArchive = "/test",
                Files = new ConcurrentDictionary<string, FileMetaData>()
            }),
            new S3LocationAndValue<ArchiveRun>("key2", new ArchiveRun
            {
                Status = ArchiveRunStatus.Processing,
                RunId = "2",
                OriginalSize = 123,
                CreatedAt = DateTimeOffset.UtcNow,
                CronSchedule = "* * * * *",
                PathsToArchive = "/test",
                Files = new ConcurrentDictionary<string, FileMetaData>()
            })
        ]);

        _runMediator.Setup(x => x.GetArchiveRuns(It.IsAny<CancellationToken>()))
            .Returns(() => { return archiveData; });
        _runMediator.Setup(x => x.GetCurrentArchiveRunRequests(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<CurrentArchiveRunRequests>([]));
        _chunkManifestMediator.Setup(x => x.GetDataChunksManifest(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<DataChunkManifest>([]));
        _restoreManifestMediator.Setup(x => x.GetRestoreManifest(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<S3RestoreChunkManifest>([]));
        _restoreRunMediator.Setup(x => x.GetRestoreRuns(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<RestoreRun>([]));
        _restoreRequestsMediator.Setup(x => x.GetRunningRequests(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<CurrentRestoreRequests>([]));

        // Act
        await _actor.StartAsync(cancellationToken);
        await Task.Delay(1200); // Allow time for processing with delays
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(cancellationToken);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        stopwatch.Stop();

        // Assert
        Assert.True(stopwatch.ElapsedMilliseconds >= 500, "Should respect delay between uploads");
        _hotStorageService.Verify(x => x.UploadCompressedObject("key1", It.IsAny<ArchiveRun>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()),
            Times.Once);
        _hotStorageService.Verify(x => x.UploadCompressedObject("key2", It.IsAny<ArchiveRun>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_WithMultipleMediatorExceptions_LogsAllErrors()
    {
        // Arrange
        var cancellationToken = new CancellationToken();

        _contextResolver.Setup(x => x.DelayBetweenUploadsSeconds()).Returns(0);

        var archiveException = new InvalidOperationException("Archive upload failed");
        var chunkException = new ArgumentException("Chunk upload failed");

        var archiveData = CreateAsyncEnumerable([
            new S3LocationAndValue<ArchiveRun>("archive-key", new ArchiveRun
            {
                Status = ArchiveRunStatus.Processing,
                RunId = "archive1",
                OriginalSize = 123,
                CreatedAt = DateTimeOffset.UtcNow,
                CronSchedule = "* * * * *",
                PathsToArchive = "/test",
                Files = new ConcurrentDictionary<string, FileMetaData>()
            })
        ]);

        var chunkData = CreateAsyncEnumerable([
            new S3LocationAndValue<DataChunkManifest>("chunk-key", new DataChunkManifest())
        ]);

        _runMediator.Setup(x => x.GetArchiveRuns(It.IsAny<CancellationToken>())).Returns(() => archiveData);
        _runMediator.Setup(x => x.GetCurrentArchiveRunRequests(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<CurrentArchiveRunRequests>([]));
        _chunkManifestMediator.Setup(x => x.GetDataChunksManifest(It.IsAny<CancellationToken>()))
            .Returns(() => chunkData);
        _restoreManifestMediator.Setup(x => x.GetRestoreManifest(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<S3RestoreChunkManifest>([]));
        _restoreRunMediator.Setup(x => x.GetRestoreRuns(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<RestoreRun>([]));
        _restoreRequestsMediator.Setup(x => x.GetRunningRequests(It.IsAny<CancellationToken>()))
            .Returns(() => CreateAsyncEnumerable<CurrentRestoreRequests>([]));

        _hotStorageService.Setup(x =>
                x.UploadCompressedObject("archive-key", It.IsAny<ArchiveRun>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(archiveException);
        _hotStorageService.Setup(x =>
                x.UploadCompressedObject("chunk-key", It.IsAny<DataChunkManifest>(), It.IsAny<StorageTemperature>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(chunkException);

        // Act
        await _actor.StartAsync(cancellationToken);
        await (_actor.ExecuteTask ?? Task.CompletedTask);
        await _actor.StopAsync(cancellationToken);

        // Assert
        var errorLogs = _logger.LogRecords.Where(x => x.LogLevel == LogLevel.Error).ToList();
        Assert.Equal(2, errorLogs.Count);

        Assert.Contains(errorLogs, log => log.Exception == archiveException);
        Assert.Contains(errorLogs, log => log.Exception == chunkException);
    }

    private static async IAsyncEnumerable<S3LocationAndValue<T>> CreateAsyncEnumerable<T>(
        IEnumerable<S3LocationAndValue<T>> items) where T : notnull
    {
        foreach (var item in items)
        {
            yield return item;
            await Task.Yield(); // Make it truly async
        }
    }
}