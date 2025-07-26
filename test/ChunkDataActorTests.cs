using System.Threading.Channels;
using aws_backup_common;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class ChunkDataActorTests
{
    private readonly Mock<IArchiveService> _archiveService = new();
    private readonly Mock<IUploadBatchMediator> _batchMediator = new();
    private readonly Mock<IUploadChunksMediator> _chunksMediator = new();
    private readonly Mock<IContextResolver> _contextResolver = new();
    private readonly Mock<IDataChunkService> _dataChunkService = new();
    private readonly Mock<ILogger<ChunkDataActor>> _logger = new();
    private readonly Mock<IRetryMediator> _retryMediator = new();

    private AwsConfiguration CreateAwsConfig(long chunkSizeBytes)
    {
        return new AwsConfiguration(
            chunkSizeBytes,
            "",
            "",
            "test-bucket",
            "",
            "",
            "",
            "",
            "",
            "",
            "");
    }

    [Fact]
    public async Task ChunkExceedsThreshold_ShouldFlushToS3()
    {
        var chunkSize = 100;
        var bufferSize = 10;
        var chunk = new DataChunkDetails(Path.GetTempFileName(), 0, 60, [1, 2, 3], 60);
        var request = new UploadChunkRequest("run-1", "file1.txt", chunk);

        var channel = Channel.CreateUnbounded<UploadChunkRequest>();
        await File.WriteAllBytesAsync(chunk.LocalFilePath, new byte[60]);
        await channel.Writer.WriteAsync(request);
        channel.Writer.Complete();

        _contextResolver.Setup(x => x.NoOfFilesToBackupConcurrently()).Returns(1);
        _contextResolver.Setup(x => x.ReadBufferSize()).Returns(bufferSize);
        _contextResolver.Setup(x => x.LocalCacheFolder()).Returns(Path.GetTempPath());
        _contextResolver.Setup(x => x.UploadAttemptLimit()).Returns(1);
        _contextResolver.Setup(x => x.ShutdownTimeoutSeconds()).Returns(1);

        _chunksMediator.Setup(m => m.RegisterReader());
        _chunksMediator.Setup(m => m.GetChunks(It.IsAny<CancellationToken>())).Returns(channel.Reader.ReadAllAsync());
        _chunksMediator.Setup(m => m.SignalReaderCompleted());

        var service = new ChunkDataActor(
            _chunksMediator.Object,
            _batchMediator.Object,
            _logger.Object,
            _contextResolver.Object,
            _dataChunkService.Object,
            _archiveService.Object,
            _retryMediator.Object,
            CreateAwsConfig(chunkSize));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await service.StartAsync(cts.Token);

        _batchMediator.Verify(m => m.ProcessBatch(
            It.Is<UploadBatch>(b => b.Requests.Count == 1),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [Fact]
    public async Task ChunkUnderThreshold_FlushesOnEnumerationComplete()
    {
        var chunkSize = 100;
        var bufferSize = 10;
        var chunk = new DataChunkDetails(Path.GetTempFileName(), 0, 40, [9, 9, 9], 40);
        var request = new UploadChunkRequest("run-2", "file2.txt", chunk);

        var channel = Channel.CreateUnbounded<UploadChunkRequest>();
        await File.WriteAllBytesAsync(chunk.LocalFilePath, new byte[40]);
        await channel.Writer.WriteAsync(request);
        channel.Writer.Complete();

        _contextResolver.Setup(x => x.NoOfFilesToBackupConcurrently()).Returns(1);
        _contextResolver.Setup(x => x.ReadBufferSize()).Returns(bufferSize);
        _contextResolver.Setup(x => x.LocalCacheFolder()).Returns(Path.GetTempPath());
        _contextResolver.Setup(x => x.UploadAttemptLimit()).Returns(1);
        _contextResolver.Setup(x => x.ShutdownTimeoutSeconds()).Returns(1);

        _chunksMediator.Setup(m => m.RegisterReader());
        _chunksMediator.Setup(m => m.GetChunks(It.IsAny<CancellationToken>())).Returns(channel.Reader.ReadAllAsync());
        _chunksMediator.Setup(m => m.SignalReaderCompleted());

        var service = new ChunkDataActor(
            _chunksMediator.Object,
            _batchMediator.Object,
            _logger.Object,
            _contextResolver.Object,
            _dataChunkService.Object,
            _archiveService.Object,
            _retryMediator.Object,
            CreateAwsConfig(chunkSize));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await service.StartAsync(cts.Token);

        _batchMediator.Verify(m => m.ProcessBatch(
            It.Is<UploadBatch>(b => b.Requests.Count == 1),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ChunkAlreadyUploaded_IsSkippedAndNotBatched()
    {
        var chunk = new DataChunkDetails(Path.GetTempFileName(), 0, 50, [5, 5, 5], 50);
        var request = new UploadChunkRequest("run-3", "file3.txt", chunk);

        var channel = Channel.CreateUnbounded<UploadChunkRequest>();
        await File.WriteAllBytesAsync(chunk.LocalFilePath, new byte[50]);
        await channel.Writer.WriteAsync(request);
        channel.Writer.Complete();

        _dataChunkService.Setup(x => x.ChunkAlreadyUploaded(chunk, It.IsAny<CancellationToken>())).ReturnsAsync(true);

        _contextResolver.Setup(x => x.NoOfFilesToBackupConcurrently()).Returns(1);
        _contextResolver.Setup(x => x.ReadBufferSize()).Returns(10);
        _contextResolver.Setup(x => x.LocalCacheFolder()).Returns(Path.GetTempPath());
        _contextResolver.Setup(x => x.UploadAttemptLimit()).Returns(1);
        _contextResolver.Setup(x => x.ShutdownTimeoutSeconds()).Returns(1);

        _chunksMediator.Setup(m => m.RegisterReader());
        _chunksMediator.Setup(m => m.GetChunks(It.IsAny<CancellationToken>())).Returns(channel.Reader.ReadAllAsync());
        _chunksMediator.Setup(m => m.SignalReaderCompleted());

        var service = new ChunkDataActor(
            _chunksMediator.Object,
            _batchMediator.Object,
            _logger.Object,
            _contextResolver.Object,
            _dataChunkService.Object,
            _archiveService.Object,
            _retryMediator.Object,
            CreateAwsConfig(100));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await service.StartAsync(cts.Token);

        _batchMediator.Verify(x => x.ProcessBatch(It.IsAny<UploadBatch>(), It.IsAny<CancellationToken>()), Times.Never);
        _archiveService.Verify(
            x => x.RecordChunkUpload("run-3", "file3.txt", chunk.HashKey, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ChunkProcessingError_ShouldTriggerRetry()
    {
        var chunk = new DataChunkDetails(Path.GetTempFileName(), 0, 50, [6, 6, 6], 50);
        var request = new UploadChunkRequest("run-4", "file4.txt", chunk);

        var channel = Channel.CreateUnbounded<UploadChunkRequest>();
        await File.WriteAllBytesAsync(chunk.LocalFilePath, new byte[50]);
        await channel.Writer.WriteAsync(request);
        channel.Writer.Complete();

        _dataChunkService.Setup(x => x.ChunkAlreadyUploaded(chunk, It.IsAny<CancellationToken>())).ReturnsAsync(false);
        _archiveService.Setup(x => x.IsTheFileSkipped("run-4", "file4.txt", It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _contextResolver.Setup(x => x.NoOfFilesToBackupConcurrently()).Returns(1);
        _contextResolver.Setup(x => x.ReadBufferSize()).Returns(10);
        _contextResolver.Setup(x => x.LocalCacheFolder()).Returns(Path.GetTempPath());
        _contextResolver.Setup(x => x.UploadAttemptLimit()).Returns(1);
        _contextResolver.Setup(x => x.ShutdownTimeoutSeconds()).Returns(1);

        _chunksMediator.Setup(m => m.RegisterReader());
        _chunksMediator.Setup(m => m.GetChunks(It.IsAny<CancellationToken>())).Returns(channel.Reader.ReadAllAsync());
        _chunksMediator.Setup(m => m.SignalReaderCompleted());

        var service = new ChunkDataActor(
            _chunksMediator.Object,
            _batchMediator.Object,
            _logger.Object,
            _contextResolver.Object,
            _dataChunkService.Object,
            _archiveService.Object,
            _retryMediator.Object,
            CreateAwsConfig(100));

        await using var lockStream =
            new FileStream(chunk.LocalFilePath, FileMode.Open, FileAccess.Read, FileShare.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await service.StartAsync(cts.Token);

        _retryMediator.Verify(x => x.RetryAttempt(request, It.IsAny<CancellationToken>()), Times.Once);
    }
}