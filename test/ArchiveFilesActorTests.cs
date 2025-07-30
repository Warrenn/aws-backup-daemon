using System.Threading.Channels;
using aws_backup_common;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class ArchiveFilesActorTests
{
    private ArchiveFilesActor CreateOrchestrator(
        IAsyncEnumerable<ArchiveFileRequest> archiveFiles,
        bool requireProcessing,
        bool keepTimeStamps,
        bool keepOwnerGroup,
        bool keepAclEntries,
        Mock<IRetryMediator> retryMediatorMock,
        Mock<IChunkedEncryptingFileProcessor> processorMock,
        Mock<IArchiveService> archiveServiceMock,
        Mock<IArchiveFileMediator> mediatorMock,
        Mock<IArchiveDataStore> dataStoreMock,
        Mock<IContextResolver> ctxMock)
    {
        mediatorMock.Setup(m => m.GetArchiveFiles(It.IsAny<CancellationToken>()))
            .Returns(archiveFiles);

        archiveServiceMock.Setup(a =>
                a.DoesFileRequireProcessing(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(requireProcessing);

        ctxMock.Setup(c => c.NoOfFilesToBackupConcurrently()).Returns(2);
        ctxMock.Setup(c => c.KeepTimeStamps()).Returns(keepTimeStamps);
        ctxMock.Setup(c => c.KeepOwnerGroup()).Returns(keepOwnerGroup);
        ctxMock.Setup(c => c.KeepAclEntries()).Returns(keepAclEntries);

        var loggerMock = new Mock<ILogger<ArchiveFilesActor>>();

        return new ArchiveFilesActor(
            Mock.Of<IFileCountDownEvent>(),
            mediatorMock.Object,
            retryMediatorMock.Object,
            processorMock.Object,
            archiveServiceMock.Object,
            loggerMock.Object,
            ctxMock.Object);
    }

    [Fact]
    public async Task SkipsFile_WhenRequireProcessingFalse()
    {
        // Arrange
        var channel = Channel.CreateUnbounded<ArchiveFileRequest>();
        channel.Writer.TryWrite(new ArchiveFileRequest("run1", "path1"));
        channel.Writer.Complete();

        var mediatorMock = new Mock<IArchiveFileMediator>();
        var retryMediatorMock = new Mock<IRetryMediator>();
        var processorMock = new Mock<IChunkedEncryptingFileProcessor>();
        var archiveServiceMock = new Mock<IArchiveService>();
        var ctxMock = new Mock<IContextResolver>();
        var dataStoreMock = new Mock<IArchiveDataStore>();

        var orch = CreateOrchestrator(
            channel.Reader.ReadAllAsync(),
            false,
            false,
            false,
            false,
            retryMediatorMock,
            processorMock,
            archiveServiceMock,
            mediatorMock,
            dataStoreMock,
            ctxMock);

        // Act
        await orch.StartAsync(CancellationToken.None);
        await orch.ExecuteTask;

        // Assert: processor never called
        processorMock.Verify(
            p => p.ProcessFileAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
        // ReportProcessingResult never called
        archiveServiceMock.Verify(
            a => a.ReportProcessingResult(It.IsAny<string>(), It.IsAny<FileProcessResult>(),It.IsAny<FileProperties>(), 
                It.IsAny<CancellationToken>()), Times.Never);
        // RetryAttempt never called
        retryMediatorMock.Verify(
            m => m.RetryAttempt(It.IsAny<RetryState>(),
                It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ProcessesFile_WhenRequireProcessingTrue_AndUpdatesAccordingFlags()
    {
        // Arrange channel with one item
        var channel = Channel.CreateUnbounded<ArchiveFileRequest>();

        var mediatorMock = new Mock<IArchiveFileMediator>();
        var processorMock = new Mock<IChunkedEncryptingFileProcessor>();
        var dataStoreMock = new Mock<IArchiveDataStore>();
        var retryMock = new Mock<IRetryMediator>();
        processorMock.Setup(p => p.ProcessFileAsync("run2", "file2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new FileProcessResult("file2", 0, [], 0));

        var archiveServiceMock = new Mock<IArchiveService>();
        archiveServiceMock.Setup(a => a.DoesFileRequireProcessing("run2", "file2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var ctxMock = new Mock<IContextResolver>();

        // Enable all features
        var orch = CreateOrchestrator(
            channel.Reader.ReadAllAsync(),
            true,
            true,
            true,
            true,
            retryMock,
            processorMock,
            archiveServiceMock,
            mediatorMock,
            dataStoreMock,
            ctxMock);

        // Stub FileHelper static calls via real file
        // Create a temp file at "file2" path
        File.WriteAllText("file2", "x");
        // Ensure timestamps, owner/group, ACL calls don't throw

        // Act
        await orch.StartAsync(CancellationToken.None);
        channel.Writer.TryWrite(new ArchiveFileRequest("run2", "file2"));
        channel.Writer.Complete();

        await orch.ExecuteTask;

        // Assert: processor called once
        processorMock.Verify(p => p.ProcessFileAsync("run2", "file2", It.IsAny<CancellationToken>()), Times.Once);
        archiveServiceMock.Verify(
            a => a.ReportProcessingResult("run2", It.IsAny<FileProcessResult>(), It.IsAny<FileProperties>(), It.IsAny<CancellationToken>()),
            Times.Once);

        // UpdateTimeStamps
        dataStoreMock.Verify(
            a => a.UpdateTimeStamps("run2", "file2", It.IsAny<DateTimeOffset>(), It.IsAny<DateTimeOffset>(),
                It.IsAny<CancellationToken>()), Times.Once);
        // UpdateOwnerGroup
        dataStoreMock.Verify(
            a => a.UpdateOwnerGroup("run2", "file2", It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<CancellationToken>()), Times.Once);
        // UpdateAclEntries
        dataStoreMock.Verify(
            a => a.UpdateAclEntries("run2", "file2", It.IsAny<AclEntry[]>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RetriesFile_WhenProcessorThrows()
    {
        // Arrange
        var channel = Channel.CreateUnbounded<ArchiveFileRequest>();
        var request = new ArchiveFileRequest("run3", "file3");
        var exception = new InvalidOperationException("fail");

        var mediatorMock = new Mock<IArchiveFileMediator>();
        var processorMock = new Mock<IChunkedEncryptingFileProcessor>();
        var retryMediatorMock = new Mock<IRetryMediator>();
        var dataStoreMock = new Mock<IArchiveDataStore>();
        retryMediatorMock
            .Setup(m => m.RetryAttempt(It.IsAny<RetryState>(), It.IsAny<CancellationToken>()))
            .Callback((RetryState state, CancellationToken token) =>
            {
                state.LimitExceeded(state, token).GetAwaiter().GetResult();
                state.Retry(state, token).GetAwaiter().GetResult();
            });

        processorMock.Setup(p =>
                p.ProcessFileAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        var archiveServiceMock = new Mock<IArchiveService>();
        archiveServiceMock.Setup(a => a.DoesFileRequireProcessing("run3", "file3", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var ctxMock = new Mock<IContextResolver>();
        var orch = CreateOrchestrator(
            channel.Reader.ReadAllAsync(),
            true,
            false,
            false,
            false,
            retryMediatorMock,
            processorMock,
            archiveServiceMock,
            mediatorMock,
            dataStoreMock,
            ctxMock);

        // Act
        await orch.StartAsync(CancellationToken.None);
        channel.Writer.TryWrite(request);
        channel.Writer.Complete();
        await orch.ExecuteTask;

        // Assert: RetryAttempt called
        retryMediatorMock.Verify(
            m => m.RetryAttempt(It.Is<RetryState>(s =>
                    s == request &&
                    s.Exception == exception),
                It.IsAny<CancellationToken>()),
            Times.Once);
        mediatorMock.Verify(
            m => m.ProcessFile(It.Is<ArchiveFileRequest>(r =>
                    r.RunId == "run3" &&
                    r.FilePath == "file3"),
                It.IsAny<CancellationToken>()), Times.Once);
        archiveServiceMock.Verify(
            a => a.RecordFailedFile("run3", "file3", exception, It.IsAny<CancellationToken>()),
            Times.Once);
    }
}