using System.Threading.Channels;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class ArchiveFilesOrchestrationTests
{
    private ArchiveFilesOrchestration CreateOrchestrator(
        IAsyncEnumerable<(string, string)> archiveFiles,
        bool requireProcessing,
        bool keepTimeStamps,
        bool keepOwnerGroup,
        bool keepAclEntries,
        Mock<IChunkedEncryptingFileProcessor> processorMock,
        Mock<IArchiveService> archiveServiceMock,
        Mock<IMediator> mediatorMock,
        Mock<IContextResolver> ctxMock)
    {
        mediatorMock.Setup(m => m.GetArchiveFiles(It.IsAny<CancellationToken>()))
            .Returns(archiveFiles);

        archiveServiceMock.Setup(a =>
                a.DoesFileRequireProcessing(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(requireProcessing);

        ctxMock.Setup(c => c.NoOfConcurrentDownloadsPerFile()).Returns(2);
        ctxMock.Setup(c => c.KeepTimeStamps()).Returns(keepTimeStamps);
        ctxMock.Setup(c => c.KeepOwnerGroup()).Returns(keepOwnerGroup);
        ctxMock.Setup(c => c.KeepAclEntries()).Returns(keepAclEntries);

        var loggerMock = new Mock<ILogger<ArchiveFilesOrchestration>>();

        return new ArchiveFilesOrchestration(
            mediatorMock.Object,
            processorMock.Object,
            archiveServiceMock.Object,
            loggerMock.Object,
            ctxMock.Object);
    }

    [Fact]
    public async Task SkipsFile_WhenRequireProcessingFalse()
    {
        // Arrange
        var channel = Channel.CreateUnbounded<(string, string)>();
        channel.Writer.TryWrite(("run1", "path1"));
        channel.Writer.Complete();

        var mediatorMock = new Mock<IMediator>();
        var processorMock = new Mock<IChunkedEncryptingFileProcessor>();
        var archiveServiceMock = new Mock<IArchiveService>();
        var ctxMock = new Mock<IContextResolver>();

        var orch = CreateOrchestrator(
            channel.Reader.ReadAllAsync(),
            false,
            false,
            false,
            false,
            processorMock,
            archiveServiceMock,
            mediatorMock,
            ctxMock);

        // Act
        await orch.StartAsync(CancellationToken.None);

        // Assert: processor never called
        processorMock.Verify(
            p => p.ProcessFileAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
        // ReportProcessingResult never called
        archiveServiceMock.Verify(
            a => a.ReportProcessingResult(It.IsAny<string>(), It.IsAny<FileProcessResult>(),
                It.IsAny<CancellationToken>()), Times.Never);
        // RetryAttempt never called
        mediatorMock.Verify(
            m => m.RetryAttempt(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Exception>(),
                It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ProcessesFile_WhenRequireProcessingTrue_AndUpdatesAccordingFlags()
    {
        // Arrange channel with one item
        var channel = Channel.CreateUnbounded<(string, string)>();

        var mediatorMock = new Mock<IMediator>();
        var processorMock = new Mock<IChunkedEncryptingFileProcessor>();
        processorMock.Setup(p => p.ProcessFileAsync("run2", "file2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new FileProcessResult("file2", 0, new byte[] { }, Array.Empty<DataChunkDetails>()));

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
            processorMock,
            archiveServiceMock,
            mediatorMock,
            ctxMock);

        // Stub FileHelper static calls via real file
        // Create a temp file at "file2" path
        File.WriteAllText("file2", "x");
        // Ensure timestamps, owner/group, ACL calls don't throw

        // Act
        await orch.StartAsync(CancellationToken.None);
        channel.Writer.TryWrite(("run2", "file2"));
        channel.Writer.Complete();

        await orch.ExecuteTask;
        
        // Assert: processor called once
        processorMock.Verify(p => p.ProcessFileAsync("run2", "file2", It.IsAny<CancellationToken>()), Times.Once);
        archiveServiceMock.Verify(
            a => a.ReportProcessingResult("run2", It.IsAny<FileProcessResult>(), It.IsAny<CancellationToken>()),
            Times.Once);

        // UpdateTimeStamps
        archiveServiceMock.Verify(
            a => a.UpdateTimeStamps("run2", "file2", It.IsAny<DateTimeOffset>(), It.IsAny<DateTimeOffset>(),
                It.IsAny<CancellationToken>()), Times.Once);
        // UpdateOwnerGroup
        archiveServiceMock.Verify(
            a => a.UpdateOwnerGroup("run2", "file2", It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<CancellationToken>()), Times.Once);
        // UpdateAclEntries
        archiveServiceMock.Verify(
            a => a.UpdateAclEntries("run2", "file2", It.IsAny<AclEntry[]>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RetriesFile_WhenProcessorThrows()
    {
        // Arrange
        var channel = Channel.CreateUnbounded<(string, string)>();
        channel.Writer.TryWrite(("run3", "file3"));
        channel.Writer.Complete();

        var mediatorMock = new Mock<IMediator>();
        var processorMock = new Mock<IChunkedEncryptingFileProcessor>();
        processorMock.Setup(p =>
                p.ProcessFileAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("fail"));

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
            processorMock,
            archiveServiceMock,
            mediatorMock,
            ctxMock);

        // Act
        await orch.StartAsync(CancellationToken.None);
        await orch.ExecuteTask;

        // Assert: RetryAttempt called
        mediatorMock.Verify(
            m => m.RetryAttempt("run3", "file3", It.IsAny<InvalidOperationException>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }
}