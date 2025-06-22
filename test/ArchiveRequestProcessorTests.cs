using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class ArchiveRequestProcessorTests
{
    [Fact]
    public async Task ExecuteAsync_FirstRun_CreatesAndProcessesFiles()
    {
        // Arrange
        var req = new RunRequest("run1", "", "* * * * *");
        var mediatorMock = new Mock<IMediator>();
        mediatorMock
            .Setup(m => m.RunRequests(It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Repeat(req, 1));

        var archiveServiceMock = new Mock<IArchiveService>();
        var fileListerMock = new Mock<IFileLister>();
        fileListerMock
            .Setup(fl => fl.GetAllFiles(It.IsAny<string>(), It.IsAny<string[]>()))
            .Returns(["a.txt", "b.txt"]);

        var config = new Configuration { IgnoreFile = Path.Combine(Path.GetTempPath(), "ignore.txt") };
        var loggerMock = new Mock<ILogger<ArchiveRequestProcessor>>();

        var processor = new ArchiveRequestProcessor(
            mediatorMock.Object,
            archiveServiceMock.Object,
            config,
            fileListerMock.Object,
            loggerMock.Object
        );

        // Act
        await processor.StartAsync(CancellationToken.None);

        // Assert
        archiveServiceMock.Verify(s => s.StartNewArchiveRun(It.IsAny<ArchiveRun>(), configuration, It.IsAny<CancellationToken>()), Times.Once);
        archiveServiceMock.Verify(s => s.RecordLocalFile("run1", "a.txt", It.IsAny<CancellationToken>()), Times.Once);
        archiveServiceMock.Verify(s => s.RecordLocalFile("run1", "b.txt", It.IsAny<CancellationToken>()), Times.Once);
        archiveServiceMock.Verify(s => s.CompleteArchiveRun("run1", It.IsAny<CancellationToken>()), Times.Once);

    }

    [Fact]
    public async Task ExecuteAsync_ExistingCompletedRun_ExitsEarly()
    {
        // Arrange
        var req = new RunRequest("run2", "/nonexistent", "* * * * *");
        var existingRun = new ArchiveRun("run2", "", "*") { Status = ArchiveRunStatus.Completed };

        var mediatorMock = new Mock<IMediator>();
        mediatorMock
            .Setup(m => m.RunRequests(It.IsAny<CancellationToken>()))
            .Returns(AsyncEnumerable.Repeat(req, 1));

        var archiveServiceMock = new Mock<IArchiveService>();
        archiveServiceMock
            .Setup(s => s.LookupArchiveRun("run2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(existingRun);

        var fileListerMock = new Mock<IFileLister>();
        var config = new Configuration();
        var loggerMock = new Mock<ILogger<ArchiveRequestProcessor>>();

        var processor = new ArchiveRequestProcessor(
            mediatorMock.Object,
            archiveServiceMock.Object,
            config,
            fileListerMock.Object,
            loggerMock.Object
        );

        // Act
        await processor.StartAsync(CancellationToken.None);

        // Assert
        archiveServiceMock.Verify(s => s.StartNewArchiveRun(It.IsAny<ArchiveRun>(), configuration, It.IsAny<CancellationToken>()), Times.Never);
        archiveServiceMock.Verify(s => s.RecordLocalFile(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        archiveServiceMock.Verify(s => s.CompleteArchiveRun(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}