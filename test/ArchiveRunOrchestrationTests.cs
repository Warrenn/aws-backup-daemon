// using aws_backup;
// using Microsoft.Extensions.Logging;
// using Moq;
//
// namespace test;
//
// public class ArchiveRunOrchestrationTests : IDisposable
// {
//     private readonly string _ignoreFilePath;
//     private readonly string _tempDir;
//
//     public ArchiveRunOrchestrationTests()
//     {
//         _tempDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
//         Directory.CreateDirectory(_tempDir);
//         _ignoreFilePath = Path.Combine(_tempDir, "ignore.txt");
//     }
//
//     public void Dispose()
//     {
//         try
//         {
//             Directory.Delete(_tempDir, true);
//         }
//         catch
//         {
//         }
//     }
//
//     private async IAsyncEnumerable<RunRequest> SingleRunAsync(string runId, string path)
//     {
//         yield return new RunRequest(runId, "", "") { PathsToArchive = path };
//     }
//
//     [Fact]
//     public async Task ExecuteAsync_NewRun_ProcessesFilesAndCompletes()
//     {
//         // Arrange
//         var mediatorMock = new Mock<IArchiveFileMediator>();
//         var archiveServiceMock = new Mock<IArchiveService>();
//         var ctxMock = new Mock<IContextResolver>();
//         var fileListerMock = new Mock<IFileLister>();
//         var loggerMock = new Mock<ILogger<ArchiveRunOrchestration>>();
//
//         var runId = "run1";
//         var path = _tempDir;
//         mediatorMock.Setup(m => m.GetArchiveFiles(It.IsAny<CancellationToken>()))
//             .Returns(SingleRunAsync(runId, path));
//         archiveServiceMock.Setup(a => a.LookupArchiveRun(runId, It.IsAny<CancellationToken>()))
//             .ReturnsAsync((ArchiveRun)null);
//         var newRun = new ArchiveRun
//         {
//             RunId = runId,
//             PathsToArchive = path,
//             CronSchedule = "* * * * *",
//             CreatedAt = DateTimeOffset.UtcNow,
//             Status = ArchiveRunStatus.Processing
//         };
//         archiveServiceMock.Setup(a => a.StartNewArchiveRun(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
//             .ReturnsAsync(newRun);
//         // After processing return Status Completed
//         newRun.Status = ArchiveRunStatus.Processing;
//         archiveServiceMock.Setup(a => a.StartNewArchiveRun(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
//             .ReturnsAsync(newRun);
//         fileListerMock.Setup(f => f.GetAllFiles(path, It.IsAny<string[]>()))
//             .Returns(new[] { "fileA", "fileB" });
//
//         var orchestrator = new ArchiveRunOrchestration(
//             mediatorMock.Object,
//             archiveServiceMock.Object,
//             ctxMock.Object,
//             fileListerMock.Object,
//             loggerMock.Object);
//
//         // Act
//         await orchestrator.StartAsync(CancellationToken.None);
//         await orchestrator.ExecuteTask;
//
//         // Assert
//         archiveServiceMock.Verify(a => a.StartNewArchiveRun(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()),
//             Times.Once);
//         archiveServiceMock.Verify(a => a.RecordLocalFile(runId, "fileA", It.IsAny<CancellationToken>()), Times.Once);
//         archiveServiceMock.Verify(a => a.RecordLocalFile(runId, "fileB", It.IsAny<CancellationToken>()), Times.Once);
//         mediatorMock.Verify(m => m.ProcessFile(runId, "fileA", It.IsAny<CancellationToken>()), Times.Once);
//         mediatorMock.Verify(m => m.ProcessFile(runId, "fileB", It.IsAny<CancellationToken>()), Times.Once);
//         archiveServiceMock.Verify(a => a.CompleteArchiveRun(runId, It.IsAny<CancellationToken>()), Times.Once);
//     }
//
//     [Fact]
//     public async Task ExecuteAsync_ExistingCompletedRun_Skips()
//     {
//         // Arrange
//         var mediatorMock = new Mock<IMediator>();
//         var archiveServiceMock = new Mock<IArchiveService>();
//         var ctxMock = new Mock<IContextResolver>();
//         var fileListerMock = new Mock<IFileLister>();
//         var loggerMock = new Mock<ILogger<ArchiveRunOrchestration>>();
//
//         var runId = "run2";
//         var path = _tempDir;
//         mediatorMock.Setup(m => m.GetRunRequests(It.IsAny<CancellationToken>()))
//             .Returns(SingleRunAsync(runId, path));
//         var existingRun = new ArchiveRun
//         {
//             RunId = runId, PathsToArchive = path, CronSchedule = "* * * * *", Status = ArchiveRunStatus.Completed,
//             CreatedAt = DateTimeOffset.UtcNow
//         };
//         archiveServiceMock.Setup(a => a.LookupArchiveRun(runId, It.IsAny<CancellationToken>()))
//             .ReturnsAsync(existingRun);
//
//         var orchestrator = new ArchiveRunOrchestration(
//             mediatorMock.Object,
//             archiveServiceMock.Object,
//             ctxMock.Object,
//             fileListerMock.Object,
//             loggerMock.Object);
//
//         // Act
//         await orchestrator.StartAsync(CancellationToken.None);
//         await orchestrator.ExecuteTask;
//
//         // Assert: nothing processed
//         archiveServiceMock.Verify(a => a.StartNewArchiveRun(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()),
//             Times.Never);
//         fileListerMock.Verify(f => f.GetAllFiles(It.IsAny<string>(), It.IsAny<string[]>()), Times.Never);
//         archiveServiceMock.Verify(
//             a => a.RecordLocalFile(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
//         mediatorMock.Verify(m => m.ProcessFile(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
//             Times.Never);
//         archiveServiceMock.Verify(a => a.CompleteArchiveRun(It.IsAny<string>(), It.IsAny<CancellationToken>()),
//             Times.Never);
//     }
//
//     [Fact]
//     public async Task ExecuteAsync_WithIgnoreFile_FiltersPatterns()
//     {
//         // Arrange write ignore file
//         File.WriteAllLines(_ignoreFilePath, new[] { "*.tmp", "#comment", " " });
//
//         var mediatorMock = new Mock<IMediator>();
//         var archiveServiceMock = new Mock<IArchiveService>();
//         var ctxMock = new Mock<IContextResolver>();
//         var fileListerMock = new Mock<IFileLister>();
//         var loggerMock = new Mock<ILogger<ArchiveRunOrchestration>>();
//
//         var runId = "run3";
//         var path = _tempDir;
//         mediatorMock.Setup(m => m.GetRunRequests(It.IsAny<CancellationToken>()))
//             .Returns(SingleRunAsync(runId, path));
//         archiveServiceMock.Setup(a => a.LookupArchiveRun(runId, It.IsAny<CancellationToken>()))
//             .ReturnsAsync((ArchiveRun)null);
//         var newRun = new ArchiveRun
//         {
//             RunId = runId, PathsToArchive = path, CronSchedule = "* * * * *", Status = ArchiveRunStatus.Processing,
//             CreatedAt = DateTimeOffset.UtcNow
//         };
//         archiveServiceMock.Setup(a => a.StartNewArchiveRun(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
//             .ReturnsAsync(newRun);
//         ctxMock.Setup(c => c.LocalIgnoreFile()).Returns(_ignoreFilePath);
//         fileListerMock.Setup(f =>
//                 f.GetAllFiles(It.IsAny<string>(),
//                     It.Is<string[]>(p => p.SequenceEqual(new[] { "*.tmp" }))))
//             .Returns(new[] { "keep.txt" });
//
//         var orchestrator = new ArchiveRunOrchestration(
//             mediatorMock.Object,
//             archiveServiceMock.Object,
//             ctxMock.Object,
//             fileListerMock.Object,
//             loggerMock.Object);
//
//         // Act
//         await orchestrator.StartAsync(CancellationToken.None);
//         await orchestrator.ExecuteTask;
//
//         // Assert only keep.txt processed
//         archiveServiceMock.Verify(a => a.RecordLocalFile(runId, "keep.txt", It.IsAny<CancellationToken>()), Times.Once);
//         mediatorMock.Verify(m => m.ProcessFile(runId, "keep.txt", It.IsAny<CancellationToken>()), Times.Once);
//     }
//
//     [Fact]
//     public async Task ExecuteAsync_IgnoreFileReadError_ContinuesWithEmptyPatterns()
//     {
//         // Arrange create a bad ignore file
//         File.WriteAllText(_ignoreFilePath, "bad");
//         // Simulate permission denied
//         File.SetAttributes(_ignoreFilePath, FileAttributes.ReadOnly);
//         var originalLines = File.ReadAllLines(_ignoreFilePath);
//
//         var mediatorMock = new Mock<IMediator>();
//         var archiveServiceMock = new Mock<IArchiveService>();
//         var ctxMock = new Mock<IContextResolver>();
//         var fileListerMock = new Mock<IFileLister>();
//         var loggerMock = new Mock<ILogger<ArchiveRunOrchestration>>();
//
//         var runId = "run4";
//         var path = _tempDir;
//         mediatorMock.Setup(m => m.GetRunRequests(It.IsAny<CancellationToken>()))
//             .Returns(SingleRunAsync(runId, path));
//         archiveServiceMock.Setup(a => a.LookupArchiveRun(runId, It.IsAny<CancellationToken>()))
//             .ReturnsAsync((ArchiveRun)null);
//         var newRun = new ArchiveRun
//         {
//             RunId = runId, PathsToArchive = path, CronSchedule = "* * * * *", Status = ArchiveRunStatus.Processing,
//             CreatedAt = DateTimeOffset.UtcNow
//         };
//         archiveServiceMock.Setup(a => a.StartNewArchiveRun(It.IsAny<RunRequest>(), It.IsAny<CancellationToken>()))
//             .ReturnsAsync(newRun);
//         ctxMock.Setup(c => c.LocalIgnoreFile()).Returns(_ignoreFilePath);
//         fileListerMock.Setup(f => f.GetAllFiles(path, originalLines))
//             .Returns(new[] { "fileX" });
//
//         var orchestrator = new ArchiveRunOrchestration(
//             mediatorMock.Object,
//             archiveServiceMock.Object,
//             ctxMock.Object,
//             fileListerMock.Object,
//             loggerMock.Object);
//
//         // Act
//         await orchestrator.StartAsync(CancellationToken.None);
//         await orchestrator.ExecuteTask;
//
//         // Assert fileX processed
//         archiveServiceMock.Verify(a => a.RecordLocalFile(runId, "fileX", It.IsAny<CancellationToken>()), Times.Once);
//     }
// }