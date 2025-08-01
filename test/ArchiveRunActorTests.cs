// using aws_backup_common;
// using aws_backup;
// using Microsoft.Extensions.Logging;
// using Moq;
//
// namespace test;
//
// public class ArchiveRunActorTests : IDisposable
// {
//     private readonly string _ignoreFilePath;
//     private readonly string _tempDir;
//
//     public ArchiveRunActorTests()
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
//             // ignored
//         }
//     }
//
//     private static async IAsyncEnumerable<RunRequest> SingleRunAsync(string runId, string path)
//     {
//         yield return new RunRequest(runId, "", "") { PathsToArchive = path };
//     }
//
//     [Fact]
//     public async Task ExecuteAsync_NewRun_ProcessesFilesAndCompletes()
//     {
//         // Arrange
//         var archiveFileMediatorMock = new Mock<IArchiveFileMediator>();
//         var mediatorMock = new Mock<IRunRequestMediator>();
//         var archiveServiceMock = new Mock<IArchiveService>();
//         var ctxMock = new Mock<IContextResolver>();
//         var fileListerMock = new Mock<IFileLister>();
//         var loggerMock = new Mock<ILogger<ArchiveRunActor>>();
//         var dataMock = new Mock<IArchiveDataStore>();
//
//         var runId = "run1";
//         var path = _tempDir;
//         dataMock.Setup(d => d.GetRunRequests(It.IsAny<CancellationToken>()))
//             .Returns(SingleRunAsync(runId, path));
//         mediatorMock.Setup(m => m.GetRunRequests(It.IsAny<CancellationToken>()))
//             .Returns(SingleRunAsync(runId, path));
//         archiveServiceMock.Setup(a => a.LookupArchiveRun(runId, It.IsAny<CancellationToken>()))
//             .ReturnsAsync((ArchiveRun)null!);
//
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
//             .Returns(["fileA", "fileB"]);
//
//         var orchestrator = new ArchiveRunActor(
//             mediatorMock.Object,
//             archiveFileMediatorMock.Object,
//             Mock.Of<IUploadChunksMediator>(),
//             archiveServiceMock.Object,
//             dataMock.Object,
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
//         archiveFileMediatorMock.Verify(m => m.ProcessFile(
//                 It.Is<ArchiveFileRequest>(r => r.RunId == runId && r.FilePath == "fileA"),
//                 It.IsAny<CancellationToken>()),
//             Times.Once);
//         archiveFileMediatorMock.Verify(m => m.ProcessFile(
//                 It.Is<ArchiveFileRequest>(r => r.RunId == runId && r.FilePath == "fileB"),
//                 It.IsAny<CancellationToken>()),
//             Times.Once);
//     }
//
//     [Fact]
//     public async Task ExecuteAsync_ExistingCompletedRun_Skips()
//     {
//         // Arrange
//         var mediatorMock = new Mock<IRunRequestMediator>();
//         var archiveFileMock = new Mock<IArchiveFileMediator>();
//         var archiveServiceMock = new Mock<IArchiveService>();
//         var ctxMock = new Mock<IContextResolver>();
//         var fileListerMock = new Mock<IFileLister>();
//         var loggerMock = new Mock<ILogger<ArchiveRunActor>>();
//         var dataStoreMock = new Mock<IArchiveDataStore>();
//
//         var runId = "run2";
//         var path = _tempDir;
//         dataStoreMock.Setup(d => d.GetRunRequests(It.IsAny<CancellationToken>()))
//             .Returns(SingleRunAsync(runId, path));
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
//         var orchestrator = new ArchiveRunActor(
//             Mock.Of<ICountDownEvent>(),
//             mediatorMock.Object,
//             archiveFileMock.Object,
//             Mock.Of<IUploadChunksMediator>(),
//             archiveServiceMock.Object,
//             dataStoreMock.Object,
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
//         archiveFileMock.Verify(m => m.ProcessFile(It.IsAny<ArchiveFileRequest>(), It.IsAny<CancellationToken>()),
//             Times.Never);
//     }
//
//     [Fact]
//     public async Task ExecuteAsync_WithIgnoreFile_FiltersPatterns()
//     {
//         // Arrange write ignore file
//         File.WriteAllLines(_ignoreFilePath, new[] { "*.tmp", "#comment", " " });
//
//         var mediatorMock = new Mock<IRunRequestMediator>();
//         var archiveFileMediatorMock = new Mock<IArchiveFileMediator>();
//         var archiveServiceMock = new Mock<IArchiveService>();
//         var ctxMock = new Mock<IContextResolver>();
//         var fileListerMock = new Mock<IFileLister>();
//         var loggerMock = new Mock<ILogger<ArchiveRunActor>>();
//         var dataStoreMock = new Mock<IArchiveDataStore>();
//
//         var runId = "run3";
//         var path = _tempDir;
//         dataStoreMock.Setup(d => d.GetRunRequests(It.IsAny<CancellationToken>()))
//             .Returns(SingleRunAsync(runId, path));
//         mediatorMock.Setup(m => m.GetRunRequests(It.IsAny<CancellationToken>()))
//             .Returns(SingleRunAsync(runId, path));
//         archiveServiceMock.Setup(a => a.LookupArchiveRun(runId, It.IsAny<CancellationToken>()))
//             .ReturnsAsync((ArchiveRun)null!);
//
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
//             .Returns(["keep.txt"]);
//
//         var orchestrator = new ArchiveRunActor(
//             Mock.Of<ICountDownEvent>(),
//             mediatorMock.Object,
//             archiveFileMediatorMock.Object,
//             Mock.Of<IUploadChunksMediator>(),
//             archiveServiceMock.Object,
//             dataStoreMock.Object,
//             ctxMock.Object,
//             fileListerMock.Object,
//             loggerMock.Object);
//
//         // Act
//         await orchestrator.StartAsync(CancellationToken.None);
//         await orchestrator.ExecuteTask;
//
//         // Assert only keep.txt processed
//         archiveFileMediatorMock.Verify(
//             m => m.ProcessFile(It.Is<ArchiveFileRequest>(r => r.RunId == runId && r.FilePath == "keep.txt"),
//                 It.IsAny<CancellationToken>()), Times.Once);
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
//         var fileMock = new Mock<IArchiveFileMediator>();
//         var mediatorMock = new Mock<IRunRequestMediator>();
//         var archiveServiceMock = new Mock<IArchiveService>();
//         var ctxMock = new Mock<IContextResolver>();
//         var fileListerMock = new Mock<IFileLister>();
//         var loggerMock = new Mock<ILogger<ArchiveRunActor>>();
//         var dataStoreMock = new Mock<IArchiveDataStore>();
//
//
//         var runId = "run4";
//         var path = _tempDir;
//         dataStoreMock.Setup(d => d.GetRunRequests(It.IsAny<CancellationToken>()))
//             .Returns(SingleRunAsync(runId, path));
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
//             .Returns(["fileX"]);
//
//         var orchestrator = new ArchiveRunActor(
//             mediatorMock.Object,
//             fileMock.Object,
//             Mock.Of<IUploadChunksMediator>(),
//             archiveServiceMock.Object,
//             dataStoreMock.Object,
//             ctxMock.Object,
//             fileListerMock.Object,
//             loggerMock.Object);
//
//         // Act
//         await orchestrator.StartAsync(CancellationToken.None);
//         await orchestrator.ExecuteTask;
//
//         // Assert fileX processed
//         fileMock.Verify(
//             m => m.ProcessFile(It.Is<ArchiveFileRequest>(r => r.RunId == runId && r.FilePath == "fileX"),
//                 It.IsAny<CancellationToken>()), Times.Once);
//     }
// }