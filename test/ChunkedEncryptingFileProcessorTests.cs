// using System.Security.Cryptography;
// using System.Text;
// using aws_backup_common;
// using aws_backup;
// using dotenv.net;
// using Moq;
//
// namespace test;
//
// public class ChunkedEncryptingFileProcessorTests : IDisposable
// {
//     private readonly string _cacheDir;
//     private readonly string _tempFile;
//
//     public ChunkedEncryptingFileProcessorTests()
//     {
//         DotEnv.Load();
//         _cacheDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
//         Directory.CreateDirectory(_cacheDir);
//         _tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
//     }
//
//     public void Dispose()
//     {
//         if (Directory.Exists(_cacheDir)) Directory.Delete(_cacheDir, true);
//         if (File.Exists(_tempFile)) File.Delete(_tempFile);
//     }
//
//     [Fact]
//     public async Task ProcessFileAsync_OneChunk_CreatesSingleChunkAndComputesHash()
//     {
//         // Arrange: create a small file (less than chunk size)
//         var content = Encoding.UTF8.GetBytes("Hello, Chunk!");
//         await File.WriteAllBytesAsync(_tempFile, content);
//
//         // Mock context resolver
//         var ctxMock = new Mock<IContextResolver>();
//         ctxMock.Setup(c => c.ReadBufferSize()).Returns(4); // small buffer
//         ctxMock.Setup(c => c.LocalCacheFolder()).Returns(_cacheDir);
//         var aesKey = Convert.FromBase64String("rShkO4lOEPhNlNJ/LRokw2h4G0HDpe4rMnvG4WGFqwA=");
//
//         var awsConfig = new AwsConfiguration(
//             16,
//             "test-bucket", "queue-in", "queue-out",
//             "archive-complete","restore-complete", "restore-errors", "exception",
//             "dynamo-table");
//
//         var aesMock = new Mock<IAesContextResolver>();
//         aesMock.Setup(a => a.FileEncryptionKey(It.IsAny<CancellationToken>()))
//             .ReturnsAsync(aesKey);
//
//         // Mock mediator
//         var mediatorMock = new Mock<IUploadChunksMediator>();
//         mediatorMock
//             .Setup(m => m.ProcessChunk(It.IsAny<UploadChunkRequest>(), It.IsAny<CancellationToken>()))
//             .Returns(Task.CompletedTask)
//             .Verifiable();
//         var archiveServiceMock = new Mock<IArchiveService>();
//         DataChunkDetails? chunk = null;
//         archiveServiceMock
//             .Setup(a => a.AddChunkToFile(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DataChunkDetails>(),
//                 It.IsAny<CancellationToken>()))
//             .Callback<string, string, DataChunkDetails, CancellationToken>((_, _, c, _) => chunk = c)
//             .Returns(Task.CompletedTask);
//
//         var processor = new ChunkedEncryptingFileProcessor(
//             ctxMock.Object,
//             awsConfig,
//             aesMock.Object,
//             mediatorMock.Object,
//             archiveServiceMock.Object);
//
//         // Act
//         var result = await processor.ProcessFileAsync("run1", _tempFile, CancellationToken.None);
//
//         // Assert
//         // Full file hash should equal SHA256 of content
//         using var sha = SHA256.Create();
//         var expectedHash = sha.ComputeHash(content);
//         Assert.Equal(expectedHash, result.FullFileHash);
//         Assert.Equal(_tempFile, result.LocalFilePath);
//         Assert.Equal(content.Length, result.OriginalSize);
//
//         // Single chunk recorded
//         Assert.NotNull(chunk);
//         //var chunk = result.Chunks[0];
//         Assert.Equal(0, chunk.ChunkIndex);
//         Assert.Equal(result.CompressedSize, chunk.Size);
//
//         // Ensure chunk file exists
//         Assert.True(File.Exists(chunk.LocalFilePath));
//
//         var request = new UploadChunkRequest("run1", _tempFile, chunk);
//         mediatorMock.Verify(m => m.ProcessChunk(request, It.IsAny<CancellationToken>()), Times.Once);
//     }
//
//     [Fact]
//     public async Task ProcessFileAsync_MultipleChunks_CreatesAllChunks()
//     {
//         // Arrange: create file larger than 1 chunk (3 chunks of size 8)
//         var data = Enumerable.Range(0, 24).Select(i => (byte)i).ToArray();
//         await File.WriteAllBytesAsync(_tempFile, data);
//
//         var awsConfig = new AwsConfiguration(
//             8,
//             "test-bucket", "queue-in", "queue-out",
//             "archive-complete","restore-complete", "restore-errors", "exception",
//             "dynamo-table");
//
//         // Mock context resolver
//         var ctxMock = new Mock<IContextResolver>();
//         ctxMock.Setup(c => c.ReadBufferSize()).Returns(8);
//         ctxMock.Setup(c => c.LocalCacheFolder()).Returns(_cacheDir);
//         var aesKey = Convert.FromBase64String("rShkO4lOEPhNlNJ/LRokw2h4G0HDpe4rMnvG4WGFqwA=");
//
//         var aesMock = new Mock<IAesContextResolver>();
//         aesMock.Setup(a => a.FileEncryptionKey(It.IsAny<CancellationToken>()))
//             .ReturnsAsync(aesKey);
//
//         // Mock mediator
//         var mediatorMock = new Mock<IUploadChunksMediator>();
//         mediatorMock
//             .Setup(m => m.ProcessChunk(It.IsAny<UploadChunkRequest>(),
//                 It.IsAny<CancellationToken>()))
//             .Returns(Task.CompletedTask);
//         var archiveServiceMock = new Mock<IArchiveService>();
//         List<DataChunkDetails> chunkList = [];
//         archiveServiceMock
//             .Setup(a => a.AddChunkToFile(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DataChunkDetails>(),
//                 It.IsAny<CancellationToken>()))
//             .Callback<string, string, DataChunkDetails, CancellationToken>((_, _, c, _) => chunkList.Add(c))
//             .Returns(Task.CompletedTask);
//         
//         var processor = new ChunkedEncryptingFileProcessor(
//             ctxMock.Object,
//             awsConfig,
//             aesMock.Object,
//             mediatorMock.Object,
//             archiveServiceMock.Object);
//
//         // Act
//         var result = await processor.ProcessFileAsync("run2", _tempFile, CancellationToken.None);
//
//         // Assert
//         Assert.Equal(3, chunkList.Count);
//         for (var i = 0; i < 3; i++)
//         {
//             var chunk = chunkList[i];
//             Assert.Equal(i, chunk.ChunkIndex);
//             Assert.Equal(32, chunk.Size);
//             Assert.True(File.Exists(chunk.LocalFilePath));
//         }
//
//         // Verify mediator called 3 times
//         mediatorMock.Verify(
//             m => m.ProcessChunk(It.IsAny<UploadChunkRequest>(), It.IsAny<CancellationToken>()),
//             Times.Exactly(3));
//     }
// }