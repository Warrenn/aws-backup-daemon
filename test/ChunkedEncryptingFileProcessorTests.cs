using System.Security.Cryptography;
using System.Text;
using aws_backup;
using dotenv.net;
using Moq;

namespace test;

public class ChunkedEncryptingFileProcessorTests : IDisposable
{
    private readonly string _cacheDir;
    private readonly string _tempFile;

    public ChunkedEncryptingFileProcessorTests()
    {
        DotEnv.Load();
        _cacheDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_cacheDir);
        _tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    }

    public void Dispose()
    {
        if (Directory.Exists(_cacheDir)) Directory.Delete(_cacheDir, true);
        if (File.Exists(_tempFile)) File.Delete(_tempFile);
    }

    [Fact]
    public async Task ProcessFileAsync_OneChunk_CreatesSingleChunkAndComputesHash()
    {
        // Arrange: create a small file (less than chunk size)
        var content = Encoding.UTF8.GetBytes("Hello, Chunk!");
        await File.WriteAllBytesAsync(_tempFile, content);

        // Mock context resolver
        var ctxMock = new Mock<IContextResolver>();
        ctxMock.Setup(c => c.ReadBufferSize()).Returns(4); // small buffer
        ctxMock.Setup(c => c.ChunkSizeBytes()).Returns(16); // chunk size bigger than content
        ctxMock.Setup(c => c.LocalCacheFolder()).Returns(_cacheDir);
        var aesKey = Convert.FromBase64String("rShkO4lOEPhNlNJ/LRokw2h4G0HDpe4rMnvG4WGFqwA=");
        ctxMock.Setup(c => c.AesFileEncryptionKey(It.IsAny<CancellationToken>())).ReturnsAsync(aesKey);

        // Mock mediator
        var mediatorMock = new Mock<IUploadChunksMediator>();
        mediatorMock
            .Setup(m => m.ProcessChunk(It.IsAny<UploadChunkRequest>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable();

        var processor = new ChunkedEncryptingFileProcessor(ctxMock.Object, mediatorMock.Object);

        // Act
        var result = await processor.ProcessFileAsync("run1", _tempFile, CancellationToken.None);

        // Assert
        // Full file hash should equal SHA256 of content
        using var sha = SHA256.Create();
        var expectedHash = sha.ComputeHash(content);
        Assert.Equal(expectedHash, result.FullFileHash);
        Assert.Equal(_tempFile, result.LocalFilePath);
        Assert.Equal(content.Length, result.OriginalSize);

        // Single chunk recorded
        Assert.Single(result.Chunks);
        var chunk = result.Chunks[0];
        Assert.Equal(0, chunk.ChunkIndex);
        Assert.Equal(content.Length, chunk.Size);

        // Ensure chunk file exists
        Assert.True(File.Exists(chunk.LocalFilePath));

        var request = new UploadChunkRequest("run1", _tempFile, chunk);
        mediatorMock.Verify(m => m.ProcessChunk(request, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ProcessFileAsync_MultipleChunks_CreatesAllChunks()
    {
        // Arrange: create file larger than 1 chunk (3 chunks of size 8)
        var data = Enumerable.Range(0, 24).Select(i => (byte)i).ToArray();
        await File.WriteAllBytesAsync(_tempFile, data);

        // Mock context resolver
        var ctxMock = new Mock<IContextResolver>();
        ctxMock.Setup(c => c.ReadBufferSize()).Returns(8);
        ctxMock.Setup(c => c.ChunkSizeBytes()).Returns(8);
        ctxMock.Setup(c => c.LocalCacheFolder()).Returns(_cacheDir);
        var aesKey = Convert.FromBase64String("rShkO4lOEPhNlNJ/LRokw2h4G0HDpe4rMnvG4WGFqwA=");
        ctxMock.Setup(c => c.AesFileEncryptionKey(It.IsAny<CancellationToken>())).ReturnsAsync(aesKey);

        // Mock mediator
        var mediatorMock = new Mock<IUploadChunksMediator>();
        mediatorMock
            .Setup(m => m.ProcessChunk(It.IsAny<UploadChunkRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var processor = new ChunkedEncryptingFileProcessor(ctxMock.Object, mediatorMock.Object);

        // Act
        var result = await processor.ProcessFileAsync("run2", _tempFile, CancellationToken.None);

        // Assert
        Assert.Equal(3, result.Chunks.Length);
        for (var i = 0; i < 3; i++)
        {
            var chunk = result.Chunks[i];
            Assert.Equal(i, chunk.ChunkIndex);
            Assert.Equal(8, chunk.Size);
            Assert.True(File.Exists(chunk.LocalFilePath));
        }

        // Verify mediator called 3 times
        mediatorMock.Verify(
            m => m.ProcessChunk(It.IsAny<UploadChunkRequest>(), It.IsAny<CancellationToken>()),
            Times.Exactly(3));
    }
}