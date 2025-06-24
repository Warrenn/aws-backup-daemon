using System.Security.Cryptography;
using System.Text;
using aws_backup;
using Moq;

namespace test;

public class ChunkedEncryptingFileProcessorTests : IDisposable
{
    private readonly string _cacheDir;
    private readonly string _tempFile;

    public ChunkedEncryptingFileProcessorTests()
    {
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
        ctxMock.Setup(c => c.ResolveReadBufferSize()).Returns(4); // small buffer
        ctxMock.Setup(c => c.ResolveChunkSizeBytes()).Returns(16); // chunk size bigger than content
        ctxMock.Setup(c => c.ResolveCacheFolder()).Returns(_cacheDir);
        var aesKey = new byte[32];
        RandomNumberGenerator.Fill(aesKey);
        ctxMock.Setup(c => c.ResolveAesKey(It.IsAny<CancellationToken>())).ReturnsAsync(aesKey);

        // Mock mediator
        var mediatorMock = new Mock<IMediator>();
        mediatorMock
            .Setup(m => m.ProcessChunk(It.IsAny<(string, string, DataChunkDetails)>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask)
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

        var tuple = ("run1", _tempFile, chunk);
        mediatorMock.Verify(m => m.ProcessChunk(tuple, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ProcessFileAsync_MultipleChunks_CreatesAllChunks()
    {
        // Arrange: create file larger than 1 chunk (3 chunks of size 8)
        var data = Enumerable.Range(0, 24).Select(i => (byte)i).ToArray();
        await File.WriteAllBytesAsync(_tempFile, data);

        // Mock context resolver
        var ctxMock = new Mock<IContextResolver>();
        ctxMock.Setup(c => c.ResolveReadBufferSize()).Returns(8);
        ctxMock.Setup(c => c.ResolveChunkSizeBytes()).Returns(8);
        ctxMock.Setup(c => c.ResolveCacheFolder()).Returns(_cacheDir);
        var aesKey = new byte[32];
        RandomNumberGenerator.Fill(aesKey);
        ctxMock.Setup(c => c.ResolveAesKey(It.IsAny<CancellationToken>())).ReturnsAsync(aesKey);

        // Mock mediator
        var mediatorMock = new Mock<IMediator>();
        mediatorMock
            .Setup(m => m.ProcessChunk(It.IsAny<(string, string, DataChunkDetails)>(),
                It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

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
            m => m.ProcessChunk(It.IsAny<(string, string, DataChunkDetails)>(), It.IsAny<CancellationToken>()),
            Times.Exactly(3));
    }
}