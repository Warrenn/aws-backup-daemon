using System.Security.Cryptography;
using Amazon.S3.Model;
using aws_backup;
using Moq;

namespace test;

public class ChunkedEncryptAndReconstructTests
{
    [Fact]
    public async Task EndToEnd_MultipleChunks_RestoresExactData()
    {
        // 1) Prepare a 10 000 byte file
        var tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var original = Enumerable.Range(0, 10_000).Select(i => (byte)(i % 256)).ToArray();
        await File.WriteAllBytesAsync(tempFile, original);

        // 2) Set up the processor
        var cacheDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(cacheDir);

        var ctxProc = new Mock<IContextResolver>();
        ctxProc.Setup(c => c.ReadBufferSize()).Returns(4_096);
        ctxProc.Setup(c => c.LocalCacheFolder()).Returns(cacheDir);

        var aesKey = new byte[32];
        RandomNumberGenerator.Fill(aesKey);
        var aesMock = new Mock<IAesContextResolver>();
        aesMock.Setup(a => a.FileEncryptionKey(It.IsAny<CancellationToken>()))
            .ReturnsAsync(aesKey);

        // A no‐op mediator is fine; we’ll upload manually in the test
        var mediatorMock = new Mock<IUploadChunksMediator>();
        mediatorMock
            .Setup(m => m.ProcessChunk(It.IsAny<UploadChunkRequest>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var awsConfig = new AwsConfiguration(
            3_000L,
            "sqs-enc", "file-enc",
            "test-bucket", "region",
            "queue-in", "queue-out",
            "complete", "complete-errors",
            "restore", "restore-errors", "exception");

        var processor = new ChunkedEncryptingFileProcessor(
            ctxProc.Object,
            awsConfig,
            aesMock.Object,
            mediatorMock.Object,
            Mock.Of<IArchiveService>());

        // 3) Process into chunks
        var result = await processor.ProcessFileAsync("run", tempFile, CancellationToken.None);

        // Sanity check: multiple chunks
        Assert.True(result.Chunks.Length > 1, "Expected more than one chunk");

        // 4) Upload those chunk files manually into an in‐memory S3
        var s3 = new S3Mock().GetObject();
        const string bucket = "test-bucket";
        foreach (var chunk in result.Chunks)
        {
            var key = Path.GetFileName(chunk.LocalFilePath);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                FilePath = chunk.LocalFilePath
            });
        }

        // 5) Set up the reconstructor
        var ctxRec = new Mock<IContextResolver>();
        ctxRec.Setup(c => c.LocalRestoreFolder(It.IsAny<string>()))
            .Returns(Path.GetTempPath());
        ctxRec.Setup(c => c.ReadBufferSize()).Returns(4096);
        ctxRec.Setup(c => c.NoOfConcurrentDownloadsPerFile()).Returns(2);

        var factory = new Mock<IAwsClientFactory>();
        factory.Setup(f => f.CreateS3Client(It.IsAny<CancellationToken>()))
            .ReturnsAsync(s3);

        var reconstructor = new S3ChunkedFileReconstructor(
            ctxRec.Object,
            factory.Object,
            aesMock.Object);

        // 6) Build the DownloadFileFromS3Request
        var cloudDetails = result.Chunks.Select(ch => new CloudChunkDetails(
            Path.GetFileName(ch.LocalFilePath),
            bucket,
            ch.ChunkSize,
            ch.HashKey.ToArray()
        )).ToArray();

        var request = new DownloadFileFromS3Request(
            "run",
            Path.GetFileName(tempFile) + ".restored",
            cloudDetails,
            result.OriginalSize
        )
        {
            Checksum = result.FullFileHash
        };

        // 7) Reconstruct
        var (output, _) = await reconstructor.ReconstructAsync(request, CancellationToken.None);

        // 8) Assert exact match
        var restored = await File.ReadAllBytesAsync(output);
        Assert.Equal(original.Length, restored.Length);
        Assert.Equal(original, restored);

        // Cleanup
        File.Delete(tempFile);
        File.Delete(output);
    }


    [Fact]
    public async Task EndToEnd_MultipleChunks_RestoresExactDataSmallerBuffer()
    {
        // 1) Prepare a 10 000 byte file
        var tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var original = Enumerable.Range(0, 10_000).Select(i => (byte)(i % 256)).ToArray();
        await File.WriteAllBytesAsync(tempFile, original);

        // 2) Set up the processor
        var cacheDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(cacheDir);

        var ctxProc = new Mock<IContextResolver>();
        ctxProc.Setup(c => c.ReadBufferSize()).Returns(1096);
        ctxProc.Setup(c => c.LocalCacheFolder()).Returns(cacheDir);

        var aesKey = new byte[32];
        RandomNumberGenerator.Fill(aesKey);
        var aesMock = new Mock<IAesContextResolver>();
        aesMock.Setup(a => a.FileEncryptionKey(It.IsAny<CancellationToken>()))
            .ReturnsAsync(aesKey);

        // A no‐op mediator is fine; we’ll upload manually in the test
        var mediatorMock = new Mock<IUploadChunksMediator>();
        mediatorMock
            .Setup(m => m.ProcessChunk(It.IsAny<UploadChunkRequest>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var awsConfig = new AwsConfiguration(
            3_000L,
            "sqs-enc", "file-enc",
            "test-bucket", "region",
            "queue-in", "queue-out",
            "complete", "complete-errors",
            "restore", "restore-errors", "exception");

        var processor = new ChunkedEncryptingFileProcessor(
            ctxProc.Object,
            awsConfig,
            aesMock.Object,
            mediatorMock.Object,
            Mock.Of<IArchiveService>());

        // 3) Process into chunks
        var result = await processor.ProcessFileAsync("run", tempFile, CancellationToken.None);

        // Sanity check: multiple chunks
        Assert.True(result.Chunks.Length > 1, "Expected more than one chunk");

        // 4) Upload those chunk files manually into an in‐memory S3
        var s3 = new S3Mock().GetObject();
        const string bucket = "test-bucket";
        foreach (var chunk in result.Chunks)
        {
            var key = Path.GetFileName(chunk.LocalFilePath);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                FilePath = chunk.LocalFilePath
            });
        }

        // 5) Set up the reconstructor
        var ctxRec = new Mock<IContextResolver>();
        ctxRec.Setup(c => c.LocalRestoreFolder(It.IsAny<string>()))
            .Returns(Path.GetTempPath());
        ctxRec.Setup(c => c.ReadBufferSize()).Returns(596);
        ctxRec.Setup(c => c.NoOfConcurrentDownloadsPerFile()).Returns(2);

        var factory = new Mock<IAwsClientFactory>();
        factory.Setup(f => f.CreateS3Client(It.IsAny<CancellationToken>()))
            .ReturnsAsync(s3);

        var reconstructor = new S3ChunkedFileReconstructor(
            ctxRec.Object,
            factory.Object,
            aesMock.Object);

        // 6) Build the DownloadFileFromS3Request
        var cloudDetails = result.Chunks.Select(ch => new CloudChunkDetails(
            Path.GetFileName(ch.LocalFilePath),
            bucket,
            ch.ChunkSize,
            ch.HashKey.ToArray()
        )).ToArray();

        var request = new DownloadFileFromS3Request(
            "run",
            Path.GetFileName(tempFile) + ".restored",
            cloudDetails,
            result.OriginalSize
        )
        {
            Checksum = result.FullFileHash
        };

        // 7) Reconstruct
        var (output, _) = await reconstructor.ReconstructAsync(request, CancellationToken.None);

        // 8) Assert exact match
        var restored = await File.ReadAllBytesAsync(output);
        Assert.Equal(original.Length, restored.Length);
        Assert.Equal(original, restored);

        // Cleanup
        File.Delete(tempFile);
        File.Delete(output);
    }
}