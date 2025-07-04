using aws_backup;
using Moq;

namespace test;

public class HotStorageServiceRestoreRunTests
{
    private const string Bucket = "hotstorage-test-bucket";
    private readonly string _key;
    private readonly RestoreRun _original;
    private readonly HotStorageService _service;

    public HotStorageServiceRestoreRunTests()
    {
        _key = $"test/restorerun/{Guid.NewGuid():N}.json.gz";

        var s3Mock = new S3Mock();

        // Build full RestoreRun
        _original = new RestoreRun
        {
            RestoreId = Guid.NewGuid().ToString(),
            RestorePaths = "/tmp/data",
            ArchiveRunId = "archive-123",
            Status = RestoreRunStatus.Processing,
            RequestedAt = DateTimeOffset.UtcNow
        };
        _original.CompletedAt = DateTimeOffset.UtcNow.AddHours(2);

        // Populate RequestedFiles
        var rnd = new Random();
        for (var i = 1; i <= 2; i++)
        {
            var chunkHashes = Enumerable.Range(0, 3)
                .Select(_ =>
                {
                    var b = new byte[16];
                    rnd.NextBytes(b);
                    return new ByteArrayKey(b);
                })
                .ToArray();

            var meta = new RestoreFileMetaData(
                chunkHashes,
                $"/tmp/data/file{i}.txt",
                i * 1000L
            )
            {
                Created = DateTimeOffset.UtcNow.AddDays(-i),
                LastModified = DateTimeOffset.UtcNow.AddDays(-i / 2.0),
                AclEntries = new[] { new AclEntry("user", "r--", "Allow") },
                Owner = $"user{i}",
                Group = $"group{i}",
                Checksum = [1, 2, 3, 4, 5],
                Status = FileRestoreStatus.PendingS3Download
            };
            _original.RequestedFiles[meta.FilePath] = meta;
        }

        // Populate FailedFiles
        _original.FailedFiles["/tmp/data/file3.txt"] = "Network error";
        _original.FailedFiles["/tmp/data/file4.txt"] = "Checksum mismatch";

        // Mocks
        var ctxMock = new Mock<IContextResolver>();
        ctxMock.Setup(c => c.S3BucketId()).Returns(Bucket);
        ctxMock.Setup(c => c.S3PartSize()).Returns(5 * 1024 * 1024);
        ctxMock.Setup(c => c.HotStorage()).Returns("STANDARD");

        var factoryMock = new Mock<IAwsClientFactory>();
        factoryMock.Setup(f => f.CreateS3Client(It.IsAny<CancellationToken>()))
            .ReturnsAsync(s3Mock.GetObject());

        _service = new HotStorageService(factoryMock.Object, ctxMock.Object);
    }

    [Fact]
    public async Task UploadAndDownload_RestoreRun_RoundTrips()
    {
        // Act
        await _service.UploadAsync(_key, _original, CancellationToken.None);
        var downloaded = await _service.DownloadAsync<RestoreRun>(_key, CancellationToken.None);

        // Assert simple props
        Assert.Equal(_original.RestoreId, downloaded.RestoreId);
        Assert.Equal(_original.RestorePaths, downloaded.RestorePaths);
        Assert.Equal(_original.ArchiveRunId, downloaded.ArchiveRunId);
        Assert.Equal(_original.Status, downloaded.Status);
        Assert.Equal(_original.RequestedAt, downloaded.RequestedAt);
        Assert.Equal(_original.CompletedAt, downloaded.CompletedAt);

        // Assert dictionaries
        Assert.Equal(_original.RequestedFiles.Count, downloaded.RequestedFiles.Count);
        foreach (var kv in _original.RequestedFiles)
        {
            Assert.True(downloaded.RequestedFiles.TryGetValue(kv.Key, out var dl));
            var orig = kv.Value;
            Assert.Equal(orig.FilePath, dl.FilePath);
            Assert.Equal(orig.Size, dl.Size);
            Assert.Equal(orig.Status, dl.Status);

            Assert.Equal(orig.Chunks.Length, dl.Chunks.Length);
            for (var i = 0; i < orig.Chunks.Length; i++)
                Assert.Equal(orig.Chunks[i], dl.Chunks[i]);

            Assert.Equal(orig.Created, dl.Created);
            Assert.Equal(orig.LastModified, dl.LastModified);

            Assert.Equal(orig.Owner, dl.Owner);
            Assert.Equal(orig.Group, dl.Group);

            Assert.Equal(orig.AclEntries.Length, dl.AclEntries.Length);
            for (var i = 0; i < orig.AclEntries.Length; i++)
                Assert.Equal(orig.AclEntries[i], dl.AclEntries[i]);

            Assert.True(orig.Checksum.AsSpan().SequenceEqual(dl.Checksum));
        }

        Assert.Equal(_original.FailedFiles.Count, downloaded.FailedFiles.Count);
        foreach (var kv in _original.FailedFiles)
            Assert.Equal(kv.Value, downloaded.FailedFiles[kv.Key]);
    }
}