using Amazon.S3;
using aws_backup_common;
using Moq;

namespace test;

public class HotStorageServiceIntegrationTests
{
    private const string _bucket = "hotstorage-test-bucket";
    private readonly string _key;
    private readonly ArchiveRun _original;
    private readonly S3Service _service;

    public HotStorageServiceIntegrationTests()
    {
        var s3Mock = new S3Mock();
        // Pick a unique key per run
        _key = $"test/archiverun/{Guid.NewGuid():N}.bin";

        // Build a fully populated ArchiveRun
        _original = new ArchiveRun
        {
            RunId = Guid.NewGuid().ToString(),
            PathsToArchive = "/tmp/foo",
            CronSchedule = "*/5 * * * *",
            Status = ArchiveRunStatus.Processing,
            CreatedAt = TimeProvider.System.GetUtcNow(),
            CompletedAt = TimeProvider.System.GetUtcNow().AddMinutes(1),
            OriginalSize = 123_456,
            CompressedSize = 78_901,
            TotalFiles = 2,
            TotalSkippedFiles = 1,
            Files =
            {
                // two file entries
                ["/tmp/foo/a.txt"] = new FileMetaData("/tmp/foo/a.txt")
                {
                    Status = FileStatus.Added,
                    Chunks =
                    {
                        [new ByteArrayKey([9, 10, 11])] =
                            new DataChunkDetails("a.chunk0", 0, 3000, [9, 10, 11], 2000, [])
                    },
                    LastModified = TimeProvider.System.GetUtcNow(),
                    Created = TimeProvider.System.GetUtcNow().AddDays(-1),
                    CompressedSize = 1000,
                    OriginalSize = 1000,
                    AclEntries = [new AclEntry("alice", "rwx", "Allow")],
                    Owner = "alice",
                    Group = "staff",
                    HashKey = [1, 2, 3, 4]
                },
                ["/tmp/foo/b.txt"] = new FileMetaData("/tmp/foo/b.txt")
                {
                    CompressedSize = 1500,
                    OriginalSize = 3000,
                    LastModified = TimeProvider.System.GetUtcNow(),
                    Created = TimeProvider.System.GetUtcNow().AddDays(-2),
                    AclEntries = [new AclEntry("bob", "rw-", "Allow")],
                    Owner = "bob",
                    Group = "users",
                    Status = FileStatus.UploadComplete,
                    HashKey = [5, 6, 7, 8],
                    Chunks =
                    {
                        [new ByteArrayKey([8, 9, 10])] = new DataChunkDetails("b.chunk0", 0, 3000, [8, 9, 10], 3000, [])
                    }
                }
            }
        };

        // Mock IContextResolver
        var ctx = new Mock<IContextResolver>();
        var awsConfig = new AwsConfiguration(
            16,
            "sqs-enc", "file-enc",
            _bucket, "region",
            "queue-in", "queue-out",
            "complete", "complete-errors",
            "restore", "restore-errors", "exception");

        ctx.Setup(c => c.S3PartSize()).Returns(5 * 1024 * 1024);
        ctx.Setup(c => c.HotStorage()).Returns(S3StorageClass.Standard);
        // other methods not used by HotStorageService.Upload/Download

        // Mock IAwsClientFactory to return a real AmazonS3Client
        var factory = new Mock<IAwsClientFactory>();
        factory.Setup(f => f.CreateS3Client(It.IsAny<CancellationToken>()))
            .ReturnsAsync(s3Mock.GetObject());
        _service = new S3Service(factory.Object, ctx.Object, awsConfig);
    }

    [Fact]
    public async Task UploadAndDownload_ArchiveRun_RoundTrips()
    {
        // Act
        await _service.UploadCompressedObject(_key, _original, StorageTemperature.Hot, CancellationToken.None);
        var downloaded = await _service.DownloadCompressedObject<ArchiveRun>(_key, CancellationToken.None);

        // Assert top-level props
        Assert.Equal(_original.RunId, downloaded.RunId);
        Assert.Equal(_original.PathsToArchive, downloaded.PathsToArchive);
        Assert.Equal(_original.CronSchedule, downloaded.CronSchedule);
        Assert.Equal(_original.Status, downloaded.Status);
        Assert.Equal(_original.CreatedAt, downloaded.CreatedAt);
        Assert.Equal(_original.CompletedAt, downloaded.CompletedAt);
        Assert.Equal(_original.OriginalSize, downloaded.OriginalSize);
        Assert.Equal(_original.CompressedSize, downloaded.CompressedSize);
        Assert.Equal(_original.TotalFiles, downloaded.TotalFiles);
        Assert.Equal(_original.TotalSkippedFiles, downloaded.TotalSkippedFiles);

        // Assert Files dictionary
        Assert.Equal(_original.Files.Count, downloaded.Files.Count);
        foreach (var kv in _original.Files)
        {
            Assert.True(downloaded.Files.ContainsKey(kv.Key), $"Missing key {kv.Key}");
            var origMeta = kv.Value;
            var dlMeta = downloaded.Files[kv.Key];

            Assert.Equal(origMeta.LocalFilePath, dlMeta.LocalFilePath);
            Assert.Equal(origMeta.CompressedSize, dlMeta.CompressedSize);
            Assert.Equal(origMeta.OriginalSize, dlMeta.OriginalSize);
            Assert.Equal(origMeta.LastModified, dlMeta.LastModified);
            Assert.Equal(origMeta.Created, dlMeta.Created);
            Assert.Equal(origMeta.Status, dlMeta.Status);
            Assert.Equal(origMeta.HashKey, dlMeta.HashKey);

            // AclEntries
            Assert.Equal(origMeta.AclEntries.Length, dlMeta.AclEntries.Length);
            for (var i = 0; i < origMeta.AclEntries.Length; i++)
                Assert.Equal(origMeta.AclEntries[i], dlMeta.AclEntries[i]);

            // Chunks
            Assert.Equal(origMeta.Chunks.Values.Count, dlMeta.Chunks.Count);
            for (var i = 0; i < origMeta.Chunks.Count; i++)
            {
                var key = origMeta.Chunks.Keys.ElementAt(i);
                var exp = origMeta.Chunks[key];
                var act = dlMeta.Chunks[key];

                Assert.Equal(exp.LocalFilePath, act.LocalFilePath);
                Assert.Equal(exp.ChunkIndex, act.ChunkIndex);
                Assert.Equal(exp.Size, act.Size);
                Assert.True(exp.HashKey.AsSpan().SequenceEqual(act.HashKey));
            }
        }
    }
}