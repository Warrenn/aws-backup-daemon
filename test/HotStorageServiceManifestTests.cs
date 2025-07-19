using aws_backup_common;
using aws_backup;
using Moq;

namespace test;

public class HotStorageServiceManifestTests
{
    private const string _bucket = "hotstorage-test-bucket";
    private readonly string _key;
    private readonly DataChunkManifest _manifest;
    private readonly S3Service _service;

    public HotStorageServiceManifestTests()
    {
        // Unique S3 key
        _key = $"test/manifest/{Guid.NewGuid():N}.json.gz";
        var s3Mock = new S3Mock();

        // Prepare a manifest with several entries
        _manifest = new DataChunkManifest();
        var random = new Random();
        for (var i = 0; i < 3; i++)
        {
            // random hash bytes
            var hash = new byte[16];
            random.NextBytes(hash);
            var baKey = new ByteArrayKey(hash);
            var details = new CloudChunkDetails(
                $"chunk-{i}.gz",
                _bucket,
                3000,
                0,
                0,
                hash
            );
            _manifest[baKey] = details;
        }

        // Mock IContextResolver
        var ctxMock = new Mock<IContextResolver>();
        var awsConfig = new AwsConfiguration(
            16,
            "sqs-enc", "file-enc",
            _bucket, "region",
            "queue-in", "queue-out",
            "complete", "complete-errors",
            "restore", "restore-errors", "exception");
        ctxMock.Setup(c => c.S3PartSize()).Returns(5 * 1024 * 1024);
        ctxMock.Setup(c => c.HotStorage()).Returns("STANDARD");

        // Mock IAwsClientFactory to real S3 client
        var factoryMock = new Mock<IAwsClientFactory>();
        factoryMock.Setup(f => f.CreateS3Client(It.IsAny<CancellationToken>()))
            .ReturnsAsync(s3Mock.GetObject());

        _service = new S3Service(factoryMock.Object, ctxMock.Object, awsConfig);
    }


    [Fact]
    public async Task UploadAndDownload_Manifest_RoundTripsSuccessfully()
    {
        // Act: upload and download
        await _service.UploadCompressedObject(_key, _manifest, StorageTemperature.Hot, CancellationToken.None);
        var downloaded = await _service.DownloadCompressedObject<DataChunkManifest>(_key, CancellationToken.None);

        // Assert count
        Assert.Equal(_manifest.Count, downloaded.Count);

        // Assert each entry
        foreach (var kv in _manifest)
        {
            Assert.True(downloaded.TryGetValue(kv.Key, out var dlDetails));
            var origDetails = kv.Value;
            Assert.Equal(origDetails.S3Key, dlDetails.S3Key);
            Assert.Equal(origDetails.BucketName, dlDetails.BucketName);
            Assert.True(origDetails.HashKey.AsSpan().SequenceEqual(dlDetails.HashKey));
        }
    }

    [Fact]
    public async Task UploadAndDownload_RestoreManifest_RoundTrips()
    {
        // Arrange
        var s3Mock = new S3Mock();

        var manifest = new S3RestoreChunkManifest();
        var rnd = new Random();
        for (var i = 0; i < 3; i++)
        {
            var keyBytes = new byte[16];
            rnd.NextBytes(keyBytes);
            var bkey = new ByteArrayKey(keyBytes);
            var status = i % 2 == 0
                ? S3ChunkRestoreStatus.PendingDeepArchiveRestore
                : S3ChunkRestoreStatus.ReadyToRestore;
            manifest[bkey] = status;
        }

        var ctx = new Mock<IContextResolver>();
        var awsConfig = new AwsConfiguration(
            16,
            "sqs-enc", "file-enc",
            _bucket, "region",
            "queue-in", "queue-out",
            "complete", "complete-errors",
            "restore", "restore-errors", "exception");
        ctx.Setup(c => c.S3PartSize()).Returns(5 * 1024 * 1024);
        ctx.Setup(c => c.HotStorage()).Returns("STANDARD");

        var factory = new Mock<IAwsClientFactory>();
        factory.Setup(f => f.CreateS3Client(It.IsAny<CancellationToken>()))
            .ReturnsAsync(s3Mock.GetObject());

        var service = new S3Service(factory.Object, ctx.Object, awsConfig);
        var key = $"test/restoremanifest/{Guid.NewGuid():N}.json.gz";

        // Act
        await service.UploadCompressedObject(key, manifest, StorageTemperature.Hot, CancellationToken.None);
        var bytes = s3Mock.GetPart($"{_bucket}/{key}");
        //await using var gzip = new BrotliStream(new MemoryStream(bytes), CompressionMode.Decompress, false);
        // var reader = new StreamReader(gzip);
        // var json = await reader.ReadToEndAsync();
        // Assert.NotEmpty(json);
        
        var downloaded = await service.DownloadCompressedObject<S3RestoreChunkManifest>(key, CancellationToken.None);

        // Assert
        Assert.Equal(manifest.Count, downloaded.Count);
        foreach (var kv in manifest)
        {
            Assert.True(downloaded.TryGetValue(kv.Key, out var ds));
            Assert.Equal(kv.Value, ds);
        }
    }
}