using System.Reflection;
using System.Security.Cryptography;
using System.Threading.Channels;
using Amazon.S3;
using Amazon.S3.Model;
using aws_backup;
using Microsoft.Extensions.Logging;
using Moq;

namespace test;

public class UploadChunkDataOrchestrationTests
{
    private readonly Mock<IArchiveService> _archiveSvc = new();
    private readonly Mock<IAwsClientFactory> _awsFactory = new();
    private readonly Mock<IDataChunkService> _chunkSvc = new();
    private readonly Mock<IContextResolver> _ctx = new();
    private readonly Mock<ILogger<UploadChunkDataOrchestration>> _logger = new();
    private readonly Mock<IUploadChunksMediator> _mediator = new();
    private readonly Mock<IRetryMediator> _retryMed = new();

    // weave together an orchestration whose WorkerLoopAsync we can call
    private UploadChunkDataOrchestration CreateOrch(Channel<UploadChunkRequest> chan)
    {
        _mediator
            .Setup(m => m.GetChunks(It.IsAny<CancellationToken>()))
            .Returns(chan.Reader.ReadAllAsync());

        // concurrency = 1 so we only spin up one worker
        _ctx.Setup(c => c.NoOfS3FilesToUploadConcurrently()).Returns(1);
        _ctx.Setup(c => c.UploadAttemptLimit()).Returns(3);

        return new UploadChunkDataOrchestration(
            _mediator.Object,
            _logger.Object,
            _awsFactory.Object,
            _ctx.Object,
            _chunkSvc.Object,
            _archiveSvc.Object,
            _retryMed.Object
        );
    }

    private MethodInfo GetWorker()
    {
        return typeof(UploadChunkDataOrchestration)
            .GetMethod("WorkerLoopAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
    }

    [Fact]
    public async Task Skip_WhenNotRequired_DeletesLocalFileAndNoUpload()
    {
        // arrange a tiny temp file
        var temp = Path.GetTempFileName();
        File.WriteAllText(temp, "hello");

        var chunk = new DataChunkDetails(temp, 0, 10, [1, 2], 5);
        var req = new UploadChunkRequest("run1", "file1", chunk);
        var chan = Channel.CreateUnbounded<UploadChunkRequest>();
        await chan.Writer.WriteAsync(req);
        chan.Writer.Complete();

        // chunkRequiresUpload = false, IsTheFileSkipped = false => skip branch
        _chunkSvc.Setup(s => s.ChunkRequiresUpload(chunk)).Returns(false);
        _archiveSvc.Setup(a => a.IsTheFileSkipped("file1")).Returns(false);

        var orch = CreateOrch(chan);
        var worker = GetWorker();

        // act
        await (Task)worker.Invoke(orch, new object[] { CancellationToken.None });

        // file should be deleted
        Assert.False(File.Exists(temp));

        // no AWS or retry invoked
        _awsFactory.VerifyNoOtherCalls();
        _retryMed.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Success_UploadsThenMarksUploadedAndDeletesFile()
    {
        // arrange file
        var temp = Path.GetTempFileName();
        File.WriteAllText(temp, "data");
        var chunk = new DataChunkDetails(temp, 0, 10, [], Size: 4);
        var req = new UploadChunkRequest("run2", "file2", chunk);
        var chan = Channel.CreateUnbounded<UploadChunkRequest>();
        await chan.Writer.WriteAsync(req);
        chan.Writer.Complete();

        _chunkSvc.Setup(s => s.ChunkRequiresUpload(chunk)).Returns(true);
        _archiveSvc.Setup(a => a.IsTheFileSkipped("file2")).Returns(false);

        // fake S3 client
        var s3 = new S3Mock();
        
        // let TransferUtility.UploadAsync succeed by mocking PutObjectAsync
        var correctHash = ComputeLocalBase64(temp);
        
        s3.GetMock().Setup(s => s.GetObjectMetadataAsync("bucket", It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetObjectMetadataResponse { ChecksumSHA256 = correctHash });

        _awsFactory.Setup(f => f.CreateS3Client(It.IsAny<CancellationToken>()))
            .ReturnsAsync(s3.GetObject());

        // context
        _ctx.Setup(c => c.S3BucketId()).Returns("bucket");
        _ctx.Setup(c => c.ColdStorage()).Returns("REDUCED_REDUNDANCY");
        _ctx.Setup(c => c.ServerSideEncryptionMethod()).Returns(ServerSideEncryptionMethod.AES256);
        _ctx.Setup(c => c.S3PartSize()).Returns(5);
        _ctx.Setup(c => c.ChunkS3Key(It.IsAny<string>(), 0, 10, It.IsAny<byte[]>(), 10))
            .Returns("the-key");

        var orch = CreateOrch(chan);
        var worker = GetWorker();

        // act
        await (Task)worker.Invoke(orch, new object[] { CancellationToken.None });

        // Verify MarkChunkAsUploaded called
        _chunkSvc.Verify(s => s.MarkChunkAsUploaded(chunk, "the-key", "bucket", It.IsAny<CancellationToken>()),
            Times.Once);
        // file deleted
        Assert.False(File.Exists(temp));
        // no retry
        _retryMed.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ChecksumMismatch_TriggersRetryAndRetryDelegateAndLimitExceededDelegate()
    {
        // arrange file
        var temp = Path.GetTempFileName();
        File.WriteAllText(temp, "abc");
        var chunk = new DataChunkDetails(temp, 1, 10, [], Size: 3);
        var req = new UploadChunkRequest("run3", "file3", chunk);
        var chan = Channel.CreateUnbounded<UploadChunkRequest>();
        await chan.Writer.WriteAsync(req);
        chan.Writer.Complete();

        _chunkSvc.Setup(s => s.ChunkRequiresUpload(chunk)).Returns(true);
        _archiveSvc.Setup(a => a.IsTheFileSkipped("file3")).Returns(false);

        // S3 client returns wrong checksum
        var s3 = new Mock<IAmazonS3>();
        s3.Setup(s => s.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PutObjectResponse());
        s3.Setup(s => s.GetObjectMetadataAsync("bucket", It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetObjectMetadataResponse
                { ChecksumSHA256 = Convert.ToBase64String(new byte[] { 9, 9, 9 }) });

        _awsFactory.Setup(f => f.CreateS3Client(It.IsAny<CancellationToken>()))
            .ReturnsAsync(s3.Object);

        // context
        _ctx.Setup(c => c.S3BucketId()).Returns("bucket");
        _ctx.Setup(c => c.ColdStorage()).Returns("CLASS");
        _ctx.Setup(c => c.ServerSideEncryptionMethod()).Returns(ServerSideEncryptionMethod.AES256);
        _ctx.Setup(c => c.S3PartSize()).Returns(5);
        _ctx.Setup(c => c.ChunkS3Key(It.IsAny<string>(), 1, 10, It.IsAny<byte[]>(), 10))
            .Returns("k3");

        bool retryCalled = false, limitExceededCalled = false;
        // when RetryAttempt is invoked we expect the request.Retry delegate to call mediator.ProcessChunk
        _mediator.Setup(m => m.ProcessChunk(req, It.IsAny<CancellationToken>()))
            .Callback(() => retryCalled = true)
            .Returns(Task.CompletedTask);

        // capture RecordFailedFile when LimitExceeded invoked eventually
        _archiveSvc
            .Setup(a => a.RecordFailedFile("run3", "file3", It.IsAny<Exception>(), It.IsAny<CancellationToken>()))
            .Callback(() => limitExceededCalled = true)
            .Returns(Task.CompletedTask);

        var orch = CreateOrch(chan);
        var worker = GetWorker();

        // act
        await (Task)worker.Invoke(orch, new object[] { CancellationToken.None });

        // retry attempt should be signaled
        _retryMed.Verify(r => r.RetryAttempt(req, It.IsAny<CancellationToken>()), Times.Once);
        // check the delegate wiring: invoking retry calls mediator.ProcessChunk
        await req.Retry!(req, CancellationToken.None);
        Assert.True(retryCalled);

        // now invoking LimitExceeded should call RecordFailedFile
        await req.LimitExceeded!(req, CancellationToken.None);
        Assert.True(limitExceededCalled);
    }

    [Fact]
    public async Task UploadThrows_Exception_TriggersRetry()
    {
        // arrange file
        var temp = Path.GetTempFileName();
        File.WriteAllText(temp, "xyz");
        var chunk = new DataChunkDetails(temp, 2, 10, [], Size: 3);
        var req = new UploadChunkRequest("run4", "file4", chunk);
        var chan = Channel.CreateUnbounded<UploadChunkRequest>();
        await chan.Writer.WriteAsync(req);
        chan.Writer.Complete();

        _chunkSvc.Setup(s => s.ChunkRequiresUpload(chunk)).Returns(true);
        _archiveSvc.Setup(a => a.IsTheFileSkipped("file4")).Returns(false);

        // s3Client.Create throws
        var s3 = new Mock<IAmazonS3>();
        _awsFactory.Setup(f => f.CreateS3Client(It.IsAny<CancellationToken>()))
            .ReturnsAsync(s3.Object);

        _ctx.Setup(c => c.S3BucketId()).Returns("bucket");
        // on instantiation of TransferUtility, UploadAsync will throw
        // easiest: give s3Client.PutObjectAsync throw when called by TransferUtility
        s3.Setup(s => s.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("network"));

        // retry mediator capture
        var retried = false;
        _mediator.Setup(m => m.ProcessChunk(req, It.IsAny<CancellationToken>()))
            .Callback(() => retried = true)
            .Returns(Task.CompletedTask);

        var orch = CreateOrch(chan);
        var worker = GetWorker();

        // act
        await (Task)worker.Invoke(orch, new object[] { CancellationToken.None });

        // RetryAttempt must be invoked
        _retryMed.Verify(r => r.RetryAttempt(req, It.IsAny<CancellationToken>()), Times.Once);
        // delegate wiring: invoking Retry must call mediator.ProcessChunk
        await req.Retry!(req, CancellationToken.None);
        Assert.True(retried);
    }

    private static string ComputeLocalBase64(string path)
    {
        using var s = File.OpenRead(path);
        return Convert.ToBase64String(SHA256.HashData(s));
    }
}