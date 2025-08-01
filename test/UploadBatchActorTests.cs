// using System.Threading.Channels;
// using Amazon.S3;
// using aws_backup_common;
// using aws_backup;
// using Microsoft.Extensions.Logging;
// using Moq;
//
// namespace test;
//
// public class UploadBatchActorTests
// {
//     private readonly Mock<IArchiveService> _archiveService = new();
//     private readonly Mock<IAwsClientFactory> _awsClientFactory = new();
//
//     private readonly AwsConfiguration _awsConfig = new(
//         12345,
//         "test-bucket",
//         "sqs-url",
//         "arn:archive-complete",
//         "arn:archive-error",
//         "arn:restore-complete",
//         "arn:restore-error",
//         "arn:exception",
//         "ddb-table"
//     );
//
//     private readonly Mock<IContextResolver> _contextResolver = new();
//     private readonly Mock<IDataChunkService> _dataChunkService = new();
//     private readonly Mock<ILogger<UploadBatchActor>> _logger = new();
//     private readonly Mock<IUploadBatchMediator> _mediator = new();
//     private readonly Mock<IRetryMediator> _retryMediator = new();
//     private readonly S3Mock _s3Mock = new();
//
//     private UploadBatchActor CreateService(Channel<UploadBatch> channel, bool mockException = false)
//     {
//         _contextResolver.Setup(x => x.NoOfConcurrentS3Uploads()).Returns(1);
//         _contextResolver.Setup(x => x.ColdStorage()).Returns(S3StorageClass.Standard);
//         _contextResolver.Setup(x => x.ServerSideEncryption()).Returns(ServerSideEncryptionMethod.AES256);
//         _contextResolver.Setup(x => x.S3PartSize()).Returns(5242880);
//         _contextResolver.Setup(x => x.BatchS3Key(It.IsAny<string>())).Returns("key/path/file.gz");
//         _contextResolver.Setup(x => x.ShutdownTimeoutSeconds()).Returns(2);
//
//         if (mockException)
//             _awsClientFactory.Setup(x => x.CreateS3Client(It.IsAny<CancellationToken>()))
//                 .ThrowsAsync(new Exception("Mocked S3 client failure"));
//         else
//             _awsClientFactory.Setup(x => x.CreateS3Client(It.IsAny<CancellationToken>()))
//                 .ReturnsAsync(_s3Mock.GetObject());
//
//         _mediator.Setup(x => x.GetUploadBatches(It.IsAny<CancellationToken>())).Returns(channel.Reader.ReadAllAsync());
//
//         return new UploadBatchActor(
//             _logger.Object,
//             _mediator.Object,
//             _contextResolver.Object,
//             _retryMediator.Object,
//             _awsClientFactory.Object,
//             _awsConfig,
//             _dataChunkService.Object,
//             _archiveService.Object);
//     }
//
//     [Fact]
//     public async Task UploadFails_ShouldRetryAndRestoreChunks()
//     {
//         var batchFile = Path.GetTempFileName();
//         await File.WriteAllBytesAsync(batchFile, new byte[10]);
//
//         var chunk = new DataChunkDetails(batchFile, 0, 10, [1, 2, 3], 10);
//         var uploadRequest = new UploadChunkRequest("run-1", batchFile, chunk);
//
//         var batch = new UploadBatch(batchFile, "run-1")
//         {
//             Requests = { uploadRequest }
//         };
//         var channel = Channel.CreateUnbounded<UploadBatch>();
//         await channel.Writer.WriteAsync(batch);
//         channel.Writer.Complete();
//
//         var service = CreateService(channel, true);
//         var cts = new CancellationTokenSource();
//         await service.StartAsync(cts.Token);
//         await service.ExecuteTask!;
//
//         _retryMediator.Verify(x => x.RetryAttempt(uploadRequest, It.IsAny<CancellationToken>()), Times.Once);
//         Assert.False(File.Exists(batchFile));
//     }
//
//     [Fact]
//     public async Task UploadSucceeds_ShouldMarkChunksAndDeleteTempFile()
//     {
//         var batchFile = Path.GetTempFileName();
//         await File.WriteAllBytesAsync(batchFile, new byte[10]);
//
//         var chunk = new DataChunkDetails(batchFile, 0, 10, [1, 2, 3], 10);
//         var uploadRequest = new UploadChunkRequest("run-1", batchFile, chunk);
//
//         var batch = new UploadBatch(batchFile, "run-1")
//         {
//             Requests = { uploadRequest },
//             FileSize = 10
//         };
//
//         var channel = Channel.CreateUnbounded<UploadBatch>();
//         await channel.Writer.WriteAsync(batch);
//         channel.Writer.Complete();
//
//         var service = CreateService(channel);
//         var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
//         await service.StartAsync(cts.Token);
//         await service.ExecuteTask!;
//
//         _dataChunkService.Verify(x => x.MarkChunkAsUploaded(
//             It.Is<DataChunkDetails>(d => d.Equals(chunk)),
//             It.IsAny<long>(),
//             It.IsAny<string>(),
//             It.IsAny<string>(),
//             It.IsAny<CancellationToken>()), Times.Once);
//
//         _archiveService.Verify(x => x.RecordChunkUpload(
//             "run-1",
//             batchFile,
//             chunk.HashKey,
//             It.IsAny<CancellationToken>()), Times.Once);
//
//         Assert.False(File.Exists(batchFile));
//     }
//
//     // [Fact]
//     // public async Task WorkerCancellation_ShouldExitGracefully()
//     // {
//     //     var channel = Channel.CreateUnbounded<UploadBatch>();
//     //     var service = CreateService(channel);
//     //
//     //     using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
//     //
//     //     var t = service.StartAsync(cts.Token);
//     //     await t;
//     //     await service.ExecuteTask!;
//     //     Assert.True(cts.IsCancellationRequested);
//     // }
//
//     [Fact]
//     public async Task StopAsync_ShouldWaitForWorkerCompletion()
//     {
//         var channel = Channel.CreateUnbounded<UploadBatch>();
//         channel.Writer.Complete();
//
//         var service = CreateService(channel);
//         await service.StartAsync(CancellationToken.None);
//         await service.ExecuteTask!;
//
//         var stopToken = new CancellationTokenSource(TimeSpan.FromSeconds(2)).Token;
//         await service.StopAsync(stopToken);
//     }
// }