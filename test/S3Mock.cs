using System.Collections.Concurrent;
using System.Net;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Moq;

namespace test;

public class S3Mock
{
    // in‐memory per‐upload part store: uploadId -> (partNumber -> bytes)
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, byte[]>> _partStore = new();
    private readonly Mock<IAmazonS3> _s3Mock = new();
    private readonly ConcurrentDictionary<string, byte[]> _store = new();

    public IAmazonS3 GetObject()
    {
        SetupMock();
        return _s3Mock.Object;
    }
    public Mock<IAmazonS3> GetMock()
    {
        return _s3Mock;
    }

    private void SetupMock()
    {
        _s3Mock.SetupGet(s3 => s3.Config)
            .Returns(new AmazonS3Config
            {
                RegionEndpoint = RegionEndpoint.USEast1, // Use any region you prefer
                ForcePathStyle = true // Use path-style URLs for local testing
            });
        // 1) InitiateMultipartUploadAsync
        _s3Mock
            .Setup(s => s.InitiateMultipartUploadAsync(
                It.IsAny<InitiateMultipartUploadRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((InitiateMultipartUploadRequest req, CancellationToken _) =>
            {
                var uploadId = Guid.NewGuid().ToString();
                _partStore[uploadId] = new ConcurrentDictionary<int, byte[]>();
                return new InitiateMultipartUploadResponse
                {
                    BucketName = req.BucketName,
                    Key = req.Key,
                    UploadId = uploadId,
                    HttpStatusCode = HttpStatusCode.OK
                };
            });

        // 2) UploadPartAsync
        _s3Mock
            .Setup(s => s.UploadPartAsync(
                It.IsAny<UploadPartRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns<UploadPartRequest, CancellationToken>(async (req, ct) =>
            {
                // read the data for this part
                await using var ms = new MemoryStream();
                await req.InputStream.CopyToAsync(ms, ct);
                var bytes = ms.ToArray();

                // store by uploadId + part number
                _partStore[req.UploadId][req.PartNumber ?? 0] = bytes;

                return new UploadPartResponse
                {
                    ETag = $"\"etag-part-{req.PartNumber}\"",
                    PartNumber = req.PartNumber,
                    HttpStatusCode = HttpStatusCode.OK
                };
            });

        // 3) CompleteMultipartUploadAsync
        _s3Mock
            .Setup(s => s.CompleteMultipartUploadAsync(
                It.IsAny<CompleteMultipartUploadRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((CompleteMultipartUploadRequest req, CancellationToken _) =>
            {
                // concatenate parts in ascending partNumber
                var parts = _partStore[req.UploadId]
                    .OrderBy(kv => kv.Key)
                    .Select(kv => kv.Value)
                    .ToArray();

                var full = parts.Aggregate(new MemoryStream(), (ms, chunk) =>
                {
                    ms.Write(chunk, 0, chunk.Length);
                    return ms;
                }).ToArray();

                // store in global store under bucket/key
                _store[$"{req.BucketName}/{req.Key}"] = full;

                return new CompleteMultipartUploadResponse
                {
                    BucketName = req.BucketName,
                    Key = req.Key,
                    HttpStatusCode = HttpStatusCode.OK
                };
            });

        // 4) AbortMultipartUploadAsync (no‐op)
        _s3Mock
            .Setup(s => s.AbortMultipartUploadAsync(
                It.IsAny<AbortMultipartUploadRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AbortMultipartUploadResponse
            {
                HttpStatusCode = HttpStatusCode.NoContent
            });

        // 5) PutObjectAsync for small objects
        _s3Mock
            .Setup(s => s.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns<PutObjectRequest, CancellationToken>(async (req, ct) =>
            {
                await using var ms = new MemoryStream();
                if (req.InputStream != null)
                    await req.InputStream.CopyToAsync(ms, ct);
                else if (!string.IsNullOrEmpty(req.FilePath))
                    await using (var fs = File.OpenRead(req.FilePath))
                    {
                        await fs.CopyToAsync(ms, ct);
                    }

                _store[$"{req.BucketName}/{req.Key}"] = ms.ToArray();
                return new PutObjectResponse
                {
                    HttpStatusCode = HttpStatusCode.OK
                };
            });

        // 6) GetObjectAsync
        _s3Mock
            .Setup(s => s.GetObjectAsync(
                It.IsAny<GetObjectRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns<GetObjectRequest, CancellationToken>((req, ct) =>
            {
                var k = $"{req.BucketName}/{req.Key}";
                if (!_store.TryGetValue(k, out var data))
                    throw new AmazonS3Exception("NotFound")
                    {
                        StatusCode = HttpStatusCode.NotFound
                    };
                return Task.FromResult(new GetObjectResponse
                {
                    BucketName = req.BucketName,
                    Key = req.Key,
                    HttpStatusCode = HttpStatusCode.OK,
                    ResponseStream = new MemoryStream(data)
                });
            });
        
        _s3Mock
            .Setup(s => s.PutObjectTaggingAsync(It.IsAny<PutObjectTaggingRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PutObjectTaggingResponse());
        
        _s3Mock
            .Setup(s3 => s3.GetObjectTaggingAsync(It.IsAny<GetObjectTaggingRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetObjectTaggingResponse
            {
                Tagging =
                [
                    new Tag { Key = "Environment", Value = "Test" },
                    new Tag { Key = "Owner", Value = "Alice" }
                ]
            });
    }
}