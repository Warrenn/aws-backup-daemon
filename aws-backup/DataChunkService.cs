using System.Collections.Concurrent;
using aws_backup_common;

namespace aws_backup;

public interface IDataChunkService
{
    Task<bool> ChunkAlreadyUploaded(DataChunkDetails chunk, CancellationToken cancellationToken);

    Task MarkChunkAsUploaded(DataChunkDetails chunk, long byteIndex, string key, string bucketName,
        CancellationToken cancellationToken);
}

public sealed class DataChunkService(
    ICloudChunkStorage cloudChunkStorage,
    IDataStoreMediator mediator
) : IDataChunkService
{
    private readonly ConcurrentDictionary<ByteArrayKey, DataChunkDetails> _cache = new();

    public async Task<bool> ChunkAlreadyUploaded(DataChunkDetails chunk, CancellationToken cancellationToken)
    {
        var key = new ByteArrayKey(chunk.HashKey);
        if (_cache.ContainsKey(key)) return true;
        var inStorage = await cloudChunkStorage.ContainsKey(key, cancellationToken);
        if (inStorage) _cache.TryAdd(key, chunk);
        return inStorage;
    }

    public async Task MarkChunkAsUploaded(DataChunkDetails chunk, long byteIndex, string s3Key, string bucketName,
        CancellationToken cancellationToken)
    {
        var alreadyUploaded = await ChunkAlreadyUploaded(chunk, cancellationToken);

        if (alreadyUploaded)
            return;

        var hashKey = new ByteArrayKey(chunk.HashKey);
        var cloudChunkDetails = new CloudChunkDetails(
            s3Key,
            bucketName,
            chunk.ChunkSize,
            byteIndex,
            chunk.Size,
            chunk.HashKey);
        var addCloudChunkDetailsCommand = new AddCloudChunkDetailsCommand(
            hashKey,
            cloudChunkDetails);
        await mediator.ExecuteCommand(addCloudChunkDetailsCommand, cancellationToken);
    }
}