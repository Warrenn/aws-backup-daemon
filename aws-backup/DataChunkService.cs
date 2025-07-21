using aws_backup_common;

namespace aws_backup;

public interface IDataChunkService
{
    Task<bool> ChunkAlreadyUploaded(DataChunkDetails chunk, CancellationToken cancellationToken);

    Task MarkChunkAsUploaded(DataChunkDetails chunk, long byteIndex, string key, string bucketName,
        CancellationToken cancellationToken);
}

public sealed class DataChunkService(
    ICloudChunkStorage cloudChunkStorage
) : IDataChunkService
{
    public async Task<bool> ChunkAlreadyUploaded(DataChunkDetails chunk, CancellationToken cancellationToken)
    {
        var key = new ByteArrayKey(chunk.HashKey);
        return await cloudChunkStorage.ContainsKey(key, cancellationToken);
    }

    public async Task MarkChunkAsUploaded(DataChunkDetails chunk, long byteIndex, string s3Key, string bucketName,
        CancellationToken cancellationToken)
    {
        var hashKey = new ByteArrayKey(chunk.HashKey);
        if (await cloudChunkStorage.ContainsKey(hashKey, cancellationToken))
            // If the chunk is already in the manifest, we don't need to re-add it
            return;
        var cloudChunkDetails = new CloudChunkDetails(
            s3Key,
            bucketName,
            chunk.ChunkSize,
            byteIndex,
            chunk.Size,
            chunk.HashKey);
        await cloudChunkStorage.AddCloudChunkDetails(hashKey, cloudChunkDetails, cancellationToken);
    }
}
