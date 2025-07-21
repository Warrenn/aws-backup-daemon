namespace aws_backup_common;

public interface ICloudChunkStorage
{
    Task<bool> ContainsKey(ByteArrayKey key, CancellationToken cancellationToken);

    Task AddCloudChunkDetails(ByteArrayKey hashKey, CloudChunkDetails cloudChunkDetails,
        CancellationToken cancellationToken);
    
    Task<CloudChunkDetails> GetCloudChunkDetails(
        ByteArrayKey hashKey,
        CancellationToken cancellationToken);
}