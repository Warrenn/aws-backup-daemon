namespace aws_backup_common;

public interface IRestoreDataStore
{
    public Task<RestoreRun?> LookupRestoreRun(string restoreId, CancellationToken cancellationToken);
    Task SaveRestoreRequest(RestoreRequest restoreRequest, CancellationToken cancellationToken);
    Task SaveRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken);
    Task RemoveRestoreRequest(string restoreId, CancellationToken cancellationToken);
    Task SaveRestoreFileMetaData(string restoreRunRestoreId, RestoreFileMetaData restoreFileMeta, CancellationToken cancellationToken);
    Task SaveRestoreChunkStatus(string restoreId, string filePath, ByteArrayKey chunkKey, S3ChunkRestoreStatus readyToRestore, CancellationToken cancellationToken);
    IAsyncEnumerable<RestoreRequest> GetRestoreRequests(CancellationToken cancellationToken);
}