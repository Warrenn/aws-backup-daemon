namespace aws_backup_common;

public interface IRestoreDataStore
{
    public Task<RestoreRun?> LookupRestoreRun(string restoreId, CancellationToken cancellationToken);
    IAsyncEnumerable<RestoreRequest> GetRestoreRequests(CancellationToken cancellationToken);
    Task SaveRestoreRequest(RestoreRequest restoreRequest, CancellationToken cancellationToken);
    Task SaveRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken);
    Task RemoveRestoreRequest(string restoreId, CancellationToken cancellationToken);
    Task SaveRestoreFileMetaData(string restoreId, RestoreFileMetaData restoreFileMeta, CancellationToken cancellationToken);
}