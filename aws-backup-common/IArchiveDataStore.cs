namespace aws_backup_common;

public interface IArchiveDataStore
{
    IAsyncEnumerable<RunRequest> GetRunRequests(CancellationToken cancellationToken);
    Task SaveRunRequest(RunRequest request, CancellationToken cancellationToken);
    Task SaveArchiveRun(ArchiveRun run, CancellationToken cancellationToken);
    Task RemoveArchiveRequest(long runId, CancellationToken cancellationToken);
    Task<ArchiveRun?> GetArchiveRun(long runId, CancellationToken cancellationToken);
}