using Amazon.S3;

namespace aws_backup;

public record S3StorageInfo(
    string BucketName,
    string Key,
    S3StorageClass StorageClass
);

public interface IS3Service
{
    Task<bool> RunExists(string runId, CancellationToken cancellationToken);
    Task<ArchiveRun> GetArchive(string runId, CancellationToken cancellationToken);
    Task<bool> RestoreExists(string restoreId, CancellationToken cancellationToken);
    Task<RestoreRun> GetRestoreRun(string restoreId, CancellationToken cancellationToken);
    Task ScheduleDeepArchiveRecovery(CloudChunkDetails cloudChunkDetails, CancellationToken cancellationToken);

    Task<IEnumerable<S3StorageInfo>>
        GetStorageClasses(CancellationToken cancellationToken);
}

public class S3Service : IS3Service
{
    public async Task<bool> RunExists(string runId, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task<ArchiveRun> GetArchive(string runId, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task<bool> RestoreExists(string restoreId, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task<RestoreRun> GetRestoreRun(string restoreId, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task ScheduleDeepArchiveRecovery(CloudChunkDetails cloudChunkDetails,
        CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task<IEnumerable<S3StorageInfo>> GetStorageClasses(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}