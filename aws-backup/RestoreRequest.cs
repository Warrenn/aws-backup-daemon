using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using aws_backup_common;

namespace aws_backup;


public interface IRestoreRequestsMediator
{
    Task RestoreBackup(RestoreRequest restoreRequest, CancellationToken cancellationToken);
    Task SaveRunningRequest(CurrentRestoreRequests currentRestoreRequests, CancellationToken cancellationToken);

    IAsyncEnumerable<S3LocationAndValue<CurrentRestoreRequests>> GetRunningRequests(
        CancellationToken cancellationToken);

    IAsyncEnumerable<RestoreRequest> GetRestoreRequests(CancellationToken cancellationToken);
}
