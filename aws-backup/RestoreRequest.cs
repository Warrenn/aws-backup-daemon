using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace aws_backup;

public record RestoreRequest(
    string ArchiveRunId,
    string RestorePaths,
    DateTimeOffset RequestedAt);

public interface IRestoreRequestsMediator
{
    Task RestoreBackup(RestoreRequest restoreRequest, CancellationToken cancellationToken);
    Task SaveRunningRequest(CurrentRestoreRequests currentRestoreRequests, CancellationToken cancellationToken);

    IAsyncEnumerable<KeyValuePair<string, CurrentRestoreRequests>> GetRunningRequests(
        CancellationToken cancellationToken);

    IAsyncEnumerable<RestoreRequest> GetRestoreRequests(CancellationToken cancellationToken);
}

[JsonConverter(typeof(JsonDictionaryConverter<RestoreRequest>))]
public class CurrentRestoreRequests : ConcurrentDictionary<string, RestoreRequest>
{
    public static CurrentRestoreRequests Current { get; } = new();
}