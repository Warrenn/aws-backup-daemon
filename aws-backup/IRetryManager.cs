public interface IRetryManager
{
    Task ClearPendingRetries(string archive, CancellationToken cancellationToken = default);

    Task<(string filePath, Exception exception)[]> GetFailedRetries(string archive,
        CancellationToken cancellationToken = default);
}