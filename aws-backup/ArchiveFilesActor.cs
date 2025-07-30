using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IArchiveFileMediator
{
    IAsyncEnumerable<ArchiveFileRequest> GetArchiveFiles(CancellationToken cancellationToken);
    Task ProcessFile(ArchiveFileRequest request, CancellationToken cancellationToken);
}

public sealed record ArchiveFileRequest(
    string RunId,
    string FilePath
) : RetryState;

public sealed class ArchiveFilesActor(
    IFileCountDownEvent fileCountDownEvent,
    IArchiveFileMediator mediator,
    IRetryMediator retryMediator,
    IChunkedEncryptingFileProcessor processor,
    IArchiveService archiveService,
    IDataStoreMediator dataStoreMediator,
    ILogger<ArchiveFilesActor> logger,
    IContextResolver contextResolver)
    : BackgroundService
{
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("ArchiveFilesActor started");
        var concurrency = contextResolver.NoOfFilesToBackupConcurrently();

        _workers = new Task[concurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        // Return a task that completes when all workers finish
        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var request in mediator.GetArchiveFiles(cancellationToken))
            try
            {
                request.Retry ??= (r, ct) => mediator.ProcessFile((ArchiveFileRequest)r, ct);
                request.LimitExceeded ??= (state, token) =>
                    archiveService.RecordFailedFile(
                        ((ArchiveFileRequest)state).RunId,
                        ((ArchiveFileRequest)state).FilePath,
                        state.Exception ?? new Exception("Exceeded limit"),
                        token);

                var runId = request.RunId;
                var filePath = request.FilePath;
                var keepTimeStamps = contextResolver.KeepTimeStamps();
                var keepOwnerGroup = contextResolver.KeepOwnerGroup();
                var keepAclEntries = contextResolver.KeepAclEntries();

                logger.LogInformation("Processing {File} for {ArchiveRunId}", filePath, runId);
                var task = processor.ProcessFileAsync(runId, filePath, cancellationToken);

                try
                {
                    if (keepTimeStamps)
                    {
                        FileHelper.GetTimestamps(filePath, out var created, out var modified);
                        var updateTimeStampsCommand = new UpdateTimeStampsCommand(
                            runId, filePath, created, modified);
                        await dataStoreMediator.ExecuteCommand(updateTimeStampsCommand, cancellationToken);
                    }

                    if (keepOwnerGroup)
                    {
                        var (owner, group) = await FileHelper.GetOwnerGroupAsync(filePath, cancellationToken);
                        var updateOwnerGroupCommand = new UpdateOwnerGroupCommand(
                            runId, filePath, owner, group);
                        await dataStoreMediator.ExecuteCommand(updateOwnerGroupCommand, cancellationToken);
                    }

                    if (keepAclEntries)
                    {
                        var aclEntries = FileHelper.GetFileAcl(filePath);
                        var updateAclEntriesCommand = new UpdateAclEntriesCommand(
                            runId, filePath, aclEntries);
                        await dataStoreMediator.ExecuteCommand(updateAclEntriesCommand, cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to update metadata for {File} in {ArchiveRunId}", filePath, runId);
                    await task;
                    request.Exception = ex;
                    await retryMediator.RetryAttempt(request, cancellationToken);
                    continue;
                }

                // this should block the producer side due to bounded channel
                var result = await task;

                if (result.Error is not null)
                {
                    request.Exception = result.Error;
                    await retryMediator.RetryAttempt(request, cancellationToken);
                    continue;
                }

                await archiveService.ReportProcessingResult(runId, result, cancellationToken);
                logger.LogInformation("File {File} processed successfully for {ArchiveRunId}", filePath, runId);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing archive file request {FilePath}: {Message}", request.FilePath,
                    ex.Message);
                request.Exception = ex;
                await retryMediator.RetryAttempt(request, cancellationToken);
            }
            finally
            {
                fileCountDownEvent.Signal();
            }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // SignalCronScheduleChange no more items
        // (Producer side should call sharedChannel.Writer.Complete())
        await base.StopAsync(cancellationToken);

        // Wait for any in-flight work to finish (optional timeout)
        using var timeoutCts =
            new CancellationTokenSource(TimeSpan.FromSeconds(contextResolver.ShutdownTimeoutSeconds()));
        await Task.WhenAny(Task.WhenAll(_workers), Task.Delay(-1, timeoutCts.Token));
    }
}