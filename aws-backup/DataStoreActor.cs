using aws_backup_common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IDataStoreMediator
{
    IAsyncEnumerable<DataStoreCommand> GetDataStoreCommands(CancellationToken cancellationToken);
    Task ExecuteCommand(DataStoreCommand request, CancellationToken cancellationToken);
}

public record DataStoreCommand;
public sealed record SaveRunRequestCommand(RunRequest Request) : DataStoreCommand;
public sealed record SaveArchiveRunCommand(ArchiveRun ArchiveRun) : DataStoreCommand;
public sealed record RemoveArchiveRequestCommand(long RunId) : DataStoreCommand;
public sealed record SaveFileMetaDataCommand(FileMetaData MetaData) : DataStoreCommand;
public sealed record AddCloudChunkDetailsCommand(CloudChunkDetails ChunkDetails) : DataStoreCommand;
public sealed record SaveRestoreRequestCommand(RestoreRequest RestoreRequest) : DataStoreCommand;
public sealed record SaveRestoreRunCommand(RestoreRun RestoreRun) : DataStoreCommand;
public sealed record RemoveRestoreRequestCommand(string RestoreId) : DataStoreCommand;
public sealed record SaveRestoreFileMetaDataCommand(string RestoreId, RestoreFileMetaData RestoreFileMeta) : DataStoreCommand;

public class DataStoreActor(
    ICloudChunkStorage cloudChunkStorage,
    IArchiveDataStore archiveDataStore,
    IFileMetaDataDataStore fileMetaDataDataStore,
    IRestoreDataStore restoreDataStore,
    IDataStoreMediator mediator,
    ILogger<DataStoreActor> logger,
    IContextResolver contextResolver
) : BackgroundService
{
    private Task[] _workers = [];

    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("DataStoreActor started");
        var concurrency = contextResolver.NoOfConcurrentDbWriters();

        _workers = new Task[concurrency];
        for (var i = 0; i < _workers.Length; i++)
            _workers[i] = Task.Run(() => WorkerLoopAsync(cancellationToken), cancellationToken);

        return Task.WhenAll(_workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (var command in mediator.GetDataStoreCommands(cancellationToken))
            try
            {
                logger.LogInformation("Processing command of type {CommandType}", command.GetType());
                switch (command)
                {
                    case SaveRunRequestCommand saveRunCommand:
                        await archiveDataStore.SaveRunRequest(saveRunCommand.Request, cancellationToken);
                        break;
                    case SaveArchiveRunCommand saveArchiveRunCommand:
                        await archiveDataStore.SaveArchiveRun(saveArchiveRunCommand.ArchiveRun, cancellationToken);
                        break;
                    case RemoveArchiveRequestCommand removeArchiveRequestCommand:
                        await archiveDataStore.RemoveArchiveRequest(removeArchiveRequestCommand.RunId,
                            cancellationToken);
                        break;
                    case SaveFileMetaDataCommand saveFileMetaDataCommand:
                        await fileMetaDataDataStore.SaveFileMetaData(
                            saveFileMetaDataCommand.MetaData,
                            cancellationToken);
                        break;
                    case AddCloudChunkDetailsCommand addCloudChunkDetailsCommand:
                        await cloudChunkStorage.AddCloudChunkDetails(
                            addCloudChunkDetailsCommand.ChunkDetails,
                            cancellationToken);
                        break;
                    case SaveRestoreRequestCommand saveRestoreRequestCommand:
                        await restoreDataStore.SaveRestoreRequest(
                            saveRestoreRequestCommand.RestoreRequest,
                            cancellationToken);
                        break;
                    case SaveRestoreRunCommand saveRestoreRunCommand:
                        await restoreDataStore.SaveRestoreRun(
                            saveRestoreRunCommand.RestoreRun,
                            cancellationToken);
                        break;
                    case RemoveRestoreRequestCommand removeRestoreRequestCommand:
                        await restoreDataStore.RemoveRestoreRequest(
                            removeRestoreRequestCommand.RestoreId,
                            cancellationToken);
                        break;
                    case SaveRestoreFileMetaDataCommand saveRestoreFileMetaDataCommand:
                        await restoreDataStore.SaveRestoreFileMetaData(
                            saveRestoreFileMetaDataCommand.RestoreId,
                            saveRestoreFileMetaDataCommand.RestoreFileMeta,
                            cancellationToken);
                        break;
                    default:
                        logger.LogWarning("Unknown command type: {CommandType}", command.GetType());
                        break;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                logger.LogInformation("DataStoreActor operation cancelled");
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing command {CommandType}: {Message}", command.GetType(),
                    ex.Message);
                // Optionally, you can publish an error message to a message bus or log it
            }
    }
}