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

public sealed record SaveRunRequestCommand(
    RunRequest Request) : DataStoreCommand;

public sealed record SaveArchiveRunCommand(
    ArchiveRun ArchiveRun) : DataStoreCommand;

public sealed record RemoveArchiveRequestCommand(long RunId) : DataStoreCommand;

public sealed record SaveChunkStatusCommand(
    long RunId,
    string LocalFilePath,
    ByteArrayKey ChunkHashKey,
    ChunkStatus ChunkStatus) : DataStoreCommand;

public sealed record SaveFileMetaDataCommand(FileMetaData MetaData) : DataStoreCommand;

public sealed record SaveChunkDetailsCommand(
    long RunId,
    string LocalFilePath,
    DataChunkDetails Details) : DataStoreCommand;

public sealed record SaveRestoreRequestCommand(
    RestoreRequest Request) : DataStoreCommand;

public sealed record SaveRestoreRunCommand(
    RestoreRun RestoreRun) : DataStoreCommand;

public sealed record RemoveRestoreRequestCommand(
    string RestoreId) : DataStoreCommand;

public sealed record SaveRestoreFileMetaDataCommand(
    string RestoreRunRestoreId,
    RestoreFileMetaData RestoreFileMeta) : DataStoreCommand;

public sealed record SaveRestoreChunkStatusCommand(
    string RestoreId,
    string FilePath,
    ByteArrayKey ChunkKey,
    S3ChunkRestoreStatus ReadyToRestore) : DataStoreCommand;

public sealed record UpdateRestoreFileStatusCommand(
    string ReqRestoreId,
    string FileMetaFilePath,
    FileRestoreStatus Status,
    string ReasonMessage) : DataStoreCommand;

public sealed record AddCloudChunkDetailsCommand(
    ByteArrayKey HashKey,
    CloudChunkDetails Details) : DataStoreCommand;

public class DataStoreActor(
    ICloudChunkStorage cloudChunkStorage,
    IArchiveDataStore archiveDataStore,
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
                        await archiveDataStore.RemoveArchiveRequest(removeArchiveRequestCommand.ArchiveRunId,
                            cancellationToken);
                        break;
                    case SaveChunkStatusCommand saveChunkStatusCommand:
                        await archiveDataStore.SaveChunkStatus(
                            saveChunkStatusCommand.RunId,
                            saveChunkStatusCommand.LocalFilePath,
                            saveChunkStatusCommand.ChunkHashKey,
                            saveChunkStatusCommand.ChunkStatus,
                            cancellationToken);
                        break;
                    case SaveFileMetaDataCommand saveFileMetaDataCommand:
                        await archiveDataStore.SaveFileMetaData(
                            saveFileMetaDataCommand.RunId,
                            saveFileMetaDataCommand.MetaData,
                            cancellationToken);
                        break;
                    case SaveChunkDetailsCommand saveChunkDetailsCommand:
                        await archiveDataStore.SaveChunkDetails(
                            saveChunkDetailsCommand.RunId,
                            saveChunkDetailsCommand.LocalFilePath,
                            saveChunkDetailsCommand.Details,
                            cancellationToken);
                        break;
                    case SaveRestoreRequestCommand saveRestoreRequestCommand:
                        await restoreDataStore.SaveRestoreRequest(saveRestoreRequestCommand.Request, cancellationToken);
                        break;
                    case SaveRestoreRunCommand saveRestoreRunCommand:
                        await restoreDataStore.SaveRestoreRun(saveRestoreRunCommand.RestoreRun, cancellationToken);
                        break;
                    case RemoveRestoreRequestCommand removeRestoreRequestCommand:
                        await restoreDataStore.RemoveRestoreRequest(removeRestoreRequestCommand.RestoreId,
                            cancellationToken);
                        break;
                    case SaveRestoreFileMetaDataCommand saveRestoreFileMetaDataCommand:
                        await restoreDataStore.SaveRestoreFileMetaData(
                            saveRestoreFileMetaDataCommand.RestoreRunRestoreId,
                            saveRestoreFileMetaDataCommand.RestoreFileMeta,
                            cancellationToken);
                        break;
                    case SaveRestoreChunkStatusCommand saveRestoreChunkStatusCommand:
                        await restoreDataStore.SaveRestoreChunkStatus(
                            saveRestoreChunkStatusCommand.RestoreId,
                            saveRestoreChunkStatusCommand.FilePath,
                            saveRestoreChunkStatusCommand.ChunkKey,
                            saveRestoreChunkStatusCommand.ReadyToRestore,
                            cancellationToken);
                        break;
                    case UpdateRestoreFileStatusCommand saveRestoreFileStatusCommand:
                        await restoreDataStore.SaveRestoreFileStatus(
                            saveRestoreFileStatusCommand.ReqRestoreId,
                            saveRestoreFileStatusCommand.FileMetaFilePath,
                            saveRestoreFileStatusCommand.Status,
                            saveRestoreFileStatusCommand.ReasonMessage,
                            cancellationToken);
                        break;
                    case AddCloudChunkDetailsCommand addCloudChunkDetailsCommand:
                        await cloudChunkStorage.AddCloudChunkDetails(
                            addCloudChunkDetailsCommand.HashKey,
                            addCloudChunkDetailsCommand.Details,
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