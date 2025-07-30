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

public sealed record RemoveArchiveRequestCommand(
    string ArchiveRunId) : DataStoreCommand;

public sealed record UpdateFileStatusCommand(
    string RunId,
    string FilePath,
    FileStatus FileStatus,
    string SkipReason) : DataStoreCommand;

public sealed record UpdateTimeStampsCommand(
    string RunId,
    string LocalFilePath,
    DateTimeOffset Created,
    DateTimeOffset Modified) : DataStoreCommand;

public sealed record UpdateOwnerGroupCommand(
    string RunId,
    string LocalFilePath,
    string Owner,
    string Group) : DataStoreCommand;

public sealed record UpdateAclEntriesCommand(
    string RunId,
    string LocalFilePath,
    AclEntry[] AclEntries) : DataStoreCommand;

public sealed record UpdateArchiveStatusCommand(
    string RunId,
    ArchiveRunStatus RunStatus) : DataStoreCommand;

public sealed record DeleteFileChunksCommand(
    string RunId,
    string LocalFilePath) : DataStoreCommand;

public sealed record SaveChunkStatusCommand(
    string RunId,
    string LocalFilePath,
    ByteArrayKey ChunkHashKey,
    ChunkStatus ChunkStatus) : DataStoreCommand;

public sealed record SaveFileMetaDataCommand(
    string RunId,
    FileMetaData MetaData) : DataStoreCommand;

public sealed record SaveChunkDetailsCommand(
    string RunId,
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

public sealed record SaveRestoreFileStatusCommand(
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
                    case UpdateFileStatusCommand updateFileStatusCommand:
                        await archiveDataStore.UpdateFileStatus(
                            updateFileStatusCommand.RunId,
                            updateFileStatusCommand.FilePath,
                            updateFileStatusCommand.FileStatus,
                            updateFileStatusCommand.SkipReason,
                            cancellationToken);
                        break;
                    case UpdateTimeStampsCommand updateTimeStampsCommand:
                        await archiveDataStore.UpdateTimeStamps(
                            updateTimeStampsCommand.RunId,
                            updateTimeStampsCommand.LocalFilePath,
                            updateTimeStampsCommand.Created,
                            updateTimeStampsCommand.Modified,
                            cancellationToken);
                        break;
                    case UpdateOwnerGroupCommand updateOwnerGroupCommand:
                        await archiveDataStore.UpdateOwnerGroup(
                            updateOwnerGroupCommand.RunId,
                            updateOwnerGroupCommand.LocalFilePath,
                            updateOwnerGroupCommand.Owner,
                            updateOwnerGroupCommand.Group,
                            cancellationToken);
                        break;
                    case UpdateAclEntriesCommand updateAclEntriesCommand:
                        await archiveDataStore.UpdateAclEntries(
                            updateAclEntriesCommand.RunId,
                            updateAclEntriesCommand.LocalFilePath,
                            updateAclEntriesCommand.AclEntries,
                            cancellationToken);
                        break;
                    case UpdateArchiveStatusCommand updateArchiveStatusCommand:
                        await archiveDataStore.UpdateArchiveStatus(
                            updateArchiveStatusCommand.RunId,
                            updateArchiveStatusCommand.RunStatus,
                            cancellationToken);
                        break;
                    case DeleteFileChunksCommand deleteFileChunksCommand:
                        await archiveDataStore.DeleteFileChunks(
                            deleteFileChunksCommand.RunId,
                            deleteFileChunksCommand.LocalFilePath,
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
                    case SaveRestoreFileStatusCommand saveRestoreFileStatusCommand:
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