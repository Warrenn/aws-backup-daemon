using System.Collections.Concurrent;
using aws_backup_common;
using Microsoft.Extensions.Logging;

namespace aws_backup;

public interface IFileMetaDataService
{
    Task UpdateStatus(FileMetaData fileMetaData, CancellationToken cancellationToken);
    Task<FileMetaData> GetFileMetaData(long runId, string filePath, CancellationToken cancellationToken);
}

public sealed class FileMetaDataService(
    IDataStoreMediator dataStoreMediator,
    IFileMetaDataDataStore archiveDataStore,
    ILogger<FileMetaDataService> logger) : IFileMetaDataService
{
    private readonly ConcurrentDictionary<string, FileMetaData> _fileMetaDataCache = new();

    public async Task UpdateStatus(FileMetaData fileMetaData, CancellationToken cancellationToken)
    {
        if (fileMetaData.Status is FileStatus.Added or FileStatus.ChunkingComplete) return;
        var filePath = fileMetaData.LocalFilePath;
        if (!_fileMetaDataCache.TryGetValue(filePath, out var cacheMetaData)) return;

        var fileStatus = fileMetaData.Status;
        logger.LogInformation("Updating status for {FilePath} to {FileStatus}", filePath, fileStatus);

        if (fileStatus is FileStatus.Skipped ||
            fileMetaData.HashId != cacheMetaData.HashId ||
            fileMetaData.LastModified != cacheMetaData.LastModified ||
            fileMetaData.Created != cacheMetaData.Created ||
            fileMetaData.Group != cacheMetaData.Group ||
            fileMetaData.Owner != cacheMetaData.Owner ||
            !(fileMetaData.AclEntries ?? []).SequenceEqual(cacheMetaData.AclEntries ?? []))
        {
            logger.LogInformation("File metadata for {FilePath} has changed, saving to data store", filePath);
            //todo: make sure this saves the file chunks and offsets too
            var saveFileMetaDataCommand = new SaveFileMetaDataCommand(fileMetaData);
            await dataStoreMediator.ExecuteCommand(saveFileMetaDataCommand, cancellationToken);
        }

        _fileMetaDataCache[filePath] = fileMetaData;
    }

    public async Task<FileMetaData> GetFileMetaData(long runId, string filePath, CancellationToken cancellationToken)
    {
        logger.LogInformation("Getting file metadata for {FilePath}", filePath);
        if (_fileMetaDataCache.TryGetValue(filePath, out var cachedMetaData))
        {
            if (cachedMetaData.RunId != runId) cachedMetaData.Status = FileStatus.Added;
            return cachedMetaData with { RunId = runId };
        }

        logger.LogInformation("File metadata for {FilePath} not found in cache, checking data store", filePath);
        cachedMetaData = await archiveDataStore.GetFileMetaData(runId, filePath, cancellationToken);
        if (cachedMetaData is null)
        {
            logger.LogInformation("File metadata for {FilePath} not found in data store, creating new metadata",
                filePath);
            cachedMetaData = new FileMetaData(filePath, runId);
            _fileMetaDataCache[filePath] = cachedMetaData;
            return cachedMetaData with { RunId = runId };
        }

        _fileMetaDataCache[filePath] = cachedMetaData;
        
        var status = cachedMetaData.Status;
        if (status is FileStatus.Skipped || cachedMetaData.RunId != runId)
            status = FileStatus.Added; // reset to added so it can be re-evaluated

        logger.LogInformation("File metadata for {FilePath} found in data store status was {cacheStatus} is now {status}", filePath, cachedMetaData.Status, status);

        return cachedMetaData with { RunId = runId, Status = status };
    }
}