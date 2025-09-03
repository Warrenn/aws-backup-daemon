namespace aws_backup_common;

public interface IFileMetaDataDataStore
{
    Task<FileMetaData?> GetFileMetaData(long runId, string filePath, CancellationToken cancellationToken);
    Task SaveFileMetaData(FileMetaData metaData, CancellationToken cancellationToken);
    IAsyncEnumerable<FileMetaData> GetRestorableFileMetaData(long runId, CancellationToken cancellationToken);
}