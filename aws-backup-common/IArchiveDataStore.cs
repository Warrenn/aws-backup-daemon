namespace aws_backup_common;

public interface IArchiveDataStore
{
    IAsyncEnumerable<RunRequest> GetRunRequests(CancellationToken cancellationToken);
    Task SaveRunRequest(RunRequest request, CancellationToken cancellationToken);
    Task SaveArchiveRun(ArchiveRun run, CancellationToken cancellationToken);
    Task RemoveArchiveRequest(string archiveRunId, CancellationToken cancellationToken);
    Task<ArchiveRun?> GetArchiveRun(string runId, CancellationToken cancellationToken);

    Task UpdateFileStatus(string runId, string filePath, FileStatus fileStatus, string skipReason,
        CancellationToken cancellationToken);

    Task UpdateTimeStamps(string runId, string localFilePath, DateTimeOffset created, DateTimeOffset modified,
        CancellationToken cancellationToken);

    Task UpdateOwnerGroup(string runId, string localFilePath, string owner, string group,
        CancellationToken cancellationToken);

    Task UpdateAclEntries(string runId, string localFilePath, AclEntry[] aclEntries,
        CancellationToken cancellationToken);

    Task UpdateArchiveStatus(string runId, ArchiveRunStatus runStatus, CancellationToken none);

    Task DeleteFileChunks(string runId, string localFilePath, CancellationToken cancellationToken);

    Task SaveChunkStatus(string runId, string localFilePath, ByteArrayKey chunkHashKey, ChunkStatus chunkStatus,
        CancellationToken cancellationToken);

    Task<FileMetaData?> GetFileMetaData(string runId, string filePath, CancellationToken cancellationToken);

    Task SaveChunkDetails(string runId, string localFilePath, DataChunkDetails details,
        CancellationToken cancellationToken);

    Task SaveFileMetaData(string runId, string localFilePath, byte[] hashKey, long originalBytes, long compressedBytes,
        CancellationToken cancellationToken);

    IAsyncEnumerable<FileMetaData> GetRestorableFileMetaData(string runId, CancellationToken cancellationToken);
}