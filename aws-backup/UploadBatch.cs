using aws_backup_common;

namespace aws_backup;

public interface IUploadBatch;

public sealed record UploadBatch(
    string LocalFilePath,
    ArchiveRun ArchiveRun
) : IUploadBatch
{
    public List<UploadChunkRequest> Requests { get; } = [];
    public long FileSize { get; set; }
}

public sealed record FinalUploadBatch(
    ArchiveRun ArchiveRun,
    TaskCompletionSource CompletionSource
) : IUploadBatch;