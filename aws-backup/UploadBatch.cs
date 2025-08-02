using aws_backup_common;

namespace aws_backup;

public sealed record UploadBatch(
    string LocalFilePath,
    ArchiveRun ArchiveRun
)
{
    public List<UploadChunkRequest> Requests { get; } = [];
    public long FileSize { get; set; }
}