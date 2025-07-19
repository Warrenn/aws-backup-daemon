using System.Threading.Channels;

namespace aws_backup;

public sealed record UploadBatch(
    string LocalFilePath,
    string ArchiveRunId
)
{
    public List<UploadChunkRequest> Requests { get; } = [];
    public long FileSize { get; set; }
}