using System.IO.Compression;
using System.Text.Json.Serialization;
using Amazon.Runtime;

namespace aws_backup_common;

[JsonSourceGenerationOptions(
    WriteIndented = true,
    Converters = [
        typeof(JsonStringEnumConverter<RestoreRunStatus>),
        typeof(JsonStringEnumConverter<S3ChunkRestoreStatus>),
        typeof(JsonStringEnumConverter<ChunkStatus>),
        typeof(JsonStringEnumConverter<ArchiveRunStatus>),
        typeof(JsonStringEnumConverter<FileStatus>),
        typeof(JsonStringEnumConverter<FileRestoreStatus>),
        typeof(JsonStringEnumConverter<RequestRetryMode>),
        typeof(JsonStringEnumConverter<CompressionLevel>)
    ],
    PropertyNameCaseInsensitive = true
)]
[JsonSerializable(typeof(Configuration))]
[JsonSerializable(typeof(RestoreRequest))]
[JsonSerializable(typeof(ArchiveRun))]
[JsonSerializable(typeof(FileMetaData))]
[JsonSerializable(typeof(RunRequest))]
[JsonSerializable(typeof(CloudChunkDetails))]
[JsonSerializable(typeof(DataChunkDetails))]
[JsonSerializable(typeof(ByteArrayKey))]
[JsonSerializable(typeof(AclEntry))]
[JsonSerializable(typeof(AclEntry[]))]
[JsonSerializable(typeof(S3ChunkRestoreStatus))]
[JsonSerializable(typeof(RestoreFileMetaData))]
[JsonSerializable(typeof(RestoreRun))]
[JsonSerializable(typeof(AwsConfiguration))]
[JsonSerializable(typeof(ArchiveRunStatus))]
[JsonSerializable(typeof(FileStatus))]
[JsonSerializable(typeof(ChunkStatus))]
public partial class SourceGenerationContext : JsonSerializerContext;