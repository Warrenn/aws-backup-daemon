namespace aws_backup;

public interface IMediator
{
    //data chunks

    //archive state

    //data chunks manifest

    //restore requests
    ValueTask RestoreBackup(RestoreRequest restoreRequest, CancellationToken cancellationToken);

    //run requests

    //restore runs

    //download files from S3

    //save S3 restore chunk manifest
}

public class Mediator(
    IContextResolver resolver) : IMediator
{
    public async ValueTask SaveRestoreManifest(S3RestoreChunkManifest current, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<UploadChunkRequest> GetChunks(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask ProcessChunk(UploadChunkRequest request, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async ValueTask RestoreBackup(RestoreRequest restoreRequest, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}