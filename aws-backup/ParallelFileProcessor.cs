using System.ComponentModel;
using System.Threading.Channels;

public class ParallelFileProcessor
{
    private readonly int _maxDegreeOfParallelism;
    private readonly IChunkedEncryptingFileProcessor _processor;

    public ParallelFileProcessor(int maxDegreeOfParallelism, IChunkedEncryptingFileProcessor? processor = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDegreeOfParallelism, 1);
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));
        _maxDegreeOfParallelism = maxDegreeOfParallelism;
    }

    public async Task ProcessFilesAsync(IEnumerable<string> filePaths)
    {
        // create an unbounded channel for file paths
        var fileChannel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true
        });

        // start background worker tasks
        var workers = new Task[_maxDegreeOfParallelism];
        for (var i = 0; i < _maxDegreeOfParallelism; i++)
        {
            workers[i] = Task.Run(async () =>
            {
                await foreach (var path in fileChannel.Reader.ReadAllAsync())
                {
                    try
                    {
                        await ProcessFileAsync(path);
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"Error processing '{path}': {ex}");
                    }
                }
            });
        }

        // producer: enqueue all files
        foreach (var path in filePaths)
        {
            await fileChannel.Writer.WriteAsync(path);
        }

        // signal no more files
        fileChannel.Writer.Complete();

        // wait for all workers to finish
        await Task.WhenAll(workers);
    }

    private async Task ProcessFileAsync(string path)
    {
        var results = await _processor.ProcessFileAsync(path);
    }
}
