public enum FileEventType { Added, Changed, Deleted }

public class FileEvent
{
    public string Path { get; init; } = "";
    public FileEventType Type { get; init; }
}

public static class FileHistoryAggregator
{
    /// <summary>
    /// Aggregates a sequence of runs (each a list of FileEvent), in order,
    /// returning the set of files present after all runs.
    /// </summary>
    public static IReadOnlyCollection<string> AggregateRuns(
        IEnumerable<IEnumerable<FileEvent>> runs)
    {
        var current = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var run in runs)
        {
            foreach (var ev in run)
            {
                switch (ev.Type)
                {
                    case FileEventType.Added:
                    case FileEventType.Changed:
                        current.Add(ev.Path);
                        break;
                    case FileEventType.Deleted:
                        current.Remove(ev.Path);
                        break;
                }
            }
        }

        return current;
    }
}