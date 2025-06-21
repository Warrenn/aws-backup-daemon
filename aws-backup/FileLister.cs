using Microsoft.Extensions.FileSystemGlobbing;

namespace aws_backup;

public static class FileLister
{
    /// <summary>
    ///     Given a colon-delimited list of root directories, recurses each and
    ///     yields all reachable files, skipping directories you can’t enter.
    /// </summary>
    public static IEnumerable<string> GetAllFiles(string colonDelimitedRoots, IEnumerable<string> ignorePatterns)
    {
        var matcher = new Matcher();
        matcher.AddInclude("**/*");
        foreach (var pat in ignorePatterns)
            matcher.AddExclude(pat);

        var roots = colonDelimitedRoots.Split(':', StringSplitOptions.RemoveEmptyEntries);
        var pending = new Stack<string>(roots);

        while (pending.Count > 0)
        {
            var dir = pending.Pop();
            string[] subDirs;
            string[] files;

            // try to get files in this directory
            try
            {
                files = Directory.GetFiles(dir);
            }
            catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
            {
                // skip directories you can’t read
                continue;
            }

            foreach (var file in files)
            {
                // compute relative path for matching
                var rel = Path.GetRelativePath(dir, file)
                    .Replace(Path.DirectorySeparatorChar, '/');
                // if matcher says “no match” → it was excluded
                if (!matcher.Match(rel).HasMatches)
                    continue;

                yield return file;
            }

            // try to get subdirectories
            try
            {
                subDirs = Directory.GetDirectories(dir);
            }
            catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
            {
                continue;
            }

            // push them to the stack to recurse later
            foreach (var sub in subDirs)
                pending.Push(sub);
        }
    }
}