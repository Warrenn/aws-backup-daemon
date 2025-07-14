using aws_backup_common;
using Microsoft.Extensions.FileSystemGlobbing;

namespace aws_backup;

public interface IFileLister
{
    /// <summary>
    ///     Given a colon-delimited list of root directories, recurses each and
    ///     yields all reachable files, skipping directories you can’t enter.
    /// </summary>
    IEnumerable<string> GetAllFiles(string colonDelimitedRoots, IEnumerable<string> ignorePatterns);
}

public sealed class FileLister : IFileLister
{
    /// <summary>
    ///     Given a colon-delimited list of root directories, recurses each and
    ///     yields all reachable files, skipping directories you can’t enter.
    /// </summary>
    public IEnumerable<string> GetAllFiles(string colonDelimitedRoots, IEnumerable<string> ignorePatterns)
    {
        if (string.IsNullOrWhiteSpace(colonDelimitedRoots))
            yield break;

        var matcher = new Matcher();
        matcher.AddInclude("**/*");
        foreach (var pat in ignorePatterns)
            matcher.AddExclude(pat);

        var roots = colonDelimitedRoots.Split(':', StringSplitOptions.RemoveEmptyEntries);
        var pending = new Stack<string>(roots);

        while (pending.Count > 0)
        {
            var dir = pending.Pop();
            if (!Directory.Exists(dir))
                continue;

            string[] subDirs;
            string[] files;

            // try to get files in this directory
            try
            {
                var dirInfo = new DirectoryInfo(dir);
                if (!dirInfo.Exists || dirInfo.LinkTarget is not null)
                    // skip symlinks to directories
                    continue;

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

                try
                {
                    var fi = new FileInfo(file);
                    if (!fi.Exists || fi.LinkTarget is not null || fi.Length <= 0)
                        continue;
                }
                catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
                {
                    continue;
                }

                yield return file.ToUnixRooted();
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