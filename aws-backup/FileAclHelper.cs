using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;

public record AclEntry(string Identity, string Permissions, string Type);

public record FileAcl(string Path, IReadOnlyList<AclEntry> Entries);

public static class FileAclHelper
{
    public static FileAcl GetFileAcl(string path)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return GetWindowsAcl(path);

        return GetUnixAcl(path);
    }

    private static FileAcl GetWindowsAcl(string path)
    {
        var fileInfo = new FileInfo(path);
        var sec = fileInfo.GetAccessControl();
        var rules = sec.GetAccessRules(
            true,
            true,
            typeof(NTAccount));

        var entries = new List<AclEntry>();
        foreach (FileSystemAccessRule rule in rules)
            entries.Add(new AclEntry(
                rule.IdentityReference.Value,
                rule.FileSystemRights.ToString(),
                rule.AccessControlType.ToString()
            ));

        return new FileAcl(path, entries);
    }

    private static FileAcl GetUnixAcl(string path)
    {
        // POSIX bits: owner, group, other
        var mode = File.GetUnixFileMode(path);
        var entries = new List<AclEntry>
        {
            new("owner", FormatPerm(mode, UnixFileMode.UserRead, UnixFileMode.UserWrite, UnixFileMode.UserExecute),
                "POSIX"),
            new("group", FormatPerm(mode, UnixFileMode.GroupRead, UnixFileMode.GroupWrite, UnixFileMode.GroupExecute),
                "POSIX"),
            new("other", FormatPerm(mode, UnixFileMode.OtherRead, UnixFileMode.OtherWrite, UnixFileMode.OtherExecute),
                "POSIX")
        };
        return new FileAcl(path, entries);
    }

    private static string FormatPerm(UnixFileMode mode, UnixFileMode read, UnixFileMode write, UnixFileMode exec)
    {
        return $"{((mode & read) > 0 ? "r" : "-")}" +
               $"{((mode & write) > 0 ? "w" : "-")}" +
               $"{((mode & exec) > 0 ? "x" : "-")}";
    }
    
    public static void ApplyAcl(FileAcl acl, string targetPath)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            ApplyWindowsAcl(acl, targetPath);
        else
            ApplyUnixAcl(acl, targetPath);
    }
    private static void ApplyWindowsAcl(FileAcl acl, string path)
    {
        // Create a fresh ACL object
        var sec = new FileSecurity();

        foreach (var entry in acl.Entries)
        {
            // Parse the identity (domain\\user or local account)
            var identity = new NTAccount(entry.Identity);
            // Parse the rights enum from its string
            var rights   = (FileSystemRights)Enum.Parse(
                                typeof(FileSystemRights),
                                entry.Permissions);
            // Parse Allow/Deny
            var control  = (AccessControlType)Enum.Parse(
                                typeof(AccessControlType),
                                entry.Type);

            // Build and add the rule
            var rule = new FileSystemAccessRule(
                identity,
                rights,
                InheritanceFlags.None,
                PropagationFlags.NoPropagateInherit,
                control);

            sec.AddAccessRule(rule);
        }

        var fileInfo = new FileInfo(path);
        fileInfo.SetAccessControl(sec);
    }

    private static void ApplyUnixAcl(FileAcl acl, string path)
    {
        // We expect exactly three POSIX entries: owner, group, other
        // with Permissions like "rwx", "rw-", etc.
        // Grab the mode bits back from the strings:
        short mode = 0;
        foreach (var e in acl.Entries)
        {
            var bits = 0;
            if (e.Permissions is ['r', ..]) bits |= 4;
            if (e.Permissions is [_, 'w', ..]) bits |= 2;
            if (e.Permissions is [_, _, 'x', ..]) bits |= 1;

            switch (e.Identity)
            {
                case "owner": mode |= (short)(bits << 6); break; // user bits
                case "group": mode |= (short)(bits << 3); break;
                case "other": mode |= (short)(bits << 0); break;
            }
        }

        // Now apply with File.SetUnixFileMode
        var fileMode = (UnixFileMode)mode;
        File.SetUnixFileMode(path, fileMode);
    }
    
    public static (DateTime created, DateTime modified) GetTimestamps(string path)
    {
        // Get creation time (Local)
        // Optionally get UTC instead:
        var created = File.GetCreationTimeUtc(path).ToLocalTime();

        // Get last‐write (modified) time (Local)
        // Optionally get UTC instead:
        var modified = File.GetLastWriteTimeUtc(path).ToLocalTime();

        return (created, modified);
    }
    
    public static void SetTimestamps(string path, DateTime created, DateTime modified)
    {
        // Set creation time (Local)
        // Optionally set UTC instead:
        File.SetCreationTimeUtc(path, created.ToUniversalTime());

        // Set last‐write (modified) time (Local)
        // Optionally set UTC instead:
        File.SetLastWriteTimeUtc(path, modified.ToUniversalTime());
    }
}