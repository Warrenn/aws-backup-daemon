using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text.RegularExpressions;

namespace aws_backup;

public partial class RegexHelper
{
    [GeneratedRegex(@"^[A-Za-z]:(?:\\|/)")]
    public static partial Regex DriveRootRegex();
    
    [GeneratedRegex( @"[^\w\d]")]
    public static partial Regex NonAlphanumericRegex();
}

public static class FileHelper
{
    public static string ToUnixRooted(this string windowsPath)
    {
        if (string.IsNullOrWhiteSpace(windowsPath)) return "";
        var returnValue = windowsPath.Trim();
        var m = RegexHelper.DriveRootRegex().Match(windowsPath);
        if (m.Success) returnValue = returnValue[2..];

        // Normalize all backslashes to forward slashes
        return returnValue.Replace('\\', '/');
    }

    /// <summary>
    ///     Runs the OS command to get "owner:group" for the given file.
    /// </summary>
    public static async Task<(string Owner, string Group)> GetOwnerGroupAsync(string path,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return ("", "");

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            // PowerShell one-liner: "(Get-Acl file).Owner"
            var cmd = $"-NoProfile -Command \"(Get-Acl -Path '{path}').Owner\"";
            var owner = (await CommandLine.RunProcessAsync("powershell", cmd, cancellationToken)).StdOut.Trim();
            // Windows doesn't have a single "group owner" concept; return empty
            return (owner, "");
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            var (osxStdOut, _, _) = await CommandLine.RunProcessAsync(
                "stat",
                $"-f \"%Su:%Sg\" \"{path}\"",
                cancellationToken);
            var osxStrings = osxStdOut.Trim().Split(':', 2);
            return osxStrings.Length == 2 ? (osxStrings[0], osxStrings[1]) : ("", "");
        }

        // stat --format '%U:%G' path
        var (output, _, _) = await CommandLine.RunProcessAsync(
            "stat",
            $"--format \"%U:%G\" \"{path}\"",
            cancellationToken);
        var parts = output.Trim().Split(':', 2);
        return parts.Length == 2 ? (parts[0], parts[1]) : ("", "");
    }

    /// <summary>
    ///     Runs the OS command to set owner:group on the given file.
    /// </summary>
    public static async Task SetOwnerGroupAsync(string path, string owner, string group,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(owner)) return;
        if (!File.Exists(path)) return;
        if (string.IsNullOrWhiteSpace(group)) group = owner;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            // PowerShell script to Set-Acl owner and (optionally) group entry
            // Owner
            var psOwner = $"""
                           -NoProfile -Command 
                           '$acl=Get-Acl -Path "{path}";
                           $acl.SetOwner([System.Security.Principal.NTAccount]"{owner}"); 
                           Set-Acl -Path "{path}" -AclObject $acl'
                           """;
            await CommandLine.RunProcessAsync("powershell", psOwner, cancellationToken);

            // (Windows has no single "group owner"; you'd typically add a group ACE instead.)
        }
        else
        {
            // chown owner:group path
            await CommandLine.RunProcessAsync("chown", $"{owner}:{group} \"{path}\"", cancellationToken);
        }
    }

    public static AclEntry[] GetFileAcl(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return [];
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? GetWindowsAcl(path) : GetUnixAcl(path);
    }

    private static AclEntry[] GetWindowsAcl(string path)
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

        return entries.ToArray();
    }

    private static AclEntry[] GetUnixAcl(string path)
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
        return entries.ToArray();
    }

    private static string FormatPerm(UnixFileMode mode, UnixFileMode read, UnixFileMode write, UnixFileMode exec)
    {
        return $"{((mode & read) > 0 ? "r" : "-")}" +
               $"{((mode & write) > 0 ? "w" : "-")}" +
               $"{((mode & exec) > 0 ? "x" : "-")}";
    }

    public static void ApplyAcl(AclEntry[]? acls, string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName)) return;
        if (!File.Exists(fileName)) return;

        if (acls is null || acls.Length == 0) return;
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            ApplyWindowsAcl(acls, fileName);
        else
            ApplyUnixAcl(acls, fileName);
    }

    private static void ApplyWindowsAcl(AclEntry[] aclEntries, string fileName)
    {
        // Create a fresh ACL object
        var sec = new FileSecurity();

        foreach (var entry in aclEntries)
        {
            // Parse the identity (domain\\user or local account)
            var identity = new NTAccount(entry.Identity);
            // Parse the rights enum from its string
            var rights = (FileSystemRights)Enum.Parse(
                typeof(FileSystemRights),
                entry.Permissions);
            // Parse Allow/Deny
            var control = (AccessControlType)Enum.Parse(
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

        var fileInfo = new FileInfo(fileName);
        fileInfo.SetAccessControl(sec);
    }

    private static void ApplyUnixAcl(AclEntry[] aclEntries, string path)
    {
        // We expect exactly three POSIX entries: owner, group, other
        // with Permissions like "rwx", "rw-", etc.
        // Grab the mode bits back from the strings:
        short mode = 0;
        foreach (var e in aclEntries)
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

    public static void GetTimestamps(string path, out DateTimeOffset created, out DateTimeOffset modified)
    {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            created = TimeProvider.System.GetUtcNow();
            modified = created;
            return;
        }

        // Get creation time (Local)
        // Optionally get UTC instead:
        created = File.GetCreationTimeUtc(path);

        // Get last‐write (modified) time (Local)
        // Optionally get UTC instead:
        modified = File.GetLastWriteTimeUtc(path);
    }

    public static void SetTimestamps(string path, DateTimeOffset created, DateTimeOffset modified)
    {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path)) return;

        // Set creation time (Local)
        // Optionally set UTC instead:
        File.SetCreationTimeUtc(path, created.DateTime);

        // Set last‐write (modified) time (Local)
        // Optionally set UTC instead:
        File.SetLastWriteTimeUtc(path, modified.DateTime);
    }
}