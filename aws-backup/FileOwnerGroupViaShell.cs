using System.Diagnostics;
using System.Runtime.InteropServices;

public static class FileOwnerGroupViaShell
{
    /// <summary>
    /// Runs the OS command to get "owner:group" for the given file.
    /// </summary>
    public static async Task<(string Owner, string Group)> GetOwnerGroupAsync(string path)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            // PowerShell one-liner: "(Get-Acl file).Owner"
            var cmd = $"-NoProfile -Command \"(Get-Acl -Path '{path}').Owner\"";
            var owner = (await RunProcessAsync("powershell", cmd)).Trim();
            // Windows doesn't have a single "group owner" concept; return empty
            return (owner, "");
        }
        else
        {
            // stat --format '%U:%G' path
            var output = await RunProcessAsync(
                "stat",
                $"--format \"%U:%G\" \"{path}\"");
            var parts = output.Trim().Split(':', 2);
            if (parts.Length == 2)
                return (parts[0], parts[1]);
            throw new InvalidOperationException($"Unexpected stat output: {output}");
        }
    }

    /// <summary>
    /// Runs the OS command to set owner:group on the given file.
    /// </summary>
    public static async Task SetOwnerGroupAsync(string path, string owner, string group)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            // PowerShell script to Set-Acl owner and (optionally) group entry
            // Owner
            var psOwner = $"-NoProfile -Command " +
                $"'$acl=Get-Acl -Path \"{path}\"; " +
                $"$acl.SetOwner([System.Security.Principal.NTAccount]\"{owner}\"); " +
                $"Set-Acl -Path \"{path}\" -AclObject $acl'";
            await RunProcessAsync("powershell", psOwner);

            // (Windows has no single "group owner"; you'd typically add a group ACE instead.)
        }
        else
        {
            // chown owner:group path
            await RunProcessAsync("chown", $"{owner}:{group} \"{path}\"");
        }
    }

    /// <summary>
    /// Helper to run an external process and capture stdout.
    /// </summary>
    private static Task<string> RunProcessAsync(string fileName, string args)
    {
        var psi = new ProcessStartInfo
        {
            FileName               = fileName,
            Arguments              = args,
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
            UseShellExecute        = false,
            CreateNoWindow         = true
        };

        var tcs = new TaskCompletionSource<string>();
        var proc = new Process { StartInfo = psi, EnableRaisingEvents = true };

        proc.Exited += (s, e) =>
        {
            var stdout = proc.StandardOutput.ReadToEnd();
            var stderr = proc.StandardError.ReadToEnd();
            proc.Dispose();

            if (proc.ExitCode != 0)
                tcs.SetException(new Exception(
                    $"Command `{fileName} {args}` failed (code {proc.ExitCode}): {stderr.Trim()}"));
            else
                tcs.SetResult(stdout);
        };

        proc.Start();
        return tcs.Task;
    }
}