using System.Diagnostics;

namespace aws_backup;

public static class CommandLine
{
    /// <summary>
    ///     Helper to run an external process and capture stdout.
    /// </summary>
    public static Task<(string stdOut, string stdErr, int exitCode)> RunProcessAsync(string fileName, string args)
    {
        var psi = new ProcessStartInfo
        {
            FileName = fileName,
            Arguments = args,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        var tcs = new TaskCompletionSource<(string stdOut, string stdErr, int exitCode)>();
        var proc = new Process { StartInfo = psi, EnableRaisingEvents = true };

        proc.Exited += (_, _) =>
        {
            var stdout = proc.StandardOutput.ReadToEnd();
            var stderr = proc.StandardError.ReadToEnd();
            proc.Dispose();

            tcs.SetResult((stdout, stderr, proc.ExitCode));
        };

        proc.Start();
        return tcs.Task;
    }
}