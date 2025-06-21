using System.Diagnostics;

namespace aws_backup;

public static class CommandLine
{
    /// <summary>
    ///     Helper to run an external process and capture stdout.
    /// </summary>
    public static async Task<(string stdOut, string stdErr, int exitCode)> RunProcessAsync(string fileName, string args,
        CancellationToken stoppingToken)
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

        (string stdOut, string stdErr, int exitCode) output = ("", "", -1);
        var proc = new Process { StartInfo = psi, EnableRaisingEvents = true };

        proc.Exited += (_, _) =>
        {
            var stdout = proc.StandardOutput.ReadToEnd();
            var stderr = proc.StandardError.ReadToEnd();
            proc.Dispose();

            output.stdOut = stdout;
            output.stdErr = stderr;
            output.exitCode = proc.ExitCode;
        };

        proc.Start();
        await proc.WaitForExitAsync(stoppingToken);
        if (!stoppingToken.IsCancellationRequested) return output;
        proc.Kill();
        return ("", "", -1);
    }
}