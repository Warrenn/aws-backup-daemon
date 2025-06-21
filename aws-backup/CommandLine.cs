using System.Diagnostics;

namespace aws_backup;

public static class CommandLine
{
    /// <summary>
    ///     Helper to run an external process and capture stdout.
    /// </summary>
    public static async Task<(string StdOut, string StdErr, int ExitCode)> RunProcessAsync(
        string fileName, string args, CancellationToken ct)
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

        using var proc = new Process();
        proc.StartInfo = psi;
        try
        {
            proc.Start();

            // Begin async reads
            var stdoutTask = proc.StandardOutput.ReadToEndAsync(ct);
            var stderrTask = proc.StandardError.ReadToEndAsync(ct);

            // Wait for process to exit or cancellation
            await proc.WaitForExitAsync(ct);

            // Collect results
            var stdOut = await stdoutTask;
            var stdErr = await stderrTask;
            return (stdOut, stdErr, proc.ExitCode);
        }
        catch (OperationCanceledException)
        {
            // kill if still running
            try
            {
                proc.Kill(true);
            }
            catch (Exception e)
            {
                return (string.Empty, e.Message, -1);
            }

            throw;
        }
    }
}