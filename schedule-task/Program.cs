using Microsoft.Win32.TaskScheduler;

using var ts = new TaskService();
System.Globalization.CultureInfo.CurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
System.Globalization.CultureInfo.CurrentUICulture = System.Globalization.CultureInfo.InvariantCulture;

var scriptPath = GetArgValue(args, "script-path");
var timeStr = GetArgValue(args, "time");
var taskName = GetArgValue(args, "task-name");
var description = GetArgValue(args, "description");

if (string.IsNullOrWhiteSpace(scriptPath) || !File.Exists(scriptPath))
{
    Console.Error.WriteLine("Script path is required");
    return -1;
}

if (string.IsNullOrWhiteSpace(timeStr) || !TimeOnly.TryParse(timeStr, out var time))
{
    Console.Error.WriteLine("Valid time is required");
    return -1;
}

if (string.IsNullOrWhiteSpace(taskName))
{
    Console.Error.WriteLine("Task name is required");
    return -1;
}

var powershellExe = FindExe([
    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "PowerShell", "7", "pwsh.exe"),
    "pwsh.exe",
    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System), @"WindowsPowerShell\v1.0\powershell.exe"),
    "powershell.exe"
]);
if (powershellExe is null)
{
    Console.Error.WriteLine("Cannot find PowerShell executable (pwsh.exe or powershell.exe)");
    return -1;
}

var scriptFull = Path.GetFullPath(scriptPath);
var startIn    = Path.GetDirectoryName(scriptFull) ?? Environment.CurrentDirectory;
var arguments  = $"-NoProfile -ExecutionPolicy Bypass -File \"{scriptFull}\"";

var now = DateTime.Now;
var start = new DateTime(now.Year, now.Month, now.Day,
    time.Hour, time.Minute, time.Second);
if (start <= now) start = start.AddDays(1);

// Define the task
var td = ts.NewTask();
td.RegistrationInfo.Description = description;

td.Principal.UserId   = "SYSTEM";
td.Principal.LogonType = TaskLogonType.ServiceAccount;
td.Principal.RunLevel  = TaskRunLevel.Highest;

td.Triggers.Add(new DailyTrigger
{
    StartBoundary = start,
    DaysInterval  = 1
});

td.Actions.Add(new ExecAction(powershellExe, arguments, startIn));

// Create or update
ts.RootFolder.RegisterTaskDefinition(taskName, td, TaskCreation.CreateOrUpdate,
    userId: "SYSTEM", password: null, logonType: TaskLogonType.ServiceAccount);

Console.WriteLine($"Registered scheduled task '{taskName}' to run daily at {timeStr} as SYSTEM.");
return 0;

static string? FindExe(string[] candidates)
{
    foreach (var c in candidates)
    {
        if (Path.GetFileName(c).Equals(c, StringComparison.OrdinalIgnoreCase))
        {
            // search PATH
            var path = Environment.GetEnvironmentVariable("PATH") ?? "";
            foreach (var dir in path.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
            {
                var p = Path.Combine(dir.Trim(), c);
                if (File.Exists(p)) return p;
            }
        }
        else if (File.Exists(c)) return c;
    }
    return null;
}

string GetArgValue(string[] args, string argName)
{
    argName = argName.StartsWith("--") ? argName : $"--{argName}";
    var argIndex = Array.FindIndex(args, a => a.Equals(argName, StringComparison.OrdinalIgnoreCase));
    if (argIndex >= 0 && argIndex < args.Length - 1)
    {
        return args[argIndex + 1];
    }   
    return string.Empty;
}