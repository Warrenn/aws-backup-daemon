using System.Runtime.InteropServices;
using aws_backup;

namespace test;

public class FileHelperTests : IDisposable
{
    private readonly string _tempFile;

    public FileHelperTests()
    {
        _tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        // Create an empty file
        File.WriteAllText(_tempFile, "hello");
    }

    public void Dispose()
    {
        try { File.Delete(_tempFile); }
        catch { }
    }

    [Fact]
    public void GetAndSetTimestamps_RoundTrips()
    {
        // Arrange
        var created  = DateTimeOffset.UtcNow.AddDays(-1);
        var modified = DateTimeOffset.UtcNow.AddHours(-2);

        // Act
        FileHelper.SetTimestamps(_tempFile, created, modified);
        FileHelper.GetTimestamps(_tempFile, out var gotCreated, out var gotModified);

        // Assert (use a small tolerance, filesystem granularity)
        Assert.InRange(gotCreated,  created.AddSeconds(-1),  created.AddSeconds(1));
        Assert.InRange(gotModified, modified.AddSeconds(-1), modified.AddSeconds(1));
    }

    [Fact]
    public void GetFileAcl_And_ApplyAcl_UnixOnly()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return;  // skip on Windows

        // Arrange: set file mode to rwxr-----  (owner=rwx, group=r--, other=---)
        var mode = UnixFileMode.UserRead  | UnixFileMode.UserWrite  | UnixFileMode.UserExecute
                   | UnixFileMode.GroupRead 
            ;
        File.SetUnixFileMode(_tempFile, mode);

        // Act
        var acls = FileHelper.GetFileAcl(_tempFile);

        // Assert initial modes
        Assert.Contains(acls, e => e.Identity == "owner" && e.Permissions == "rwx");
        Assert.Contains(acls, e => e.Identity == "group" && e.Permissions == "r--");
        Assert.Contains(acls, e => e.Identity == "other" && e.Permissions == "---");

        // Now invert owner permissions to --- and group to rwx
        var newAcls = acls
            .Select(e =>
            {
                return e.Identity switch
                {
                    "owner" => new AclEntry("owner", "---", "POSIX"),
                    "group" => new AclEntry("group", "rwx", "POSIX"),
                    "other" => e,
                    _       => e
                };
            })
            .ToArray();

        // Act apply
        FileHelper.ApplyAcl(newAcls, _tempFile);

        // Read back
        var after = FileHelper.GetFileAcl(_tempFile);
        Assert.Contains(after, e => e is { Identity: "owner", Permissions: "---" });
        Assert.Contains(after, e => e is { Identity: "group", Permissions: "rwx" });
    }

    [Fact]
    public async Task GetOwnerGroupAsync_UnixOrWindows()
    {
        // We just assert it doesn't throw and returns non-null strings.
        var (owner, group) = await FileHelper.GetOwnerGroupAsync(_tempFile, CancellationToken.None);
        Assert.False(string.IsNullOrEmpty(owner));
        // On Windows group may be empty
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            Assert.False(string.IsNullOrEmpty(group));
    }

    [Fact]
    public async Task SetOwnerGroupAsync_DoesNotThrow()
    {
        // We call SetOwnerGroupAsync with no-op inputs to verify no exception
        await FileHelper.SetOwnerGroupAsync(_tempFile, "nobody", "nogroup", CancellationToken.None);
        await FileHelper.SetOwnerGroupAsync(_tempFile, "", "", CancellationToken.None);
    }
}