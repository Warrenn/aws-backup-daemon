using System.Text.RegularExpressions;

namespace aws_backup;

public partial class RegexHelper
{
    [GeneratedRegex(@"^[A-Za-z]:(?:\\|/)")]
    public static partial Regex DriveRootRegex();
    
    [GeneratedRegex( @"[^\w\d]")]
    public static partial Regex NonAlphanumericRegex();
}