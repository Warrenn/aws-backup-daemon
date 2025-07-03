using System.Text.RegularExpressions;

namespace aws_backup;

public sealed partial class RegexHelper
{
    [GeneratedRegex(@"^[A-Za-z]:(?:\\|/)")]
    public static partial Regex DriveRootRegex();
    
    [GeneratedRegex( @"([^\w\d_-])")]
    public static partial Regex NonAlphanumericRegex();
}