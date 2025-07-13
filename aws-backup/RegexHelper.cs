using System.Text.RegularExpressions;

namespace aws_backup;

public sealed partial class RegexHelper
{
    [GeneratedRegex(@"^[A-Za-z]:(?:\\|/)")]
    public static partial Regex DriveRootRegex();
    
    [GeneratedRegex( @"([^\w\d_-])")]
    public static partial Regex NonAlphanumericRegex();
    
    [GeneratedRegex(@"(\d{4})[-]?(\d{2})[-]?(\d{2})")]
    public static partial Regex DateRegex();

    [GeneratedRegex(@"[^A-Za-z0-9+/\\ _\-.:=]")]
    public static partial Regex InValidTagValuesRegex();
}