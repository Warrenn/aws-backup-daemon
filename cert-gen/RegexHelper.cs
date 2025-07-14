using System.Text.RegularExpressions;

namespace cert_gen;

public static partial class RegexHelper
{
    [GeneratedRegex( @"([^\w\d_-])")]
    public static partial Regex NonAlphanumericRegex();
}