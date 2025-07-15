using System.Security.Cryptography;
using System.Text;

namespace aws_backup_common;

public static class Base64Url
{
    /// <summary>
    /// Encode bytes into a URL-safe Base64 string (no padding).
    /// </summary>
    public static string Encode(byte[] data)
    {
        var b64 = Convert.ToBase64String(data);
        return b64
            .TrimEnd('=')       // remove any trailing '='s
            .Replace('+', '-')  // 62nd char of encoding
            .Replace('/', '_'); // 63rd char of encoding
    }
    
            
    public static string ComputeSimpleHash(string input)
    {
        var data = Encoding.UTF8.GetBytes(input);
        var hash = MD5.HashData(data);
        return Encode(hash);
    }


    /// <summary>
    /// Decode a URL-safe Base64 string back into the original bytes.
    /// </summary>
    public static byte[] Decode(string urlSafe)
    {
        // 1) Reverse URL-safe replacements
        var b64 = urlSafe
            .Replace('-', '+')
            .Replace('_', '/');

        // 2) Pad with '=' to multiple of 4
        switch (b64.Length % 4)
        {
            case 2: b64 += "=="; break;
            case 3: b64 += "=";  break;
            case 0: break;
            default:
                throw new FormatException("Invalid Base64Url string!");
        }

        // 3) Standard Base64 decode
        return Convert.FromBase64String(b64);
    }
}