using System.Text;

namespace aws_backup;

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
    
    /// <summary>
    /// Convenience overload for encoding a UTF8 string.
    /// </summary>
    public static string EncodeUtf8(string text)
    {
        return Encode(Encoding.UTF8.GetBytes(text));
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
    
    /// <summary>
    /// Convenience overload for decoding a URL-safe Base64 string to a UTF8 string.
    /// </summary>
    public static string DecodeUrl64ToUtf8(string urlSafe)
    {
        var bytes = Decode(urlSafe);
        return Encoding.UTF8.GetString(bytes);
    }
}