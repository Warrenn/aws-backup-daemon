using System.Security.Cryptography;
using System.Text;

namespace aws_backup_common;

public static class AesHelper
{
    /// <summary>
    /// Encrypts a UTF-8 string using AES-256-CBC with PKCS7 padding.
    /// Prepends the 16-byte IV to the ciphertext, and returns the whole thing as Base64.
    /// </summary>
    /// <param name="plainText">The text to encrypt.</param>
    /// <param name="key">32-byte AES key.</param>
    public static string EncryptString(string plainText, byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (key.Length != 32) throw new ArgumentException("Key must be 32 bytes for AES-256", nameof(key));

        using var aes = Aes.Create();
        aes.KeySize   = 256;
        aes.Key       = key;
        aes.Mode      = CipherMode.CBC;
        aes.Padding   = PaddingMode.PKCS7;
        aes.GenerateIV();

        using var msEncrypt = new MemoryStream();
        // first write out the IV
        msEncrypt.Write(aes.IV, 0, aes.IV.Length);

        using (var encryptor = aes.CreateEncryptor())
        using (var csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
        using (var writer    = new StreamWriter(csEncrypt, Encoding.UTF8))
        {
            writer.Write(plainText);
        }

        // get the IV + ciphertext
        var encryptedBytes = msEncrypt.ToArray();
        return Convert.ToBase64String(encryptedBytes);
    }

    /// <summary>
    /// Decrypts a Base64-encoded AES payload where the first 16 bytes are the IV.
    /// Assumes AES-256-CBC with PKCS7 padding.
    /// </summary>
    /// <param name="base64CipherText">The Base64-encoded IV + ciphertext.</param>
    /// <param name="key">32-byte AES key.</param>
    public static string DecryptString(string base64CipherText, byte[] key)
    {
        ArgumentNullException.ThrowIfNull(base64CipherText);
        ArgumentNullException.ThrowIfNull(key);
        if (key.Length != 32) throw new ArgumentException("Key must be 32 bytes for AES-256", nameof(key));

        var fullCipher = Convert.FromBase64String(base64CipherText);
        if (fullCipher.Length < 16)
            throw new ArgumentException("Invalid cipher text, too short to contain IV.", nameof(base64CipherText));

        // Extract IV and ciphertext
        var iv = new byte[16];
        Array.Copy(fullCipher, 0, iv, 0, iv.Length);
        var cipherBytes = new byte[fullCipher.Length - iv.Length];
        Array.Copy(fullCipher, iv.Length, cipherBytes, 0, cipherBytes.Length);

        using var aes = Aes.Create();
        aes.KeySize   = 256;
        aes.Key       = key;
        aes.IV        = iv;
        aes.Mode      = CipherMode.CBC;
        aes.Padding   = PaddingMode.PKCS7;

        using var decryptor = aes.CreateDecryptor();
        using var msDecrypt = new MemoryStream(cipherBytes);
        using var csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read);
        using var reader    = new StreamReader(csDecrypt, Encoding.UTF8);
        return reader.ReadToEnd();
    }
}