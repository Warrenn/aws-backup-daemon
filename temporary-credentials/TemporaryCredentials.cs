using System.Net.Http.Headers;
using System.Numerics;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;

namespace temporary_credentials;

public sealed record AwsTemporaryCredentials(
    string? AccessKeyId,
    string? SecretAccessKey,
    string? SessionToken,
    string? Expiration);

public interface ITemporaryCredentialsServer
{
    Task<(AwsTemporaryCredentials? credentials, string? errorMessage)> GetCredentials(
        string profileArn,
        string roleArn,
        string trustAnchorArn,
        string certificateFileName,
        string privateKeyFileName,
        string region,
        int sessionDuration,
        CancellationToken cancellationToken = default);
}

public sealed class RolesAnywhere : ITemporaryCredentialsServer
{
    public async Task<(AwsTemporaryCredentials? credentials, string? errorMessage)> GetCredentials(
        string profileArn,
        string roleArn,
        string trustAnchorArn,
        string certificateFileName,
        string privateKeyFileName,
        string region,
        int sessionDuration,
        CancellationToken cancellationToken = default)
    {
        var endpoint = new Uri($"https://rolesanywhere.{region}.amazonaws.com");
        var url = new Uri(endpoint, "/sessions");
        sessionDuration = Math.Max(sessionDuration, 900);

        var keyPem = await File.ReadAllTextAsync(privateKeyFileName, cancellationToken);
        using var rsa = RSA.Create();
        rsa.ImportFromPem(keyPem.ToCharArray());

        var certPem = await File.ReadAllTextAsync(certificateFileName, cancellationToken);
        var cert = X509Certificate2.CreateFromPem(certPem);
        var serialHex = cert.SerialNumber; // hex string
        var derPub = cert.Export(X509ContentType.Cert);
        var x509Header = Convert.ToBase64String(derPub);

        var serialBytes = Enumerable.Range(0, serialHex.Length / 2)
            .Select(i => Convert.ToByte(serialHex.Substring(i * 2, 2), 16))
            .Reverse() // reverse to get DER order
            .ToArray();

        var serialBigInt =
            new BigInteger(serialBytes.Concat(new byte[] { 0 }).ToArray()); // add 0 to ensure non-negative
        var serial = serialBigInt.ToString();

        var now = DateTime.UtcNow; // example date, replace with DateTime.UtcNow for real use
        var amzDate = now.ToString("yyyyMMdd'T'HHmmss'Z'");
        var dateStamp = now.ToString("yyyyMMdd");

        var payload =
            $"{{\"durationSeconds\":{sessionDuration},\"profileArn\":\"{profileArn}\",\"roleArn\":\"{roleArn}\",\"trustAnchorArn\":\"{trustAnchorArn}\"}}";
        var payloadHash = ToHex(HashSha256(Encoding.UTF8.GetBytes(payload)));

        var headers = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["Content-Type"] = "application/json",
            ["Host"] = url.Host,
            ["X-Amz-Date"] = amzDate,
            ["X-Amz-X509"] = x509Header
        };

        var signedHeaders = string.Join(";", headers.Keys.Select(k => k.ToLowerInvariant().Trim()));
        var canonicalHeaders =
            string.Join('\n', headers.Select(kv => $"{kv.Key.ToLowerInvariant().Trim()}:{kv.Value.Trim()}")) + '\n';
        var canonicalRequest =
            string.Join('\n', "POST", url.AbsolutePath, "", canonicalHeaders, signedHeaders, payloadHash);
        var canonicalRequestHash = ToHex(HashSha256(Encoding.UTF8.GetBytes(canonicalRequest)));

        const string algorithm = "AWS4-X509-RSA-SHA256";
        var credentialScope = $"{dateStamp}/{region}/rolesanywhere/aws4_request";
        var stringToSign = new StringBuilder()
            .Append(algorithm).Append('\n')
            .Append(amzDate).Append('\n')
            .Append(credentialScope).Append('\n')
            .Append(canonicalRequestHash).ToString();

        var sig = rsa.SignData(
            Encoding.UTF8.GetBytes(stringToSign),
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        var signature = ToHex(sig);

        var credential = $"{serial}/{credentialScope}";
        var auth = $"{algorithm} Credential={credential}, SignedHeaders={signedHeaders}, Signature={signature}";
        headers["Authorization"] = auth;

        using var client = new HttpClient();
        var req = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(payload, Encoding.UTF8, "application/json")
        };
        req.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

        foreach (var kv in headers)
            req.Headers.TryAddWithoutValidation(kv.Key, kv.Value);

        var resp = await client.SendAsync(req, cancellationToken);

        if (!resp.IsSuccessStatusCode) return (null, resp.ReasonPhrase);

        var json = await resp.Content.ReadAsStringAsync(cancellationToken);
        var credentials = Parse(json);
        return (credentials, null);
    }

    private static AwsTemporaryCredentials Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        if (!root.TryGetProperty("credentialSet", out var set) || set.GetArrayLength() == 0)
            throw new FormatException("credentialSet array not found or empty");

        var credentialsElem = set[0]
            .GetProperty("credentials"); // will throw if missing

        var accessKeyId = credentialsElem.GetProperty("accessKeyId").GetString();
        var secretAccessKey = credentialsElem.GetProperty("secretAccessKey").GetString();
        var sessionToken = credentialsElem.GetProperty("sessionToken").GetString();
        var expiration = credentialsElem.GetProperty("expiration").GetString();

        return new AwsTemporaryCredentials(accessKeyId, secretAccessKey, sessionToken, expiration);
    }

    private static byte[] HashSha256(byte[] data)
    {
        return SHA256.HashData(data);
    }

    private static string ToHex(byte[] data)
    {
        return Convert.ToHexStringLower(data);
    }
}