using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;

namespace aws_backup;

public sealed record AwsTemporaryCredentials(
    string? AccessKeyId,
    string? SecretAccessKey,
    string? SessionToken);

public interface ITemporaryCredentialsServer
{
    Task<AwsTemporaryCredentials> GetCredentials(
        string profileArn,
        string roleArn,
        string trustAnchorArn,
        string certificateFileName,
        string privateKeyFileName,
        string region,
        int sessionDuration = 43200, // default 12 hours
        CancellationToken cancellation = default);
}

public sealed class RolesAnywhere : ITemporaryCredentialsServer
{
    public async Task<AwsTemporaryCredentials> GetCredentials(
        string profileArn,
        string roleArn,
        string trustAnchorArn,
        string certificateFileName,
        string privateKeyFileName,
        string region,
        int sessionDuration = 43200, // default 12 hours
        CancellationToken cancellation = default)
    {
        var endpoint = new Uri($"https://rolesanywhere.{region}.amazonaws.com");
        var url = new Uri(endpoint, "/sessions");

        // 1) Load CA cert and key
        var certPem = await File.ReadAllTextAsync(certificateFileName, cancellation);
        var cert = X509Certificate2.CreateFromPem(certPem);
        cert = cert.CopyWithPrivateKey(null!); // ensure public-only
        var serial = cert.SerialNumber; // hex string
        var derPub = cert.Export(X509ContentType.Cert);
        var x509Header = Convert.ToBase64String(derPub);

        // 2) Load private key
        var keyPem = await File.ReadAllTextAsync(privateKeyFileName, cancellation);
        using var rsa = RSA.Create();
        rsa.ImportFromPem(keyPem.ToCharArray());

        // 4) Dates
        var now = DateTime.UtcNow;
        var amzDate = now.ToString("yyyyMMdd'T'HHmmss'Z'");
        var dateStamp = now.ToString("yyyyMMdd");

        // 5) Payload
        var payload = $$"""
                        {
                            ""durationSeconds"": {{sessionDuration}},
                            ""profileArn"" : ""{{profileArn}}"",
                            ""roleArn"" : ""{{roleArn}}"",
                            ""trustAnchorArn"" : ""{{trustAnchorArn}}""
                        }
                        """;
        var payloadHash = ToHex(HashSha256(Encoding.UTF8.GetBytes(payload)));

        // 6) Headers
        var headers = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["content-type"] = "application/json",
            ["host"] = url.Host,
            ["x-amz-date"] = amzDate,
            ["x-amz-x509"] = x509Header
        };

        // 7) Canonical request
        var signedHeaders = string.Join(";", headers.Keys);
        var canonicalHeaders = string.Join("\n", headers.Select(kv => $"{kv.Key}:{kv.Value}")) + "\n";
        var canonicalRequest = new StringBuilder()
            .AppendLine("POST")
            .AppendLine(url.AbsolutePath)
            .AppendLine("") // no query
            .AppendLine(canonicalHeaders)
            .AppendLine(signedHeaders)
            .Append(payloadHash)
            .ToString();
        var canonicalRequestHash = ToHex(HashSha256(Encoding.UTF8.GetBytes(canonicalRequest)));

        // 8) String to sign
        const string algorithm = "AWS4-X509-RSA-SHA256";
        var credentialScope = $"{dateStamp}/{region}/rolesanywhere/aws4_request";
        var stringToSign = new StringBuilder()
            .AppendLine(algorithm)
            .AppendLine(amzDate)
            .AppendLine(credentialScope)
            .Append(canonicalRequestHash)
            .ToString();

        // 9) Signature
        var sig = rsa.SignData(
            Encoding.UTF8.GetBytes(stringToSign),
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        var signature = ToHex(sig);

        // 10) Authorization header
        var credential = $"{serial}/{credentialScope}";
        var auth = $"{algorithm} Credential={credential}, SignedHeaders={signedHeaders}, Signature={signature}";
        headers["authorization"] = auth;

        // 11) Send
        using var client = new HttpClient();
        var req = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(payload, Encoding.UTF8, "application/json")
        };
        foreach (var kv in headers)
            req.Headers.TryAddWithoutValidation(kv.Key, kv.Value);

        var resp = await client.SendAsync(req, cancellation);
        var json = await resp.Content.ReadAsStringAsync(cancellation);
        return Parse(json);
    }

    private static AwsTemporaryCredentials Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // navigate to credentialSet[0].credentials
        if (!root.TryGetProperty("credentialSet", out var set) || set.GetArrayLength() == 0)
            throw new FormatException("credentialSet array not found or empty");

        var credentialsElem = set[0]
            .GetProperty("credentials"); // will throw if missing

        var accessKeyId = credentialsElem.GetProperty("accessKeyId").GetString();
        var secretAccessKey = credentialsElem.GetProperty("secretAccessKey").GetString();
        var sessionToken = credentialsElem.GetProperty("sessionToken").GetString();

        return new AwsTemporaryCredentials(accessKeyId, secretAccessKey, sessionToken);
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