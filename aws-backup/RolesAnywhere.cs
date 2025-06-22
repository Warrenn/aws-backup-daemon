using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;

namespace aws_backup;

public class RolesAnywhere
{
    // private static async Task<int> Main(string[] args)
    // {
    //     var sessionDurationOpt = new Option<int>("--session-duration") { DefaultValueFactory = _ => 43200 };
    //     var profileArnOpt = new Option<string>("--profile-arn") { Required = true };
    //     var roleArnOpt = new Option<string>("--role-arn") { Required = true };
    //     var trustAnchorArnOpt = new Option<string>("--trust-anchor-arn") { Required = true };
    //     var certificateOpt = new Option<FileInfo>("--certificate") { Required = true };
    //     var privateKeyOpt = new Option<FileInfo>("--private-key") { Required = true };
    //     var regionOpt = new Option<string>("--region") { DefaultValueFactory = _ => "us-east-1" };
    //
    //     var root = new RootCommand
    //     {
    //         sessionDurationOpt,
    //         profileArnOpt,
    //         roleArnOpt,
    //         trustAnchorArnOpt,
    //         certificateOpt,
    //         privateKeyOpt,
    //         regionOpt
    //     };
    //
    //     root.Description = "AWS RolesAnywhere session creator";
    //     root.SetAction(async context =>
    //     {
    //         var sessionDuration = context.GetValue(sessionDurationOpt);
    //         var profileArn = context.GetValue(profileArnOpt);
    //         var roleArn = context.GetValue(roleArnOpt);
    //         var trustAnchorArn = context.GetValue(trustAnchorArnOpt);
    //         var certificate = context.GetValue(certificateOpt);
    //         var privateKey = context.GetValue(privateKeyOpt);
    //         var region = context.GetValue(regionOpt);
    //         
    //     });
    //     return await root.Parse(args).InvokeAsync();
    // }

    public static async Task<string> Run(
        string profileArn,
        string roleArn,
        string trustAnchorArn,
        string certificateFileName,
        string privateKeyFileName,
        string region,
        int sessionDuration = 43200) // default 12 hours
    {
        var endpoint = new Uri($"https://rolesanywhere.{region}.amazonaws.com");
        var url = new Uri(endpoint, "/sessions");

        // 1) Load CA cert and key
        var certPem = await File.ReadAllTextAsync(certificateFileName);
        var cert = X509Certificate2.CreateFromPem(certPem);
        cert = cert.CopyWithPrivateKey(null); // ensure public-only
        var serial = cert.SerialNumber; // hex string
        var derPub = cert.Export(X509ContentType.Cert);
        var x509Header = Convert.ToBase64String(derPub);

        // 2) Load private key
        var keyPem = await File.ReadAllTextAsync(privateKeyFileName);
        using var rsa = RSA.Create();
        rsa.ImportFromPem(keyPem.ToCharArray());

        // 4) Dates
        var now = DateTime.UtcNow;
        var amzDate = now.ToString("yyyyMMdd'T'HHmmss'Z'");
        var dateStamp = now.ToString("yyyyMMdd");

        // 5) Payload
        var payloadObj = new Dictionary<string, object>
        {
            ["durationSeconds"] = sessionDuration,
            ["profileArn"] = profileArn,
            ["roleArn"] = roleArn,
            ["trustAnchorArn"] = trustAnchorArn
        };
        var payload = JsonSerializer.Serialize(payloadObj, new JsonSerializerOptions { WriteIndented = false });
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

        var resp = await client.SendAsync(req);
        return await resp.Content.ReadAsStringAsync();
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