using System.CommandLine;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Operators;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;
using Org.BouncyCastle.X509.Extension;

namespace cert_gen;

public static class Program
{
    private static SecureRandom GetRandom()
    {
        return new SecureRandom(new CryptoApiRandomGenerator());
    }

    public static async Task<int> Main(string[] args)
    {
        var usernameOpt = new Option<string>("--username", "-u")
        {
            Description = "Username to use in the certificate CN (defaults to current user name).",
            Required = false,
            DefaultValueFactory = _ => Environment.UserName
        };
        var caPrivateKeyOpt = new Option<string>("--ca-private-key", "-c")
        {
            Description = "Path to the CA private key file (PEM format).",
            Required = true
        };
        var caCertOpt = new Option<string>("--ca-cert", "-C")
        {
            Description = "Path to the public CA certificate file (PEM format).",
            Required = true
        };
        var clientPrivateKeyOpt = new Option<string>("--client-private-key", "-k")
        {
            Description = "Path to the client private key file (PEM format).",
            Required = true
        };
        var clientCertOpt = new Option<string>("--client-cert", "-K")
        {
            Description = "Path to the public client certificate file (PEM format).",
            Required = true
        };
        var expiryOpt = new Option<DateTime>("--expiry", "-e")
        {
            Description = "Certificate expiry date in ISO 8601 format (defaults to 10 years from now).",
            Required = false,
            DefaultValueFactory = _ => DateTime.UtcNow.AddYears(10)
        };

        var rootCommand = new RootCommand
        {
            usernameOpt,
            caPrivateKeyOpt,
            caCertOpt,
            clientPrivateKeyOpt,
            clientCertOpt,
            expiryOpt
        };

        rootCommand.SetAction(parsedArgs =>
        {
            var username = parsedArgs.GetRequiredValue(usernameOpt);
            var caPrivateKeyPath = parsedArgs.GetRequiredValue(caPrivateKeyOpt);
            var caCertPath = parsedArgs.GetRequiredValue(caCertOpt);
            var clientPrivateKeyPath = parsedArgs.GetRequiredValue(clientPrivateKeyOpt);
            var clientCertPath = parsedArgs.GetRequiredValue(clientCertOpt);
            var expiryDate = parsedArgs.GetValue(expiryOpt);

            username = RegexHelper
                .NonAlphanumericRegex()
                .Replace(username, string.Empty)
                .Trim()
                .ToLowerInvariant();

            // 1) Generate CA
            var caKeyPair = GenerateRsaKeyPair(2048);
            var caCert = GenerateCaCertificate(caKeyPair, $"Root CA {username}-ca", expiryDate);

            // 2) Generate client cert
            var clientKeyPair = GenerateRsaKeyPair(2048);
            var clientCert = GenerateClientCertificate(
                clientKeyPair.Public, caKeyPair.Private, caCert, $"{username}-client", expiryDate);

            // 3) Export as PEM
            ExportPrivateKeyPem(caKeyPair.Private, caPrivateKeyPath);

            ExportCertificatePem(caCert, caCertPath);

            ExportPrivateKeyPem(clientKeyPair.Private, clientPrivateKeyPath);

            ExportCertificatePem(clientCert, clientCertPath);

            return 0;
        });

        rootCommand.Description = "Generate the certificates needed for AWS RolesAnywhere";
        var parseResult = rootCommand.Parse(args);
        return await parseResult.InvokeAsync();
    }

    private static AsymmetricCipherKeyPair GenerateRsaKeyPair(int bits)
    {
        var gen = new RsaKeyPairGenerator();
        gen.Init(new KeyGenerationParameters(GetRandom(), bits));
        return gen.GenerateKeyPair();
    }

    private static X509Certificate GenerateCaCertificate(
        AsymmetricCipherKeyPair keyPair,
        string subjectCn,
        DateTime notAfter)
    {
        var certGen = new X509V3CertificateGenerator();
        var cn = new X509Name($"CN={subjectCn}");
        var serial = BigInteger.ProbablePrime(120, GetRandom());
        var notBefore = DateTime.UtcNow.Date;

        certGen.SetSerialNumber(serial);
        certGen.SetIssuerDN(cn);
        certGen.SetNotBefore(notBefore);
        certGen.SetNotAfter(notAfter);
        certGen.SetSubjectDN(cn);
        certGen.SetPublicKey(keyPair.Public);
        certGen.SetSignatureAlgorithm("SHA256WithRSAEncryption");

        // v3_ca extensions
        certGen.AddExtension(
            X509Extensions.BasicConstraints,
            true,
            new BasicConstraints(true)); // CA:TRUE
        certGen.AddExtension(
            X509Extensions.KeyUsage,
            true,
            new KeyUsage(KeyUsage.KeyCertSign | KeyUsage.CrlSign));
        certGen.AddExtension(
            X509Extensions.SubjectKeyIdentifier,
            false,
            new SubjectKeyIdentifierStructure(keyPair.Public));
        certGen.AddExtension(
            X509Extensions.AuthorityKeyIdentifier,
            false,
            new AuthorityKeyIdentifierStructure(keyPair.Public));

        return certGen.Generate(new Asn1SignatureFactory(
            "SHA256WithRSAEncryption", keyPair.Private, GetRandom()));
    }

    private static X509Certificate GenerateClientCertificate(
        AsymmetricKeyParameter clientPub,
        AsymmetricKeyParameter caPrivate,
        X509Certificate caCert,
        string subjectCn,
        DateTime notAfter)
    {
        var certGen = new X509V3CertificateGenerator();
        var subj = new X509Name($"CN={subjectCn}");
        var issuer = caCert.SubjectDN;
        var serial = BigInteger.ProbablePrime(120, GetRandom());
        var notBefore = DateTime.UtcNow.Date;

        certGen.SetSerialNumber(serial);
        certGen.SetIssuerDN(issuer);
        certGen.SetNotBefore(notBefore);
        certGen.SetNotAfter(notAfter);
        certGen.SetSubjectDN(subj);
        certGen.SetPublicKey(clientPub);
        certGen.SetSignatureAlgorithm("SHA256WithRSAEncryption");

        // v3_req extensions
        certGen.AddExtension(
            X509Extensions.KeyUsage,
            true,
            new KeyUsage(KeyUsage.DigitalSignature));
        certGen.AddExtension(
            X509Extensions.ExtendedKeyUsage,
            false,
            new ExtendedKeyUsage(KeyPurposeID.IdKPClientAuth));

        // include authority key identifier so AWS RolesAnywhere can verify chain
        certGen.AddExtension(
            X509Extensions.AuthorityKeyIdentifier,
            false,
            new AuthorityKeyIdentifierStructure(caCert.GetPublicKey()));

        return certGen.Generate(new Asn1SignatureFactory(
            "SHA256WithRSAEncryption", caPrivate, GetRandom()));
    }

    private static void ExportPrivateKeyPem(AsymmetricKeyParameter privateKey, string filePath)
    {
        using var writer = new StreamWriter(filePath, false);
        var w = new PemWriter(writer);
        w.WriteObject(privateKey);
        w.Writer.Flush();
    }

    private static void ExportCertificatePem(X509Certificate cert, string filePath)
    {
        using var writer = new StreamWriter(filePath, false);
        var w = new PemWriter(writer);
        w.WriteObject(cert);
        w.Writer.Flush();
    }
}