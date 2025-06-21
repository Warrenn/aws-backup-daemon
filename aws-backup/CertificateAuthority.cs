using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Operators;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;
using Org.BouncyCastle.X509.Extension;

namespace aws_backup;

public static class CertificateAuthority
{
    private static SecureRandom GetRandom() => new(new CryptoApiRandomGenerator());

    public static void Main()
    {
        string username = Environment.UserName;

        // 1) Generate CA
        var caKeyPair = GenerateRsaKeyPair(2048);
        var caCert    = GenerateCaCertificate(caKeyPair, $"Tierpoint Root CA {username}");

        // 2) Generate client cert
        var clientKeyPair = GenerateRsaKeyPair(2048);
        var clientCert    = GenerateClientCertificate(
            clientKeyPair.Public, caKeyPair.Private, caCert, username);

        // 3) Export as PEM
        Console.WriteLine("-----BEGIN CA PRIVATE KEY-----");
        ExportPrivateKeyPem(caKeyPair.Private);
        Console.WriteLine("-----END CA PRIVATE KEY-----\n");

        Console.WriteLine("-----BEGIN CA CERTIFICATE-----");
        ExportCertificatePem(caCert);
        Console.WriteLine("-----END CA CERTIFICATE-----\n");

        Console.WriteLine("-----BEGIN CLIENT PRIVATE KEY-----");
        ExportPrivateKeyPem(clientKeyPair.Private);
        Console.WriteLine("-----END CLIENT PRIVATE KEY-----\n");

        Console.WriteLine("-----BEGIN CLIENT CERTIFICATE-----");
        ExportCertificatePem(clientCert);
        Console.WriteLine("-----END CLIENT CERTIFICATE-----");
    }

    private static AsymmetricCipherKeyPair GenerateRsaKeyPair(int bits)
    {
        var gen = new Org.BouncyCastle.Crypto.Generators.RsaKeyPairGenerator();
        gen.Init(new KeyGenerationParameters(GetRandom(), bits));
        return gen.GenerateKeyPair();
    }

    static X509Certificate GenerateCaCertificate(
        AsymmetricCipherKeyPair keyPair,
        string subjectCn)
    {
        var certGen = new X509V3CertificateGenerator();
        var cn        = new X509Name($"CN={subjectCn}");
        var serial    = BigInteger.ProbablePrime(120, GetRandom());
        var notBefore = DateTime.UtcNow.Date;
        var notAfter  = notBefore.AddYears(10);

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
        string subjectCn)
    {
        var certGen = new X509V3CertificateGenerator();
        var subj     = new X509Name($"CN={subjectCn}");
        var issuer   = caCert.SubjectDN;
        var serial   = BigInteger.ProbablePrime(120, GetRandom());
        var notBefore= DateTime.UtcNow.Date;
        var notAfter = notBefore.AddYears(1);

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

    private static void ExportPrivateKeyPem(AsymmetricKeyParameter privateKey)
    {
        var w = new PemWriter(Console.Out);
        w.WriteObject(privateKey);
        w.Writer.Flush();
    }

    private static void ExportCertificatePem(X509Certificate cert)
    {
        var w = new PemWriter(Console.Out);
        w.WriteObject(cert);
        w.Writer.Flush();
    }
}