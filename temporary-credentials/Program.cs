// See https://aka.ms/new-console-template for more information

using Cocona;
using temporary_credentials;

await CoconaApp.RunAsync(async (
        [Argument(Name = "certificate")] string certificateFile,
        [Argument(Name = "private-key")] string privateKeyFile,
        [Argument(Name = "profile-arn")] string profileArn,
        [Argument(Name = "trust-anchor-arn")] string trustAnchorArn,
        [Argument(Name = "role-arn")] string roleArn,
        [Option(ValueName = "region")] string? region,
        [Option(ValueName = "session-duration")]
        int? sessionDuration) =>
    {
        if (string.IsNullOrWhiteSpace(certificateFile) || !File.Exists(certificateFile))
        {
            Console.Error.WriteLine("Certificate file is required");
            return -1;
        }

        if (string.IsNullOrWhiteSpace(privateKeyFile) || !File.Exists(privateKeyFile))
        {
            Console.Error.WriteLine("Private key file is required");
            return -1;
        }

        if (string.IsNullOrWhiteSpace(profileArn))
        {
            Console.Error.WriteLine("Profile ARN is required");
            return -1;
        }

        if (string.IsNullOrWhiteSpace(trustAnchorArn))
        {
            Console.Error.WriteLine("Trust Anchor ARN is required");
        }

        if (string.IsNullOrWhiteSpace(roleArn))
        {
            Console.Error.WriteLine("Role ARN is required");
            return -1;
        }

        var awsRegion = string.IsNullOrWhiteSpace(region)
            ? string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("AWS_DEFAULT_REGION"))
                ? string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("AWS_REGION"))
                    ? "us-east-1"
                    : Environment.GetEnvironmentVariable("AWS_REGION")!
                : Environment.GetEnvironmentVariable("AWS_DEFAULT_REGION")!
            : region;

        if (sessionDuration is null or < 900 or > 3600)
        {
            sessionDuration = 3600;
        }

        var generator = new RolesAnywhere();
        var credentials = await generator.GetCredentials(
            profileArn,
            roleArn,
            trustAnchorArn,
            certificateFile,
            privateKeyFile,
            awsRegion,
            sessionDuration.Value);
        if (credentials.credentials is null)
        {
            Console.Error.WriteLine($"Error getting credentials: {credentials.errorMessage}");
            return -1;
        }

        Console.WriteLine($$"""
                            {
                            "AccessKeyId": "{{credentials.credentials.AccessKeyId}}",
                            "SecretAccessKey": "{{credentials.credentials.SecretAccessKey}}",
                            "SessionToken": "{{credentials.credentials.SessionToken}}",
                            "Expiration": "{{credentials.credentials.Expiration}}"
                            }
                            """);
        return 0;
    });
    return 0;