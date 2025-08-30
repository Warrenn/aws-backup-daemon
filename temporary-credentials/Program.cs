// See https://aka.ms/new-console-template for more information

using temporary_credentials;

var certificateFile = GetArgValue(args, "certificate");
var privateKeyFile = GetArgValue(args, "private-key");
var profileArn = GetArgValue(args, "profile-arn");
var trustAnchorArn = GetArgValue(args, "trust-anchor-arn");
var roleArn = GetArgValue(args, "role-arn");
var region = GetArgValue(args, "region");
var sessionDurationStr = GetArgValue(args, "session-duration");
int? sessionDuration = null;
if (int.TryParse(sessionDurationStr, out var duration))
{
    sessionDuration = duration;
}

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

Console.WriteLine(
    $$"""
    {
        "AccessKeyId": "{{credentials.credentials.AccessKeyId}}",
        "SecretAccessKey": "{{credentials.credentials.SecretAccessKey}}",
        "SessionToken": "{{credentials.credentials.SessionToken}}",
        "Expiration": "{{credentials.credentials.Expiration}}"
    }
    """);
return 0;
        
string GetArgValue(string[] args, string argName)
{
    argName = argName.StartsWith("--") ? argName : $"--{argName}";
    var argIndex = Array.FindIndex(args, a => a.Equals(argName, StringComparison.OrdinalIgnoreCase));
    if (argIndex >= 0 && argIndex < args.Length - 1)
    {
        return args[argIndex + 1];
    }   
    return string.Empty;
}