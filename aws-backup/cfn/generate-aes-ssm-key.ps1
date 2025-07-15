param (
    [string]$ParamName
)

$ErrorActionPreference = "Stop"

if (-not $ParamName) {
    Write-Host "Usage: .\generate-aes-ssm-key.ps1 -ParamName <ssm-parameter-name>"
    exit 1
}

$REGION = $env:AWS_REGION
if (-not $REGION) { $REGION = "us-east-1" }

# 1. Generate a raw 32-byte key and base64 encode
$rawKey = New-Object byte[] 32
[System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($rawKey)
$base64Key = [Convert]::ToBase64String($rawKey)

# 2. Validate it decodes to exactly 32 bytes
$decodedBytes = [System.Convert]::FromBase64String($base64Key)
if ($decodedBytes.Length -ne 32) {
    Write-Host "Error: Generated key is not 32 bytes after base64 decoding"
    exit 1
}

# 3. Check if parameter exists
$exists = & {
    aws ssm get-parameter --name $ParamName --region $REGION --with-decryption
    if ($LASTEXITCODE -ne 0) {
        return $false
    }
    return $true
}

if ($exists) {
    Write-Host "✅ AES Parameter found for $ParamName. No need to create a new one."
    exit 0
}

# 4. Store in SSM
aws ssm put-parameter `
    --name $ParamName `
    --value $base64Key `
    --type SecureString `
    --overwrite `
    --region $REGION

Write-Host "✅ AES key stored in SSM parameter: $ParamName"
