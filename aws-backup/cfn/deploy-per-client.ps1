param (
    [string]$RawClientId,
    [string]$PemFile = ""
)

$ErrorActionPreference = "Stop"

if (-not $RawClientId -or -not $PemFile) {
    Write-Host "Usage: .\deploy-per-client.ps1 <client-id> <path-to-pem-file>"
    exit 1
}

# Sanitize client ID
$ClientId = ($RawClientId.ToLower() -replace '[^a-z0-9_-]', '')
if (-not $ClientId) {
    Write-Host "Error: scrubbed client-id is empty after sanitization"
    exit 1
}

if (-not (Test-Path $PemFile)) {
    Write-Host "Error: PEM file '$PemFile' does not exist"
    exit 1
}

# Read certificate contents
$CertificateData = Get-Content -Path $PemFile -Raw

# Template and stack info
$TemplateFile = "per-client.yaml"
$StackName = "per-client-$ClientId"
$Region = $env:AWS_REGION
if (-not $Region) { $Region = "us-east-1" }

function Get-StackStatus {
    aws cloudformation describe-stacks `
        --region $Region `
        --stack-name $StackName `
        --query 'Stacks[0].StackStatus' `
        --output text 
    if ([string]::IsNullOrEmpty($status)) {
        return "STACK_NOT_FOUND"
    }
    return $status
}

function Wait-ForStableStatus {
    $timeout = 300
    $interval = 2
    $elapsed = 0
    while ($true) {
        $status = Get-StackStatus
        if ($status -notlike "*_IN_PROGRESS") { break }

        if ($elapsed -ge $timeout) {
            Write-Host "Timeout: Stack status still in progress after $timeout seconds"
            exit 1
        }

        Start-Sleep -Seconds $interval
        $elapsed += $interval
    }
}

Write-Host "Checking stack status for client: $ClientId"
$currentStatus = Get-StackStatus

if ($currentStatus -like "*ROLLBACK_COMPLETE" -or $currentStatus -like "*CREATE_FAILED") {
    Write-Host "Deleting failed stack: $currentStatus"
    aws cloudformation delete-stack `
        --stack-name $StackName `
        --region $Region
    aws cloudformation wait stack-delete-complete `
        --stack-name $StackName `
        --region $Region
    $currentStatus = "STACK_NOT_FOUND"
    Write-Host "Stack deleted"
} elseif ($currentStatus -like "*_IN_PROGRESS") {
    Write-Host "Stack is in progress, waiting..."
    Wait-ForStableStatus
    $currentStatus = Get-StackStatus
}

# Define parameters
$params = @(
    "ParameterKey=ClientId,ParameterValue=$ClientId"
    "ParameterKey=CertificateData,ParameterValue=$CertificateData"
)

if ($currentStatus -eq "STACK_NOT_FOUND") {
    Write-Host "Creating stack..."
    aws cloudformation create-stack `
        --stack-name $StackName `
        --region $Region `
        --template-body "file://$TemplateFile" `
        --capabilities CAPABILITY_NAMED_IAM `
        --parameters $params
    Write-Host "Waiting for stack to complete..."
    aws cloudformation wait stack-create-complete `
        --stack-name $StackName `
        --region $Region 2>$null
} else {
    Write-Host "Updating stack..."

    aws cloudformation update-stack `
        --stack-name $StackName `
        --region $Region `
        --template-body "file://$TemplateFile" `
        --capabilities CAPABILITY_NAMED_IAM `
        --parameters $params

    $updateCode = $LASTEXITCODE

    if ($updateCode -eq 254) {
        Write-Host "No updates to be performed."
        exit 0
    } elseif ($updateCode -ne 0) {
        Write-Host "Update failed with code $updateCode"
        exit $updateCode
    }

    Write-Host "Waiting for stack to update..."
    aws cloudformation wait stack-update-complete `
        --stack-name $StackName `
        --region $Region
}

$finalStatus = Get-StackStatus
if ($finalStatus -eq "CREATE_COMPLETE" -or $finalStatus -eq "UPDATE_COMPLETE") {
    Write-Host "✅ Stack $StackName deployment succeeded for client: $ClientId"
} else {
    Write-Host "❌ Stack $StackName failed: $finalStatus"
    exit 1
}

# Get ParamBasePath output
$cfn_outputs = aws cloudformation describe-stacks `
    --stack-name $StackName `
    --region $Region `
    --query "Stacks[0].Outputs" `
    --output json
    
$jsonArray = $cfn_outputs | ConvertFrom-Json

# Build key-value hashtable
$flat = @{}
foreach ($item in $jsonArray) {
  $flat[$item.OutputKey] = $item.OutputValue
}
$flat.ChunkSize = [long]$flat.ChunkSize

# Convert hashtable to JSON string
$flattenedJson = $flat | ConvertTo-Json -Depth 2
$paramBasePath = $flat["ParamBasePath"]

# Call external script
& ./generate-aes-ssm-key.ps1 -ParamName "$paramBasePath/$ClientId/aes-sqs-encryption"
& ./generate-aes-ssm-key.ps1 -ParamName "$paramBasePath/$ClientId/aes-file-encryption"

aws ssm put-parameter `
  --name "$paramBasePath/$ClientId/aws-config" `
  --value "$flattenedJson" `
  --type String `
  --overwrite `
  --region "$Region"