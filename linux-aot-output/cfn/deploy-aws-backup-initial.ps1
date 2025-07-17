$ErrorActionPreference = "Stop"

$STACK_NAME = "aws-backup-test"
$TEMPLATE_FILE = "aws-backup-initial.yaml"
$REGION = $env:AWS_REGION
if (-not $REGION) { $REGION = "us-east-1" }
$BUCKET_PREFIX = "backup-bucket-test"

function Check-StackStatus {
    $status = aws cloudformation describe-stacks `
        --region $REGION `
        --stack-name $STACK_NAME `
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
        $status = Check-StackStatus
        if ($status -notlike "*_IN_PROGRESS") {
            break
        }

        if ($elapsed -ge $timeout) {
            Write-Host "Timeout: Stack status still in progress after $timeout seconds"
            exit 1
        }

        Start-Sleep -Seconds $interval
        $elapsed += $interval
    }
}

Write-Host "Checking existing stack status..."
$currentStatus = Check-StackStatus

if ($currentStatus -like "*ROLLBACK_COMPLETE" -or $currentStatus -like "*CREATE_FAILED") {
    Write-Host "Deleting failed stack: $currentStatus"
    aws cloudformation delete-stack `
        --stack-name $STACK_NAME `
        --region $REGION
    aws cloudformation wait stack-delete-complete `
        --stack-name $STACK_NAME `
        --region $REGION
    $currentStatus = "STACK_NOT_FOUND"
    Write-Host "Stack deleted"
} elseif ($currentStatus -like "*_IN_PROGRESS") {
    Write-Host "Stack is in progress: waiting until stable..."
    Wait-ForStableStatus
    $currentStatus = Check-StackStatus
}


if ($currentStatus -eq "STACK_NOT_FOUND") {
    Write-Host "Creating new stack..."
    aws cloudformation create-stack `
        --stack-name $STACK_NAME `
        --region $REGION `
        --template-body "file://$TEMPLATE_FILE" `
        --capabilities CAPABILITY_NAMED_IAM `
        --parameters "ParameterKey=BucketNamePrefix,ParameterValue=$BUCKET_PREFIX"
    Write-Host "Waiting for stack operation to complete..."
    aws cloudformation wait stack-create-complete `
        --stack-name $STACK_NAME `
        --region $REGION 2>$null
} else {
    Write-Host "Updating existing stack..."
    aws cloudformation update-stack `
        --stack-name $STACK_NAME `
        --region $REGION `
        --template-body "file://$TEMPLATE_FILE" `
        --capabilities CAPABILITY_NAMED_IAM `
        --parameters "ParameterKey=BucketNamePrefix,ParameterValue=$BUCKET_PREFIX"
    
    $updateResult = $LASTEXITCODE
    
    if ($updateResult -eq 254) {
        Write-Host "No updates to be performed."
        exit 0
    } elseif ($updateResult -ne 0) {
        Write-Host "Update failed with code $updateResult"
        exit $updateResult
    }

    Write-Host "Waiting for stack update to complete..."
    aws cloudformation wait stack-update-complete `
        --stack-name $STACK_NAME `
        --region $REGION
}

$finalStatus = Check-StackStatus
if ($finalStatus -eq "CREATE_COMPLETE" -or $finalStatus -eq "UPDATE_COMPLETE") {
    Write-Host "✅ Stack operation succeeded: $finalStatus"
} else {
    Write-Host "❌ Stack operation failed: $finalStatus"
    exit 1
}