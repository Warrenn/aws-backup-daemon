#!/bin/bash
set -euo pipefail

STACK_NAME="aws-backup-initial"
TEMPLATE_FILE="aws-backup-initial.yaml"
REGION="${AWS_REGION:-us-east-1}"
BUCKET_PREFIX="backup-bucket"

check_stack_status() {
  aws cloudformation describe-stacks \
    --region "$REGION" \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "STACK_NOT_FOUND"
}

wait_for_stable_status() {
  local timeout=300  # 5 minutes
  local interval=2
  local elapsed=0

  while true; do
    STATUS=$(check_stack_status)

    if [[ "$STATUS" != *_IN_PROGRESS ]]; then
      break
    fi

    if (( elapsed >= timeout )); then
      echo "Timeout: Stack status still in progress after $timeout seconds"
      exit 1
    fi

    sleep "$interval"
    ((elapsed+=interval))
  done
}

echo "Checking existing stack status..."
CURRENT_STATUS=$(check_stack_status)

if [[ "$CURRENT_STATUS" == *"ROLLBACK_COMPLETE" || "$CURRENT_STATUS" == *"CREATE_FAILED" ]]; then
  echo "Deleting failed stack: $CURRENT_STATUS"
  aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"
  aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"
  CURRENT_STATUS="STACK_NOT_FOUND"
  echo "Stack deleted"
elif [[ "$CURRENT_STATUS" == *_IN_PROGRESS ]]; then
  echo "Stack is in progress: waiting until stable..."
  wait_for_stable_status
  CURRENT_STATUS=$(check_stack_status)
fi

if [[ "$CURRENT_STATUS" == "STACK_NOT_FOUND" ]]; then
  echo "Creating new stack..."
  aws cloudformation create-stack \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --template-body "file://${TEMPLATE_FILE}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameters ParameterKey=BucketNamePrefix,ParameterValue="$BUCKET_PREFIX"
else
  echo "Updating existing stack..."
  set +e
  aws cloudformation update-stack \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --template-body "file://${TEMPLATE_FILE}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameters ParameterKey=BucketNamePrefix,ParameterValue="$BUCKET_PREFIX"
  UPDATE_CODE=$?
  set -e
  if [[ $UPDATE_CODE -eq 254 ]]; then
    echo "No updates to be performed."
    exit 0
  elif [[ $UPDATE_CODE -ne 0 ]]; then
    echo "Update failed with code $UPDATE_CODE"
    exit $UPDATE_CODE
  fi
fi

echo "Waiting for stack operation to complete..."
aws cloudformation wait stack-create-complete \
  --stack-name "$STACK_NAME" \
  --region "$REGION" 2>/dev/null || \
aws cloudformation wait stack-update-complete \
  --stack-name "$STACK_NAME" \
  --region "$REGION"

FINAL_STATUS=$(check_stack_status)
if [[ "$FINAL_STATUS" == "CREATE_COMPLETE" || "$FINAL_STATUS" == "UPDATE_COMPLETE" ]]; then
  echo "✅ Stack operation succeeded: $FINAL_STATUS"
else
  echo "❌ Stack operation failed: $FINAL_STATUS"
  exit 1
fi