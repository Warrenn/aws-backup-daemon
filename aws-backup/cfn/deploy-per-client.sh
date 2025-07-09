#!/bin/bash
set -euo pipefail

RAW_CLIENT_ID="${1:-}"
PEM_FILE="${2:-}"

if [[ -z "$RAW_CLIENT_ID" || -z "$PEM_FILE" ]]; then
  echo "Usage: $0 <client-id> <path-to-pem-file>"
  exit 1
fi

# Scrub client ID: lowercase and remove invalid characters
CLIENT_ID=$(echo "$RAW_CLIENT_ID" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]//g')
if [[ -z "$CLIENT_ID" ]]; then
  echo "Error: scrubbed client-id is empty after sanitization"
  exit 1
fi

if [[ ! -f "$PEM_FILE" ]]; then
  echo "Error: PEM file '$PEM_FILE' does not exist"
  exit 1
fi

# Read and encode PEM content safely
CERTIFICATE_DATA=$(<"$PEM_FILE")

# Template and stack info
TEMPLATE_FILE="per-client.yaml"
STACK_NAME="per-client-${CLIENT_ID}"
REGION="${AWS_REGION:-us-east-1}"

check_stack_status() {
  aws cloudformation describe-stacks \
    --region "$REGION" \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "STACK_NOT_FOUND"
}

wait_for_stable_status() {
  local timeout=300
  local interval=2
  local elapsed=0
  while true; do
    STATUS=$(check_stack_status)
    if [[ "$STATUS" != *_IN_PROGRESS ]]; then break; fi
    if (( elapsed >= timeout )); then
      echo "Timeout: Stack status still in progress after $timeout seconds"
      exit 1
    fi
    sleep "$interval"
    ((elapsed+=interval))
  done
}

echo "Checking stack status for client: $CLIENT_ID"
CURRENT_STATUS=$(check_stack_status)

if [[ "$CURRENT_STATUS" == *"ROLLBACK_COMPLETE" || "$CURRENT_STATUS" == *"CREATE_FAILED" ]]; then
  echo "Deleting failed stack: $CURRENT_STATUS"
  aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"
  aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"
  CURRENT_STATUS="STACK_NOT_FOUND"
  echo "Stack deleted"
elif [[ "$CURRENT_STATUS" == *_IN_PROGRESS ]]; then
  echo "Stack is in progress, waiting..."
  wait_for_stable_status
  CURRENT_STATUS=$(check_stack_status)
fi

# shellcheck disable=SC2054
PARAMS=(
  ParameterKey=ClientId,ParameterValue="$CLIENT_ID"
  ParameterKey=CertificateData,ParameterValue="$CERTIFICATE_DATA"
)

if [[ "$CURRENT_STATUS" == "STACK_NOT_FOUND" ]]; then
  echo "Creating stack..."
  aws cloudformation create-stack \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --template-body "file://${TEMPLATE_FILE}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameters "${PARAMS[@]}"
  echo "Waiting for stack to complete..."
  aws cloudformation wait stack-create-complete \
    --stack-name "$STACK_NAME" --region "$REGION" 2>/dev/null
else
  echo "Updating stack..."
  set +e
  aws cloudformation update-stack \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --template-body "file://${TEMPLATE_FILE}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameters "${PARAMS[@]}"
  UPDATE_CODE=$?
  set -e
  if [[ $UPDATE_CODE -eq 254 ]]; then
    echo "No updates to be performed."
    exit 0
  elif [[ $UPDATE_CODE -ne 0 ]]; then
    echo "Update failed with code $UPDATE_CODE"
    exit $UPDATE_CODE
  fi
  echo "Waiting for stack to update..."
  aws cloudformation wait stack-update-complete \
    --stack-name "$STACK_NAME" --region "$REGION"
fi

FINAL_STATUS=$(check_stack_status)
if [[ "$FINAL_STATUS" == "CREATE_COMPLETE" || "$FINAL_STATUS" == "UPDATE_COMPLETE" ]]; then
  echo "✅ Stack $STACK_NAME deployment succeeded for client: $CLIENT_ID"
else
  echo "❌ Stack $STACK_NAME failed: $FINAL_STATUS"
  exit 1
fi

./gererate-aes-ssm-key.sh "aes-key-${CLIENT_ID}" "$REGION"
