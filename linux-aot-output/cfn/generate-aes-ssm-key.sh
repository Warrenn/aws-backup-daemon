#!/bin/bash
set -euo pipefail

PARAM_NAME="${1:-}"
REGION="${AWS_REGION:-us-east-1}"

if [[ -z "$PARAM_NAME" ]]; then
  echo "Usage: $0 <ssm-parameter-name>"
  exit 1
fi

# 1. Generate a raw 32-byte key
BASE64_KEY=$(openssl rand -out /dev/stdout 32 | base64 -w 0)

# 3. Validate it decodes to exactly 32 bytes
DECODED_LENGTH=$(echo "$BASE64_KEY" | base64 -d | wc -c)
if [[ "$DECODED_LENGTH" -ne 32 ]]; then
  echo "Error: Generated key is not 32 bytes after base64 decoding"
  exit 1
fi

if aws ssm get-parameter --name "$PARAM_NAME" --region "$REGION" --with-decryption >/dev/null 2>&1; then
  echo "✅ AES Parameter found for $PARAM_NAME. No need to create a new one."
  exit 0
fi

# 4. (Optional) Store in SSM Parameter Store
aws ssm put-parameter \
  --name "$PARAM_NAME" \
  --value "$BASE64_KEY" \
  --type SecureString \
  --overwrite \
  --region "$REGION"

echo "✅ AES key stored in SSM parameter: $PARAM_NAME"