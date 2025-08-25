#!/bin/bash
set -euo pipefail

TABLE_NAME="archive-data-test-client"
REGION="${AWS_REGION:-us-east-1}"

# Get key schema
KEY_SCHEMA=$(aws dynamodb describe-table --table-name "$TABLE_NAME" --region "$REGION" \
  --query 'Table.KeySchema' --output json)

HASH_KEY=$(echo "$KEY_SCHEMA" | jq -r '.[] | select(.KeyType == "HASH") | .AttributeName')
SORT_KEY=$(echo "$KEY_SCHEMA" | jq -r '.[] | select(.KeyType == "RANGE") | .AttributeName // empty')

echo "Deleting from table: $TABLE_NAME (PrimaryKey: $HASH_KEY${SORT_KEY:+, $SORT_KEY})"

EXCLUSIVE_START_KEY=""

while :; do
  # Fetch 100 items at a time using pagination
  SCAN_OUTPUT=$(aws dynamodb scan \
    --table-name "$TABLE_NAME" \
    --region "$REGION" \
    --max-items 100 \
    ${EXCLUSIVE_START_KEY:+--starting-token "$EXCLUSIVE_START_KEY"} \
    --output json)

  ITEMS=$(echo "$SCAN_OUTPUT" | jq -c '.Items[]')

  if [[ -z "$ITEMS" ]]; then break; fi

  # Buffer for batch of 25
  BATCH=()
  COUNT=0

  while IFS= read -r ITEM; do
    DELETE_KEY=$(jq -c --arg hk "$HASH_KEY" --arg sk "$SORT_KEY" '
      if $sk == "" then {($hk): .[$hk]}
      else {($hk): .[$hk], ($sk): .[$sk]}
      end' <<< "$ITEM")

    BATCH+=("{\"DeleteRequest\": {\"Key\": $DELETE_KEY}}")
    COUNT=$((COUNT + 1))

    if (( COUNT == 25 )); then
      BATCH_JSON=$(printf "%s\n" "${BATCH[@]}" | jq -s .)
      aws dynamodb batch-write-item \
        --region "$REGION" \
        --request-items "{\"$TABLE_NAME\": $BATCH_JSON}"
      BATCH=()
      COUNT=0
    fi
  done <<< "$ITEMS"

  # Process remaining <25
  if (( ${#BATCH[@]} > 0 )); then
    BATCH_JSON=$(printf "%s\n" "${BATCH[@]}" | jq -s .)
    aws dynamodb batch-write-item \
      --region "$REGION" \
      --request-items "{\"$TABLE_NAME\": $BATCH_JSON}"
  fi

  # Handle pagination
  EXCLUSIVE_START_KEY=$(echo "$SCAN_OUTPUT" | jq -r '.NextToken // empty')
  [[ -z "$EXCLUSIVE_START_KEY" ]] && break
done

echo "✅ All items deleted from $TABLE_NAME"
