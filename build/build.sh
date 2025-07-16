#!/bin/bash

set +e

cd "$(dirname "$0")" || exit 1
# Build image
docker system prune -af
docker build --no-cache --platform=linux/amd64 -t aws-backup-builder .

# Create output folder
mkdir -p output

# Copy binary from container to host
docker create --name temp aws-backup-builder
docker cp temp:/app ./output
docker rm temp
docker system prune -af