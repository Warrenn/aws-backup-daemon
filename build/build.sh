#!/bin/bash

set +e

cd "$(dirname "$0")" || exit 1
# Build image
docker system prune -af
cd ../ || exit 1
docker build -f build/Dockerfile --no-cache --platform=linux/amd64 -t aws-backup-aot .

# Create output folder
mkdir -p output

# Copy binary from container to host
docker create --name temp aws-backup-aot
docker cp temp:/app/aws-backup/bin/Release/net9.0/linux-x64/publish ./output
docker cp temp:/app/aws-backup-commands/bin/Release/net9.0/linux-x64/publish ./output
docker cp temp:/app/cert-gen/bin/Release/net9.0/linux-x64/publish ./output
docker rm temp
docker system prune -af