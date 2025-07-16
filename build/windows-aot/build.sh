#!/bin/bash

set +e

cd "$(dirname "$0")" || exit 1
# Build image
docker system prune -af
cd ../../ || exit 1
docker build -f build/windows-aot/Dockerfile --no-cache --platform=windows/amd64 -t aws-backup-windows-aot .

# Create output folder
mkdir -p windows-aot-output

# Copy binary from container to host
docker create --name temp aws-backup-windows-aot
docker cp temp:/app/aws-backup/bin/Release/net9.0/win-x64/publish ./windows-aot-output
docker cp temp:/app/aws-backup-commands/bin/Release/net9.0/win-x64/publish ./windows-aot-output
docker cp temp:/app/cert-gen/bin/Release/net9.0/win-x64/publish ./windows-aot-output
docker rm temp
docker system prune -af