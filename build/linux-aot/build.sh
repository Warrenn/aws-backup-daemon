#!/bin/bash

set +e

cd "$(dirname "$0")" || exit 1
# Build image
docker system prune -af
cd ../../ || exit 1
docker build -f build/linux-aot/Dockerfile --no-cache --platform=linux/amd64 -t aws-backup-linux-aot .

# Create output folder
mkdir -p linux-aot-output/publish

# Copy binary from container to host
docker create --name temp aws-backup-linux-aot
cp -rf ./aws-backup/cfn ./linux-aot-output/publish
docker cp temp:/app/aws-backup/bin/Release/net9.0/linux-x64/publish ./linux-aot-output
docker cp temp:/app/aws-backup-commands/bin/Release/net9.0/linux-x64/publish ./linux-aot-output
docker cp temp:/app/cert-gen/bin/Release/net9.0/linux-x64/publish ./linux-aot-output
docker rm temp
docker system prune -af