FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/sdk:9.0 AS build

RUN apt-get update && apt-get install -y \
    clang \
    lld \
    zlib1g-dev \
    libpthread-stubs0-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN dotnet publish /app/aws-backup.sln -c Release -r linux-x64 \
    -p:UseAppHost=true \
    -p:DebugType=None \
    -p:PublishAot=false \
    -p:SelfContained=true \
    -p:DebugSymbols=false \
    -p:EnableCompressionInSingleFile=true