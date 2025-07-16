FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build

# Optional: reduce build image size
SHELL ["powershell", "-Command"]

# Set working directory
WORKDIR /app

# Copy all sources
COPY . .

# Publish as AOT, self-contained, single file
RUN dotnet publish /app/aws-backup.sln -c Release -r win-x64 -p:PublishAot=true -p:SelfContained=true -p:PublishSingleFile=true -p:DebugType=None -p:DebugSymbols=false -p:EnableCompressionInSingleFile=true -p:IncludeAllContentForSelfExtract=true