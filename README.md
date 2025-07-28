# AWS Backup Daemon

`aws-backup-daemon` is a cross-platform .NET tool that automates archival and restoration of files to and from Amazon S3. It is built as a set of services and background actors and is designed to run continuously as a daemon on Linux (systemd) or Windows. The project is implemented in C# and targets the .NET 9 runtime. It splits large files into chunks, optionally encrypts them, uploads them to S3 and stores metadata in DynamoDB; it also exposes commands for listing and restoring archives from S3. The root command of the daemon describes its purpose as “AWS Backup Tool – Archive and restore files to/from AWS S3”.

## Features

- **Scheduled archiving** – The daemon watches specified directories and runs according to a cron schedule defined in the configuration. Each backup run is identified by a unique `RunId` and tracked by the `ArchiveService`. Archive runs are cached in-memory to reduce repeated lookups.

- **Chunk-based uploads** – Files are split into configurable chunk sizes and uploaded in parallel to S3. Metadata about files and chunks (status, hash, skip reason) is stored in a data store and exposed through the `ArchiveService`. Failed uploads or chunks trigger retries and are recorded for later inspection.

- **Encryption and secure messaging** – The common library provides helpers for AES encryption. SQS messages can be encrypted, and encryption keys are retrieved from AWS Parameter Store. The daemon can encrypt both the archive files and command messages using AES.

- **Modular actors and services** – Each piece of functionality runs in its own hosted service (actor), including cron scheduling, archiving files, uploading chunks, polling SQS for restore requests, restoring files, retrying failed tasks, managing S3 storage class transitions and sending SNS notifications. Services are registered using dependency injection in the `Program.cs` entry point.

- **Command-line utilities** – The separate `aws-backup-commands` project exposes commands to list archives, list paths within an archive and submit restore requests. For example, `restore-archive` takes an archive ID and paths to restore and posts a restore request to SQS. `list-archives` enumerates available archive runs by listing objects in the S3 bucket.

- **Configurable and secure** – Runtime behaviour is controlled by `appsettings.json` in combination with environment variables. Settings are divided into a user-modifiable section (`Configuration`) and an AWS-managed section. The `CommonConfiguration` record lists many tunable parameters such as AWS region, storage classes, cache directories, encryption options, concurrency limits and retry intervals. AWS-specific values (chunk size, encryption key paths, bucket name, SQS queue, SNS topics and DynamoDB table name) are resolved from AWS Systems Manager Parameter Store through `AwsConfigurationFactory`.

- **AWS Roles Anywhere support** – The project can authenticate using AWS Roles Anywhere by specifying certificate and role ARNs in the configuration.

- **Logging** – Logs are produced via Serilog and written to rolling log files with configurable size and retention.

- **MIT-licensed** – The source code is provided under the MIT licence.

## Repository structure

| Path or file            | Purpose |
|-------------------------|---------|
| `aws-backup/`           | Main daemon project with the backup service, actors and `Program.cs` entry point. |
| `aws-backup-commands/`  | Command-line project providing utilities such as `restore-archive`, `list-paths` and `list-archives`. |
| `aws-backup-common/`    | Shared library containing configuration classes, encryption helpers and AWS client factories. |
| `build-push.sh`         | Helper script for committing, tagging and pushing releases. |
| `Directory.Build.props` | Central version file for the solution; version `1.0.5` is defined here. |
| `aws-backup.sln`        | Solution file that ties the projects together. |
| `LICENSE`               | MIT license terms. |

## Prerequisites

1. **.NET 9 SDK** – The daemon targets `net9.0`, so you need the .NET 9 SDK installed. At the time of writing .NET 9 is in preview; download it from dotnet.microsoft.com. Verify installation with `dotnet --version`.

2. **AWS account & IAM roles** – Create an S3 bucket to store archives, an SQS queue for restore requests, and SNS topics for notifications. Provision a DynamoDB table for metadata. Create or reuse an IAM role with permissions to S3, SQS, SNS, DynamoDB and Systems Manager Parameter Store. If using AWS Roles Anywhere, provision a trust anchor, profile and x.509 certificate and specify their ARNs and file names in `appsettings.json`.

3. **AWS Systems Manager Parameter Store** – Store a JSON document under a parameter path that holds the AWS-managed configuration. The parameter should deserialize into the `AwsConfiguration` record (fields include `ChunkSizeBytes`, `AesSqsEncryptionPath`, `AesFileEncryptionPath`, `BucketName`, `SqsInboxQueueUrl`, `ArchiveCompleteTopicArn`, `ArchiveCompleteErrorsTopicArn`, `RestoreCompleteTopicArn`, `RestoreCompleteErrorsTopicArn`, `ExceptionTopicArn` and `DynamoDbTableName`).

4. **Application settings file** – Create `appsettings.json` with a `Configuration` section containing the client-specific settings described below.

## Configuration

### `appsettings.json`

Create an `appsettings.json` file in the root of the project or specify its location with `--app-settings`. The file must include a `Configuration` section that binds to the `Configuration` record. At minimum you need to specify:

```json
{
  "Configuration": {
    "ClientId": "my-client",
    "CronSchedule": "0 0 * * *",
    "PathsToArchive": "/home/user/data",
    "ParamBasePath": "/aws/backup/config/my-client"
  }
}
```

Additional optional settings are available in the `CommonConfiguration` record. These include concurrency settings (`NoOfFilesToBackupConcurrently`, `NoOfConcurrentDownloadsPerFile`, `NoOfS3FilesToDownloadConcurrently`, `NoOfConcurrentS3Uploads`), timeouts (`ShutdownTimeoutSeconds`, `AwsTimeoutSeconds`), retry intervals (`RetryCheckIntervalMs`, `GeneralRetryLimit`), storage classes (`ColdStorage`, `HotStorage`, `LowCostStorage`) and encryption flags (`EncryptSqs`, `ServerSideEncryption`). See the source of `CommonConfiguration.cs` for a complete list.

### AWS configuration parameter

Under the SSM parameter path specified in `ParamBasePath` (for example `/aws/backup/config/my-client`) store a JSON document matching the `AwsConfiguration` schema:

```json
{
  "ChunkSizeBytes": 16777216,
  "AesSqsEncryptionPath": "/aws/backup/keys/sqs",
  "AesFileEncryptionPath": "/aws/backup/keys/file",
  "BucketName": "my-archive-bucket",
  "SqsInboxQueueUrl": "https://sqs.<region>.amazonaws.com/123456789012/my-restore-queue",
  "ArchiveCompleteTopicArn": "arn:aws:sns:<region>:123456789012:archive-complete",
  "ArchiveCompleteErrorsTopicArn": "arn:aws:sns:<region>:123456789012:archive-errors",
  "RestoreCompleteTopicArn": "arn:aws:sns:<region>:123456789012:restore-complete",
  "RestoreCompleteErrorsTopicArn": "arn:aws:sns:<region>:123456789012:restore-errors",
  "ExceptionTopicArn": "arn:aws:sns:<region>:123456789012:backup-exceptions",
  "DynamoDbTableName": "my-backup-metadata"
}
```

The daemon retrieves this configuration at startup via the `AwsConfigurationFactory`.

## Building and installing

1. **Clone the repository**:

   ```bash
   git clone https://github.com/Warrenn/aws-backup-daemon.git
   cd aws-backup-daemon
   ```

2. **Restore dependencies and build** the solution:

   ```bash
   dotnet restore
   dotnet build -c Release
   ```

   This produces two console applications: `aws-backup` (the daemon) and `aws-backup-commands` (the CLI utilities).

3. **Create your application settings file** (`appsettings.json`) and place it alongside the built executable, or specify its location at runtime with `--app-settings`.

4. **Configure AWS credentials**. The daemon uses the default AWS credential chain. You can set environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), configure a named profile in `~/.aws/credentials`, or use AWS Roles Anywhere by specifying the certificate and role ARNs in the configuration.

5. **Create a systemd or Windows service** to run the daemon continuously:

   **Linux (systemd)**
   1. Copy the `aws-backup` binary and `appsettings.json` to `/opt/aws-backup-daemon/`.
   2. Create `/etc/systemd/system/aws-backup-daemon.service` with contents similar to:

      ```ini
      [Unit]
      Description=AWS Backup Daemon
      After=network.target

      [Service]
      Type=simple
      ExecStart=/usr/bin/dotnet /opt/aws-backup-daemon/aws-backup.dll --app-settings /opt/aws-backup-daemon/appsettings.json
      WorkingDirectory=/opt/aws-backup-daemon
      Restart=on-failure
      User=backup

      [Install]
      WantedBy=multi-user.target
      ```

   3. Enable and start the service:

      ```bash
      sudo systemctl daemon-reload
      sudo systemctl enable aws-backup-daemon
      sudo systemctl start aws-backup-daemon
      ```

   **Windows service**
   4. Build a self-contained Windows publish:

      ```powershell
      dotnet publish aws-backup -c Release -r win-x64 --sc
      ```

   5. Use `sc.exe` or PowerShell to create a service that runs the published executable and passes the `--app-settings` argument.

6. **Run the command-line utilities**. Use `aws-backup-commands` to list or restore archives:

   ```bash
   # list all archives for the client
   dotnet run --project aws-backup-commands -- list-archives --client-id my-client --app-settings appsettings.json

   # list the files stored in a particular archive
   dotnet run --project aws-backup-commands -- list-paths --archive-id <archive-run-id> --client-id my-client --app-settings appsettings.json

   # submit a restore request for specific paths
   dotnet run --project aws-backup-commands -- restore-archive --archive-id <archive-run-id> --paths-to-restore /home/user/data/file1:/home/user/data/file2 --client-id my-client --app-settings appsettings.json
   ```

## Contributing

Contributions, issues and feature requests are welcome! Please open an issue or submit a pull request on GitHub. By participating in this project you agree to abide by the MIT license.

## Acknowledgements

The AWS Backup Daemon uses several open-source libraries, including Serilog for structured logging, Cocona for command-line parsing and Cronos for cron expressions. It relies on AWS services such as S3, SQS, SNS, DynamoDB and Systems Manager Parameter Store to provide scalable, reliable backups and restoration.
