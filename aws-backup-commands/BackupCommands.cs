using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.S3.Model;
using Amazon.SQS.Model;
using aws_backup_common;
using Cocona;
// ReSharper disable ClassNeverInstantiated.Global
// ReSharper disable NotAccessedPositionalProperty.Global

namespace aws_backup_commands;

public record CommonParameters(
    // Global options
    [Option("client-id", ['c'], Description = "The client ID for the restore operation")]
    string? ClientId = "",
    [Option("app-settings", ['a'], Description = "Path to the application settings file")]
    string? AppSettings = ""
) : ICommandParameterSet;

public sealed class BackupCommands(
    Configuration configuration,
    AwsConfiguration awsConfiguration,
    IContextResolver contextResolver,
    IAesContextResolver aesContextResolver,
    IAwsClientFactory awsClientFactory,
    IS3Service s3Service
)
{
    [Command("restore-archive", Description = "Restore an archive from AWS Backup")]
    public async Task RestoreArchive(
        CommonParameters commonParams,
        [Option("archive-id", Description = "The archive Id that you want to restore")]
        string archiveId,
        [Option("paths-to-restore", Description = "The paths you want to restore from the archive")]
        string[] pathsToRestore,
        [Ignore]
        CancellationToken cancellationToken = default
    )
    {
        Console.WriteLine("Restoring archive...");
        var sqsDecryptionKey = await aesContextResolver.SqsEncryptionKey(cancellationToken);
        var joinedPaths = string.Join(":", pathsToRestore);
        var sqs = await awsClientFactory.CreateSqsClient(cancellationToken);
        var queueUrl = awsConfiguration.SqsInboxQueueUrl;
        
        var request = new RestoreRequest
        (
            ArchiveRunId: archiveId,
            RestorePaths: joinedPaths,
            RequestedAt: DateTimeOffset.UtcNow
        );
        
        using var ms = new MemoryStream();
        await JsonSerializer.SerializeAsync(ms, request, SourceGenerationContext.Default.RestoreRequest, cancellationToken);
        ms.Position = 0;

        using var streamReader = new StreamReader(ms, Encoding.UTF8);
        var messageBody = await streamReader.ReadToEndAsync(cancellationToken);
        
        var encryptedString = bool.FalseString;
        if (contextResolver.EncryptSqs())
        {
            messageBody = AesHelper.EncryptString(messageBody, sqsDecryptionKey);
            encryptedString = bool.TrueString;
        }

        AWSConfigs.InitializeCollections = true;
        var restoreRequest = new SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = messageBody,
            MessageAttributes =
            {
                ["command"] = new MessageAttributeValue
                {
                    DataType = "String",
                    StringValue = "restore-backup"
                },
                ["encrypted"] = new MessageAttributeValue
                {
                    StringValue = encryptedString,
                    DataType = "String"
                }
            }
        };
        
        await sqs.SendMessageAsync(restoreRequest, cancellationToken);
        
        Console.WriteLine($"Restore request for archive {archiveId} with paths {joinedPaths} sent to SQS.");
    }
    
    [Command("list-paths", Description = "List all paths in the archive")]
    public async Task ListPaths(
        CommonParameters commonParams,
        [Option("archive-id", Description = "The archive Id to list paths for")]
        string archiveId,
        [Ignore]
        CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Listing all paths in the archive...");

        var runArchive = await s3Service.GetArchive(archiveId, cancellationToken);

        foreach (var filePath in runArchive.Files.Keys) Console.WriteLine(filePath);
    }

    [Command("list-archives", Description = "List all backup archives for client")]
    public async Task ListArchives(
        CommonParameters commonParams,
        [Ignore]
        CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Listing all archives...");
        // Logic to list archives would go here

        var bucketName = awsConfiguration.BucketName;
        var clientId = configuration.ClientId;
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = $"{clientId}/archive-runs"
        };

        ListObjectsV2Response response;
        do
        {
            var s3Client = await awsClientFactory.CreateS3Client(cancellationToken);
            response = await s3Client.ListObjectsV2Async(request, cancellationToken);

            if ((response.KeyCount ?? 0) <= 0) return; // No objects found

            foreach (var parts in
                     from s3Object in response.S3Objects
                     select Path.GetFileName(s3Object.Key)
                     into fileName
                     where fileName.Contains(".json")
                     select fileName.Split(".json"))
                Console.WriteLine(parts[0]);

            // If the response is truncated, set the token to get the next page
            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated ?? false);
    }
}