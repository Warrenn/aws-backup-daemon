using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Amazon.DynamoDBv2.Model;

namespace aws_backup_common;

public class DynamoDbDataStore(
    IAwsClientFactory clientFactory,
    AwsConfiguration awsConfiguration,
    IContextResolver contextResolver) :
    IArchiveDataStore,
    IRestoreDataStore,
    IFileMetaDataDataStore,
    ICloudChunkStorage
{
    public async IAsyncEnumerable<RunRequest> GetRunRequests(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        Dictionary<string, AttributeValue>? lastKey = null;
        do
        {
            var req = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "PK = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = "ARCHIVE_REQUEST" }
                },
                ProjectionExpression = "SK, PathsToArchive, CronSchedule",
                ExclusiveStartKey = lastKey
            };

            var resp = await dynamoDbClient.QueryAsync(req, cancellationToken);

            foreach (var item in resp.Items)
            {
                var sk = item["SK"].S;
                if (string.IsNullOrWhiteSpace(sk) ||
                    !sk.Contains('#') ||
                    !long.TryParse(sk.Split('#').Last(), out var runId))
                    continue;

                var paths = GetSIfExists(item, "PathsToArchive", s => s) ?? "";
                var cron = GetSIfExists(item, "CronSchedule", s => s) ?? "";

                yield return new RunRequest(runId, paths, cron);
            }

            lastKey = resp.LastEvaluatedKey;
        } while (lastKey is { Count: > 0 });
    }

    public async Task RemoveArchiveRequest(long runId, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var deleteReq = new DeleteItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "ARCHIVE_REQUEST" },
                ["SK"] = new() { S = $"ARCHIVE_REQUEST#{runId}" }
            },
            ConditionExpression = "attribute_exists(PK) AND attribute_exists(SK)"
        };

        await dynamoDbClient.DeleteItemAsync(deleteReq, cancellationToken);
    }

    public async Task SaveRunRequest(RunRequest request, CancellationToken cancellationToken)
    {
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var item = new Dictionary<string, AttributeValue>
        {
            // partition key
            ["PK"] = new() { S = "ARCHIVE_REQUEST" },
            // sort key
            ["SK"] = new() { S = $"ARCHIVE_REQUEST#{request.RunId}" },
            // other attributes
            ["PathsToArchive"] = new() { S = request.PathsToArchive },
            ["CronSchedule"] = new() { S = request.CronSchedule },
            ["Type"] = new() { S = nameof(RunRequest) }
        };

        var updateItemRequest = CreateUpdateItemRequest(item);
        // one round‑trip to DynamoDB
        await dynamoDbClient.UpdateItemAsync(updateItemRequest, cancellationToken);
    }

    public async Task SaveArchiveRun(ArchiveRun run, CancellationToken cancellationToken)
    {
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var item = new Dictionary<string, AttributeValue>
        {
            // partition key
            ["PK"] = new() { S = "RUN" },
            // sort key
            ["SK"] = new() { S = $"RUN#{run.RunId}" },
            ["Type"] = new() { S = nameof(ArchiveRun) },
            ["PathsToArchive"] = new() { S = run.PathsToArchive },
            ["CronSchedule"] = new() { S = run.CronSchedule },
            ["CreatedAt"] = new() { S = run.CreatedAt.ToString("O") },
            ["Status"] = new() { S = Enum.GetName(run.Status) }
        };

        SetNIfNotNull(item, "CompressedSize", run.CompressedSize);
        SetNIfNotNull(item, "OriginalSize", run.OriginalSize);
        SetSIfNotNull(item, "CompletedAt", run.CompletedAt?.ToString("O"));
        SetNIfNotNull(item, "TotalFiles", run.TotalFiles);
        SetNIfNotNull(item, "TotalSkippedFiles", run.TotalSkippedFiles);

        var updateItemRequest = CreateUpdateItemRequest(item);
        // one round‑trip to DynamoDB
        await dynamoDbClient.UpdateItemAsync(updateItemRequest, cancellationToken);
    }

    public async Task<ArchiveRun?> GetArchiveRun(long runId, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var itemRequest = new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "RUN" },
                ["SK"] = new() { S = $"RUN#{runId}" }
            },
            ProjectionExpression =
                "#a, #b, #c, #d, #e, #f, #g, #h, #i, #j",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#a"] = "PathsToArchive",
                ["#b"] = "CronSchedule",
                ["#c"] = "CreatedAt",
                ["#d"] = "Status",
                ["#e"] = "Type",
                ["#f"] = "CompressedSize",
                ["#g"] = "OriginalSize",
                ["#h"] = "TotalFiles",
                ["#i"] = "TotalSkippedFiles",
                ["#j"] = "CompletedAt"
            },
            ConsistentRead = false
        };

        var resp = await dynamoDbClient.GetItemAsync(itemRequest, cancellationToken);
        if (resp.Item == null || resp.Item.Count == 0)
            return null;

        var item = resp.Item;

        var run = new ArchiveRun
        {
            RunId = runId,
            PathsToArchive = GetSIfExists(item, "PathsToArchive", s => s) ?? "",
            CronSchedule = GetSIfExists(item, "CronSchedule", s => s) ?? "",
            CreatedAt = GetSIfExists(item, "CreatedAt", DateTimeOffset.Parse),
            Status = GetSIfExists(item, "Status", Enum.Parse<ArchiveRunStatus>),
            CompressedSize = GetNIfExists(item, "CompressedSize", long.Parse, 0),
            OriginalSize = GetNIfExists(item, "OriginalSize", long.Parse, 0),
            TotalFiles = GetNIfExists(item, "TotalFiles", int.Parse, 0),
            TotalSkippedFiles = GetNIfExists(item, "TotalSkippedFiles", int.Parse, 0),
            CompletedAt = GetSIfExists(item, "CompletedAt", DateTimeOffset.Parse)
        };

        return run;
    }

    public async Task<bool> ContainsChunkKey(ByteArrayKey key, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var chunkHash = Base64Url.Encode(key.ToArray());

        var itemRequest = new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "CLOUD_CHUNK" },
                ["SK"] = new() { S = $"CLOUD_CHUNK#{chunkHash}" }
            },
            ProjectionExpression = "#pk",
            ExpressionAttributeNames = new Dictionary<string, string> { ["#pk"] = "PK" },
            ConsistentRead = false
        };

        var resp = await dynamoDbClient.GetItemAsync(itemRequest, cancellationToken);
        return resp.Item is not null && resp.Item.Count != 0;
    }

    public async Task AddCloudChunkDetails(CloudChunkDetails cloudChunkDetails, CancellationToken cancellationToken)
    {
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var chunkHash = Base64Url.Encode(cloudChunkDetails.HashId.ToArray());
        var item = new Dictionary<string, AttributeValue>
        {
            // partition key
            ["PK"] = new() { S = "CLOUD_CHUNK" },
            // sort key
            ["SK"] = new() { S = $"CLOUD_CHUNK#{chunkHash}" },
            ["Type"] = new() { S = nameof(CloudChunkDetails) },
            ["S3Key"] = new() { S = cloudChunkDetails.S3Key },
            ["BucketName"] = new() { S = cloudChunkDetails.BucketName },
            ["OffsetInS3BatchFile"] = new() { N = cloudChunkDetails.OffsetInS3BatchFile.ToString() },
            ["CompressedSize"] = new() { N = cloudChunkDetails.CompressedSize.ToString() },
            ["Size"] = new() { N = cloudChunkDetails.Size.ToString() }
        };

        var updateItemRequest = CreateUpdateItemRequest(item);
        await dynamoDbClient.UpdateItemAsync(updateItemRequest, cancellationToken);
    }

    public async Task<CloudChunkDetails?> GetCloudChunkDetails(ByteArrayKey hashKey,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var chunkHash = Base64Url.Encode(hashKey.ToArray());

        var itemRequest = new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "CLOUD_CHUNK" },
                ["SK"] = new() { S = $"CLOUD_CHUNK#{chunkHash}" }
            },
            ProjectionExpression = "#a, #b, #c, #d, #e, #f",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#a"] = "SK",
                ["#b"] = "S3Key",
                ["#c"] = "BucketName",
                ["#d"] = "CompressedSize",
                ["#e"] = "OffsetInS3BatchFile",
                ["#f"] = "Size"
            },
            ConsistentRead = false
        };

        var resp = await dynamoDbClient.GetItemAsync(itemRequest, cancellationToken);
        if (resp.Item is null || resp.Item.Count == 0) return null;
        var details = new CloudChunkDetails(
            GetSIfExists(resp.Item, "S3Key", s => s) ?? "",
            GetSIfExists(resp.Item, "BucketName", s => s) ?? "",
            CompressedSize: GetNIfExists(resp.Item, "CompressedSize", long.Parse, 0),
            OffsetInS3BatchFile: GetNIfExists(resp.Item, "OffsetInS3BatchFile", long.Parse, 0),
            Size: GetNIfExists(resp.Item, "Size", long.Parse, 0),
            HashId: hashKey.ToArray()
        );
        return details;
    }

    public async Task<FileMetaData?> GetFileMetaData(long runId, string filePath, CancellationToken cancellationToken)
    {
        var prefix = $"FILE_METADATA#{WebUtility.UrlEncode(filePath)}#RUN_ID#";
        var targetSk = $"{prefix}{runId:0000000000000000000}";
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        const string projectionExpression = "#a, #b, #c, #d, #e, #f, #g, #h, #i, #j, #k, #l";
        var expressionAttributeNames = new Dictionary<string, string>
        {
            ["#a"] = "SK",
            ["#b"] = "AclEntries",
            ["#c"] = "CompressedSize",
            ["#d"] = "OriginalSize",
            ["#e"] = "Created",
            ["#f"] = "LastModified",
            ["#g"] = "Group",
            ["#h"] = "Owner",
            ["#i"] = "Status",
            ["#j"] = "Type",
            ["#k"] = "SkipReason",
            ["#l"] = "HashId"
        };

        var getReq = new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "FILE_METADATA" },
                ["SK"] = new() { S = targetSk }
            },
            ProjectionExpression = projectionExpression,
            ExpressionAttributeNames = expressionAttributeNames,
            ConsistentRead = false
        };
        FileMetaData? fileMetaData = null;

        var resp = await dynamoDbClient.GetItemAsync(getReq, cancellationToken);
        if (resp.Item is { Count: > 0 }) fileMetaData = MapFileMetaData(resp.Item, filePath);

        if (fileMetaData is null)
        {
            var queryReq = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "#pk = :pk AND #sk BETWEEN :lo AND :hi",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#pk"] = "PK",
                    ["#sk"] = "SK"
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = "FILE_METADATA" },
                    [":lo"] = new() { S = prefix }, // inclusive lower bound (same filename)
                    [":hi"] = new() { S = targetSk } // inclusive upper bound (<= target runId)
                },
                ScanIndexForward = false, // descending
                Limit = 1,
                ProjectionExpression = projectionExpression,
                ConsistentRead = false
            };

            var queryResp = await dynamoDbClient.QueryAsync(queryReq, cancellationToken);
            if (queryResp.Count == 0) return null;
            fileMetaData = MapFileMetaData(queryResp.Items[0], filePath);
        }

        if (fileMetaData.HashId is null) return fileMetaData;

        var fileHash = Base64Url.Encode(fileMetaData.HashId.Value.ToArray());
        var pk = $"FILE_CHUNK#{fileHash}";
        var sk = $"FILE_CHUNK#{fileHash}#CHUNK#";
        Dictionary<string, AttributeValue> lastKey = new();

        do
        {
            var chunkReq = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "PK = :pk AND begins_with(SK, :skp)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = pk },
                    [":skp"] = new() { S = sk }
                },
                ProjectionExpression = "#a, #b, #c, #d",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#a"] = "SK",
                    ["#b"] = "Type",
                    ["#c"] = "OffsetInSourceFile",
                    ["#d"] = "OriginalSize",
                    ["#e"] = "CompressedSize"
                },
                ConsistentRead = false,
                ExclusiveStartKey = lastKey
            };

            var chunkResp = await dynamoDbClient.QueryAsync(chunkReq, cancellationToken);
            if (chunkResp.Items == null || chunkResp.Items.Count == 0)
                return null;

            // process this page’s items
            foreach (var item in chunkResp.Items)
            {
                var keyParts = item["SK"].S.Split('#');
                var keyBytes = Base64Url.Decode(keyParts.Last());
                var chunkKey = new ByteArrayKey(keyBytes);

                fileMetaData.Chunks.TryAdd(chunkKey, new DataChunkDetails(
                    filePath,
                    GetNIfExists(item, "CompressedSize", long.Parse),
                    GetNIfExists(item, "OffsetInSourceFile", long.Parse),
                    GetNIfExists(item, "OriginalSize", long.Parse),
                    keyBytes)
                {
                    Status = GetSIfExists(item, "Status", Enum.Parse<ChunkStatus>)
                });
            }

            // DynamoDB gives you this if there's more data
            lastKey = chunkResp.LastEvaluatedKey;
        } while (lastKey is { Count: > 0 });

        return fileMetaData;
    }

    public async Task<bool> SaveFileMetaData(FileMetaData metaData, CancellationToken cancellationToken)
    {
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var localFilePath = metaData.LocalFilePath;
        if (string.IsNullOrWhiteSpace(localFilePath)) return false;
        var runId = metaData.RunId;
        string? hashId = null;
        if (metaData.HashId is not null)
            hashId = Base64Url.Encode(metaData.HashId.Value.ToArray());

        var status = metaData.Status;
        var aclEntryString = metaData.AclEntries is not null
            ? JsonSerializer.Serialize(metaData.AclEntries, SourceGenerationContext.Default.AclEntryArray)
            : null;

        var encodedFilePath = WebUtility.UrlEncode(localFilePath);
        var item = new Dictionary<string, AttributeValue>
        {
            // partition key
            ["PK"] = new() { S = "FILE_METADATA" },
            // sort key
            ["SK"] = new() { S = $"FILE_METADATA#{encodedFilePath}#RUN_ID#{runId:0000000000000000000}" },
            ["Type"] = new() { S = nameof(FileMetaData) },
            ["Status"] = new() { S = Enum.GetName(status) }
        };

        SetSIfNotNull(item, "HashId", hashId);
        SetSIfNotNull(item, "AclEntries", aclEntryString);
        SetSIfNotNull(item, "Created", metaData.Created?.ToString("O"));
        SetSIfNotNull(item, "LastModified", metaData.LastModified?.ToString("O"));
        SetSIfNotNull(item, "SkipReason", metaData.SkipReason);
        SetSIfNotNull(item, "Owner", metaData.Owner);
        SetSIfNotNull(item, "Group", metaData.Group);
        SetNIfNotNull(item, "CompressedSize", metaData.CompressedSize);
        SetNIfNotNull(item, "OriginalSize", metaData.OriginalSize);

        var updateItemRequest = CreateUpdateItemRequest(item);
        await dynamoDbClient.UpdateItemAsync(updateItemRequest, cancellationToken);
        return true;
    }

    public async IAsyncEnumerable<FileMetaData> GetRestorableFileMetaData(long runId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var cache = new Dictionary<string, FileMetaData>();
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        Dictionary<string, AttributeValue> lastKeyFileQuery = new();
        do
        {
            var queryReq = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "PK = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = "FILE_METADATA" }
                },
                ProjectionExpression = "#a, #b, #c, #d, #e, #f, #g, #h, #i, #j, #k, #l",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#a"] = "SK",
                    ["#b"] = "AclEntries",
                    ["#c"] = "CompressedSize",
                    ["#d"] = "OriginalSize",
                    ["#e"] = "Created",
                    ["#f"] = "LastModified",
                    ["#g"] = "Group",
                    ["#h"] = "Owner",
                    ["#i"] = "Status",
                    ["#j"] = "Type",
                    ["#k"] = "SkipReason",
                    ["#l"] = "HashId"
                },
                ExclusiveStartKey = lastKeyFileQuery
            };

            var resp = await dynamoDbClient.QueryAsync(queryReq, cancellationToken);
            if (resp.Items == null || resp.Items.Count == 0)
                yield break;

            foreach (var fileMetaData in resp.Items
                         .Select(item => MapFileMetaData(item))
                         .Where(fileMetaData => fileMetaData.RunId <= runId))
                if (!cache.TryGetValue(fileMetaData.LocalFilePath, out var existingMetaData) ||
                    existingMetaData.RunId < fileMetaData.RunId)
                    cache[fileMetaData.LocalFilePath] = fileMetaData;

            // DynamoDB gives you this if there's more data
            lastKeyFileQuery = resp.LastEvaluatedKey;
        } while (lastKeyFileQuery is { Count: > 0 });

        foreach (var fileMetaData in cache.Values)
        {
            var chunkLastKey = new Dictionary<string, AttributeValue>();
            var filePath = fileMetaData.LocalFilePath;
            if (fileMetaData.HashId is null) continue;
            var fileHash = Base64Url.Encode(fileMetaData.HashId.Value.ToArray());
            var pk = $"FILE_CHUNK#{fileHash}";
            var sk = $"FILE_CHUNK#{fileHash}#CHUNK#";
            do
            {
                var chunkReq = new QueryRequest
                {
                    TableName = tableName,
                    KeyConditionExpression = "PK = :pk AND begins_with(SK, :skp)",
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        [":pk"] = new() { S = pk },
                        [":skp"] = new() { S = sk }
                    },
                    ProjectionExpression = "#a, #b, #c, #d, #e",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        ["#a"] = "SK",
                        ["#b"] = "Type",
                        ["#c"] = "OffsetInSourceFile",
                        ["#d"] = "OriginalSize",
                        ["#e"] = "CompressedSize"
                    },
                    ConsistentRead = false,
                    ExclusiveStartKey = chunkLastKey
                };

                var chunkResp = await dynamoDbClient.QueryAsync(chunkReq, cancellationToken);
                if (chunkResp?.Items is null || chunkResp.Items.Count == 0)
                {
                    yield return fileMetaData;
                    continue;
                }

                foreach (var item in chunkResp.Items)
                {
                    var keyParts = item["SK"].S.Split('#');
                    var keyBytes = Base64Url.Decode(keyParts.Last());
                    var chunkKey = new ByteArrayKey(keyBytes);

                    fileMetaData.Chunks.TryAdd(chunkKey, new DataChunkDetails(
                        filePath,
                        GetNIfExists(item, "CompressedSize", long.Parse),
                        GetNIfExists(item, "OffsetInSourceFile", long.Parse),
                        GetNIfExists(item, "OriginalSize", long.Parse),
                        keyBytes)
                    {
                        Status = GetSIfExists(item, "Status", Enum.Parse<ChunkStatus>)
                    });
                }

                // DynamoDB gives you this if there's more data
                chunkLastKey = chunkResp.LastEvaluatedKey;
                yield return fileMetaData;
            } while (chunkLastKey is { Count: > 0 });
        }
    }

    public async Task<RestoreRun?> LookupRestoreRun(string restoreId, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        Dictionary<string, AttributeValue>? lastKey;

        RestoreRun? restoreRun = null;

        var pk = $"RESTORE#{restoreId}";
        var skPrefix = $"RESTORE#{restoreId}#";
        do
        {
            var queryReq = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "PK = :pk AND begins_with(SK, :skp)",
                ScanIndexForward = true,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = pk },
                    [":skp"] = new() { S = skPrefix }
                },
                ProjectionExpression =
                    "#a, #b, #c, #d, #e, #f, #g, #h, #i, #j, #k, #l, #m, #n, #o, #p, #q, #r, #s, #t, #u",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#a"] = "SK",
                    ["#b"] = "Type",
                    ["#c"] = "RequestedAt",
                    ["#d"] = "RestorePathStrategy",
                    ["#e"] = "RunId",
                    ["#f"] = "RestorePaths",
                    ["#g"] = "CreatedAt",
                    ["#h"] = "CompletedAt",
                    ["#i"] = "Status",
                    ["#j"] = "FailedMessage",
                    ["#k"] = "RestoreStartedAt",
                    ["#l"] = "ArchiveRunId",
                    ["#m"] = "Size",
                    ["#n"] = "LastModified",
                    ["#o"] = "Created",
                    ["#p"] = "AclEntries",
                    ["#q"] = "Owner",
                    ["#r"] = "Group",
                    ["#s"] = "HashId",
                    ["#t"] = "RestoreFolder",
                    ["#u"] = "RestoreCompletedAt"
                }
            };

            var resp = await dynamoDbClient.QueryAsync(queryReq, cancellationToken);
            if (resp.Items == null || resp.Items.Count == 0)
                return null;

            // process this page’s items
            foreach (var item in resp.Items)
            {
                var type = item["Type"].S;
                switch (type)
                {
                    case nameof(RestoreRun):
                        restoreRun = new RestoreRun
                        {
                            RestoreId = restoreId,
                            RestorePaths = GetSIfExists(item, "RestorePaths", s => s) ?? "",
                            ArchiveRunId = GetNIfExists(item, "ArchiveRunId", long.Parse, 0),
                            Status = GetSIfExists(item, "Status", Enum.Parse<RestoreRunStatus>),
                            RequestedAt = GetSIfExists(item, "RequestedAt", DateTimeOffset.Parse),
                            CompletedAt = GetSIfExists(item, "CompletedAt", DateTimeOffset.Parse)
                        };
                        break;
                    case nameof(RestoreFileMetaData):
                        // If we have FileMetaData, we can add it to the run
                        if (restoreRun is null) break;
                        var filePath = WebUtility.UrlDecode(item["SK"].S.Split('#').Last());

                        var fileMeta = new RestoreFileMetaData(filePath)
                        {
                            Status = GetSIfExists(item, "Status", Enum.Parse<FileRestoreStatus>),
                            FailedMessage = GetSIfExists(item, "FailedMessage", s => s),
                            Size = GetNIfExists(item, "Size", long.Parse),
                            LastModified = GetSIfExists(item, "LastModified", DateTimeOffset.Parse),
                            Created = GetSIfExists(item, "Created", DateTimeOffset.Parse),
                            AclEntries = GetSIfExists(item, "AclEntries",
                                s => JsonSerializer.Deserialize<AclEntry[]>(s,
                                    SourceGenerationContext.Default.AclEntryArray)),
                            Owner = GetSIfExists(item, "Owner", s => s),
                            Group = GetSIfExists(item, "Group", s => s),
                            HashId = GetSIfExists(item, "HashId", Base64Url.Decode),
                            RestorePathStrategy = item.TryGetValue("RestorePathStrategy", out var strategy)
                                ? Enum.Parse<RestorePathStrategy>(strategy.S)
                                : RestorePathStrategy.Nested,
                            RestoreFolder = GetSIfExists(item, "RestoreFolder", s => s),
                            RestoreStartedAt = GetSIfExists(item, "RestoreStartedAt", DateTimeOffset.Parse),
                            RestoreCompletedAt = GetSIfExists(item, "RestoreCompletedAt", DateTimeOffset.Parse)
                        };

                        restoreRun.RequestedFiles.TryAdd(filePath, fileMeta);
                        break;
                }
            }

            // DynamoDB gives you this if there's more data
            lastKey = resp.LastEvaluatedKey;
        } while (lastKey is { Count: > 0 });

        return restoreRun;
    }

    public async IAsyncEnumerable<RestoreRequest> GetRestoreRequests([EnumeratorCancellation]CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        
        Dictionary<string, AttributeValue>? lastKey = null;
        do
        {
            var req = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "PK = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = "RESTORE_REQUEST" }
                },
                ProjectionExpression =
                    "ArchiveRunId, RestorePaths, RequestedAt, RestorePathStrategy, RestoreDestination",
                ExclusiveStartKey = lastKey
            };
        
            var resp = await dynamoDbClient.QueryAsync(req, cancellationToken);
        
            foreach (var item in resp.Items)
            {
                var paths = item["RestorePaths"].S;
                var archiveRunId = GetNIfExists(item,"ArchiveRunId", long.Parse, 0);
                var requestedAt = GetSIfExists(item, "RequestedAt", DateTimeOffset.Parse);
                var restorePathStrategy = GetSIfExists(item, "RestorePathStrategy", Enum.Parse<RestorePathStrategy>);
                var restoreFolder = GetSIfExists(item, "RestoreDestination", s => s);
        
                yield return new RestoreRequest(
                    archiveRunId,
                    paths,
                    requestedAt,
                    restorePathStrategy,
                    restoreFolder);
            }
        
            lastKey = resp.LastEvaluatedKey;
        } while (lastKey is { Count: > 0 });
    }

    public async Task SaveRestoreRequest(RestoreRequest restoreRequest, CancellationToken cancellationToken)
    {
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var restoreId = contextResolver.RestoreId(restoreRequest.ArchiveRunId, restoreRequest.RestorePaths,
            restoreRequest.RequestedAt);
        
        var item = new Dictionary<string, AttributeValue>
        {
            // partition key
            ["PK"] = new() { S = "RESTORE_REQUEST" },
            // sort key
            ["SK"] = new() { S = $"RESTORE_REQUEST#{restoreId}" },
            ["Type"] = new() { S = nameof(RestoreRequest) },
            // other attributes
            ["RequestedAt"] = new() { S = restoreRequest.RequestedAt.ToString("O") },
            ["RestorePathStrategy"] = new() { S = Enum.GetName(restoreRequest.RestorePathStrategy) },
            ["RestoreDestination"] = new() { S = restoreRequest.RestorePaths },
            ["ArchiveRunId"] = new() { S = restoreRequest.ArchiveRunId.ToString() }
        };
        
        var updateItemRequest = CreateUpdateItemRequest(item);
        await dynamoDbClient.UpdateItemAsync(updateItemRequest, cancellationToken);

    }

    public async Task SaveRestoreRun(RestoreRun restoreRun, CancellationToken cancellationToken)
    {
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var pk = $"RESTORE#{restoreRun.RestoreId}";

        var metaItem = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = pk },
            ["SK"] = new() { S = pk },
            ["Type"] = new() { S = nameof(RestoreRun) }
        };
        SetSIfNotNull(metaItem, "RestoreId", restoreRun.RestoreId);
        SetNIfNotNull(metaItem, "ArchiveRunId", restoreRun.ArchiveRunId);
        SetSIfNotNull(metaItem, "RestorePaths", restoreRun.RestorePaths);
        SetSIfNotNull(metaItem, "Status", Enum.GetName(restoreRun.Status));
        SetSIfNotNull(metaItem, "RequestedAt", restoreRun.RequestedAt.ToString("O"));
        SetSIfNotNull(metaItem, "CompletedAt", restoreRun.CompletedAt?.ToString("O"));

        var updateRequest = CreateUpdateItemRequest(metaItem);
        await dynamoDbClient.UpdateItemAsync(updateRequest, cancellationToken);

        foreach (var (filePath, fileMeta) in restoreRun.RequestedFiles)
        {
            var fileSk = $"{pk}#FILENAME#{WebUtility.UrlEncode(filePath)}";

            var fileItem = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = pk },
                ["SK"] = new() { S = fileSk },
                ["Type"] = new() { S = nameof(RestoreFileMetaData) },
                ["Status"] = new() { S = Enum.GetName(fileMeta.Status) },
                ["RestorePathStrategy"] = new() { S = Enum.GetName(fileMeta.RestorePathStrategy) },
                ["Size"] = new() { N = fileMeta.Size.ToString() }
            };
            var aclEntriesString = fileMeta.AclEntries is not null
                ? JsonSerializer.Serialize(fileMeta.AclEntries, SourceGenerationContext.Default.AclEntryArray)
                : null;
            SetSIfNotNull(fileItem, "AclEntries", aclEntriesString);
            SetSIfNotNull(fileItem, "RestoreDestination", fileMeta.RestoreFolder);
            SetSIfNotNull(fileItem, "FailedMessage", fileMeta.FailedMessage);
            SetSIfNotNull(fileItem, "LastModified", fileMeta.LastModified?.ToString("O"));
            SetSIfNotNull(fileItem, "Created", fileMeta.Created?.ToString("O"));
            SetSIfNotNull(fileItem, "Owner", fileMeta.Owner);
            SetSIfNotNull(fileItem, "Group", fileMeta.Group);
            var hashId = fileMeta.HashId is not null
                ? Base64Url.Encode(fileMeta.HashId)
                : null;
            SetSIfNotNull(fileItem, "HashId", hashId);
            SetSIfNotNull(fileItem, "StartedAt", fileMeta.RestoreStartedAt?.ToString("O"));
            SetSIfNotNull(fileItem, "CompletedAt", fileMeta.RestoreCompletedAt?.ToString("O"));

            var fileItemUpdateRequest = CreateUpdateItemRequest(fileItem);
            await dynamoDbClient.UpdateItemAsync(fileItemUpdateRequest, cancellationToken);
        }
    }

    public async Task RemoveRestoreRequest(string restoreId, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        
        var deleteReq = new DeleteItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "RESTORE_REQUEST" },
                ["SK"] = new() { S = $"RESTORE_REQUEST#{restoreId}" }
            },
            ConditionExpression = "attribute_exists(PK) AND attribute_exists(SK)"
        };
        
        await dynamoDbClient.DeleteItemAsync(deleteReq, cancellationToken);
    }

    public async Task SaveRestoreFileMetaData(string restoreId, RestoreFileMetaData restoreFileMeta,
        CancellationToken cancellationToken)
    {
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        
        var pk = $"RESTORE#{restoreId}";
        var filePath = restoreFileMeta.FilePath;
        
        var fileSk = $"{pk}#FILENAME#{WebUtility.UrlEncode(filePath)}";
        
        var fileItem = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = pk },
            ["SK"] = new() { S = fileSk },
            ["Status"] = new() { S = Enum.GetName(restoreFileMeta.Status) },
            ["Type"] = new() { S = nameof(RestoreFileMetaData) },
            ["RestorePathStrategy"] = new() { S = Enum.GetName(restoreFileMeta.RestorePathStrategy) },
            ["Size"] = new() { N = restoreFileMeta.Size.ToString() }
        };
        
        var aclEntriesString = restoreFileMeta.AclEntries is not null
            ? JsonSerializer.Serialize(restoreFileMeta.AclEntries, SourceGenerationContext.Default.AclEntryArray)
            : null;
        SetSIfNotNull(fileItem, "AclEntries", aclEntriesString);
        SetSIfNotNull(fileItem, "RestoreDestination", restoreFileMeta.RestoreFolder);
        SetSIfNotNull(fileItem, "FailedMessage", restoreFileMeta.FailedMessage);
        SetSIfNotNull(fileItem, "LastModified", restoreFileMeta.LastModified?.ToString("O"));
        SetSIfNotNull(fileItem, "Created", restoreFileMeta.Created?.ToString("O"));
        SetSIfNotNull(fileItem, "Owner", restoreFileMeta.Owner);
        SetSIfNotNull(fileItem, "Group", restoreFileMeta.Group);
        var hashId = restoreFileMeta.HashId is not null
            ? Base64Url.Encode(restoreFileMeta.HashId)
            : null;
        SetSIfNotNull(fileItem, "HashId", hashId);
        SetSIfNotNull(fileItem, "RestoreStartedAt", restoreFileMeta.RestoreStartedAt?.ToString("O"));
        SetSIfNotNull(fileItem, "RestoreCompletedAt", restoreFileMeta.RestoreCompletedAt?.ToString("O"));
        
        var fileItemUpdateRequest = CreateUpdateItemRequest(fileItem);
        await dynamoDbClient.UpdateItemAsync(fileItemUpdateRequest, cancellationToken);
    }

    private static FileMetaData MapFileMetaData(Dictionary<string, AttributeValue> item, string filePath = "")
    {
        var skParts = item["SK"].S.Split('#');
        var id = long.Parse(skParts.Last());

        filePath = string.IsNullOrWhiteSpace(filePath) ? WebUtility.UrlDecode(skParts[1]) : filePath;

        return new FileMetaData(filePath, id)
        {
            Status = GetSIfExists(item, "Status", Enum.Parse<FileStatus>),
            SkipReason = GetSIfExists(item, "SkipReason", s => s) ?? "",
            HashId = new ByteArrayKey(GetSIfExists(item, "HashId", Base64Url.Decode) ?? []),
            Created = GetSIfExists(item, "Created", DateTimeOffset.Parse),
            CompressedSize = GetNIfExists(item, "CompressedSize", long.Parse),
            OriginalSize = GetNIfExists(item, "OriginalSize", long.Parse),
            Owner = GetSIfExists(item, "Owner", s => s),
            Group = GetSIfExists(item, "Group", s => s),
            LastModified = GetSIfExists(item, "LastModified", DateTimeOffset.Parse),
            AclEntries = GetSIfExists(item, "AclEntries",
                s => JsonSerializer.Deserialize<AclEntry[]>(s,
                    SourceGenerationContext.Default.AclEntryArray))
        };
    }
    
    private UpdateItemRequest CreateUpdateItemRequest(Dictionary<string, AttributeValue> attributes)
    {
        var expressionBuilder = new StringBuilder("SET");
        var expressionAttributeNames = new Dictionary<string, string>();
        var expressionAttributeValues = new Dictionary<string, AttributeValue>();

        var count = 0;
        foreach (var (key, value) in attributes)
        {
            if (key is "PK" or "SK")
                continue; // PK and SK are not updated

            var letter = ToLetters(count);
            expressionAttributeNames.Add($"#{letter}", key);
            expressionAttributeValues.Add($":{letter}", value);
            expressionBuilder.Append($" #{letter} = :{letter},");

            count++;
        }

        var expression = expressionBuilder.ToString().TrimEnd(',');

        return new UpdateItemRequest
        {
            TableName = awsConfiguration.DynamoDbTableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = attributes["PK"],
                ["SK"] = attributes["SK"]
            },
            ExpressionAttributeNames = expressionAttributeNames,
            ExpressionAttributeValues = expressionAttributeValues,
            UpdateExpression = expression
        };
    }

    private static string ToLetters(int zeroBasedIndex)
    {
        // shift to 1-based so that 1 → 'a', …, 26 → 'z', 27 → 'aa', etc.
        var n = zeroBasedIndex + 1;
        var sb = new StringBuilder();

        while (n > 0)
        {
            n--; // make 0…25
            sb.Insert(0, (char)('a' + n % 26));
            n /= 26;
        }

        return sb.ToString();
    }

    private static T GetNIfExists<T>(Dictionary<string, AttributeValue> item, string key, Func<string, T> conversion,
        T defaultValue)
    {
        if (!item.TryGetValue(key, out var value) || value.N is null)
            return defaultValue;
        return conversion(value.N);
    }

    private static T? GetNIfExists<T>(Dictionary<string, AttributeValue> item, string key, Func<string, T> conversion)
    {
        if (!item.TryGetValue(key, out var value) || value.N is null)
            return default;
        return conversion(value.N);
    }

    private static T? GetSIfExists<T>(Dictionary<string, AttributeValue> item, string key, Func<string, T> conversion)
    {
        if (!item.TryGetValue(key, out var value) || value.S is null || string.IsNullOrWhiteSpace(value.S))
            return default;
        return conversion(value.S);
    }

    private static void SetSIfNotNull<T>(Dictionary<string, AttributeValue> item, string key, T? value)
    {
        if (value is not null && !string.IsNullOrWhiteSpace(value.ToString()))
            item[key] = new AttributeValue { S = value.ToString()! };
    }

    private static void SetNIfNotNull<T>(Dictionary<string, AttributeValue> item, string key, T? value)
    {
        if (value is not null && !string.IsNullOrWhiteSpace(value.ToString()))
            item[key] = new AttributeValue { N = value.ToString()! };
    }
}