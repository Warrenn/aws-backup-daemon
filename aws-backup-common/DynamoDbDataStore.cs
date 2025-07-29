using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Amazon.DynamoDBv2.Model;

namespace aws_backup_common;

public class DynamoDbDataStore(
    IAwsClientFactory clientFactory,
    AwsConfiguration awsConfiguration,
    IContextResolver contextResolver) :
    IArchiveDataStore,
    IRestoreDataStore,
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
                    [":pk"] = new() { S = "ARCHIVE_REQUESTS" }
                },
                ProjectionExpression = "SK, PathsToArchive, CronSchedule",
                ExclusiveStartKey = lastKey
            };

            var resp = await dynamoDbClient.QueryAsync(req, cancellationToken);

            foreach (var item in resp.Items)
            {
                var sk = item["SK"].S;
                var runId = sk.Split('#', 2)[1];

                var paths = item.TryGetValue("PathsToArchive", out var p)
                    ? p.S
                    : string.Empty;

                var cron = item.TryGetValue("CronSchedule", out var c)
                    ? c.S
                    : string.Empty;

                yield return new RunRequest(runId, paths, cron);
            }

            lastKey = resp.LastEvaluatedKey;
        } while (lastKey is { Count: > 0 });
    }

    public async Task SaveRunRequest(RunRequest request, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var item = new Dictionary<string, AttributeValue>
        {
            // partition key
            ["PK"] = new() { S = "ARCHIVE_REQUESTS" },
            // sort key
            ["SK"] = new() { S = $"RUN_ID#{request.RunId}" },
            // other attributes
            ["PathsToArchive"] = new() { S = request.PathsToArchive },
            ["CronSchedule"] = new() { S = request.CronSchedule },
            ["Type"] = new() { S = nameof(RunRequest) }
        };

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = item
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task SaveArchiveRun(ArchiveRun run, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var item = new Dictionary<string, AttributeValue>
        {
            // partition key
            ["PK"] = new() { S = $"RUN_ID#{run.RunId}" },
            // sort key
            ["SK"] = new() { S = $"RUN_ID#{run.RunId}" },
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

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = item
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task RemoveArchiveRequest(string archiveRunId, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var deleteReq = new DeleteItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = "ARCHIVE_REQUESTS" },
                ["SK"] = new() { S = $"RUN_ID#{archiveRunId}" }
            },
            ConditionExpression = "attribute_exists(PK) AND attribute_exists(SK)"
        };

        await dynamoDbClient.DeleteItemAsync(deleteReq, cancellationToken);
    }

    public async Task<ArchiveRun?> GetArchiveRun(string runId, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        Dictionary<string, AttributeValue>? lastKey;

        ArchiveRun? run = null;

        var pk = $"RUN_ID#{runId}";
        do
        {
            var queryReq = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "PK = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = pk }
                },
                ProjectionExpression = "#a, #b, #c, #d, #e, #f, #g, #h, #i, #j, #k",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#a"] = "SK",
                    ["#b"] = "PathsToArchive",
                    ["#c"] = "CronSchedule",
                    ["#d"] = "CreatedAt",
                    ["#e"] = "Status",
                    ["#f"] = "SkipReason",
                    ["#g"] = "Type",
                    ["#h"] = "LocalFilePath",
                    ["#i"] = "ChunkIndex",
                    ["#j"] = "ChunkSize",
                    ["#k"] = "Size",
                    ["#l"] = "CompressedSize",
                    ["#m"] = "OriginalSize",
                    ["#n"] = "TotalFiles",
                    ["#o"] = "TotalSkippedFiles",
                    ["#p"] = "CompletedAt",
                    ["#q"] = "Owner",
                    ["#r"] = "Group",
                    ["#s"] = "AclEntries",
                    ["#t"] = "HashKey",
                    ["#u"] = "Created",
                    ["#v"] = "LastModified"
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
                    case nameof(ArchiveRun):
                        run = new ArchiveRun
                        {
                            RunId = runId,
                            PathsToArchive = item["PathsToArchive"].S,
                            CronSchedule = item["CronSchedule"].S,
                            CreatedAt = DateTimeOffset.Parse(item["CreatedAt"].S),
                            Status = Enum.Parse<ArchiveRunStatus>(item["Status"].S),
                            CompressedSize = GetNIfExists(item, "CompressedSize", long.Parse),
                            OriginalSize = GetNIfExists(item, "OriginalSize", long.Parse),
                            TotalFiles = GetNIfExists(item, "TotalFiles", int.Parse),
                            TotalSkippedFiles = GetNIfExists(item, "TotalSkippedFiles", int.Parse),
                            CompletedAt = GetSIfExists(item, "CompletedAt", DateTimeOffset.Parse)
                        };
                        break;
                    case nameof(FileMetaData):
                        // If we have FileMetaData, we can add it to the run
                        if (run is null) break;
                        //RUN_ID#<runID>#FILE#<filePath>
                        var filePath = WebUtility.UrlDecode(item["SK"].S.Split('#').Last());

                        var fileMeta = new FileMetaData(filePath)
                        {
                            Status = Enum.Parse<FileStatus>(item["Status"].S),
                            SkipReason = item.TryGetValue("SkipReason", out var value) ? value.S : "",
                            HashKey = GetSIfExists(item, "HashKey", Base64Url.Decode) ?? [],
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

                        run.Files.TryAdd(filePath, fileMeta);
                        break;
                    case nameof(DataChunkDetails):
                        if (run is null) break;
                        //RUN_ID#<runID>#FILE#<filePath>#CHUNK#<chunkHashKey>
                        var keyParts = item["SK"].S.Split('#');
                        var filePathPart = WebUtility.UrlDecode(keyParts[3]);
                        if (!run.Files.TryGetValue(filePathPart, out var metaData)) break;
                        var keyBytes = Base64Url.Decode(keyParts[5]);
                        var chunkKey = new ByteArrayKey(keyBytes);
                        metaData.Chunks.TryAdd(chunkKey, new DataChunkDetails(
                            item["LocalFilePath"].S,
                            int.Parse(item["ChunkIndex"].N),
                            long.Parse(item["ChunkSize"].N),
                            keyBytes,
                            long.Parse(item["Size"].N))
                        {
                            Status = Enum.Parse<ChunkStatus>(item["Status"].S)
                        });
                        break;
                }
            }

            // DynamoDB gives you this if there's more data
            lastKey = resp.LastEvaluatedKey;
        } while (lastKey is { Count: > 0 });

        return run;
    }

    public async Task UpdateFileStatus(string runId, string filePath, FileStatus fileStatus, string skipReason,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var encodedFilePath = WebUtility.UrlEncode(filePath);

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                // partition key
                ["PK"] = new() { S = $"RUN_ID#{runId}" },
                // sort key
                ["SK"] = new() { S = $"RUN_ID#{runId}#FILE#{encodedFilePath}" },
                ["Status"] = new() { S = Enum.GetName(fileStatus) },
                ["SkipReason"] = new() { S = skipReason }
            }
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task UpdateTimeStamps(string runId, string localFilePath, DateTimeOffset created,
        DateTimeOffset modified,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var encodedFilePath = WebUtility.UrlEncode(localFilePath);

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                // partition key
                ["PK"] = new() { S = $"RUN_ID#{runId}" },
                // sort key
                ["SK"] = new() { S = $"RUN_ID#{runId}#FILE#{encodedFilePath}" },
                ["CreatedAt"] = new() { S = created.ToString("O") },
                ["ModifiedAt"] = new() { S = modified.ToString("O") }
            }
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task UpdateOwnerGroup(string runId, string localFilePath, string owner, string group,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var encodedFilePath = WebUtility.UrlEncode(localFilePath);

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                // partition key
                ["PK"] = new() { S = $"RUN_ID#{runId}" },
                // sort key
                ["SK"] = new() { S = $"RUN_ID#{runId}#FILE#{encodedFilePath}" },
                ["Owner"] = new() { S = owner },
                ["Group"] = new() { S = group }
            }
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task UpdateAclEntries(string runId, string localFilePath, AclEntry[] aclEntries,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var encodedFilePath = WebUtility.UrlEncode(localFilePath);
        var entriesString = JsonSerializer.Serialize(aclEntries, SourceGenerationContext.Default.AclEntryArray);

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                // partition key
                ["PK"] = new() { S = $"RUN_ID#{runId}" },
                // sort key
                ["SK"] = new() { S = $"RUN_ID#{runId}#FILE#{encodedFilePath}" },
                ["AclEntries"] = new() { S = entriesString }
            }
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task UpdateArchiveStatus(string runId, ArchiveRunStatus runStatus, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var item = new Dictionary<string, AttributeValue>
        {
            // partition key
            ["PK"] = new() { S = $"RUN_ID#{runId}" },
            // sort key
            ["SK"] = new() { S = $"RUN_ID#{runId}" },
            ["Status"] = new() { S = Enum.GetName(runStatus) }
        };

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = item
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task DeleteFileChunks(string runId, string localFilePath, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var pk = $"RUN_ID#{runId}";
        var filePath = WebUtility.UrlEncode(localFilePath);
        var chunkPrefix = $"RUN_ID#{runId}#FILE#{filePath}#CHUNK#";

        // 1) Query only for the keys
        var keysToDelete = new List<Dictionary<string, AttributeValue>>();
        Dictionary<string, AttributeValue>? lastKey = null;

        do
        {
            var q = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "PK = :pk AND begins_with(SK, :skp)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = pk },
                    [":skp"] = new() { S = chunkPrefix }
                },
                ProjectionExpression = "PK, SK",
                ExclusiveStartKey = lastKey
            };

            var resp = await dynamoDbClient.QueryAsync(q, cancellationToken);
            lastKey = resp.LastEvaluatedKey;

            // collect just the key(s)
            keysToDelete.AddRange(resp.Items
                .Select(item => new Dictionary<string, AttributeValue>
                {
                    ["PK"] = item["PK"],
                    ["SK"] = item["SK"]
                }));
        } while (lastKey is { Count: > 0 });

        if (keysToDelete.Count == 0)
            return;

        // 2) Batch‐delete in chunks of 25
        const int batchSize = 25;
        for (var i = 0; i < keysToDelete.Count; i += batchSize)
        {
            var batch = keysToDelete.Skip(i).Take(batchSize)
                .Select(key => new WriteRequest { DeleteRequest = new DeleteRequest { Key = key } })
                .ToList();

            var batchReq = new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    [tableName] = batch
                }
            };

            var batchResp = await dynamoDbClient.BatchWriteItemAsync(batchReq, cancellationToken);

            // handle any unprocessed keys (retry)
            while (batchResp.UnprocessedItems != null &&
                   batchResp.UnprocessedItems.TryGetValue(tableName, out var unProc) && unProc.Count > 0)
            {
                batchReq.RequestItems[tableName] = unProc;
                batchResp = await dynamoDbClient.BatchWriteItemAsync(batchReq, cancellationToken);
            }
        }
    }

    public async Task SaveChunkStatus(string runId, string localFilePath, ByteArrayKey chunkHashKey,
        ChunkStatus chunkStatus,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var encodedFilePath = WebUtility.UrlEncode(localFilePath);
        var chunkKey = $"RUN_ID#{runId}#FILE#{encodedFilePath}#CHUNK#{Base64Url.Encode(chunkHashKey.ToArray())}";

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                // partition key
                ["PK"] = new() { S = $"RUN_ID#{runId}" },
                // sort key
                ["SK"] = new() { S = chunkKey },
                ["Status"] = new() { S = Enum.GetName(chunkStatus) }
            }
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task<FileMetaData?> GetFileMetaData(string runId, string filePath, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        Dictionary<string, AttributeValue>? lastKey;

        FileMetaData? fileMetaData = null;

        var pk = $"RUN_ID#{runId}";
        var sk = $"RUN_ID#{runId}#FILE#{WebUtility.UrlEncode(filePath)}";

        do
        {
            var queryReq = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "PK = :pk AND begins_with(SK, :skp)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = pk },
                    [":skp"] = new() { S = sk }
                },
                ProjectionExpression = "#a, #b, #c, #d, #e, #f, #g, #h, #i, #j, #k, #l, #m, #n, #o, #p",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#a"] = "SK",
                    ["#b"] = "Created",
                    ["#c"] = "Status",
                    ["#d"] = "SkipReason",
                    ["#e"] = "Type",
                    ["#f"] = "LocalFilePath",
                    ["#g"] = "ChunkIndex",
                    ["#h"] = "ChunkSize",
                    ["#i"] = "Size",
                    ["#j"] = "CompressedSize",
                    ["#k"] = "OriginalSize",
                    ["#l"] = "Owner",
                    ["#m"] = "Group",
                    ["#n"] = "AclEntries",
                    ["#o"] = "LastModified",
                    ["#p"] = "HashKey"
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
                    case nameof(FileMetaData):
                        //RUN_ID#<runID>#FILE#<filePath>                    
                        fileMetaData = new FileMetaData(filePath)
                        {
                            Status = Enum.Parse<FileStatus>(item["Status"].S),
                            SkipReason = item.TryGetValue("SkipReason", out var value) ? value.S : "",
                            HashKey = GetSIfExists(item, "HashKey", Base64Url.Decode) ?? [],
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
                        break;
                    case nameof(DataChunkDetails):
                        //RUN_ID#<runID>#FILE#<filePath>#CHUNK#<chunkHashKey>
                        var keyParts = item["SK"].S.Split('#');
                        var keyBytes = Base64Url.Decode(keyParts[5]);
                        var chunkKey = new ByteArrayKey(keyBytes);

                        fileMetaData?.Chunks.TryAdd(chunkKey, new DataChunkDetails(
                            item["LocalFilePath"].S,
                            int.Parse(item["ChunkIndex"].N),
                            long.Parse(item["ChunkSize"].N),
                            keyBytes,
                            long.Parse(item["Size"].N))
                        {
                            Status = Enum.Parse<ChunkStatus>(item["Status"].S)
                        });
                        break;
                }
            }

            // DynamoDB gives you this if there's more data
            lastKey = resp.LastEvaluatedKey;
        } while (lastKey is { Count: > 0 });

        return fileMetaData;
    }

    public async Task SaveChunkDetails(string runId, string localFilePath, DataChunkDetails details,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var encodedFilePath = WebUtility.UrlEncode(localFilePath);
        var chunkKey = $"RUN_ID#{runId}#FILE#{encodedFilePath}#CHUNK#{Base64Url.Encode(details.HashKey)}";

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                // partition key
                ["PK"] = new() { S = $"RUN_ID#{runId}" },
                // sort key
                ["SK"] = new() { S = chunkKey },
                ["Status"] = new() { S = Enum.GetName(details.Status) },
                ["Type"] = new() { S = nameof(DataChunkDetails) },
                ["LocalFilePath"] = new() { S = details.LocalFilePath },
                ["ChunkIndex"] = new() { N = details.ChunkIndex.ToString() },
                ["ChunkSize"] = new() { N = details.ChunkSize.ToString() },
                ["Size"] = new() { N = details.Size.ToString() }
            }
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task SaveFileMetaData(string runId, FileMetaData metaData,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var localFilePath = metaData.LocalFilePath;
        var hashKey = metaData.HashKey;
        var status = metaData.Status;
        var aclEntryString = metaData.AclEntries is not null
            ? JsonSerializer.Serialize(metaData.AclEntries, SourceGenerationContext.Default.AclEntryArray)
            : null;

        var encodedFilePath = WebUtility.UrlEncode(localFilePath);
        var hashString = Base64Url.Encode(hashKey);
        var item = new Dictionary<string, AttributeValue>
        {
            // partition key
            ["PK"] = new() { S = $"RUN_ID#{runId}" },
            // sort key
            ["SK"] = new() { S = $"RUN_ID#{runId}#FILE#{encodedFilePath}" },
            ["Type"] = new() { S = nameof(FileMetaData) },
            ["HashKey"] = new() { S = hashString },
            ["Status"] = new() { S = Enum.GetName(status) },
            ["LocalFilePath"] = new() { S = localFilePath }
        };

        SetSIfNotNull(item, "AclEntries", aclEntryString);
        SetSIfNotNull(item, "Created", metaData.Created?.ToString("O"));
        SetSIfNotNull(item, "LastModified", metaData.LastModified?.ToString("O"));
        SetSIfNotNull(item, "SkipReason", metaData.SkipReason);
        SetSIfNotNull(item, "Owner", metaData.Owner);
        SetSIfNotNull(item, "Group", metaData.Group);
        SetNIfNotNull(item, "CompressedSize", metaData.CompressedSize);
        SetNIfNotNull(item, "OriginalSize", metaData.OriginalSize);

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = item
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async IAsyncEnumerable<FileMetaData> GetRestorableFileMetaData(string runId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        Dictionary<string, AttributeValue>? lastKey;

        var pk = $"RUN_ID#{runId}";
        var sk = $"RUN_ID#{runId}#FILE#";

        var currentFilePath = string.Empty;
        FileMetaData? fileMetaData = null;

        do
        {
            var queryReq = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "PK = :pk AND begins_with(SK, :skp)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = pk },
                    [":skp"] = new() { S = sk }
                },
                ProjectionExpression = "#a, #b, #c, #d, #e, #f, #g, #h, #i, #j, #k, #l, #m, #n, #o, #p",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#a"] = "SK",
                    ["#b"] = "Created",
                    ["#c"] = "Status",
                    ["#d"] = "SkipReason",
                    ["#e"] = "Type",
                    ["#f"] = "LocalFilePath",
                    ["#g"] = "ChunkIndex",
                    ["#h"] = "ChunkSize",
                    ["#i"] = "Size",
                    ["#j"] = "CompressedSize",
                    ["#k"] = "OriginalSize",
                    ["#l"] = "Owner",
                    ["#m"] = "Group",
                    ["#n"] = "AclEntries",
                    ["#o"] = "LastModified",
                    ["#p"] = "HashKey"
                }
            };

            var resp = await dynamoDbClient.QueryAsync(queryReq, cancellationToken);
            if (resp.Items == null || resp.Items.Count == 0)
                yield break;

            // process this page’s items
            foreach (var item in resp.Items)
            {
                var filePath = string.Empty;
                var type = item["Type"].S;
                switch (type)
                {
                    case nameof(FileMetaData):
                        //RUN_ID#<runID>#FILE#<filePath>
                        var skParts = item["SK"].S.Split('#');
                        var encodedFilePath = skParts.Last();
                        filePath = WebUtility.UrlDecode(encodedFilePath);

                        fileMetaData = new FileMetaData(filePath)
                        {
                            Status = Enum.Parse<FileStatus>(item["Status"].S),
                            SkipReason = item.TryGetValue("SkipReason", out var value) ? value.S : "",
                            HashKey = GetSIfExists(item, "HashKey", Base64Url.Decode) ?? [],
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
                        break;
                    case nameof(DataChunkDetails):
                        //RUN_ID#<runID>#FILE#<filePath>#CHUNK#<chunkHashKey>
                        var keyParts = item["SK"].S.Split('#');
                        var keyBytes = Base64Url.Decode(keyParts[5]);
                        var chunkKey = new ByteArrayKey(keyBytes);

                        fileMetaData?.Chunks.TryAdd(chunkKey, new DataChunkDetails(
                            item["LocalFilePath"].S,
                            int.Parse(item["ChunkIndex"].N),
                            long.Parse(item["ChunkSize"].N),
                            keyBytes,
                            long.Parse(item["Size"].N))
                        {
                            Status = Enum.Parse<ChunkStatus>(item["Status"].S)
                        });
                        break;
                }

                if (string.IsNullOrWhiteSpace(currentFilePath)) currentFilePath = filePath;
                if (filePath == currentFilePath || fileMetaData?.Status is not FileStatus.UploadComplete) continue;

                yield return fileMetaData;
                currentFilePath = filePath;
                fileMetaData = null;
            }

            // DynamoDB gives you this if there's more data
            lastKey = resp.LastEvaluatedKey;
        } while (lastKey is { Count: > 0 });

        if (fileMetaData?.Status is FileStatus.UploadComplete)
            // yield the last file metadata if it is complete
            yield return fileMetaData;
    }

    public async Task<bool> ContainsKey(ByteArrayKey key, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var keyString = Base64Url.Encode(key.ToArray());

        var resp = await dynamoDbClient.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = $"CLOUD_CHUNK#{keyString}" },
                ["SK"] = new() { S = "CLOUD_CHUNK" }
            },
            // only fetch the keys
            ProjectionExpression = "PK, SK",
            ConsistentRead = false // eventual‐consistency is fine for existence checks
        }, cancellationToken);

        // if no item, resp.Item will be empty
        return resp.Item is { Count: > 0 };
    }

    public async Task AddCloudChunkDetails(ByteArrayKey hashKey, CloudChunkDetails cloudChunkDetails,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var keyString = Base64Url.Encode(hashKey.ToArray());

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                // partition key
                ["PK"] = new() { S = $"CLOUD_CHUNK#{keyString}" },
                // sort key
                ["SK"] = new() { S = "CLOUD_CHUNK" },
                ["Type"] = new() { S = nameof(CloudChunkDetails) },
                ["S3Key"] = new() { S = cloudChunkDetails.S3Key },
                ["BucketName"] = new() { S = cloudChunkDetails.BucketName },
                ["ChunkSize"] = new() { N = cloudChunkDetails.ChunkSize.ToString() },
                ["Offset"] = new() { N = cloudChunkDetails.Offset.ToString() },
                ["Size"] = new() { N = cloudChunkDetails.Size.ToString() }
            }
        };

        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task<CloudChunkDetails?> GetCloudChunkDetails(ByteArrayKey hashKey,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var keyString = Base64Url.Encode(hashKey.ToArray());

        var queryReq = new QueryRequest
        {
            TableName = tableName,
            KeyConditionExpression = "PK = :pk",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pk"] = new() { S = $"CLOUD_CHUNK#{keyString}" }
            },
            ProjectionExpression = "#a, #b, #c, #d, #e",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#a"] = "S3Key",
                ["#b"] = "BucketName",
                ["#c"] = "ChunkSize",
                ["#d"] = "Offset",
                ["#e"] = "Size"
            }
        };

        var resp = await dynamoDbClient.QueryAsync(queryReq, cancellationToken);
        if (resp.Items == null || resp.Items.Count == 0)
            return null;

        var item = resp.Items[0];
        var returnChunk = new CloudChunkDetails(
            item["S3Key"].S,
            item["BucketName"].S,
            long.Parse(item["ChunkSize"].N),
            long.Parse(item["Offset"].N),
            long.Parse(item["Size"].N),
            hashKey.ToArray());

        return returnChunk;
    }

    public async Task<RestoreRun?> LookupRestoreRun(string restoreId, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        Dictionary<string, AttributeValue>? lastKey;

        RestoreRun? restoreRun = null;

        var pk = $"RESTORE_ID#{restoreId}";
        do
        {
            var queryReq = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = "PK = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pk"] = new() { S = pk }
                },
                ProjectionExpression =
                    "#a, #b, #c, #d, #e, #f, #g, #h, #i, #j, #k, #l, #m, #n, #o, #p, #q, #r, #s, #t, #u, #v, #w, #x",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#a"] = "SK",
                    ["#b"] = "RestorePaths",
                    ["#c"] = "ArchiveRunId",
                    ["#d"] = "Status",
                    ["#e"] = "RequestedAt",
                    ["#f"] = "SkipReason",
                    ["#g"] = "CompletedAt",
                    ["#h"] = "FailedMessage",
                    ["#i"] = "Size",
                    ["#j"] = "LastModified",
                    ["#k"] = "Created",
                    ["#l"] = "Type",
                    ["#m"] = "Owner",
                    ["#n"] = "Group",
                    ["#o"] = "AclEntries",
                    ["#p"] = "Sha256Checksum",
                    ["#q"] = "RestorePathStrategy",
                    ["#r"] = "RestoreFolder",
                    ["#s"] = "S3Key",
                    ["#t"] = "BucketName",
                    ["#u"] = "ChunkSize",
                    ["#v"] = "Offset",
                    ["#w"] = "Size",
                    ["#x"] = "Index"
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
                            RestorePaths = item["RestorePaths"].S,
                            ArchiveRunId = item["ArchiveRunId"].S,
                            Status = Enum.Parse<RestoreRunStatus>(item["Status"].S),
                            RequestedAt = DateTimeOffset.Parse(item["RequestedAt"].S),
                            CompletedAt = GetSIfExists(item, "CompletedAt", DateTimeOffset.Parse)
                        };
                        break;
                    case nameof(RestoreFileMetaData):
                        // If we have FileMetaData, we can add it to the run
                        if (restoreRun is null) break;
                        //RESTORE_ID#<restoreID>#FILE#<filePath>
                        var filePath = WebUtility.UrlDecode(item["SK"].S.Split('#').Last());

                        var fileMeta = new RestoreFileMetaData(filePath)
                        {
                            Status = Enum.Parse<FileRestoreStatus>(item["Status"].S),
                            FailedMessage = GetSIfExists(item, "FailedMessage", s => s),
                            Size = GetNIfExists(item, "Size", long.Parse),
                            LastModified = GetSIfExists(item, "LastModified", DateTimeOffset.Parse),
                            Created = GetSIfExists(item, "Created", DateTimeOffset.Parse),
                            AclEntries = GetSIfExists(item, "AclEntries",
                                s => JsonSerializer.Deserialize<AclEntry[]>(s,
                                    SourceGenerationContext.Default.AclEntryArray)),
                            Owner = GetSIfExists(item, "Owner", s => s),
                            Sha256Checksum = GetSIfExists(item, "Sha256Checksum", Base64Url.Decode),
                            RestorePathStrategy = item.TryGetValue("RestorePathStrategy", out var strategy)
                                ? Enum.Parse<RestorePathStrategy>(strategy.S)
                                : RestorePathStrategy.Nested,
                            RestoreFolder = GetSIfExists(item, "RestoreFolder", s => s)
                        };

                        restoreRun.RequestedFiles.TryAdd(filePath, fileMeta);
                        break;
                    case nameof(RestoreChunkDetails):
                        if (restoreRun is null) break;
                        //RESTORE_ID#<restoreID>#FILE#<filePath>#CHUNK#<chunkHashKey>
                        var keyParts = item["SK"].S.Split('#');
                        var filePathPart = WebUtility.UrlDecode(keyParts[3]);
                        if (!restoreRun.RequestedFiles.TryGetValue(filePathPart, out var metaData)) break;
                        var keyBytes = Base64Url.Decode(keyParts.Last());
                        var chunkKey = new ByteArrayKey(keyBytes);
                        metaData.CloudChunkDetails.TryAdd(chunkKey, new RestoreChunkDetails(
                            item["S3Key"].S,
                            item["BucketName"].S,
                            long.Parse(item["ChunkSize"].N),
                            long.Parse(item["Offset"].N),
                            long.Parse(item["Size"].N),
                            keyBytes,
                            int.Parse(item["Index"].N))
                        {
                            Status = Enum.Parse<S3ChunkRestoreStatus>(item["Status"].S)
                        });
                        break;
                }
            }

            // DynamoDB gives you this if there's more data
            lastKey = resp.LastEvaluatedKey;
        } while (lastKey is { Count: > 0 });

        return restoreRun;
    }

    public async Task SaveRestoreRequest(RestoreRequest restoreRequest, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);
        var restoreId = contextResolver.RestoreId(restoreRequest.ArchiveRunId, restoreRequest.RestorePaths,
            restoreRequest.RequestedAt);

        var item = new Dictionary<string, AttributeValue>
        {
            // partition key
            ["PK"] = new() { S = "RESTORE_REQUESTS" },
            // sort key
            ["SK"] = new() { S = $"RESTORE_ID#{restoreId}" },
            ["Type"] = new() { S = nameof(RestoreRequest) },
            // other attributes
            ["RequestedAt"] = new() { S = restoreRequest.RequestedAt.ToString("O") },
            ["RestorePathStrategy"] = new() { S = Enum.GetName(restoreRequest.RestorePathStrategy) },
            ["RestorePaths"] = new() { S = restoreRequest.RestorePaths },
            ["ArchiveRunId"] = new() { S = restoreRequest.ArchiveRunId }
        };

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = item
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async Task SaveRestoreRun(RestoreRun run, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var pk = $"RESTORE_ID#{run.RestoreId}";
        var writeRequests = new List<WriteRequest>();

        // 1) Meta item
        var metaItem = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = pk },
            ["SK"] = new() { S = pk },
            ["Type"] = new() { S = nameof(RestoreRun) },
            ["RestoreId"] = new() { S = run.RestoreId },
            ["RestorePaths"] = new() { S = run.RestorePaths },
            ["ArchiveRunId"] = new() { S = run.ArchiveRunId },
            ["Status"] = new() { S = run.Status.ToString() },
            ["RequestedAt"] = new() { S = run.RequestedAt.ToString("O") }
        };

        SetSIfNotNull(metaItem, "CompletedAt", run.CompletedAt?.ToString("O"));
        writeRequests.Add(new WriteRequest { PutRequest = new PutRequest { Item = metaItem } });

        // 2) File‐level items
        foreach (var (filePath, fileMeta) in run.RequestedFiles)
        {
            var fileSk = $"{pk}#FILE#{WebUtility.UrlEncode(filePath)}";

            var fileItem = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = pk },
                ["SK"] = new() { S = fileSk },
                ["Status"] = new() { S = Enum.GetName(fileMeta.Status) },
                ["Type"] = new() { S = nameof(RestoreFileMetaData) },
                ["RestorePathStrategy"] = new() { S = Enum.GetName(fileMeta.RestorePathStrategy) },
                ["Size"] = new() { N = fileMeta.Size.ToString() }
            };
            var aclEntriesString = fileMeta.AclEntries is not null
                ? JsonSerializer.Serialize(fileMeta.AclEntries, SourceGenerationContext.Default.AclEntryArray)
                : null;
            SetSIfNotNull(fileItem, "AclEntries", aclEntriesString);
            SetSIfNotNull(fileItem, "RestoreFolder", fileMeta.RestoreFolder);
            SetSIfNotNull(fileItem, "FailedMessage", fileMeta.FailedMessage);
            SetSIfNotNull(fileItem, "LastModified", fileMeta.LastModified?.ToString("O"));
            SetSIfNotNull(fileItem, "Created", fileMeta.Created?.ToString("O"));
            SetSIfNotNull(fileItem, "Owner", fileMeta.Owner);
            SetSIfNotNull(fileItem, "Group", fileMeta.Group);
            var sha256Checksum = fileMeta.Sha256Checksum is not null
                ? Base64Url.Encode(fileMeta.Sha256Checksum)
                : null;
            SetSIfNotNull(fileItem, "Sha256Checksum", sha256Checksum);
            writeRequests.Add(new WriteRequest { PutRequest = new PutRequest { Item = fileItem } });

            // 3) Chunk‐level items
            writeRequests.AddRange(from chunkKv in fileMeta.CloudChunkDetails
                select chunkKv.Value
                into chunk
                let chunkKeyB64 = Base64Url.Encode(chunk.HashKey)
                let chunkSk = $"{fileSk}#CHUNK_ID#{chunkKeyB64}"
                select new Dictionary<string, AttributeValue>
                {
                    ["PK"] = new() { S = pk },
                    ["SK"] = new() { S = chunkSk },
                    ["Type"] = new() { S = nameof(RestoreChunkDetails) },
                    ["S3Key"] = new() { S = chunk.S3Key },
                    ["BucketName"] = new() { S = chunk.BucketName },
                    ["Index"] = new() { N = chunk.Index.ToString() },
                    ["ChunkSize"] = new() { N = chunk.ChunkSize.ToString() },
                    ["Offset"] = new() { N = chunk.Offset.ToString() },
                    ["Size"] = new() { N = chunk.Size.ToString() },
                    ["Status"] = new() { S = Enum.GetName(chunk.Status) }
                }
                into chunkItem
                select new WriteRequest { PutRequest = new PutRequest { Item = chunkItem } });
        }

        // 4) BatchWrite in chunks of 25
        const int batchSize = 25;
        for (var i = 0; i < writeRequests.Count; i += batchSize)
        {
            var batch = writeRequests.Skip(i).Take(batchSize).ToList();
            var batchReq = new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    [tableName] = batch
                }
            };

            var batchResp = await dynamoDbClient.BatchWriteItemAsync(batchReq, cancellationToken);

            // retry any unprocessed items
            while (batchResp.UnprocessedItems.TryGetValue(tableName, out var unProc) && unProc.Count > 0)
            {
                batchReq.RequestItems[tableName] = unProc;
                batchResp = await dynamoDbClient.BatchWriteItemAsync(batchReq, cancellationToken);
            }
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
                ["PK"] = new() { S = "RESTORE_REQUESTS" },
                ["SK"] = new() { S = $"RESTORE_ID#{restoreId}" }
            },
            ConditionExpression = "attribute_exists(PK) AND attribute_exists(SK)"
        };

        await dynamoDbClient.DeleteItemAsync(deleteReq, cancellationToken);
    }

    public async Task SaveRestoreFileMetaData(string restoreRunRestoreId, RestoreFileMetaData fileMeta,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var pk = $"RESTORE_ID#{restoreRunRestoreId}";
        var writeRequests = new List<WriteRequest>();
        var filePath = fileMeta.FilePath;

        var fileSk = $"{pk}#FILE#{WebUtility.UrlEncode(filePath)}";

        var fileItem = new Dictionary<string, AttributeValue>
        {
            ["PK"] = new() { S = pk },
            ["SK"] = new() { S = fileSk },
            ["Status"] = new() { S = Enum.GetName(fileMeta.Status) },
            ["Type"] = new() { S = nameof(RestoreFileMetaData) },
            ["RestorePathStrategy"] = new() { S = Enum.GetName(fileMeta.RestorePathStrategy) },
            ["Size"] = new() { N = fileMeta.Size.ToString() }
        };

        var aclEntriesString = fileMeta.AclEntries is not null
            ? JsonSerializer.Serialize(fileMeta.AclEntries, SourceGenerationContext.Default.AclEntryArray)
            : null;
        SetSIfNotNull(fileItem, "AclEntries", aclEntriesString);
        SetSIfNotNull(fileItem, "RestoreFolder", fileMeta.RestoreFolder);
        SetSIfNotNull(fileItem, "FailedMessage", fileMeta.FailedMessage);
        SetSIfNotNull(fileItem, "LastModified", fileMeta.LastModified?.ToString("O"));
        SetSIfNotNull(fileItem, "Created", fileMeta.Created?.ToString("O"));
        SetSIfNotNull(fileItem, "Owner", fileMeta.Owner);
        SetSIfNotNull(fileItem, "Group", fileMeta.Group);
        var sha256Checksum = fileMeta.Sha256Checksum is not null
            ? Base64Url.Encode(fileMeta.Sha256Checksum)
            : null;
        SetSIfNotNull(fileItem, "Sha256Checksum", sha256Checksum);
        writeRequests.Add(new WriteRequest { PutRequest = new PutRequest { Item = fileItem } });

        // 3) Chunk‐level items
        writeRequests.AddRange(from chunkKv in fileMeta.CloudChunkDetails
            select chunkKv.Value
            into chunk
            let chunkKeyB64 = Base64Url.Encode(chunk.HashKey)
            let chunkSk = $"{fileSk}#CHUNK_ID#{chunkKeyB64}"
            select new Dictionary<string, AttributeValue>
            {
                ["PK"] = new() { S = pk },
                ["SK"] = new() { S = chunkSk },
                ["Type"] = new() { S = nameof(RestoreChunkDetails) },
                ["S3Key"] = new() { S = chunk.S3Key },
                ["BucketName"] = new() { S = chunk.BucketName },
                ["Index"] = new() { N = chunk.Index.ToString() },
                ["ChunkSize"] = new() { N = chunk.ChunkSize.ToString() },
                ["Offset"] = new() { N = chunk.Offset.ToString() },
                ["Size"] = new() { N = chunk.Size.ToString() },
                ["Status"] = new() { S = Enum.GetName(chunk.Status) }
            }
            into chunkItem
            select new WriteRequest { PutRequest = new PutRequest { Item = chunkItem } });

        const int batchSize = 25;
        for (var i = 0; i < writeRequests.Count; i += batchSize)
        {
            var batch = writeRequests.Skip(i).Take(batchSize).ToList();
            var batchReq = new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    [tableName] = batch
                }
            };

            var batchResp = await dynamoDbClient.BatchWriteItemAsync(batchReq, cancellationToken);

            // retry any unprocessed items
            while (batchResp.UnprocessedItems.TryGetValue(tableName, out var unProc) && unProc.Count > 0)
            {
                batchReq.RequestItems[tableName] = unProc;
                batchResp = await dynamoDbClient.BatchWriteItemAsync(batchReq, cancellationToken);
            }
        }
    }

    public async Task SaveRestoreChunkStatus(string restoreId, string filePath, ByteArrayKey chunkKey,
        S3ChunkRestoreStatus readyToRestore, CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var encodedFilePath = WebUtility.UrlEncode(filePath);
        var chunkKeyB64 = Base64Url.Encode(chunkKey.ToArray());
        var chunkSk = $"RESTORE_ID#{restoreId}#FILE#{encodedFilePath}#CHUNK_ID#{chunkKeyB64}";

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                // partition key
                ["PK"] = new() { S = $"RESTORE_ID#{restoreId}" },
                // sort key
                ["SK"] = new() { S = chunkSk },
                ["Status"] = new() { S = Enum.GetName(readyToRestore) }
            }
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async IAsyncEnumerable<RestoreRequest> GetRestoreRequests(
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
                    [":pk"] = new() { S = "RESTORE_REQUESTS" }
                },
                ProjectionExpression = "ArchiveRunId, RestorePaths, RequestedAt, RestorePathStrategy, RestoreFolder",
                ExclusiveStartKey = lastKey
            };

            var resp = await dynamoDbClient.QueryAsync(req, cancellationToken);

            foreach (var item in resp.Items)
            {
                var paths = item["RestorePaths"].S;
                var archiveRunId = item["ArchiveRunId"].S;
                var requestedAt = DateTimeOffset.Parse(item["RequestedAt"].S);
                var restorePathStrategy = GetSIfExists(item, "RestorePathStrategy", Enum.Parse<RestorePathStrategy>);
                var restoreFolder = GetSIfExists(item, "RestoreFolder", s => s);

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

    public async Task SaveRestoreFileStatus(string restoreId, string fileMetaFilePath, FileRestoreStatus status,
        string reasonMessage,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var encodedFilePath = WebUtility.UrlEncode(fileMetaFilePath);
        var chunkSk = $"RESTORE_ID#{restoreId}#FILE#{encodedFilePath}";

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                // partition key
                ["PK"] = new() { S = $"RESTORE_ID#{restoreId}" },
                // sort key
                ["SK"] = new() { S = chunkSk },
                ["Status"] = new() { S = Enum.GetName(status) },
                ["FailedMessage"] = new() { S = reasonMessage }
            }
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
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
        if (value is not null) item[key] = new AttributeValue { N = value.ToString()! };
    }
}