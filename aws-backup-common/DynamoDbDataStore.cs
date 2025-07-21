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
                    ["#k"] = "Size"
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
                            Status = Enum.Parse<ArchiveRunStatus>(item["Status"].S)
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
                            SkipReason = item.TryGetValue("SkipReason", out var value) ? value.S : ""
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
                ["SkipReason"] = new() { S = skipReason },
                ["Type"] = new() { S = nameof(FileMetaData) }
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
                ["ModifiedAt"] = new() { S = modified.ToString("O") },
                ["Type"] = new() { S = nameof(FileMetaData) }
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
                ["Group"] = new() { S = group },
                ["Type"] = new() { S = nameof(FileMetaData) }
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
                ["AclEntries"] = new() { S = entriesString },
                ["Type"] = new() { S = nameof(FileMetaData) }
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
            ["Type"] = new() { S = nameof(ArchiveRun) },
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
                ["Status"] = new() { S = Enum.GetName(chunkStatus) },
                ["Type"] = new() { S = nameof(DataChunkDetails) }
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
                ProjectionExpression = "#a, #b, #c, #d, #e, #f, #g, #h, #i, #j, #k, #l, #m, #n, #o, #p, #q, #r",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#a"] = "SK",
                    ["#d"] = "Created",
                    ["#e"] = "Status",
                    ["#f"] = "SkipReason",
                    ["#g"] = "Type",
                    ["#h"] = "LocalFilePath",
                    ["#i"] = "ChunkIndex",
                    ["#j"] = "ChunkSize",
                    ["#k"] = "Size",
                    ["#l"] = "CompressedSize",
                    ["#m"] = "OriginalSize",
                    ["#n"] = "Owner",
                    ["#o"] = "Group",
                    ["#p"] = "AclEntries",
                    ["#q"] = "LastModified",
                    ["#r"] = "HashKey"
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
                        var aclEntries = item.TryGetValue("AclEntries", out var acl)
                            ? JsonSerializer.Deserialize<AclEntry[]>(acl.S,
                                SourceGenerationContext.Default.AclEntryArray)
                            : null;
                        fileMetaData = new FileMetaData(filePath)
                        {
                            Status = Enum.Parse<FileStatus>(item["Status"].S),
                            SkipReason = item.TryGetValue("SkipReason", out var skipReason) ? skipReason.S : "",
                            Created = DateTimeOffset.Parse(item["Created"].S),
                            CompressedSize = item.TryGetValue("CompressedSize", out var cs) ? long.Parse(cs.N) : null,
                            OriginalSize = item.TryGetValue("OriginalSize", out var os) ? long.Parse(os.N) : null,
                            Owner = item.TryGetValue("Owner", out var owner) ? owner.S : null,
                            Group = item.TryGetValue("Group", out var group) ? group.S : null,
                            LastModified = item.TryGetValue("LastModified", out var lm)
                                ? DateTimeOffset.Parse(lm.S)
                                : null,
                            HashKey = Base64Url.Decode(item["HashKey"].S),
                            AclEntries = aclEntries
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

    public async Task SaveFileMetaData(string runId, string localFilePath, byte[] hashKey, long originalBytes,
        long compressedBytes,
        CancellationToken cancellationToken)
    {
        var tableName = awsConfiguration.DynamoDbTableName;
        var dynamoDbClient = await clientFactory.CreateDynamoDbClient(cancellationToken);

        var encodedFilePath = WebUtility.UrlEncode(localFilePath);
        var hashString = Base64Url.Encode(hashKey);

        var putReq = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                // partition key
                ["PK"] = new() { S = $"RUN_ID#{runId}" },
                // sort key
                ["SK"] = new() { S = $"RUN_ID#{runId}#FILE#{encodedFilePath}" },
                ["Type"] = new() { S = nameof(FileMetaData) },
                ["HashKey"] = new() { S = hashString },
                ["OriginalSize"] = new() { N = originalBytes.ToString() },
                ["CompressedSize"] = new() { N = compressedBytes.ToString() }
            }
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
                ProjectionExpression = "#a, #b, #c, #d, #e, #f, #g, #h, #i, #j, #k, #l, #m, #n, #o, #p, #q, #r",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#a"] = "SK",
                    ["#d"] = "Created",
                    ["#e"] = "Status",
                    ["#f"] = "SkipReason",
                    ["#g"] = "Type",
                    ["#h"] = "LocalFilePath",
                    ["#i"] = "ChunkIndex",
                    ["#j"] = "ChunkSize",
                    ["#k"] = "Size",
                    ["#l"] = "CompressedSize",
                    ["#m"] = "OriginalSize",
                    ["#n"] = "Owner",
                    ["#o"] = "Group",
                    ["#p"] = "AclEntries",
                    ["#q"] = "LastModified",
                    ["#r"] = "HashKey"
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

                        var aclEntries = item.TryGetValue("AclEntries", out var acl)
                            ? JsonSerializer.Deserialize<AclEntry[]>(acl.S,
                                SourceGenerationContext.Default.AclEntryArray)
                            : null;
                        fileMetaData = new FileMetaData(filePath)
                        {
                            Status = Enum.Parse<FileStatus>(item["Status"].S),
                            SkipReason = item.TryGetValue("SkipReason", out var skipReason) ? skipReason.S : "",
                            Created = DateTimeOffset.Parse(item["Created"].S),
                            CompressedSize = item.TryGetValue("CompressedSize", out var cs) ? long.Parse(cs.N) : null,
                            OriginalSize = item.TryGetValue("OriginalSize", out var os) ? long.Parse(os.N) : null,
                            Owner = item.TryGetValue("Owner", out var owner) ? owner.S : null,
                            Group = item.TryGetValue("Group", out var group) ? group.S : null,
                            LastModified = item.TryGetValue("LastModified", out var lm)
                                ? DateTimeOffset.Parse(lm.S)
                                : null,
                            HashKey = Base64Url.Decode(item["HashKey"].S),
                            AclEntries = aclEntries
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

        RestoreRun? run = null;

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
                        run = new RestoreRun
                        {
                            RestoreId = restoreId,
                            RestorePaths = item["RestorePaths"].S,
                            ArchiveRunId = item["ArchiveRunId"].S,
                            Status = Enum.Parse<RestoreRunStatus>(item["Status"].S),
                            RequestedAt = DateTimeOffset.Parse(item["RequestedAt"].S),
                            CompletedAt = item.TryGetValue("CompletedAt", out var completedAt)
                                ? DateTimeOffset.Parse(completedAt.S)
                                : null
                        };
                        break;
                    case nameof(RestoreFileMetaData):
                        // If we have FileMetaData, we can add it to the run
                        if (run is null) break;
                        //RESTORE_ID#<restoreID>#FILE#<filePath>
                        var filePath = WebUtility.UrlDecode(item["SK"].S.Split('#').Last());

                        var fileMeta = new RestoreFileMetaData(filePath)
                        {
                            Status = Enum.Parse<FileRestoreStatus>(item["Status"].S),
                            FailedMessage = item.TryGetValue("FailedMessage", out var failedMessage)
                                ? failedMessage.S
                                : "",
                            Created = item.TryGetValue("Created", out var created)
                                ? DateTimeOffset.Parse(created.S)
                                : null,
                            Size = item.TryGetValue("Size", out var size) ? long.Parse(size.N) : 0,
                            Owner = item.TryGetValue("Owner", out var owner) ? owner.S : null,
                            Group = item.TryGetValue("Group", out var group) ? group.S : null,
                            LastModified = item.TryGetValue("LastModified", out var lm)
                                ? DateTimeOffset.Parse(lm.S)
                                : null,
                            Sha256Checksum = item.TryGetValue("Sha256Checksum", out var checksumAttr)
                                ? Base64Url.Decode(checksumAttr.S)
                                : null,
                            AclEntries = item.TryGetValue("AclEntries", out var acl)
                                ? JsonSerializer.Deserialize<AclEntry[]>(acl.S,
                                    SourceGenerationContext.Default.AclEntryArray)
                                : null,
                            RestorePathStrategy = item.TryGetValue("RestorePathStrategy", out var strategy)
                                ? Enum.Parse<RestorePathStrategy>(strategy.S)
                                : RestorePathStrategy.Nested,
                            RestoreFolder = item.TryGetValue("RestoreFolder", out var folder) ? folder.S : null
                        };

                        run.RequestedFiles.TryAdd(filePath, fileMeta);
                        break;
                    case nameof(RestoreChunkDetails):
                        if (run is null) break;
                        //RESTORE_ID#<restoreID>#FILE#<filePath>#CHUNK#<chunkHashKey>
                        var keyParts = item["SK"].S.Split('#');
                        var filePathPart = WebUtility.UrlDecode(keyParts[3]);
                        if (!run.RequestedFiles.TryGetValue(filePathPart, out var metaData)) break;
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

        return run;
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
        var metaItem = new Dictionary<string, AttributeValue?>
        {
            ["PK"] = new() { S = pk },
            ["SK"] = new() { S = pk },
            ["Type"] = new() { S = nameof(RestoreRun) },
            ["CompletedAt"] = run.CompletedAt.HasValue
                ? new AttributeValue { S = run.CompletedAt.Value.ToString("O") }
                : null,
            ["RestoreId"] = new() { S = run.RestoreId },
            ["RestorePaths"] = new() { S = run.RestorePaths },
            ["ArchiveRunId"] = new() { S = run.ArchiveRunId },
            ["Status"] = new() { S = run.Status.ToString() },
            ["RequestedAt"] = new() { S = run.RequestedAt.ToString("O") }
        };
        writeRequests.Add(new WriteRequest { PutRequest = new PutRequest { Item = metaItem } });

        // 2) File‐level items
        foreach (var (filePath, fileMeta) in run.RequestedFiles)
        {
            var fileSk = $"{pk}#FILE#{WebUtility.UrlEncode(filePath)}";

            var fileItem = new Dictionary<string, AttributeValue?>
            {
                ["PK"] = new() { S = pk },
                ["SK"] = new() { S = fileSk },
                ["Status"] = new() { S = Enum.GetName(fileMeta.Status) },
                ["Type"] = new() { S = nameof(RestoreFileMetaData) },
                ["RestorePathStrategy"] = new() { S = Enum.GetName(fileMeta.RestorePathStrategy) },
                ["Size"] = new() { N = fileMeta.Size.ToString() }
            };
            if (fileMeta.AclEntries is not null)
                fileItem["AclEntries"] = new AttributeValue
                {
                    S = JsonSerializer.Serialize(fileMeta.AclEntries, SourceGenerationContext.Default.AclEntryArray)
                };
            if (fileMeta.RestoreFolder is not null)
                fileItem["RestoreFolder"] = new AttributeValue { S = fileMeta.RestoreFolder };
            if (fileMeta.FailedMessage is not null)
                fileItem["FailedMessage"] = new AttributeValue { S = fileMeta.FailedMessage };
            if (fileMeta.LastModified is not null)
                fileItem["LastModified"] = new AttributeValue { S = fileMeta.LastModified.Value.ToString("O") };
            if (fileMeta.Created is not null)
                fileItem["Created"] = new AttributeValue { S = fileMeta.Created.Value.ToString("O") };
            if (fileMeta.Owner is not null)
                fileItem["Owner"] = new AttributeValue { S = fileMeta.Owner };
            if (fileMeta.Group is not null)
                fileItem["Group"] = new AttributeValue { S = fileMeta.Group };
            if (fileMeta.Sha256Checksum is not null)
                fileItem["Sha256Checksum"] = new AttributeValue { S = Base64Url.Encode(fileMeta.Sha256Checksum) };

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

        var fileItem = new Dictionary<string, AttributeValue?>
        {
            ["PK"] = new() { S = pk },
            ["SK"] = new() { S = fileSk },
            ["Status"] = new() { S = Enum.GetName(fileMeta.Status) },
            ["Type"] = new() { S = nameof(RestoreFileMetaData) },
            ["RestorePathStrategy"] = new() { S = Enum.GetName(fileMeta.RestorePathStrategy) },
            ["Size"] = new() { N = fileMeta.Size.ToString() }
        };
        if (fileMeta.AclEntries is not null)
            fileItem["AclEntries"] = new AttributeValue
            {
                S = JsonSerializer.Serialize(fileMeta.AclEntries, SourceGenerationContext.Default.AclEntryArray)
            };
        if (fileMeta.RestoreFolder is not null)
            fileItem["RestoreFolder"] = new AttributeValue { S = fileMeta.RestoreFolder };
        if (fileMeta.FailedMessage is not null)
            fileItem["FailedMessage"] = new AttributeValue { S = fileMeta.FailedMessage };
        if (fileMeta.LastModified is not null)
            fileItem["LastModified"] = new AttributeValue { S = fileMeta.LastModified.Value.ToString("O") };
        if (fileMeta.Created is not null)
            fileItem["Created"] = new AttributeValue { S = fileMeta.Created.Value.ToString("O") };
        if (fileMeta.Owner is not null)
            fileItem["Owner"] = new AttributeValue { S = fileMeta.Owner };
        if (fileMeta.Group is not null)
            fileItem["Group"] = new AttributeValue { S = fileMeta.Group };
        if (fileMeta.Sha256Checksum is not null)
            fileItem["Sha256Checksum"] = new AttributeValue { S = Base64Url.Encode(fileMeta.Sha256Checksum) };

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
                ["Status"] = new() { S = Enum.GetName(readyToRestore) },
                ["Type"] = new() { S = nameof(RestoreChunkDetails) }
            }
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    public async IAsyncEnumerable<RestoreRequest> GetRestoreRequests([EnumeratorCancellation] CancellationToken cancellationToken)
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
                var restorePathStrategy = item.TryGetValue("RestorePathStrategy", out var strategy)
                    ? Enum.Parse<RestorePathStrategy>(strategy.S)
                    : RestorePathStrategy.Nested;
                var restoreFolder = item.TryGetValue("RestoreFolder", out var folder) ? folder.S : null;
                
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
                ["Type"] = new() { S = nameof(RestoreFileMetaData) },
                ["FailedMessage"] = new() { S = reasonMessage }
            }
        };

        // one round‑trip to DynamoDB
        await dynamoDbClient.PutItemAsync(putReq, cancellationToken);
    }

    private static void SetSIfNotNull<T>(Dictionary<string, AttributeValue> item, string key, T? value)
    {
        if (value is not null) item[key] = new AttributeValue { S = value.ToString()! };
    }

    private static void SetNIfNotNull<T>(Dictionary<string, AttributeValue> item, string key, T? value)
    {
        if (value is not null) item[key] = new AttributeValue { N = value.ToString()! };
    }
}