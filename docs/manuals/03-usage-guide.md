# Usage Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Introduction

extenddb implements the DynamoDB wire protocol. Any AWS SDK or tool that speaks DynamoDB can connect to extenddb by changing the endpoint URL. This guide covers all supported operations, SDK configuration, expression syntax, error handling, and feature support.

## SDK Configuration

Point your SDK at extenddb by setting the endpoint URL. extenddb uses TLS with a self-signed certificate — set `AWS_CA_BUNDLE` to trust it. All requests must be signed with valid access keys created via the management API.

### AWS CLI

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:8000
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
```

Or per-command:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
aws dynamodb list-tables --endpoint-url https://127.0.0.1:8000
```

### Python (boto3)

```python
import os
import boto3

os.environ["AWS_CA_BUNDLE"] = os.path.expanduser("~/.extenddb/tls/cert.pem")

dynamodb = boto3.client(
    "dynamodb",
    endpoint_url="https://127.0.0.1:8000",
    region_name="us-east-1",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)
```

### Java (AWS SDK v2)

```java
DynamoDbClient client = DynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8000"))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("your-access-key", "your-secret-key")))
    .build();
```

Note: For Java, configure the trust store to include the self-signed cert, or set `AWS_CA_BUNDLE` before launching the JVM.

### Rust (aws-sdk-dynamodb)

```rust
let config = aws_config::defaults(BehaviorVersion::latest())
    .endpoint_url("https://127.0.0.1:8000")
    .region(Region::new("us-east-1"))
    .credentials_provider(Credentials::new("key", "secret", None, None, "extenddb"))
    .load()
    .await;
let client = aws_sdk_dynamodb::Client::new(&config);
```

### JavaScript/TypeScript (AWS SDK v3)

```typescript
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const client = new DynamoDBClient({
    endpoint: "https://127.0.0.1:8000",
    region: "us-east-1",
    credentials: { accessKeyId: "key", secretAccessKey: "secret" },
});
```

## Table Operations

### CreateTable

Creates a new DynamoDB table. Supports hash-only and hash+range key schemas, PAY_PER_REQUEST and PROVISIONED billing modes, GSIs, LSIs, and stream specifications.

```bash
aws dynamodb create-table \
    --table-name Orders \
    --attribute-definitions \
        AttributeName=CustomerId,AttributeType=S \
        AttributeName=OrderId,AttributeType=S \
        AttributeName=OrderDate,AttributeType=S \
    --key-schema \
        AttributeName=CustomerId,KeyType=HASH \
        AttributeName=OrderId,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --global-secondary-indexes '[{
        "IndexName": "OrderDateIndex",
        "KeySchema": [
            {"AttributeName": "CustomerId", "KeyType": "HASH"},
            {"AttributeName": "OrderDate", "KeyType": "RANGE"}
        ],
        "Projection": {"ProjectionType": "ALL"}
    }]'
```

Tables transition through `CREATING` → `ACTIVE` status. PostgreSQL can use the `control_plane_delay_seconds` runtime setting to simulate a delay. TiDB reconciles immediately and lets TiDB native online DDL schedule physical table and index changes. Poll with DescribeTable until `TableStatus` is `ACTIVE` before performing operations on the table.

### DeleteTable

```bash
aws dynamodb delete-table --table-name Orders
```

Tables transition through `DELETING` before removal.

### DescribeTable

```bash
aws dynamodb describe-table --table-name Orders
```

Returns full table metadata including key schema, attribute definitions, indexes, stream specification, item count, table size, and ARN.

### ListTables

```bash
aws dynamodb list-tables
aws dynamodb list-tables --max-items 10
```

Supports pagination via `ExclusiveStartTableName` and `Limit`.

### UpdateTable

```bash
aws dynamodb update-table \
    --table-name Orders \
    --billing-mode PROVISIONED \
    --provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=50
```

Supports updating billing mode, provisioned throughput, stream specification, deletion protection, and GSI updates (create/delete).

## Item Operations

### PutItem

```bash
aws dynamodb put-item \
    --table-name Orders \
    --item '{
        "CustomerId": {"S": "C001"},
        "OrderId": {"S": "O001"},
        "Amount": {"N": "99.99"},
        "Items": {"L": [{"S": "Widget"}, {"S": "Gadget"}]}
    }' \
    --condition-expression "attribute_not_exists(OrderId)" \
    --return-values ALL_OLD
```

Supports `ConditionExpression`, `ExpressionAttributeNames`, `ExpressionAttributeValues`, and `ReturnValues` (NONE, ALL_OLD).

### GetItem

```bash
aws dynamodb get-item \
    --table-name Orders \
    --key '{"CustomerId": {"S": "C001"}, "OrderId": {"S": "O001"}}' \
    --projection-expression "Amount, Items" \
    --consistent-read
```

Supports `ProjectionExpression`, `ConsistentRead`, and `ReturnConsumedCapacity`.

### UpdateItem

```bash
aws dynamodb update-item \
    --table-name Orders \
    --key '{"CustomerId": {"S": "C001"}, "OrderId": {"S": "O001"}}' \
    --update-expression "SET Amount = :new, #s = :status ADD Version :one" \
    --expression-attribute-names '{"#s": "Status"}' \
    --expression-attribute-values '{
        ":new": {"N": "109.99"},
        ":status": {"S": "Updated"},
        ":one": {"N": "1"}
    }' \
    --condition-expression "attribute_exists(OrderId)" \
    --return-values ALL_NEW
```

UpdateItem is an upsert — if the item does not exist, it is created with the key attributes plus the update expression result.

Supports `ReturnValues`: NONE, ALL_OLD, ALL_NEW, UPDATED_OLD, UPDATED_NEW.

Also supports the legacy `Expected` and `AttributeUpdates` parameters for backward compatibility.

### DeleteItem

```bash
aws dynamodb delete-item \
    --table-name Orders \
    --key '{"CustomerId": {"S": "C001"}, "OrderId": {"S": "O001"}}' \
    --condition-expression "Amount < :max" \
    --expression-attribute-values '{":max": {"N": "1000"}}' \
    --return-values ALL_OLD
```

## Query and Scan

### Query

```bash
aws dynamodb query \
    --table-name Orders \
    --key-condition-expression \
        "CustomerId = :cid AND OrderId BETWEEN :start AND :end" \
    --filter-expression "Amount > :min" \
    --expression-attribute-values '{
        ":cid": {"S": "C001"},
        ":start": {"S": "O001"},
        ":end": {"S": "O100"},
        ":min": {"N": "50"}
    }' \
    --scan-index-forward false \
    --limit 25 \
    --select ALL_ATTRIBUTES \
    --return-consumed-capacity INDEXES
```

Supports: `IndexName` (for GSI/LSI queries), `ScanIndexForward`, `Limit`, `ExclusiveStartKey`, `Select`, `ProjectionExpression`, `FilterExpression`, `ConsistentRead`, and `ReturnConsumedCapacity`. Index reads follow DynamoDB projection rules: default index reads return projected attributes, GSI reads cannot request attributes that are not projected into that GSI, and LSI reads may fetch non-projected attributes from the base table.

### Scan

```bash
aws dynamodb scan \
    --table-name Orders \
    --filter-expression "Amount > :min" \
    --expression-attribute-values '{":min": {"N": "50"}}' \
    --limit 100
```

Supports the same parameters as Query except `KeyConditionExpression` and `ScanIndexForward`. Also supports `Segment` and `TotalSegments` for parallel scan.

## Batch Operations

### BatchWriteItem

```bash
aws dynamodb batch-write-item \
    --request-items '{
        "Orders": [
            {
              "PutRequest": {
                "Item": {
                  "CustomerId": {"S": "C002"},
                  "OrderId": {"S": "O001"},
                  "Amount": {"N": "25"}
                }
              }
            },
            {
              "DeleteRequest": {
                "Key": {
                  "CustomerId": {"S": "C001"},
                  "OrderId": {"S": "O001"}
                }
              }
            }
        ]
    }'
```

Up to 25 items per request, across up to 100 tables. Returns `UnprocessedItems` for items that could not be written.

### BatchGetItem

```bash
aws dynamodb batch-get-item \
    --request-items '{
        "Orders": {
            "Keys": [
                {"CustomerId": {"S": "C001"}, "OrderId": {"S": "O001"}},
                {"CustomerId": {"S": "C002"}, "OrderId": {"S": "O001"}}
            ],
            "ProjectionExpression": "CustomerId, Amount"
        }
    }'
```

Up to 100 items per request. Returns `UnprocessedKeys` for items that could not be read.

## Transactions

### TransactWriteItems

```bash
aws dynamodb transact-write-items \
    --transact-items '[
        {
          "Put": {
            "TableName": "Orders",
            "Item": {
              "CustomerId": {"S": "C003"},
              "OrderId": {"S": "O001"},
              "Amount": {"N": "75"}
            }
          }
        },
        {
          "ConditionCheck": {
            "TableName": "Orders",
            "Key": {
              "CustomerId": {"S": "C001"},
              "OrderId": {"S": "O001"}
            },
            "ConditionExpression":
              "attribute_exists(CustomerId)"
          }
        }
    ]'
```

Up to 100 actions per transaction. Supports Put, Update, Delete, and ConditionCheck actions. All actions succeed or all fail atomically. Returns `TransactionCanceledException` with per-item `CancellationReasons` on failure.

### TransactGetItems

```bash
aws dynamodb transact-get-items \
    --transact-items '[
        {
          "Get": {
            "TableName": "Orders",
            "Key": {
              "CustomerId": {"S": "C001"},
              "OrderId": {"S": "O001"}
            }
          }
        },
        {
          "Get": {
            "TableName": "Orders",
            "Key": {
              "CustomerId": {"S": "C002"},
              "OrderId": {"S": "O001"}
            }
          }
        }
    ]'
```

Up to 100 items per transaction. Provides a consistent snapshot across all items.

## DynamoDB Streams

Enable streams when creating a table:

```bash
aws dynamodb create-table \
    --table-name StreamTable \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

Stream view types: `KEYS_ONLY`, `NEW_IMAGE`, `OLD_IMAGE`, `NEW_AND_OLD_IMAGES`.

**Important: SDK users need a separate `dynamodbstreams` client.** DynamoDB and DynamoDB Streams are separate services in every AWS SDK. Both clients must point at the same extenddb endpoint URL. See `getting-started.md` for Python examples and the polling pattern.

Read stream records:

```bash
# List streams
aws dynamodbstreams list-streams --endpoint-url https://127.0.0.1:8000

# Describe stream
aws dynamodbstreams describe-stream \
    --endpoint-url https://127.0.0.1:8000 \
    --stream-arn "<stream-arn>"

# Get shard iterator
aws dynamodbstreams get-shard-iterator \
    --endpoint-url https://127.0.0.1:8000 \
    --stream-arn "<stream-arn>" \
    --shard-id "shard-0" \
    --shard-iterator-type TRIM_HORIZON

# Read records
aws dynamodbstreams get-records \
    --endpoint-url https://127.0.0.1:8000 \
    --shard-iterator "<iterator>"
```

Both DynamoDB and DynamoDB Streams endpoints use the same extenddb server URL. See `samples/stream_consumer.py` for a complete working example.

## Other Operations

### DescribeEndpoints

```bash
aws dynamodb describe-endpoints
```

### DescribeLimits

```bash
aws dynamodb describe-limits
```

### TagResource / UntagResource / ListTagsOfResource

```bash
aws dynamodb tag-resource \
    --resource-arn "arn:aws:dynamodb:us-east-1:<account-id>:table/Orders" \
    --tags Key=Environment,Value=Dev

aws dynamodb list-tags-of-resource \
    --resource-arn "arn:aws:dynamodb:us-east-1:<account-id>:table/Orders"
```

### DescribeTimeToLive / UpdateTimeToLive

```bash
aws dynamodb update-time-to-live \
    --table-name Orders \
    --time-to-live-specification Enabled=true,AttributeName=ExpiresAt
```

`DescribeTimeToLive` may report `ENABLING` or `DISABLING` while backend-native
TTL artifacts are being reconciled. TiDB records the TTL intent durably and uses
its native online DDL/TTL scheduler; ExtendDB does not run a frontend TTL item
sweeper for TiDB.

## Expression Syntax Reference

### Attribute Names

Use `#name` placeholders for reserved words:

```
--expression-attribute-names '{"#s": "Status", "#d": "Date"}'
```

### Attribute Values

Use `:value` placeholders:

```
--expression-attribute-values '{":val": {"S": "active"}}'
```

### Nested Paths

Access nested attributes with dot notation and array indexes:

```
address.city
orders[0].amount
metadata.tags[2].name
```

### Condition Functions

| Function | Example |
|----------|---------|
| `attribute_exists(path)` | `attribute_exists(Email)` |
| `attribute_not_exists(path)` | `attribute_not_exists(DeletedAt)` |
| `attribute_type(path, type)` | `attribute_type(Age, :t)` where `:t = {"S": "N"}` |
| `begins_with(path, substr)` | `begins_with(Name, :prefix)` |
| `contains(path, operand)` | `contains(Tags, :tag)` |
| `size(path)` | `size(Items) > :min` |

### Update Expression Clauses

| Clause | Example |
|--------|---------|
| `SET` | `SET #count = #count + :one, #name = :name` |
| `SET` (if_not_exists) | `SET #count = if_not_exists(#count, :zero) + :one` |
| `SET` (list_append) | `SET #items = list_append(#items, :new)` |
| `REMOVE` | `REMOVE #temp, #items[2]` |
| `ADD` | `ADD #count :one, #tags :newset` |
| `DELETE` | `DELETE #tags :oldset` |

## Error Reference

extenddb reproduces DynamoDB error responses exactly. Common errors:

| Error | HTTP Status | Cause |
|-------|-------------|-------|
| `ResourceNotFoundException` | 400 | Table does not exist or is not ACTIVE |
| `ConditionalCheckFailedException` | 400 | Condition expression evaluated to false |
| `ValidationException` | 400 | Invalid input (bad expression, missing key, etc.) |
| `ResourceInUseException` | 400 | Table already exists or is being deleted |
| `TransactionCanceledException` | 400 | Transaction failed (with per-item reasons) |
| `ProvisionedThroughputExceededException` | 400 | Throughput limit exceeded |
| `ItemCollectionSizeLimitExceededException` | 400 | LSI item collection > 10 GB |
| `AccessDeniedException` | 400 | IAM policy denied the request |
| `UnrecognizedClientException` | 403 | Invalid access key |
| `InternalServerError` | 500 | Unexpected server error |

## Supported Features Summary

| Feature | Status |
|---------|--------|
| CreateTable / DeleteTable / DescribeTable / ListTables / UpdateTable | ✓ |
| PutItem / GetItem / UpdateItem / DeleteItem | ✓ |
| Query / Scan (with filter, projection, pagination) | ✓ |
| BatchWriteItem / BatchGetItem | ✓ |
| TransactWriteItems / TransactGetItems | ✓ |
| Global Secondary Indexes (GSI) | ✓ |
| Local Secondary Indexes (LSI) | ✓ |
| DynamoDB Streams | ✓ |
| ConditionExpression / FilterExpression / UpdateExpression / ProjectionExpression | ✓ |
| KeyConditionExpression | ✓ |
| ReturnValues / ReturnConsumedCapacity | ✓ |
| TTL (DescribeTimeToLive / UpdateTimeToLive) | ✓ |
| Tagging (TagResource / UntagResource / ListTagsOfResource) | ✓ |
| SigV4 Authentication | ✓ |
| IAM Policy Evaluation | ✓ |
| Legacy API (Expected, AttributeUpdates, AttributesToGet) | ✓ |
| PartiQL | ✗ (not planned) |
| DAX | ✗ (not applicable) |
| Global Tables | ✗ (future) |
| Backups / PITR | Backups supported by backend; table-level PITR restore is backend-specific and may be unsupported |
| Import/Export (local filesystem) | ✓ (FileSource instead of S3) |

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
