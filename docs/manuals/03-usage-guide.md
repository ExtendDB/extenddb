# Usage Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Introduction

extenddb implements the DynamoDB wire protocol. Any AWS SDK or tool that speaks DynamoDB can connect to extenddb by changing the endpoint URL. This guide covers all supported operations, SDK configuration, expression syntax, error handling, and feature support.

## SDK Configuration

Point your SDK at extenddb by setting the endpoint URL. extenddb uses TLS with a self-signed certificate — set `AWS_CA_BUNDLE` to trust it. All requests must be signed with valid access keys created via the management API.

### AWS CLI

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:18443
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
```

Or per-command:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
aws dynamodb list-tables --endpoint-url https://127.0.0.1:18443
```

### Python (boto3)

```python
import os
import boto3

os.environ["AWS_CA_BUNDLE"] = os.path.expanduser("~/.extenddb/tls/cert.pem")

dynamodb = boto3.client(
    "dynamodb",
    endpoint_url="https://127.0.0.1:18443",
    region_name="us-east-1",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)
```

### Java (AWS SDK v2)

```java
DynamoDbClient client = DynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:18443"))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("your-access-key", "your-secret-key")))
    .build();
```

Note: For Java, configure the trust store to include the self-signed cert, or set `AWS_CA_BUNDLE` before launching the JVM.

### Rust (aws-sdk-dynamodb)

```rust
let config = aws_config::defaults(BehaviorVersion::latest())
    .endpoint_url("https://127.0.0.1:18443")
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
    endpoint: "https://127.0.0.1:18443",
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

Tables transition through `CREATING` → `ACTIVE` status. The transition delay is configurable via the `control_plane_delay_seconds` runtime setting (default: 5 seconds). Poll with DescribeTable until `TableStatus` is `ACTIVE` before performing operations on the table.

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

Supports: `IndexName` (for GSI/LSI queries), `ScanIndexForward`, `Limit`, `ExclusiveStartKey`, `Select`, `ProjectionExpression`, `FilterExpression`, `ConsistentRead`, and `ReturnConsumedCapacity`.

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
aws dynamodbstreams list-streams --endpoint-url https://127.0.0.1:18443

# Describe stream
aws dynamodbstreams describe-stream \
    --endpoint-url https://127.0.0.1:18443 \
    --stream-arn "<stream-arn>"

# Get shard iterator
aws dynamodbstreams get-shard-iterator \
    --endpoint-url https://127.0.0.1:18443 \
    --stream-arn "<stream-arn>" \
    --shard-id "shard-0" \
    --shard-iterator-type TRIM_HORIZON

# Read records
aws dynamodbstreams get-records \
    --endpoint-url https://127.0.0.1:18443 \
    --shard-iterator "<iterator>"
```

Both DynamoDB and DynamoDB Streams endpoints use the same extenddb server URL. See `samples/stream_consumer.py` for a complete working example.

## Vector Search

Available on the PostgreSQL backend when the pgvector extension is present on the
data database, and on the SQLite backend. A deployment without pgvector refuses
every vector operation and is otherwise unaffected. The refusal comes in two
strings, which matters if you grep logs for one of them. `CreateTable` and
`UpdateTable` carrying `VectorIndexes` are refused with:

`Vector indexes are not supported by this storage backend`

and a search is refused with:

`SearchVectors is not supported by this storage backend`

See the admin guide for the detection and the restart requirement.

A vector index is declared on a table that uses on-demand capacity, either at
CreateTable or by UpdateTable on an existing table. The index names the attribute
holding the vector, its dimension count, and one of three distance functions
(`COSINE`, `EUCLIDEAN`, `DOT_PRODUCT`). Five vector indexes per table is the limit.

Items are written normally: the vector attribute is a list of numbers with exactly
the declared number of dimensions. An item that omits it is stored and simply does
not appear in that index, the same way a GSI ignores an item missing its key.

`SearchVectors` returns the nearest items with a `Score` per hit, ordered nearest
first, and takes an optional `TopK` up to 100.

Four things are worth knowing before building on it.

**Amazon DynamoDB serves SearchVectors on a separate endpoint**,
`search-dynamodb.<region>.amazonaws.com`, and rejects it on the standard endpoint.
ExtendDB serves every operation on one endpoint, so an SDK that resolves a distinct
search hostname from its endpoint ruleset needs its endpoint overridden.

**A vector index is eventually consistent**, like a GSI: a search immediately after
a write may not see the item. Adding an index to a table that already holds items
starts a backfill, during which the index reports `CREATING`, the table stays
writable, and searches against that index are refused until it is `ACTIVE`.

**On-demand capacity is required.** A table holding a vector index cannot be
switched to provisioned mode, and a vector index cannot be added to a provisioned
table.

**Scores and ranking differ from the service in documented ways** at extreme
component magnitudes, and tie ordering is deterministic here where the service's is
measured unstable. Both are in `docs/differences-from-dynamodb.md`.

## Bulk Put Items

extenddb implements the four bulk operation APIs: `StartBulkPutItems`,
`DescribeBulkOperation`, `ListBulkOperations`, and `CancelBulkOperation`. A
bulk put reads newline-delimited DynamoDB JSON objects from the bucket and
prefix its `InputS3Configuration` names and writes every item into an
existing table, asynchronously, with `PutItem` semantics. Writes go through
the same code path as `PutItem`, so secondary indexes, vector indexes, TTL
bookkeeping, and stream records are maintained the way they are for a client
write. Rows that fail are reported to objects under the bucket and prefix its
`OutputS3Configuration` names.

These operations are not in the AWS CLI or SDK service models, so a
model-driven client cannot call them by name. Send a SigV4-signed POST whose
`X-Amz-Target` header names the operation
(`DynamoDB_20120810.StartBulkPutItems` and so on), the framing every other
operation uses. The examples below show the JSON request and response bodies.

The two configurations carry the members the service defines:

| Member | Required | Rule |
|---|---|---|
| `InputS3Configuration.S3Bucket` | yes | The input bucket: 3 to 255 characters, starting and ending with a letter or digit, holding only letters, digits, `.`, `-`, and `_`. |
| `InputS3Configuration.S3KeyPrefix` | no | 3 to 1024 characters. Every object whose key starts with it is an input; absent, every object in the bucket is. |
| `InputS3Configuration.S3BucketOwner` | no | Twelve digits: the account the input bucket must belong to. Absent, the description carries the caller's account id. |
| `InputS3Configuration.InputFormat` | yes | `DYNAMODB_JSON`. `ION` is in the model and is refused with `InputFormat ION is not supported by this server`. |
| `InputS3Configuration.InputCompressionType` | no | `NONE`, `GZIP`, or `ZSTD`. Absent is read as `NONE` and echoed as `NONE`. |
| `OutputS3Configuration.S3Bucket` | yes | The report bucket, under the input bucket's name rules. |
| `OutputS3Configuration.S3KeyPrefix` | no | At most 1024 characters, and at least 3 when present. The report keys start with it. |
| `OutputS3Configuration.S3BucketOwner` | no | Twelve digits: the account the report bucket must belong to. Absent, the description carries the caller's account id. |
| `OutputS3Configuration.OutputType` | yes | The report kinds to write: `ERROR`, `CONDITION_CHECK_FAILURE`, or both. |
| `OutputS3Configuration.S3SseAlgorithm` | no | `AES256` or `KMS`. |
| `OutputS3Configuration.S3SseKmsKeyId` | no | 1 to 2048 characters: the KMS key for `KMS`. |

The operator chooses where those buckets resolve, with `[bulk] store` in the
server configuration (see the [Admin Guide](05-admin-guide.md)): AWS S3, an
S3-compatible service at a configured endpoint, or the filesystem store, one
directory tree beneath a root on the server. On the S3 store `S3Bucket` is
the S3 bucket of that name, read and written with the server's own AWS
credentials once the caller's ExtendDB policies allow the S3 actions the
operation performs (see Permissions). On the filesystem store `S3Bucket`
names a directory under the caller's account subtree of the root. The
request and response shapes and the report keys are the same on both
stores; the sections below say where the two behave differently.

An operator must configure the `[bulk]` store before the operations accept
requests; without it `StartBulkPutItems` returns `Bulk operations are
disabled. Configure [bulk] in extenddb.toml to enable.` On the MongoDB and
Cassandra backends the four operations return `ValidationException` with the
message `Bulk operations are not supported on this storage backend`. On the
filesystem store `StartBulkPutItems` is available on unix targets only;
elsewhere it is refused with `Bulk operations are not supported on this
platform`. The S3 store has no such restriction. `DescribeBulkOperation`,
`ListBulkOperations`, and `CancelBulkOperation` answer on every target,
since they touch no input or report files.

### Input files

The input is every object in `InputS3Configuration.S3Bucket` whose key
starts with `S3KeyPrefix`, compared byte-wise; an absent prefix selects
every object in the bucket. A bucket name spelled as a dotted IPv4 literal,
four dot-separated groups of one to three digits such as `10.20.30.40`, is
refused with `Invalid Request: InputS3Configuration.S3Bucket is invalid`
even though it satisfies the name rules, as on the hosted service; the same
applies to the output bucket, reported as `OutputS3Configuration.S3Bucket`.
On the filesystem store neither prefix may contain a `..` component, `\`, or
control characters, and an output prefix must not begin with `/`; such a
prefix is refused with `S3KeyPrefix contains an invalid path component`. The
S3 store applies only the length rules and the rule every store applies to
U+0000 (`S3 configuration contains an invalid character`), and uses both
prefixes as given. With an `endpoint_url`, the S3 store also refuses a
prefix whose `..` segments outnumber the segments before them, such as
`../other/` or `a/../../other/`, with `S3KeyPrefix contains an invalid path
component`: an S3-compatible service resolves `..`, and such a prefix
would name a key above the bucket. An
input prefix may carry doubled separators and a leading `/` (`orders//2026/`,
`/orders/2026/`), as on the hosted service: it is compared with the keys as
given.

The input bucket is checked when the operation starts. A bucket that does
not exist fails the operation with `FailureCode` `S3NoSuchBucket`. A caller
whose policies do not allow `s3:ListBucket` on the bucket (see Permissions),
or an `InputS3Configuration.S3BucketOwner` the bucket does not match, fails
it with `S3AccessDenied`. The `s3:ListBucket` check comes first. Such an
operation is created already `FAILED`, before any row is read: the Start
response and every later description carry `FAILED`, an `EndTime` equal to
`StartTime`, zero item counters, and no `TotalInputBytes` or
`ProcessedInputBytes`. Its `ClientToken`
is consumed like any other, and a retry with the same token returns the same
failed operation. These failed-at-Start operations are capped at
`max_bulk_failed_starts_per_hour` per account per hour (default 100); at or
above the cap, Start answers `ThrottlingException` with `Rate of requests
exceeds the allowed throughput.` and records nothing. A prefix that matches
nothing in an existing bucket is not a failure: the operation completes with
every counter at zero.

Of the output configuration, Start checks the `S3Bucket` member's presence
and its name, and on the S3 store a `KMS` encryption key (see the error
report section). A report bucket that does not exist, or a mismatched output
`S3BucketOwner`, does not stop the operation from starting: both are checked
when the operation's first report record is produced.

The object list and `TotalInputBytes` are fixed when the operation starts:
an object added under the prefix afterwards is not read, and neither are
bytes appended to an object afterwards. Objects are processed one at a time
in the byte order of their keys, and lines in order within an object, so a
later row with the same key overwrites an earlier one.

#### On the S3 store

`S3Bucket` is the S3 bucket of that name and `S3KeyPrefix` a key prefix.
Keys are used as S3 lists them: a key holding `//`, a leading `/`, `.` or
`..` segments, or `\` is read by that key. Every object under the prefix is
an input, zero-byte objects whose keys end in `/` included; they hold no
rows. The objects are listed at Start with ListObjectsV2, 1,000 keys per
page.

The bucket check is a ListObjectsV2 call for at most one key under the
prefix, so S3 decides whether the bucket exists, whether it has the expected
owner, and whether the server may list it. S3's code and message reach the
description: a missing bucket is `S3NoSuchBucket` with `The specified bucket
does not exist` followed by S3's request ids (see Failure messages).

An `S3BucketOwner`, on either configuration, that is absent or equal to the
caller's ExtendDB account id sets no expectation. Any other value is sent as
the `x-amz-expected-bucket-owner` header on every request for that bucket,
and S3 refuses a request to a bucket another AWS account owns with
`S3AccessDenied`, `Access Denied`. ExtendDB assigns its account ids itself,
and they need not match any AWS account, so name the bucket's AWS account
for S3 to check it.

An object is read with ranged GETs of 8 MiB, up to the size the listing
gave: bytes appended after Start are not read, and an object that shrank
ends early. An object listed at 0 bytes is read as one byte, `bytes=0-0`,
and gives no rows whatever it holds by then. Every GET carries `If-Match`
with the ETag the listing gave at Start, on the first range and on a read
resumed after a restart, so a read takes bytes only from the object
listed. An object overwritten after Start is recorded as the service
records it: the object counts one processed and failed item, its error
record names the object with no `itemIndex` and carries `The S3 object could
not be imported because it was overwritten`, the run goes on with the next
object, and the operation ends `FAILED` with `ValidationException`, `Some of
the items failed validation checks and were not ingested`. The service
records this for an object overwritten after its listing and before its read
(measured), counting none of the object's bytes as `ProcessedInputBytes` and
marking it `PARTIALLY_PROCESSED` in the manifest with no range and the ETag
listed at Start. ExtendDB records an object overwritten during its read, or
between a stop and the resumed read, the same way, with the rows it had
delivered kept and their range in the manifest. `ProcessedInputBytes` keeps
the bytes of those rows for a replacement found during the read, and the
figure the last checkpoint counted for the object for one found at the
resumed read; the service's answer for those two cases is not measured. A
service other than AWS S3 may list an object without an
ETag. Such an object's first range is read without `If-Match` and its
later ranges carry the first answer's ETag, so a replacement during one
read is detected. A first answer that gives a size other than the listed
one is recorded as overwritten too, so a replacement of another size is
detected at the open and at a resumed read, and one of the same size
between a stop and the resumed read is not. A service that ignores `Range`
and answers with the whole object gives the size by the answer's length.
The manifest line of an object listed without an ETag carries the ETag of
the first answer the operation accepted for it, an answer at the listed
size. It carries an empty ETag when the operation accepted no answer for
the object: the object was replaced at another size before its read, its
open failed, or the operation ended before it. The store is not asked for
the ETag of the object now at the key, which can be a replacement. After a
restart, an object abandoned before the restart, or one whose reopen
accepted no answer, carries an empty ETag.

S3 decrypts an object under SSE-KMS with the server's `kms:Decrypt`
permission. A key S3 cannot use fails the operation with S3's code:
`KmsDisabledException` for a disabled key, `S3AccessDenied` when the
server's credentials may not use the key.

On AWS S3 an input or report bucket in a region other than the store's is
reached in the bucket's region: S3's redirect names the region, and the
store sends the request there and remembers the region for the bucket.

#### On the filesystem store

`S3Bucket` names a directory under the caller's account subtree of the bulk
store root, and `S3KeyPrefix` selects files inside it by their paths
relative to the bucket directory:

```
{store root}/{account id}/{S3Bucket}/{S3KeyPrefix}...
```

The server creates `{store root}/{account id}` at startup for every account,
when an account is created, and at Start when it is missing, so the account
directory is always there to stage buckets into. The bucket directory itself
is the caller's to create. A directory whose name breaks the bucket name
rules cannot be referenced.

Every regular file whose path relative to the bucket directory starts with
the prefix is an input file. Symbolic links are not followed and not
included. Relative paths hold no doubled or leading `/`, so an input prefix
carrying one matches no file. An input bucket directory that does not exist,
or is not a directory, is `S3NoSuchBucket` with `The specified bucket does
not exist`. An `InputS3Configuration.S3BucketOwner` that is not the caller's
account id is `S3AccessDenied` with `Access Denied`, decided before
existence: on the filesystem store the caller's account is the only owner a
bucket directory can have.

#### Input format

Each input object, a file on the filesystem store, holds one JSON object per
line. `Item` is required and is a DynamoDB JSON item. `ConditionExpression`,
`ExpressionAttributeNames`, and `ExpressionAttributeValues` are optional and
carry the semantics they have on `PutItem`. Any other top-level key abandons
the rest of the object with `Unexpected top-level field. Expected one of
[Item, ConditionExpression, ExpressionAttributeNames,
ExpressionAttributeValues]. Remainder of the file will not be processed.`,
as on the hosted service; the check runs before the missing-`Item` check,
so a misspelled `Item` answers this message. Blank lines are skipped.

```json
{"Item": {"pk": {"S": "a"}, "sk": {"S": "1"}, "n": {"N": "1"}}}
{"Item": {"pk": {"S": "b"}, "sk": {"S": "1"}}, "ConditionExpression": "attribute_not_exists(pk)"}
{"Item": {"pk": {"S": "c"}, "sk": {"S": "1"}, "v": {"N": "2"}}, "ConditionExpression": "#v < :max", "ExpressionAttributeNames": {"#v": "v"}, "ExpressionAttributeValues": {":max": {"N": "10"}}}
```

With `InputCompressionType: GZIP` each object is a gzip stream and with
`ZSTD` each object is a zstd stream, decompressed while reading; an object
holding several concatenated gzip members or zstd frames is read in full.
An object that does not begin with the named codec's magic bytes counts one
failed row with `The S3 object could not be decompressed` and the
operation ends `FAILED`; a zstd frame that stops decoding part way counts
the rows of its complete blocks, then one failed row with the same
message, and the operation continues. A zstd frame that declares a
decompression window larger than 128 MiB is not read: the object counts one
failed row with the same message. `InputFormat` must be
`DYNAMODB_JSON`: `ION` returns `InputFormat ION is not supported by this
server`.

### StartBulkPutItems

Request body (`X-Amz-Target: DynamoDB_20120810.StartBulkPutItems`):

```json
{
  "TableName": "orders",
  "ClientToken": "load-2026-09-10-a",
  "InputS3Configuration": {
    "S3Bucket": "loads",
    "S3KeyPrefix": "orders/2026-09-10/",
    "InputFormat": "DYNAMODB_JSON",
    "InputCompressionType": "GZIP"
  },
  "OutputS3Configuration": {
    "S3Bucket": "loads",
    "S3KeyPrefix": "reports/",
    "OutputType": ["ERROR", "CONDITION_CHECK_FAILURE"]
  },
  "MaxWriteRequestUnits": 2000,
  "PerformanceTier": "STANDARD"
}
```

`TableName` accepts a bare name or a table ARN in the caller's account and
region. A table ARN naming another account returns `AccessDeniedException`
with `Access is denied`; one naming another region returns
`ValidationException` with `1 validation error detected: Invalid AWS region
in '{arn}'`. Both are answered before the table is looked up. The table
must exist and be `ACTIVE` or `UPDATING`. A table in `DELETING` answers
`ValidationException` with `Invalid Request: Table not found`, as on the
hosted service; a table in `CREATING` answers `ResourceInUseException`.
`OutputS3Configuration` is required, and so are its `OutputType`, which
lists the report kinds to write (`ERROR`, `CONDITION_CHECK_FAILURE`), and
its `S3Bucket`. An output configuration carrying `OutputType` with no
`S3Bucket` is refused at Start with `Invalid Request:
OutputS3Configuration.S3Bucket is invalid`. `S3KeyPrefix` stays optional;
without it the report tree is written at the bucket root. `MaxWriteRequestUnits`
is at least 1,024 and is a ceiling on the operation's write rate: when present, the runner paces
the operation's writes at that many write request units per second (1 KB of
item, rounded up, per unit). The server's own
`bulk_put_write_units_per_second` ceiling applies on top of it.
`PerformanceTier` is validated, stored, and returned; it does not change how
fast the operation runs. The description carries `TableDetails`, a one-entry
list of the target table's bare name and its id, on the Start response and
on every Describe. Two members are echoed with resolved values rather than
as submitted: an absent `S3BucketOwner` is echoed as the caller's account id
on both configurations, and an absent `InputS3Configuration.InputCompressionType`
is echoed as `NONE`. A retry that supplies one of those resolved values, or a
`PerformanceTier` of `STANDARD`, is the same request as one that omits it and
replays the operation. A retry that adds an `S3SseAlgorithm` of `AES256` is a
different request and returns `IdempotentParameterMismatchException`. `ACCELERATED` is refused with a `ValidationException`
on a table that has a vector index, as on the hosted service; `STANDARD` is
accepted on every table.

Response:

```json
{
  "BulkOperationDescription": {
    "BulkOperationArn": "arn:aws:dynamodb:us-east-1:111122223333:bulk-operation/01789060589767-da9ddd4f",
    "BulkOperationStatus": "IN_PROGRESS",
    "TableDetails": [{ "TableName": "orders", "TableId": "..." }],
    "ClientToken": "load-2026-09-10-a",
    "InputS3Configuration": { "...": "as submitted, resolved values filled in" },
    "OutputS3Configuration": { "...": "as submitted, resolved values filled in" },
    "StartTime": 1789060589.767,
    "ProcessedItemCount": 0,
    "FailedItemCount": 0,
    "ProcessedInputBytes": 0,
    "TotalInputBytes": 4096,
    "MaxWriteRequestUnits": 2000,
    "PerformanceTier": "STANDARD"
  }
}
```

The status is `IN_PROGRESS`, even for empty input; poll
`DescribeBulkOperation` for the terminal status. The one exception is an
operation created already `FAILED` by a check at Start (see Input files and
Permissions), whose Start response carries `FAILED`. `TotalInputBytes` is
the sum of the enumerated object sizes (compressed sizes for `GZIP` and
`ZSTD`) and is present in this first response.

### Permissions

Besides `dynamodb:StartBulkOperation` and `dynamodb:BulkPutItems` (see the
[Security Model](10-security-model.md)), an operation performs three S3
actions as its caller, and ExtendDB evaluates each against the caller's
ExtendDB policies, on both stores:

| Action | Resource | Evaluated | Denied |
|---|---|---|---|
| `s3:ListBucket` | `arn:aws:s3:::{input bucket}`, with `s3:prefix` the request's `InputS3Configuration.S3KeyPrefix`, or the empty string when it has none, and, on the S3 store with an `endpoint_url`, again with the prefix as the service resolves it when that differs | At Start, before the input bucket check | The operation is created already `FAILED` with `S3AccessDenied`: no byte members, no manifest |
| `s3:GetObject` | `arn:aws:s3:::{input bucket}/{key}` | Before each input object is opened, and at every checkpoint boundary (`bulk_put_batch_size` rows) while it is read | `FAILED` with `S3AccessDenied`. Rows already written stay written. The manifest marks the object `NOT_PROCESSED` when none of its rows was counted and `NOT_AVAILABLE` when some were |
| `s3:PutObject` | `arn:aws:s3:::{output bucket}/{key}` | At the first record of each report kind, on report object 0 of that kind; when the reports are written, on each report object; and on the manifest's key | At a kind's first record, the fault is held to the end of the input (see Error report). When the reports are written, `FAILED` with `S3AccessDenied`: that report kind is not installed, and the manifest is still written. On the manifest's key, `FAILED` with `S3AccessDenied` and the message prefixed `Manifest is not available. ` |

A denial names the caller, the action and the resource, in S3's spelling:

```
User: arn:aws:iam::111122223333:user/loader is not authorized to perform: s3:GetObject on resource: "arn:aws:s3:::loads/orders/2026-09-10/part-000.json" because no identity-based policy allows the s3:GetObject action
```

A matching Deny statement gives `... with an explicit deny in an
identity-based policy` in place of the `because` clause, and a role session
is named by its `arn:aws:sts::{account}:assumed-role/{role}/{session}` ARN.
A denial that a session policy or a permissions boundary decides has the
same identity-based wording. The message carries no request-id tail, since
no request reached S3.

On the filesystem store, and on the S3 store with an `endpoint_url`, a key
is checked as composed from the request and, when it differs, as the store
writes it: the filesystem store drops empty and `.` segments, and an
S3-compatible service can also resolve `..`. Both must be allowed. The
denial names the key that was denied, and the composed key is checked
first. A Deny on `arn:aws:s3:::{output bucket}/protected/*` therefore also
refuses the output prefixes `./protected` and `.//protected` on the
filesystem store. On the S3 store with an `endpoint_url` the input prefix
is checked for `s3:ListBucket` the same way. The filesystem store compares
the input prefix as given with the paths it finds, so `./orders/` lists
nothing there and is checked once. On AWS S3 the key as composed is the
key S3 stores, and it is checked alone.

Start records the caller on the operation, and the runner evaluates the
`s3:GetObject` and `s3:PutObject` checks for the recorded caller with the
policies in force when each check runs, so a permission removed during a
load takes effect at the next check. In developer mode no S3 action is
evaluated. On the S3 store each check that passes is followed by the
server's request to S3, which S3 decides for the server's credentials; a
denial S3 returns carries S3's text, which names the principal the server
signs as, and S3's request ids.

### DescribeBulkOperation

Request `{"BulkOperationArn": "arn:aws:dynamodb:...:bulk-operation/..."}`.
The response carries the same `BulkOperationDescription` with the current
counters. Once the operation is terminal it also carries `EndTime` and
`BulkOperationUsage`, and for `FAILED`, `FailureCode` and `FailureMessage`.
An ARN naming another account returns `AccessDeniedException` with `User:
{caller arn} is not authorized to perform: dynamodb:DescribeBulkOperation
on resource: {arn}`, decided before any policy or stored operation is read.
An ARN that names no operation in the caller's account, including an ARN in
another region, returns `ResourceNotFoundException` with `Bulk operation
with arn: {arn} does not exist`. Neither answer depends on whether the
foreign operation exists.

Terminal records are kept for `bulk_operation_retention_days` (default 30)
and then purged; a purged operation answers as not found. Reports and
manifests are not deleted.

### ListBulkOperations

Request `{"MaxResults": 25, "NextToken": "..."}`. `MaxResults` is 1 to 25;
absent means 10. Only the caller's account is listed, newest first. The
response holds one summary per operation and a `NextToken` when more exist:

```json
{
  "BulkOperationsSummaryList": [
    {"BulkOperationArn": "...", "BulkOperationStatus": "COMPLETED",
     "StartTime": 1789060589.767, "EndTime": 1789060650.634}
  ],
  "NextToken": "..."
}
```

### CancelBulkOperation

Request `{"BulkOperationArn": "..."}`. Response:

```json
{"BulkOperationArn": "...", "BulkOperationStatus": "CANCELLING"}
```

The operation moves to `CANCELLING` and ends `CANCELLED`. The runner reads
the cancel at each checkpoint, and while the read of an input object waits
on S3 it checkpoints every third of the lease (20 seconds by default), so a
cancel also takes effect during such a wait and the S3 request the read
waited on is dropped. Items already
written stay written; there is no rollback. The reports are written
from the records accumulated up to the cancel, so a cancelled load still
has its error report. Cancelling an operation that is
already terminal returns `ValidationException` with `Invalid Request:
Operation already in terminal state: {STATUS}`. Cancelling an operation
already `CANCELLING` returns `CANCELLING` again.

### Status machine

```
IN_PROGRESS --> COMPLETED               (no failed rows)
IN_PROGRESS --> COMPLETED_WITH_ERRORS   (one or more failed rows)
IN_PROGRESS --> FAILED                  (the operation could not continue)
IN_PROGRESS --> CANCELLING --> CANCELLED
```

`COMPLETED`, `COMPLETED_WITH_ERRORS`, `FAILED`, and `CANCELLED` are terminal.
Failed rows move the terminal status to `COMPLETED_WITH_ERRORS`; condition
failures do not, so an operation whose only problems are false conditions
ends `COMPLETED`. `FAILED` means the operation could not continue: the target
table was deleted or left the `ACTIVE`/`UPDATING` states, the consumed line
count passed `max_bulk_item_count`, the input or output location could not
be used (`S3NoSuchBucket`, `S3AccessDenied`, `S3NoSuchKey`, and the other
S3 and KMS codes below), or an internal error stopped it; `FailureCode` and
`FailureMessage` say which. A failure decided at Start creates the operation
already `FAILED` (see Input files). A cancel ends the operation
`CANCELLED`, including a cancel that lands after every row was processed.

### Failure messages

On the filesystem store the location failures carry fixed messages: `The
specified bucket does not exist`, `Access Denied`, and `The specified key
does not exist.` for an input file removed after Start.

On the S3 store a failure S3 answered carries S3's message, followed by the
tail the hosted service renders for the call that failed. The input bucket
check and listing, the input reads, the Start-time encryption check, and
the manifest carry this form:

```
The specified bucket does not exist (Service: Amazon S3; Status Code: 404; Error Code: NoSuchBucket; Request ID: {request id}; S3 Extended Request ID: {extended request id}; Proxy: null)
```

Report writes, the check at the first report record included, carry this
one:

```
Access Denied (Service: S3, Status Code: 403, Request ID: {request id}, Extended Request ID: {extended request id}) (SDK Attempt Count: 1)
```

A service that sends no extended request id gives `null` in its place. The
`FailureCode` is `S3` followed by S3's error code (`S3NoSuchBucket`,
`S3AccessDenied`, `S3NoSuchKey`, `S3InvalidObjectState`), or `Kms` followed
by the exception name for a `KMS.` code (`KmsDisabledException`,
`KmsInvalidKeyUsageException`). A call that meets a 5xx or 429 answer
(`SlowDown` among them), `RequestTimeout`, a timeout, or a transport failure
is sent again for up to 120 seconds, and a call that gets no answer at all
takes about three minutes (see the Admin Guide, S3 store). A running
operation ends with `InternalServerError` when the failure lasts past that.
An S3 error code ExtendDB does not map, an S3 code about the server's own
credentials, account, or clock (`InvalidAccessKeyId`,
`SignatureDoesNotMatch`, `ExpiredToken`, `RequestTimeTooSkewed`, and the
others the Admin Guide lists), or credentials the server cannot resolve
end a running operation with `InternalServerError` at once. At Start each
of these answers the request itself with `InternalServerError`, and no
operation is created. S3's code and message are in the server log.

### Counters

| Field | Meaning |
|---|---|
| `ProcessedItemCount` | Rows read and attempted, including failed rows and condition failures. Rows after a parse-level abandon are not included. |
| `FailedItemCount` | Rows that failed a parse-level or item-level check. Condition failures are not counted. |
| `ProcessedInputBytes` | Input bytes consumed, advanced to the end of each object as it finishes or is abandoned. Compressed bytes for `GZIP` and `ZSTD`. A `CANCELLED` or `FAILED` operation reports what was read before it stopped, except one that failed on its output location, which reports 0 (see Error report). |
| `TotalInputBytes` | Sum of the enumerated input object sizes, fixed at start. |
| `BulkOperationUsage` | Present once terminal: one entry for the table. The entry's `TableName` is the bare table name. `Table.BulkPutBytes` is the sum, over rows that were written, of the item's logical size in bytes with a 1,024 byte floor and no rounding above it; a row that replaced an item is charged the larger of the replaced item's size and its own. A row whose condition evaluated false is charged the 1,024 byte floor alone; rows that failed a parse-level or item-level check are not charged. Each secondary index entry is the same sum over the bytes its projection stores, and an index no row reached has no entry. A row that replaced an item and left an index's projection unchanged under the same index key charges that index nothing; one that changed the projection under the same key charges the larger of the old and new projections; one that moved the index key charges the old and the new projection. The top-level `BulkPutBytes` is the table figure plus every secondary index figure. Vector index entries carry `BulkPutVectorBytes` and are not included in the top-level figure. |

### Error report

Reports are written once the operation ends, under these keys in the output
bucket:

```
{S3KeyPrefix}/AWSDynamoDB/BulkOperation/{account}:{operation id}-0/output/errors/output-00000.json.gz
{S3KeyPrefix}/AWSDynamoDB/BulkOperation/{account}:{operation id}-0/output/condition-check-failures/output-00000.json.gz
```

The keys are the same on both stores. One `/` separates the prefix from
`AWSDynamoDB/` unless the prefix is empty or already ends with `/`; with no
prefix the keys start at `AWSDynamoDB/`. On the S3 store they are object
keys in the output bucket, written as composed, doubled or `.` segments of
the prefix included. On the filesystem store they are paths beneath
`{store root}/{account}/{S3Bucket}/`: the report bucket directory is the
caller's to create, the tree beneath it is created as needed, and empty and
`.` components of the prefix are dropped.

The output configuration is checked as report records are produced. At the
operation's first report record the bucket and its owner are checked against
report object 0 of that record's kind, and the caller's `s3:PutObject` is
checked at the first record of each kind, on report object 0 of that kind. A
report bucket that does not exist fails the operation with `FailureCode`
`S3NoSuchBucket`. An output `S3BucketOwner` the bucket does not match, or a
caller whose policies do not allow `s3:PutObject` on the report object (see
Permissions), fails it with `S3AccessDenied`. On the S3 store the check
starts a multipart upload on that key, with the expected owner and the
encryption settings, and aborts it at once, so S3 reports a missing bucket,
an owner mismatch, or a write the server's credentials may not make with
S3's code and message, and nothing is written.
On the filesystem store a missing directory is `The specified bucket does
not exist` and an owner other than the caller's account is `Access Denied`.

The fault is held: the operation reads on to the end of its input and
writes the rows that pass their checks, then ends `FAILED` with the fault,
as the hosted service does. `ProcessedItemCount` is then the processed rows
less the failed ones, `FailedItemCount` 0, `ProcessedInputBytes` 0, and
`BulkOperationUsage` zero, and neither a report nor a manifest is written.
The held fault is saved with each checkpoint, so an operation that another
process resumes after a restart ends as it would have without the restart.
A cancel, or another failure met before the end of the input, ends the
operation instead, with the counters as they stand. An operation that
produces no report record is checked when the manifest is written instead,
and fails with the same codes and the message prefixed
`Manifest is not available. `. A manifest write that the store refuses after
that check passed, because the bucket was removed or its policy changed in
between, fails with the same prefix on its message.

On the S3 store an output configuration with `S3SseAlgorithm` `KMS` is also
checked at Start: the same write check runs on error report object 0 with
the key. A key S3 cannot use creates the operation already `FAILED` with
S3's KMS code, `KmsDisabledException` for a disabled key and
`KmsInvalidKeyUsageException` for a key of the wrong usage, and no byte
members. A bucket or owner fault that check meets is left to the first
report record. So is an S3 `AccessDenied` for the key, which S3 returns
when the server's credentials may not use it, with a message that names
the `kms:` action. The operation then fails with `S3AccessDenied` at its
first report record, held to the end of the input as other faults found
there are, or at the manifest when it produces no report record.

On the S3 store every report object and the manifest are written with the
requested encryption: `AES256` sends `x-amz-server-side-encryption:
AES256`, and `KMS` sends `aws:kms` with `S3SseKmsKeyId` when one is given.
On the filesystem store both settings are recorded and returned and have no
effect on the files.

An object is written only when it has at least one record. Each object is
gzip-compressed newline-delimited JSON, one record per row. Once the
compressed bytes of an object reach 200 MiB the writer closes it and the next
records go to `output-00001.json.gz`, then `output-00002.json.gz`, and so on
under the same prefix, as the hosted service does; the object that passed the
threshold keeps the records that took it there, so it closes a little above
200 MiB. Read every `output-NNNNN.json.gz` under the prefix to get the whole
report. On the S3 store an object whose compressed bytes reach 8 MiB is sent
as a multipart upload in 8 MiB parts and completed when the reports are
written; a smaller one is sent as a single upload at that point. No report
object is visible before then.

```json
{"itemLocation": {"inputSourceArn": "arn:aws:s3:::loads/orders/2026-09-10/part-000.json", "itemIndex": 2},
 "errorMessages": ["One or more parameter values were invalid: Missing the key sk in the item"],
 "newImage": {"Item": {"pk": {"S": "a"}, "n": {"N": "3"}}}}
```

`itemLocation.inputSourceArn` names the input object as an S3 object ARN,
`arn:aws:s3:::{bucket}/{key}`, on both stores (the key is the
bucket-relative path on the filesystem store), so tooling written for the
DynamoDB report format parses it unchanged. `itemLocation.itemIndex` is the
zero-based line index within the object; it is absent on the record for a
parse-level abandon.
`errorMessages` holds one message. `newImage` is the submitted item, wrapped
in `{"Item": ...}`; it is present on missing-key records whose submitted
item is within the item size limit, and on condition-failure records.
`oldImage` appears only on condition-failure records and holds the stored
item the condition was evaluated against.

A record goes to its own kind's prefix when that kind was requested in
`OutputType`, and otherwise to the one kind that was requested. With only
`ERROR` requested, condition-failure records go to `errors/`; with only
`CONDITION_CHECK_FAILURE` requested, parse-level and item-level error
records go to `condition-check-failures/`; with both requested, each kind
has its own prefix.

The records kept per operation are capped by `max_bulk_report_records` and
their serialized size by `max_bulk_report_bytes`; records beyond either cap
are dropped and the drop is logged. The counters are unaffected.

After the reports, every operation that read input writes a manifest to
`{S3KeyPrefix}/AWSDynamoDB/BulkOperation/{account}:{operation id}/manifest-files.json`,
with the reports' encryption: one line per input object, carrying its
`inputSourceArn`, its `fileProcessingStatus` (`FULLY_PROCESSED`,
`PARTIALLY_PROCESSED`, `NOT_PROCESSED`, or `NOT_AVAILABLE`), its
`processedRanges`, and its `etag`. On the S3 store the `etag` is S3's ETag,
without quotes: of the version read for an object read to its end, and the
one the listing gave otherwise. On the filesystem store it is the MD5 of the
enumerated bytes. A file that is refused or deleted before its open cannot
be hashed, and its line has an empty `etag`.

### Row errors

Three classes of row problem behave differently.

A parse-level error abandons the rest of the file: a line that is not
well-formed JSON, a missing or non-object `Item`, an attribute value whose
type tag is not one of the ten DynamoDB types, a line longer than
`max_bulk_line_bytes`, or a condition field of the wrong JSON type. The
offending row is counted as processed and failed, the remaining rows of
that file are not read and appear in no counter, and processing continues
with the next file. `ProcessedInputBytes` advances to the end of the
abandoned file. The report record carries `Expected 'Item' top level
container. Remainder of the file will not be processed.` for a missing
`Item`, `Expected character {. Remainder of the file will not be
processed.` for an `Item` that is not a JSON object, and `Expected
DynamoDB type to be one of [S, N, B, BOOL, NULL, L, M, SS, NS, BS].
Remainder of the file will not be processed.` for every other cause.

An item-level error fails the row alone: a missing or wrongly typed key
attribute, an item over the size limit, a condition expression that does not
parse, expression maps supplied without a `ConditionExpression`, or any
other item check `PutItem` applies. The row is counted as processed and
failed, a record carrying the message `PutItem` returns for the same fault
goes to the error report, and the next row is read.

A `ConditionExpression` that evaluates false leaves the item unwritten. The
row is counted as processed and is not counted as failed. A record with the
message `The conditional request failed`, the submitted `newImage`, and the
stored `oldImage` is written to the condition report.

### One operation per table

At most one operation per table is non-terminal at a time. A second
`StartBulkPutItems` on that table returns `ResourceInUseException` with
`Table has ongoing bulk operations, please wait and try again`, and nothing
is created, whatever its input: a Start whose input bucket does not exist
or whose prefix matches no object gets the same answer, as on the hosted
service. Starting
again after the first operation is terminal succeeds. A Start on a table
that is being deleted answers `ValidationException` `Invalid Request: Table
not found`, and so does a Start whose table a `DeleteTable` removed while
the Start was being handled. A Start and an
`UpdateTable` that creates an index on the same table are never both
accepted: each side checks the other inside the transaction that acts, so
when the two arrive together one of them answers `ResourceInUseException`
with its usual message. An account may have at
most `max_concurrent_bulk_operations` non-terminal operations (default 50);
above that, `StartBulkPutItems` returns `LimitExceededException` with
`Subscriber limit exceededService quota exceeded You have reached the
maximum number of concurrent bulk operations for the selected
PerformanceTier, which is {limit}. Wait until other jobs finish, and try
again. You can also reach out to AWS Support to request a quota
increase.`, the configured limit substituted. The text matches the
DynamoDB message, missing separators included.

### Idempotency

`ClientToken` makes a start retryable. The token is any non-empty string
without `$`, at most `max_bulk_client_token_bytes` bytes. A repeated
request with the same token and the same parameters returns the existing
operation's current
description with HTTP 200 and creates nothing. The same token with different
parameters returns `IdempotentParameterMismatchException` with `Specified
idempotent token was already used with different request parameters within
the idempotency window`. A token replays its operation for eight hours from
the operation's start. After that, the same request starts a new operation.
The token is looked up before any call to the store, so a replay or a
mismatch gets the same answer while the store is unreachable, after the
input prefix has grown past `max_bulk_input_bytes`, and while the account is
at `max_bulk_failed_starts_per_hour`.

### Configuration

Inputs are read and reports are written through the one store `[bulk] store`
selects, independent of the `[import]` and `[export]` sections that serve
`ImportTable` and `ExportTableToPointInTime`: `s3` for AWS S3 or an
S3-compatible service (`[bulk.s3]`), or `filesystem` for one directory tree
beneath `[bulk.filesystem] path`. The [Admin Guide](05-admin-guide.md)
describes both. An `http` `[bulk.s3] endpoint_url` must name a loopback host
(`127.x.y.z`, `::1` or `localhost`); any other `http` endpoint refuses
startup unless `[bulk.s3] allow_insecure_endpoint = true`. In dev mode with
no `[bulk]` section the filesystem store is enabled at `./.extenddb/buckets`.
The
`[limits]` keys governing bulk operations
(`max_concurrent_bulk_operations`, `max_bulk_input_bytes`,
`max_bulk_input_files`, `max_bulk_input_object_bytes`, `max_bulk_client_token_bytes`,
`max_bulk_line_bytes`, `max_bulk_item_count`, `max_bulk_report_records`,
`max_bulk_report_bytes`, `bulk_put_batch_size`, `bulk_put_write_batch_rows`,
`bulk_operation_lease_seconds`, `bulk_operation_worker_concurrency`,
`bulk_operation_worker_per_account`, `bulk_put_write_units_per_second`,
`bulk_operation_retention_days`) are
listed in the [Admin Guide](05-admin-guide.md) and in
`extenddb.sample.toml`. These limits are configuration of this server, not
properties of the DynamoDB API; the values in force are whatever the
deployment sets.

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
| Vector indexes and SearchVectors | ✓ (PostgreSQL requires the pgvector extension) |
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
| Bulk operations (StartBulkPutItems, DescribeBulkOperation, ListBulkOperations, CancelBulkOperation) | ✓ (on an S3 or filesystem store; not on the MongoDB or Cassandra backends) |
| PartiQL | ✗ (not planned) |
| DAX | ✗ (not applicable) |
| Global Tables | ✗ (future) |
| On-demand backups (CreateBackup / RestoreTableFromBackup) | ✓ |
| Point-in-time recovery | ✗ (`UpdateContinuousBackups` refuses to enable it; see differences-from-dynamodb.md) |
| Import/Export (local filesystem) | ✓ (FileSource instead of S3) |

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
