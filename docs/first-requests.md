---
description: "Tables, items, batches, transactions and streams from the AWS CLI against a running ExtendDB server, whichever way you started it."
---

# First requests

Every command on this page is an ordinary AWS CLI command. The one thing that changes is where it points. Where the server came from decides the endpoint, the certificate and the credentials:

| Server started with | Endpoint | Certificate | Credentials |
|---|---|---|---|
| The [development image](dev-image.md) | `http://127.0.0.1:18080` | none (plain HTTP) | the seeded example key pair, below |
| [Docker Compose](local-dev-docker-compose.md) | `https://127.0.0.1:18443` | `/tmp/extenddb-cert.pem`, extracted on that page | the access key created on that page |
| A [build from source](install-from-source.md) | `https://127.0.0.1:18443` | `~/.extenddb/tls/cert.pem` | the access key created in the smoke test |

The development image seeds the example key pair from the AWS documentation:

```
AKIAIOSFODNN7EXAMPLE
wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Set the endpoint once with `AWS_ENDPOINT_URL_DYNAMODB` (the AWS CLI version 2 and the current SDKs read it), or pass `--endpoint-url <endpoint>` on each command. The commands below assume the variable is set. For a TLS server, `AWS_CA_BUNDLE` points at the certificate:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:18443
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
```

The same settings as an AWS config profile, in `~/.aws/config`:

```ini
[profile extenddb]
region = us-east-1
ca_bundle = ~/.extenddb/tls/cert.pem
services = extenddb-services

[services extenddb-services]
dynamodb =
  endpoint_url = https://127.0.0.1:18443
```

and in `~/.aws/credentials`:

```ini
[extenddb]
aws_access_key_id = <access-key-id>
aws_secret_access_key = <secret-access-key>
```

Then `export AWS_PROFILE=extenddb`. The [usage guide](manuals/03-usage-guide.md#sdk-configuration) has the same configuration for the Java, Rust, JavaScript and Python SDKs.

## Tables

```bash
aws dynamodb create-table \
    --table-name MyTable \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

The table is created in the `CREATING` state and becomes `ACTIVE` shortly after, as on the service. Wait for it before writing:

```bash
aws dynamodb wait table-exists --table-name MyTable
```

```bash
aws dynamodb list-tables
aws dynamodb describe-table --table-name MyTable
```

## Items

```bash
aws dynamodb put-item \
    --table-name MyTable \
    --item '{"pk": {"S": "user-1"}, "name": {"S": "Alice"}, "age": {"N": "30"}}'
```

```bash
aws dynamodb get-item \
    --table-name MyTable \
    --key '{"pk": {"S": "user-1"}}'
```

```bash
aws dynamodb update-item \
    --table-name MyTable \
    --key '{"pk": {"S": "user-1"}}' \
    --update-expression "SET age = :newage" \
    --expression-attribute-values '{":newage": {"N": "31"}}' \
    --return-values ALL_NEW
```

```bash
aws dynamodb delete-item \
    --table-name MyTable \
    --key '{"pk": {"S": "user-1"}}' \
    --return-values ALL_OLD
```

`user-1` is gone after this; the sections below work on the items the batch write creates.

## Batches

```bash
aws dynamodb batch-write-item \
    --request-items '{
        "MyTable": [
            {
              "PutRequest": {
                "Item": {
                  "pk": {"S": "user-2"},
                  "name": {"S": "Bob"}
                }
              }
            },
            {
              "PutRequest": {
                "Item": {
                  "pk": {"S": "user-3"},
                  "name": {"S": "Carol"}
                }
              }
            }
        ]
    }'
```

```bash
aws dynamodb batch-get-item \
    --request-items '{
        "MyTable": {
            "Keys": [
                {"pk": {"S": "user-2"}},
                {"pk": {"S": "user-3"}}
            ]
        }
    }'
```

## Query and scan

```bash
aws dynamodb query \
    --table-name MyTable \
    --key-condition-expression "pk = :pk" \
    --expression-attribute-values '{":pk": {"S": "user-2"}}'
```

```bash
aws dynamodb scan --table-name MyTable
```

## Transactions

```bash
aws dynamodb transact-write-items \
    --transact-items '[
        {
          "Put": {
            "TableName": "MyTable",
            "Item": {
              "pk": {"S": "tx-1"},
              "data": {"S": "hello"}
            }
          }
        },
        {
          "ConditionCheck": {
            "TableName": "MyTable",
            "Key": {"pk": {"S": "user-2"}},
            "ConditionExpression":
              "attribute_exists(pk)"
          }
        }
    ]'
```

```bash
aws dynamodb transact-get-items \
    --transact-items '[
        {
          "Get": {
            "TableName": "MyTable",
            "Key": {"pk": {"S": "tx-1"}}
          }
        },
        {
          "Get": {
            "TableName": "MyTable",
            "Key": {"pk": {"S": "user-2"}}
          }
        }
    ]'
```

## DynamoDB Streams

Enable a stream when creating a table:

```bash
aws dynamodb create-table \
    --table-name StreamTable \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

**SDK users need a separate `dynamodbstreams` client.** In every AWS SDK, DynamoDB and DynamoDB Streams are separate services. Both clients point at the same ExtendDB endpoint URL. The Python examples on this page use boto3 (`python3 -m pip install boto3`):

```python
import os
import boto3

# Trust the self-signed certificate
os.environ["AWS_CA_BUNDLE"] = os.path.expanduser("~/.extenddb/tls/cert.pem")

# DynamoDB client, for table and item operations
dynamodb = boto3.client("dynamodb", endpoint_url="https://127.0.0.1:18443")

# DynamoDB Streams client, for stream operations
streams = boto3.client("dynamodbstreams", endpoint_url="https://127.0.0.1:18443")
```

On the CLI, `AWS_ENDPOINT_URL_DYNAMODB` does not apply to the `dynamodbstreams` service, so pass the endpoint explicitly (or set `AWS_ENDPOINT_URL_DYNAMODB_STREAMS`):

```bash
aws dynamodbstreams list-streams \
    --endpoint-url https://127.0.0.1:18443
```

Describe a stream (use the `LatestStreamArn` from `DescribeTable`; the label after `stream/` is the creation time with milliseconds):

```bash
aws dynamodbstreams describe-stream \
    --endpoint-url https://127.0.0.1:18443 \
    --stream-arn \
      "arn:aws:dynamodb:us-east-1:<account-id>:table/StreamTable/stream/2026-04-08T07:00:00.000"
```

Get a shard iterator and read records:

```bash
# Get an iterator for a shard (use a ShardId from DescribeStream)
aws dynamodbstreams get-shard-iterator \
    --endpoint-url https://127.0.0.1:18443 \
    --stream-arn \
      "arn:aws:dynamodb:us-east-1:<account-id>:table/StreamTable/stream/2026-04-08T07:00:00.000" \
    --shard-id "<shard-id>" \
    --shard-iterator-type TRIM_HORIZON

# Read records with the iterator
aws dynamodbstreams get-records \
    --endpoint-url https://127.0.0.1:18443 \
    --shard-iterator "<iterator-from-above>"
```

### Polling pattern

The usual way to consume a stream is a polling loop:

```python
import time
import boto3

streams = boto3.client("dynamodbstreams", endpoint_url="https://127.0.0.1:18443")
dynamodb = boto3.client("dynamodb", endpoint_url="https://127.0.0.1:18443")

# Get the stream ARN from the table.
table = dynamodb.describe_table(TableName="StreamTable")
stream_arn = table["Table"]["LatestStreamArn"]

# Discover shards.
desc = streams.describe_stream(StreamArn=stream_arn)
shards = desc["StreamDescription"]["Shards"]

# Get an iterator for each shard.
iterators = {}
for shard in shards:
    resp = streams.get_shard_iterator(
        StreamArn=stream_arn,
        ShardId=shard["ShardId"],
        ShardIteratorType="TRIM_HORIZON",
    )
    iterators[shard["ShardId"]] = resp["ShardIterator"]

# Poll loop.
while True:
    for shard_id, iterator in list(iterators.items()):
        if not iterator:
            continue
        resp = streams.get_records(ShardIterator=iterator, Limit=100)
        for record in resp.get("Records", []):
            print(f"{record['eventName']}: {record['dynamodb']['Keys']}")
        iterators[shard_id] = resp.get("NextShardIterator")
    time.sleep(1)
```

[`samples/stream_consumer.py`](https://github.com/ExtendDB/extenddb/blob/main/samples/stream_consumer.py) is a complete example with concurrent writer and poller threads. Stream records are retained for 24 hours; a background worker removes expired records every hour. The [usage guide](manuals/03-usage-guide.md#dynamodb-streams) covers the shard iterator types and the stream view types.

## Health check

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health
```

```json
{"status":"healthy"}
```

For the development image the endpoint is `http://127.0.0.1:18080/health` with no certificate.

## Delete the tables

```bash
aws dynamodb delete-table --table-name MyTable
aws dynamodb delete-table --table-name StreamTable
```

## Next

- [Using ExtendDB](manuals/03-usage-guide.md): the operations, expressions, transactions and streams, with examples in each SDK.
- [Administration](manuals/05-admin-guide.md): IAM users, groups, roles and policies, the web console, runtime settings and monitoring.
- [Sample application](https://github.com/ExtendDB/extenddb/blob/main/samples/sample_app.py): a Python program that creates `SampleUsers`, `SampleOrders` and `SampleTournamentMatches`, loads data, queries, updates, runs transactions and removes the tables. It reads the endpoint from `EXTENDDB_ENDPOINT` and the key pair from the AWS variables; against a TLS server it also needs the certificate:

  ```bash
  export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
  export EXTENDDB_ENDPOINT=https://127.0.0.1:18443    # http://127.0.0.1:18080 for the development image, no certificate
  python3 samples/sample_app.py
  ```
- [Event ticketing demo](manuals/13-event-ticketing-demo.md): a worked application with single-table design.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is maintained by
engineers at Amazon Web Services. It is not Amazon DynamoDB and contains no DynamoDB source
code. "DynamoDB" is a trademark of Amazon.com, Inc.
