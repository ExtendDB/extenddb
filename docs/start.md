---
title: Getting started
description: Start an ExtendDB server in one command, send your first DynamoDB requests to it, and connect the SDK you already use.
slug: /start
---

# Getting started

ExtendDB speaks the DynamoDB wire protocol. The AWS CLI and every AWS SDK talk to it the same way they talk to the DynamoDB service, with one change: the endpoint URL points at your server.

This page gets a server running in one command and sends the first requests to it. The install pages cover the other ways to run ExtendDB: [Docker Compose with PostgreSQL](local-dev-docker-compose.md), and a [build from source](install-from-source.md) on Linux or macOS.

## 1. Start a server

The development image is one container over SQLite, plain HTTP, with a seeded example credential. It needs no database, no initialization step and no certificate to trust. Data persists in the named volume.

```bash
docker run -d --name extenddb -p 127.0.0.1:18080:18080 \
  -v extenddb:/var/lib/extenddb extenddb/extenddb-dev
```

Check it answers:

```bash
curl -s http://127.0.0.1:18080/health
```

```json
{"components":{"backup_store":{"kind":"filesystem","status":"healthy"}},"status":"healthy"}
```

The image has no TLS and no access control, so it is bound to the loopback interface only. It is for development and CI; the [development image](dev-image.md) page explains what it does and does not protect. For a server with TLS and PostgreSQL, use [Docker Compose](local-dev-docker-compose.md).

## 2. Point the AWS CLI at it

The image seeds the example key pair from the AWS documentation. Export it, the region (both variables, so a region already set in your shell does not win), and the endpoint. The AWS CLI version 2 and the current SDKs read `AWS_ENDPOINT_URL_DYNAMODB`, so no `--endpoint-url` flag is needed on each call.

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-east-1
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL_DYNAMODB=http://127.0.0.1:18080
```

```bash
aws dynamodb list-tables
```

```json
{
    "TableNames": []
}
```

## 3. Create a table and write to it

Tables are created in the `CREATING` state and become `ACTIVE` after the control-plane delay (0.25 s by default; the service takes several seconds), so wait for the table before writing.

```bash
aws dynamodb create-table \
  --table-name Music \
  --attribute-definitions AttributeName=Artist,AttributeType=S AttributeName=SongTitle,AttributeType=S \
  --key-schema AttributeName=Artist,KeyType=HASH AttributeName=SongTitle,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

aws dynamodb wait table-exists --table-name Music
```

```bash
aws dynamodb put-item --table-name Music \
  --item '{"Artist": {"S": "No One You Know"}, "SongTitle": {"S": "Call Me Today"}, "Year": {"N": "2015"}}'

aws dynamodb put-item --table-name Music \
  --item '{"Artist": {"S": "No One You Know"}, "SongTitle": {"S": "Howdy"}, "Year": {"N": "2017"}}'
```

Read one item back, then query the partition:

```bash
aws dynamodb get-item --table-name Music \
  --key '{"Artist": {"S": "No One You Know"}, "SongTitle": {"S": "Call Me Today"}}'
```

```bash
aws dynamodb query --table-name Music \
  --key-condition-expression 'Artist = :a' \
  --expression-attribute-values '{":a": {"S": "No One You Know"}}' \
  --query 'Items[].SongTitle.S'
```

```json
[
    "Call Me Today",
    "Howdy"
]
```

## 4. Connect an SDK

Every SDK takes an endpoint URL and a credential. With the four environment variables from step 2 set, boto3 needs no arguments at all; the explicit form is:

```python
import boto3

dynamodb = boto3.client(
    "dynamodb",
    endpoint_url="http://127.0.0.1:18080",
    region_name="us-east-1",
    aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
    aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
)

print(dynamodb.list_tables()["TableNames"])
```

The [usage guide](manuals/03-usage-guide.md#sdk-configuration) has the same configuration for Java, Rust and JavaScript, and for a server with TLS, where the client also needs the server's certificate.

## 5. Stop and clean up

```bash
docker rm -f extenddb
docker volume rm extenddb
```

## Next

- [First requests](first-requests.md): tables, items, batches, transactions and streams from the AWS CLI against the server you have.
- [Docker Compose with PostgreSQL](local-dev-docker-compose.md) and TLS, or a [build from source](install-from-source.md).
- [Using ExtendDB](manuals/03-usage-guide.md): the operations, expressions, transactions and streams, with examples.
- [Differences from DynamoDB](differences-from-dynamodb.md): every behavior that is measured to differ from the service.
- [Deployment](manuals/11-deployment-guide.md): running ExtendDB on your own servers.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
