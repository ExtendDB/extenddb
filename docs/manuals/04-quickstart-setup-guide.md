# Quick Start & Setup Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Quick Start (5 Minutes)

This section gets you from zero to a working extenddb instance as fast as possible. Detailed explanations follow in later chapters.

### Prerequisites

- TiDB 8.5.4+ running locally or reachable over the network
- Rust toolchain 1.85+
- AWS CLI v2

### Steps

```bash
# 1. Clone and build
git clone <repo-url> extenddb && cd extenddb
cargo build -j12 --release

# 2. Initialize (creates databases, admin user, TLS cert, config file)
./target/release/extenddb init

# ⚠ Save the admin credentials and account ID printed here — shown once only!

# 3. Verify
./target/release/extenddb verify --config extenddb.toml

# 4. Start the server
./target/release/extenddb serve --config extenddb.toml

# 5. Create an IAM user and access key
./target/release/extenddb manage --user admin --password <admin-pw> \
    create-user --account-id <account-id> \
    --user-name quickstart --user-password secret

./target/release/extenddb manage --user admin --password <admin-pw> \
    put-user-policy --account-id <account-id> --user-name quickstart \
    --policy-name FullAccess \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}'

./target/release/extenddb manage --user <account-id>/quickstart --password secret \
    create-access-key

# 6. Configure AWS CLI (use access key from step 5)
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:8000
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
export AWS_DEFAULT_REGION=us-east-1

# 7. Create a table
aws dynamodb create-table \
    --table-name QuickTest \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

# 8. Insert an item
aws dynamodb put-item \
    --table-name QuickTest \
    --item '{"pk": {"S": "hello"}, "message": {"S": "extenddb is working!"}}'

# 9. Read it back
aws dynamodb get-item \
    --table-name QuickTest \
    --key '{"pk": {"S": "hello"}}'
```

If step 8 returns your item, extenddb is working correctly.

## Detailed Setup

### TiDB Installation

The default extenddb build uses TiDB. For local development, run a TiDB
playground cluster or point extenddb at an existing TiDB 8.5.4+ deployment.
TiDB listens on port 4000 by default.

One local development option is TiUP playground:

```bash
tiup playground v8.5.4 --db 1 --pd 1 --kv 3 --without-monitor
```

Verify TiDB is reachable:

```bash
mysql -h 127.0.0.1 -P 4000 -uroot -e "SELECT VERSION();"
```

### TiDB User Setup

`extenddb init` connects as the TiDB admin user and creates the `extenddb`
application SQL user automatically. For local TiUP playground, the default
admin user is `root` with no password, which matches extenddb's TiDB bootstrap
defaults.

### Building extenddb

```bash
# Debug build (faster compilation, slower runtime)
cargo build

# Release build (slower compilation, optimized runtime)
cargo build -j12 --release
```

The binary is at `target/release/extenddb` (or `target/debug/extenddb`).

Check the version:

```bash
./target/release/extenddb version
# extenddb 0.1.0
# catalog 0.0.26 (tidb)
# commit abc1234
# built 2026-04-17T12:00:00Z
```

### Initialization

`extenddb init` creates the catalog and data databases, runs schema migrations, generates an encryption key, creates a default account, and creates an admin user.

```bash
./target/release/extenddb init
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog-db` | `extenddb_catalog` | Catalog database name |
| `--data-db` | `extenddb` | Data database name |
| `--storage-host` | `localhost` | TiDB SQL host |
| `--storage-port` | `4000` | TiDB SQL port |
| `--storage-admin-user` | `root` | TiDB admin user |
| `--storage-admin-password` | unset | TiDB admin password |

The command generates `extenddb.toml` with the connection details. If `extenddb.toml` already exists, `init` loads defaults from it.

**Save the admin credentials** printed during init. They are shown once and cannot be retrieved later.

### Verification

```bash
./target/release/extenddb verify --config extenddb.toml
```

Expected output:

```
=== extenddb verify ===
--- Checking catalog connection...
  OK: Connected to catalog.
--- Checking catalog version...
  OK: Catalog version 0.0.26
--- Checking data database...
  OK: Connected to data database 'extenddb_catalog_data'.
--- Enumerating tables...
  Tables: 0
  Indexes: 0

=== HEALTHY: All checks passed ===
```

### Starting the Server

```bash
./target/release/extenddb serve --config extenddb.toml
```

extenddb runs as a daemon (background process). It prints a startup banner, then forks to background. All logging goes to syslog.

Check status:

```bash
./target/release/extenddb status --config extenddb.toml
```

Read logs:

**Linux:**

```bash
journalctl -t extenddb -f
```

**macOS:**

```bash
# Live stream
log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info

# Historical
log show --predicate 'processImagePath ENDSWITH "extenddb"' --last 1h
```

### Stopping the Server

```bash
./target/release/extenddb stop --config extenddb.toml
```

Alternatively, find the PID and send SIGTERM manually:

```bash
./target/release/extenddb status --config extenddb.toml
kill <pid>
```

### AWS CLI Configuration

extenddb uses TLS with a self-signed certificate. Set `AWS_CA_BUNDLE` to trust it:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
```

Three options for endpoint configuration, from simplest to most structured:

**Option A: Environment variables**

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:8000
export AWS_ACCESS_KEY_ID=<access-key-from-create-access-key>
export AWS_SECRET_ACCESS_KEY=<secret-key-from-create-access-key>
export AWS_DEFAULT_REGION=us-east-1
```

**Option B: AWS config profile**

`~/.aws/config`:

```ini
[profile extenddb]
region = us-east-1
ca_bundle = ~/.extenddb/tls/cert.pem
services = extenddb-services

[services extenddb-services]
dynamodb =
  endpoint_url = https://127.0.0.1:8000
```

`~/.aws/credentials`:

```ini
[extenddb]
aws_access_key_id = <access-key-from-create-access-key>
aws_secret_access_key = <secret-key-from-create-access-key>
```

Then: `export AWS_PROFILE=extenddb`

**Option C: Per-command `--endpoint-url`**

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
aws dynamodb list-tables --endpoint-url https://127.0.0.1:8000
```

### Setting Up Credentials

extenddb ships with builtin IAM authentication enabled. After `extenddb init`, create an IAM user and access key:

```bash
# Create an account (--account-id is optional; auto-generated if omitted)
./target/release/extenddb manage --user admin --password <admin-pw> \
    create-account --account-name dev-team
# Account ID: <printed-id>   ← save this

# Create an IAM user with console password
./target/release/extenddb manage --user admin --password <admin-pw> \
    create-user --account-id <printed-id> \
    --user-name alice --user-password secret

# Grant DynamoDB full access
./target/release/extenddb manage --user admin --password <admin-pw> \
    put-user-policy --account-id <printed-id> \
    --user-name alice \
    --policy-name FullAccess \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": "dynamodb:*",
        "Resource": "*"
      }]
    }'

# Create an access key (self-service — inferred from --user)
./target/release/extenddb manage --user <printed-id>/alice --password secret \
    create-access-key
# Save the access key ID and secret key!
```

Configure your SDK with the access key and secret key:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
export AWS_DEFAULT_REGION=us-east-1
```

5. Create a table with streams enabled:

```bash
aws dynamodb create-table \
    --table-name StreamTest \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
    --endpoint-url https://127.0.0.1:8000
```

6. Wait for the table to become ACTIVE, then insert an item:

```bash
aws dynamodb put-item \
    --table-name StreamTest \
    --item '{"pk": {"S": "key1"}, "data": {"S": "hello streams"}}' \
    --endpoint-url https://127.0.0.1:8000
```

7. Read the stream:

```bash
# List streams
aws dynamodbstreams list-streams \
    --table-name StreamTest \
    --endpoint-url https://127.0.0.1:8000

# Describe the stream (use the StreamArn from above)
aws dynamodbstreams describe-stream \
    --stream-arn <stream-arn> \
    --endpoint-url https://127.0.0.1:8000

# Get a shard iterator (use the ShardId from above)
aws dynamodbstreams get-shard-iterator \
    --stream-arn <stream-arn> \
    --shard-id <shard-id> \
    --shard-iterator-type TRIM_HORIZON \
    --endpoint-url https://127.0.0.1:8000

# Get records (use the ShardIterator from above)
aws dynamodbstreams get-records \
    --shard-iterator <shard-iterator> \
    --endpoint-url https://127.0.0.1:8000
```

### Management Web Console

Navigate to `https://127.0.0.1:8000/console/` in your browser. Log in with admin credentials or IAM user credentials (`account_id/user_name`).

The console provides a GUI for managing accounts, users, groups, roles, policies, and access keys.

### Runtime Settings

Adjust behavior without restarting:

```bash
# Change log level
./target/release/extenddb settings --config extenddb.toml set log_level debug
```

Changes take effect within 30 seconds.

### Tear Down

```bash
./target/release/extenddb destroy --config extenddb.toml
```

This drops both databases after confirmation.

## Troubleshooting Quick Reference

| Symptom | Fix |
|---------|-----|
| `connection refused` on port 8000 | Server not running. Start with `extenddb serve --config extenddb.toml` |
| `CATALOG_VERSION_MISMATCH` | Run `extenddb migrate --config extenddb.toml` |
| `ResourceNotFoundException` on CreateTable | Table is still in CREATING status. Poll DescribeTable until `TableStatus` is `ACTIVE` |
| `UnrecognizedClientException` | Invalid access key. Check credentials — verify the access key ID and secret match what was returned by `create-access-key` |
| `AccessDeniedException` | IAM policy does not allow the operation. Attach a policy with the required permissions |

See `docs/troubleshooting.md` for the full troubleshooting guide.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
