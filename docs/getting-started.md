# Getting Started with extenddb

> See [NOTICE](NOTICE.md) for important disclaimers.

This guide walks you through initializing a extenddb deployment, starting the server, and running your first DynamoDB commands against it.

### Platform-specific installation guides

- [macOS (Homebrew)](manuals/09-install-macos.md) — covers Homebrew PostgreSQL, macOS syslog, and `--pg-user $(whoami)`
- [Linux (Ubuntu/Debian, Amazon Linux, Fedora/RHEL)](manuals/08-install-linux.md) — covers system PostgreSQL, `journalctl`, and `--pg-user postgres`

### Installer scripts

The fastest way to build extenddb from source is the platform installer script.
It checks dependencies, builds the binary, sets up a Python venv, and
generates PDF documentation:

```bash
# Linux
scripts/install-linux.sh

# macOS
scripts/install-macos.sh
```

The scripts report missing dependencies and exit — they never install
software on your behalf. After the script completes, continue from
[Step 2: Initialize the deployment](#2-initialize-the-deployment) below.

## Prerequisites

- PostgreSQL 14+ running locally (see `docs/local-postgres-setup.md`)
- Rust toolchain (1.85+)
- AWS CLI v2 (for testing)
- Python 3.10+ with virtual environment (see [Python Environment Setup](../README.md#python-environment-setup) in the README)

## 1. Build extenddb

```bash
cargo build --release
```

The binary is at `target/release/extenddb`.

## 2. Initialize the deployment

Run `extenddb init` to create the catalog and data databases:

```bash
./target/release/extenddb init
```

This will:
- Create a `extenddb` PostgreSQL user (if it doesn't exist)
- Create the `extenddb_catalog` database (catalog metadata)
- Create the `extenddb` database (user item data)
- Run schema migrations
- Generate an AES-256-GCM encryption key (for future access key storage)
- Create a default account and print the account ID
- Create an `admin` user and print the credentials once
- Generate a self-signed TLS certificate at `~/.extenddb/tls/`
- Generate `extenddb.toml`

**Important:** Save the admin credentials printed during init. They are shown once and cannot be retrieved later. These credentials are used to authenticate to the management API.

**Re-initialization:** `extenddb init` will abort if either the catalog or data database already exists. To re-initialize, first run `extenddb destroy --config extenddb.toml --yes` to drop the existing databases, then run `extenddb init` again.

To use a custom catalog database name:

```bash
./target/release/extenddb init --catalog-db my_catalog
```

To use a custom data database name:

```bash
./target/release/extenddb init --data-db my_data_db
```

### Remote PostgreSQL / Aurora

For remote PostgreSQL or Aurora, supply the admin password with `--pg-pass`:

```bash
# Pass the password inline:
./target/release/extenddb init \
  --pg-host my-aurora-cluster.cluster-xxxx.us-east-1.rds.amazonaws.com \
  --pg-user postgres --pg-pass <admin-password>
```

When `--pg-pass` is omitted entirely, `extenddb init` connects without a password, relying on
PostgreSQL peer/ident authentication (works only on localhost via Unix socket).

### Custom bind address

To bind the server to a specific address (e.g., for remote access), pass `--bind-addr` during init. The address is included as a SAN in the self-signed certificate and written to the generated config file:

```bash
./target/release/extenddb init --bind-addr 10.0.1.5
```

This generates a certificate with SANs: `localhost`, `127.0.0.1`, and `10.0.1.5`.

### Generating a self-signed certificate manually

`extenddb init` auto-generates a self-signed TLS certificate at `~/.extenddb/tls/`. If you need a certificate with different SANs (e.g., binding to `0.0.0.0` and connecting via a specific hostname), generate one manually with `openssl`:

```bash
# Create the TLS directory
mkdir -p ~/.extenddb/tls

# Generate a certificate with custom SANs
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout ~/.extenddb/tls/key.pem \
  -out ~/.extenddb/tls/cert.pem \
  -days 3650 \
  -subj "/CN=extenddb self-signed/O=extenddb" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:myhost.example.com,IP:10.0.1.5"

# Restrict key file permissions
chmod 600 ~/.extenddb/tls/key.pem
```

Adjust the `-addext` SANs to match the hostnames and IP addresses clients will use to connect. Every address that appears in `endpoint_url` must be listed as a SAN, or TLS verification will fail.

If the certificate already exists when `extenddb init` runs, it is preserved — init only generates a certificate when `~/.extenddb/tls/cert.pem` and `~/.extenddb/tls/key.pem` are both absent. To regenerate, delete the existing files first:

```bash
rm ~/.extenddb/tls/cert.pem ~/.extenddb/tls/key.pem
./target/release/extenddb init --bind-addr 10.0.1.5
```

## 3. Verify the deployment

```bash
./target/release/extenddb verify --config extenddb.toml
```

You should see all checks pass:

```
=== extenddb verify ===
--- Checking catalog connection...
  OK: Connected to catalog.
--- Checking catalog version...
  OK: Catalog version 0.0.2
--- Checking data database...
  OK: Connected to data database 'extenddb_catalog_data'.
--- Enumerating tables...
  Tables: 0
  Indexes: 0

=== HEALTHY: All checks passed ===
```

## 4. Start the server

extenddb runs as a daemon (background process) and logs to syslog. On startup it prints a banner to stdout confirming the version, catalog version, and bind address, then forks to background.

```bash
./target/release/extenddb serve --config extenddb.toml
# extenddb 0.0.2 (catalog 0.0.2) listening on 127.0.0.1:8000
```

Check status (includes the daemon PID):

```bash
./target/release/extenddb status --config extenddb.toml
# extenddb is running on port 8000 (pid 12345)
```

Read logs:

```bash
# Linux
journalctl -t extenddb -f           # follow live
journalctl -t extenddb -n 50        # last 50 lines
journalctl -t extenddb --no-pager -o cat  # plain output, no metadata

# macOS
log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info
log show --predicate 'processImagePath ENDSWITH "extenddb"' --last 5m
```

Change the log level at runtime (takes effect within 30 seconds):

```bash
./target/release/extenddb settings --config extenddb.toml set log_level debug
```

### sqlx Log Separation

sqlx query traces are suppressed by default (level `warn`) so they don't flood the main log stream. The sqlx log level is independently configurable via the `sqlx_log_level` runtime setting:

```bash
# Enable sqlx debug logging for query troubleshooting
./target/release/extenddb settings --config extenddb.toml set sqlx_log_level debug

# Restore default (suppress most sqlx output)
./target/release/extenddb settings --config extenddb.toml set sqlx_log_level warn
```

When sqlx logging is enabled, messages appear in the `extenddb` syslog with `sqlx::query` as the target. Filter them:

```bash
# Exclude sqlx messages
journalctl -t extenddb | grep -v sqlx

# Show only sqlx messages
journalctl -t extenddb | grep sqlx
```

### Control Plane Delay

By default, control plane operations (CreateTable, DeleteTable) emulate real DynamoDB's async behavior — tables transition through CREATING → ACTIVE and DELETING → removed states over a configurable delay (default: 5 seconds). Adjust with:

```bash
# Set to 0 for instant transitions (useful for fast test cycles)
./target/release/extenddb settings --config extenddb.toml set \
    control_plane_delay_seconds 0

# Set to 10 seconds for more realistic behavior
./target/release/extenddb settings --config extenddb.toml set \
    control_plane_delay_seconds 10
```

### Credential Import

Controls whether `extenddb manage import-access-key` is allowed (default: `true`).

```bash
# Disable credential import
./target/release/extenddb settings --config extenddb.toml set \
    allow_credential_import false

# Re-enable
./target/release/extenddb settings --config extenddb.toml set \
    allow_credential_import true
```

### GSI Propagation Delay

GSI updates are applied asynchronously with a configurable delay, simulating real DynamoDB's eventually consistent GSI behavior. The system-wide default is 10ms. Each GSI can override this with a per-index `propagation_delay_ms` stored in the catalog.

```bash
# Set system-wide default to 0 for synchronous GSI updates (fast tests)
./target/release/extenddb settings --config extenddb.toml set \
    gsi_propagation_delay_ms 0

# Set to 50ms for more realistic eventual consistency
./target/release/extenddb settings --config extenddb.toml set \
    gsi_propagation_delay_ms 50
```

### Throttling

extenddb enforces provisioned throughput limits using a token bucket per table and partition, matching DynamoDB's burst and refill behavior. Throttling is enabled by default.

```bash
# Disable throttling (useful for bulk loading)
./target/release/extenddb settings --config extenddb.toml set \
    throttling_enabled false

# Re-enable throttling
./target/release/extenddb settings --config extenddb.toml set \
    throttling_enabled true
```

### TTL Deletion Target

Controls the target maximum time (in seconds) between an item's TTL expiry and its actual deletion. The TTL sweeper uses an indexed scan and runs every 60 seconds. Default: 300 seconds.

```bash
# Set to 60 seconds for faster TTL cleanup
./target/release/extenddb settings --config extenddb.toml set \
    ttl_deletion_target_seconds 60

# Set to 600 seconds for less aggressive cleanup
./target/release/extenddb settings --config extenddb.toml set \
    ttl_deletion_target_seconds 600
```

Stop the server:

```bash
./target/release/extenddb stop --config extenddb.toml
```

If `extenddb stop` is unavailable (e.g., older binary), fall back to manual process management:

1. Find the process listening on the extenddb port (default 8000):
   ```bash
   ss -tlnp | grep :8000
   ```
2. Kill it:
   ```bash
   kill <pid>
   ```

## 5. Configure AWS CLI

extenddb uses TLS with a self-signed certificate. To make AWS CLI and SDKs trust it, set `AWS_CA_BUNDLE` to the generated certificate:

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
```

### Option A: Environment variables (simplest)

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:8000
export AWS_ACCESS_KEY_ID=<access-key-from-create-access-key>
export AWS_SECRET_ACCESS_KEY=<secret-key-from-create-access-key>
export AWS_DEFAULT_REGION=us-east-1
```

### Option B: AWS config profile

Add to `~/.aws/config`:

```ini
[profile extenddb]
region = us-east-1
ca_bundle = ~/.extenddb/tls/cert.pem
services = extenddb-services

[services extenddb-services]
dynamodb =
  endpoint_url = https://127.0.0.1:8000
```

Add to `~/.aws/credentials`:

```ini
[extenddb]
aws_access_key_id = <access-key-from-create-access-key>
aws_secret_access_key = <secret-key-from-create-access-key>
```

Then: `export AWS_PROFILE=extenddb`

### Option C: Explicit `--endpoint-url` per command

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
aws dynamodb list-tables --endpoint-url https://127.0.0.1:8000
```

### Post-init workflow

After `extenddb init`, create an IAM user and access key for SDK use:

```bash
# Create an account (use the account ID printed during init, or create a new one)
./target/release/extenddb manage --user admin --password <admin-pw> \
    create-account --account-name dev-team

# Create an IAM user with a console password
./target/release/extenddb manage --user admin --password <admin-pw> \
    create-user --account-id <account-id> \
    --user-name alice --user-password secret

# Attach a policy granting DynamoDB access
./target/release/extenddb manage --user admin --password <admin-pw> \
    put-user-policy --account-id <account-id> --user-name alice \
    --policy-name FullAccess \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": "dynamodb:*",
        "Resource": "*"
      }]
    }'

# Create an access key (shown once — save it)
./target/release/extenddb manage --user <account-id>/alice --password secret \
    create-access-key
```

Then configure your SDK with the access key ID and secret access key returned by `create-access-key`.

## 6. Try it out

### Create a table

```bash
aws dynamodb create-table \
    --table-name MyTable \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

### List tables

```bash
aws dynamodb list-tables
```

### Describe a table

```bash
aws dynamodb describe-table --table-name MyTable
```

### Delete a table

```bash
aws dynamodb delete-table --table-name MyTable
```

### Put an item

```bash
aws dynamodb put-item \
    --table-name MyTable \
    --item '{"pk": {"S": "user-1"}, "name": {"S": "Alice"}, "age": {"N": "30"}}'
```

### Get an item

```bash
aws dynamodb get-item \
    --table-name MyTable \
    --key '{"pk": {"S": "user-1"}}'
```

### Delete an item

```bash
aws dynamodb delete-item \
    --table-name MyTable \
    --key '{"pk": {"S": "user-1"}}' \
    --return-values ALL_OLD
```

### Update an item

```bash
aws dynamodb update-item \
    --table-name MyTable \
    --key '{"pk": {"S": "user-1"}}' \
    --update-expression "SET age = :newage" \
    --expression-attribute-values '{":newage": {"N": "31"}}' \
    --return-values ALL_NEW
```

### Query items

```bash
aws dynamodb query \
    --table-name MyTable \
    --key-condition-expression "pk = :pk" \
    --expression-attribute-values '{":pk": {"S": "user-1"}}'
```

### Scan a table

```bash
aws dynamodb scan --table-name MyTable
```

### Batch write items

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

### Batch get items

```bash
aws dynamodb batch-get-item \
    --request-items '{
        "MyTable": {
            "Keys": [
                {"pk": {"S": "user-1"}},
                {"pk": {"S": "user-2"}}
            ]
        }
    }'
```

### Transactional write

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
            "Key": {"pk": {"S": "user-1"}},
            "ConditionExpression":
              "attribute_exists(pk)"
          }
        }
    ]'
```

### Transactional get

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
            "Key": {"pk": {"S": "user-1"}}
          }
        }
    ]'
```

### DynamoDB Streams

extenddb supports DynamoDB Streams for change data capture. Enable streams when creating a table:

```bash
aws dynamodb create-table \
    --table-name StreamTable \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

**Important: SDK users need a separate `dynamodbstreams` client.** In every AWS SDK, DynamoDB and DynamoDB Streams are separate services. Both clients must point at the same extenddb endpoint URL:

```python
import boto3

# Trust the self-signed certificate
import os
os.environ["AWS_CA_BUNDLE"] = os.path.expanduser("~/.extenddb/tls/cert.pem")

# DynamoDB client — for table/item operations
dynamodb = boto3.client("dynamodb", endpoint_url="https://127.0.0.1:8000")

# DynamoDB Streams client — for stream operations
streams = boto3.client("dynamodbstreams", endpoint_url="https://127.0.0.1:8000")
```

List streams:

```bash
aws dynamodbstreams list-streams \
    --endpoint-url https://127.0.0.1:8000
```

Describe a stream (use the `LatestStreamArn` from `DescribeTable`):

```bash
aws dynamodbstreams describe-stream \
    --endpoint-url https://127.0.0.1:8000 \
    --stream-arn \
      "arn:aws:dynamodb:us-east-1:<account-id>:table/StreamTable/stream/2026-04-08T07:00:00"
```

Get a shard iterator and read records:

```bash
# Get iterator for a shard (use ShardId from DescribeStream)
aws dynamodbstreams get-shard-iterator \
    --endpoint-url https://127.0.0.1:8000 \
    --stream-arn \
      "arn:aws:dynamodb:us-east-1:<account-id>:table/StreamTable/stream/2026-04-08T07:00:00" \
    --shard-id "shard-0" \
    --shard-iterator-type TRIM_HORIZON

# Read records using the iterator
aws dynamodbstreams get-records \
    --endpoint-url https://127.0.0.1:8000 \
    --shard-iterator "<iterator-from-above>"
```

#### Streams polling pattern

The standard pattern for consuming a DynamoDB stream is a polling loop:

```python
import time
import boto3

streams = boto3.client("dynamodbstreams", endpoint_url="https://127.0.0.1:8000")
dynamodb = boto3.client("dynamodb", endpoint_url="https://127.0.0.1:8000")

# Get stream ARN from the table.
table = dynamodb.describe_table(TableName="StreamTable")
stream_arn = table["Table"]["LatestStreamArn"]

# Discover shards.
desc = streams.describe_stream(StreamArn=stream_arn)
shards = desc["StreamDescription"]["Shards"]

# Get iterators for each shard.
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

See `samples/stream_consumer.py` for a complete working example with concurrent writer and poller threads.

Stream records are retained for 24 hours. A background worker cleans up expired records every hour.

### Health check

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:8000/health
# {"status":"healthy"}
```

## 7. Manage admin users and accounts

The management API is available at `/management/*` on the running extenddb server. The `extenddb manage` CLI subcommand is a thin client that calls these endpoints.

Admin commands require admin credentials (the username and password printed during `extenddb init`). IAM user self-service commands accept `account_id/user_name` as the `--user` value.

**Note:** The `--password` flag is visible in process listings (`ps aux`). For sensitive environments, use the `EXTENDDB_ADMIN_PASSWORD` environment variable instead.

### List admin users

```bash
./target/release/extenddb manage --user admin --password <pw> list-admins
```

### Create another admin user

```bash
./target/release/extenddb manage --user admin --password <pw> \
    create-admin --admin-name ops --admin-password secret123
```

### Change an admin password

```bash
./target/release/extenddb manage --user admin --password <pw> \
    change-admin-password --admin-name admin --new-password newpw
```

### Delete an admin user

```bash
./target/release/extenddb manage --user admin --password <pw> \
    delete-admin --admin-name ops
```

### Create an account

Account IDs must be 12-digit numeric strings (matching AWS account ID format):

```bash
./target/release/extenddb manage --user admin --password <pw> \
    create-account --account-id 123456789012 --account-name dev-team
```

### List accounts

```bash
./target/release/extenddb manage --user admin --password <pw> list-accounts
```

### Delete an account

Accounts with existing tables cannot be deleted. Delete all tables first.

```bash
./target/release/extenddb manage --user admin --password <pw> \
    delete-account --account-id 123456789012
```

### Create an IAM user

Create an IAM user with optional console password. If `--user-password` is provided, the user can authenticate to the management API for self-service operations.

```bash
./target/release/extenddb manage --user admin --password <pw> \
    create-user --account-id 123456789012 \
    --user-name alice --user-password secret
```

A default self-service policy is automatically attached, allowing the user to manage their own access keys and change their own password.

### List IAM users

```bash
./target/release/extenddb manage --user admin --password <pw> \
    list-users --account-id 123456789012
```

### Delete an IAM user

Deleting a user also removes their access keys, group memberships, tags, and policies (via CASCADE).

```bash
./target/release/extenddb manage --user admin --password <pw> \
    delete-user --account-id 123456789012 --user-name alice
```

### Create an access key (self-service)

IAM users create their own access keys by authenticating with `account_id/user_name:password`. When authenticating as an IAM user, `--account-id` and `--user-name` are inferred automatically. The secret key is shown once and cannot be retrieved later.

Generated access keys are branded with extenddb-specific prefixes to distinguish them from real AWS credentials:
- Long-lived keys: `AKIAEXTENDDB` + 8 random chars (20 total)
- Temporary credentials (AssumeRole): `ASIAEXTENDDB` + 8 random chars (20 total)
- Secret keys: `extenddb` + 32 random chars (40 total)

```bash
# Self-service: account_id and user_name inferred from --user
./target/release/extenddb manage --user 123456789012/alice --password secret \
    create-access-key
```

Admins can also create access keys on behalf of any user:

```bash
./target/release/extenddb manage --user admin --password <pw> \
    create-access-key --account-id 123456789012 --user-name alice
```

### List access keys

```bash
./target/release/extenddb manage --user 123456789012/alice --password secret \
    list-access-keys --account-id 123456789012 --user-name alice
```

### Delete an access key

```bash
./target/release/extenddb manage --user 123456789012/alice --password secret \
    delete-access-key --account-id 123456789012 \
    --user-name alice --access-key-id AKIAEXTENDDB...
```

### Import an existing access key

Import real AWS credentials (or any AKIA* key) into extenddb. This enables the "just change the endpoint URL" workflow — use the same credentials against both extenddb and real DynamoDB.

Both `--secret-access-key` and `--yes` are required:

```bash
./target/release/extenddb manage --user admin --password <pw> \
    import-access-key --account-id 123456789012 --user-name alice \
    --access-key-id AKIAIOSFODNN7EXAMPLE \
    --secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
    --yes
```

> **Security note:** `--secret-access-key` and `--password` are visible in `ps` output and shell history. For sensitive environments, use the `EXTENDDB_ADMIN_PASSWORD` environment variable for the password.

Credential import is gated by the `allow_credential_import` runtime setting (default: `true`). To disable:

```bash
./target/release/extenddb settings --config extenddb.toml set \
    allow_credential_import false
```

Every import is logged to syslog (access key ID, account, user — never the secret).

### Change IAM user password

```bash
./target/release/extenddb manage --user 123456789012/alice --password secret \
    change-user-password --account-id 123456789012 \
    --user-name alice --new-password newsecret
```

### Create an IAM group

```bash
./target/release/extenddb manage --user admin --password <pw> \
    create-group --account-id 123456789012 --group-name developers
```

### Add a user to a group

```bash
./target/release/extenddb manage --user admin --password <pw> \
    add-group-member --account-id 123456789012 \
    --group-name developers --user-name alice
```

### Remove a user from a group

```bash
./target/release/extenddb manage --user admin --password <pw> \
    remove-group-member --account-id 123456789012 \
    --group-name developers --user-name alice
```

### List groups

```bash
./target/release/extenddb manage --user admin --password <pw> \
    list-groups --account-id 123456789012
```

### Delete a group

```bash
./target/release/extenddb manage --user admin --password <pw> \
    delete-group --account-id 123456789012 --group-name developers
```

### Put a user policy

Creates or replaces a named policy on an IAM user. The policy document must be valid JSON with `Version` and `Statement` fields.

```bash
./target/release/extenddb manage --user admin --password <pw> \
    put-user-policy --account-id 123456789012 \
    --user-name alice \
    --policy-name ReadOnly \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": "dynamodb:GetItem",
        "Resource": "*"
      }]
    }'
```

### List user policies

```bash
./target/release/extenddb manage --user admin --password <pw> \
    list-user-policies --account-id 123456789012 --user-name alice
```

### Delete a user policy

```bash
./target/release/extenddb manage --user admin --password <pw> \
    delete-user-policy --account-id 123456789012 \
    --user-name alice --policy-name ReadOnly
```

### Put a group policy

```bash
./target/release/extenddb manage --user admin --password <pw> \
    put-group-policy --account-id 123456789012 \
    --group-name developers \
    --policy-name FullAccess \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": "dynamodb:*",
        "Resource": "*"
      }]
    }'
```

### List group policies

```bash
./target/release/extenddb manage --user admin --password <pw> \
    list-group-policies --account-id 123456789012 --group-name developers
```

### Delete a group policy

```bash
./target/release/extenddb manage --user admin --password <pw> \
    delete-group-policy --account-id 123456789012 \
    --group-name developers \
    --policy-name FullAccess
```

### Tag an IAM user

```bash
./target/release/extenddb manage --user admin --password <pw> \
    tag-user --account-id 123456789012 --user-name alice \
    --tags '[{"key":"Department","value":"Engineering"}]'
```

### List user tags

```bash
./target/release/extenddb manage --user admin --password <pw> \
    list-user-tags --account-id 123456789012 --user-name alice
```

### Untag an IAM user

```bash
./target/release/extenddb manage --user admin --password <pw> \
    untag-user --account-id 123456789012 --user-name alice --tag-keys Department
```

### Create an IAM role

```bash
./target/release/extenddb manage --user admin --password <pw> \
    create-role --account-id 123456789012 --role-name data-reader \
    --trust-policy '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "arn:aws:iam::123456789012:user/alice"},
        "Action": "sts:AssumeRole"
      }]
    }'
```

### List roles

```bash
./target/release/extenddb manage --user admin --password <pw> \
    list-roles --account-id 123456789012
```

### Delete a role

```bash
./target/release/extenddb manage --user admin --password <pw> \
    delete-role --account-id 123456789012 --role-name data-reader
```

### Put a role policy

```bash
./target/release/extenddb manage --user admin --password <pw> \
    put-role-policy --account-id 123456789012 \
    --role-name data-reader \
    --policy-name ReadOnly \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": "dynamodb:GetItem",
        "Resource": "*"
      }]
    }'
```

### List role policies

```bash
./target/release/extenddb manage --user admin --password <pw> \
    list-role-policies --account-id 123456789012 --role-name data-reader
```

### Delete a role policy

```bash
./target/release/extenddb manage --user admin --password <pw> \
    delete-role-policy --account-id 123456789012 \
    --role-name data-reader --policy-name ReadOnly
```

### Tag an IAM role

```bash
./target/release/extenddb manage --user admin --password <pw> \
    tag-role --account-id 123456789012 --role-name data-reader \
    --tags '[{"key":"Team","value":"Backend"}]'
```

### List role tags

```bash
./target/release/extenddb manage --user admin --password <pw> \
    list-role-tags --account-id 123456789012 --role-name data-reader
```

### Untag an IAM role

```bash
./target/release/extenddb manage --user admin --password <pw> \
    untag-role --account-id 123456789012 --role-name data-reader --tag-keys Team
```

### Assume a role

Generates temporary ASIA* credentials for the specified role. The trust policy must allow the caller ARN.

```bash
./target/release/extenddb manage --user admin --password <pw> \
    assume-role --account-id 123456789012 --role-name data-reader \
    --caller-arn arn:aws:iam::123456789012:user/alice \
    --session-name test-session
```

With optional session tags and session policy:

```bash
./target/release/extenddb manage --user admin --password <pw> \
    assume-role --account-id 123456789012 --role-name data-reader \
    --caller-arn arn:aws:iam::123456789012:user/alice \
    --session-name test-session \
    --session-tags '{"Project":"Alpha"}' \
    --session-policy '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": "dynamodb:GetItem",
        "Resource": "*"
      }]
    }' \
    --duration-seconds 1800
```

### Set a user permissions boundary

```bash
./target/release/extenddb manage --user admin --password <pw> \
    set-user-boundary --account-id 123456789012 \
    --user-name alice \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": "dynamodb:*",
        "Resource": "*"
      }]
    }'
```

### Get a user permissions boundary

```bash
./target/release/extenddb manage --user admin --password <pw> \
    get-user-boundary --account-id 123456789012 --user-name alice
```

### Delete a user permissions boundary

```bash
./target/release/extenddb manage --user admin --password <pw> \
    delete-user-boundary --account-id 123456789012 --user-name alice
```

### Set a role permissions boundary

```bash
./target/release/extenddb manage --user admin --password <pw> \
    set-role-boundary --account-id 123456789012 \
    --role-name data-reader \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": "dynamodb:GetItem",
        "Resource": "*"
      }]
    }'
```

### Get a role permissions boundary

```bash
./target/release/extenddb manage --user admin --password <pw> \
    get-role-boundary --account-id 123456789012 --role-name data-reader
```

### Delete a role permissions boundary

```bash
./target/release/extenddb manage --user admin --password <pw> \
    delete-role-boundary --account-id 123456789012 --role-name data-reader
```

### Using a custom endpoint

By default, `extenddb manage` reads the server address from `extenddb.toml`. To target a different server:

```bash
./target/release/extenddb manage --user admin --password <pw> \
    --endpoint 127.0.0.1:9000 list-admins
```

## 8. Management web console

extenddb includes a built-in web console for managing accounts, users, groups, roles, and policies through a browser. The console is served at `/console/` on the same port as the DynamoDB API.

### Accessing the console

Navigate to `https://127.0.0.1:8000/console/` in your browser (adjust the host and port to match your `extenddb.toml` configuration). Accept the self-signed certificate warning on first visit.

### Login

- **Admin users:** Enter your admin username and password (the credentials printed during `extenddb init`).
- **IAM users:** Enter `account_id/user_name` as the username and your console password.

### Features

- **Dashboard:** Overview of accounts and admin users.
- **Account management:** Create, view, and delete accounts (admin only).
- **User management:** Create and delete IAM users, view access keys, policies, tags, and group memberships.
- **Access key management:** Create and delete access keys. The secret key is shown once at creation time.
- **Group management:** Create and delete groups, add and remove members.
- **Role management:** Create and delete roles, view trust policies.
- **Policy management:** Add and delete inline policies for users, groups, and roles. Includes a JSON editor with a default policy template.

Admin users have full access to all management operations. IAM users can view their own details and manage their own access keys.

Sessions expire after 8 hours of inactivity. Click "Logout" to end a session immediately.

## 9. Audit logging

All management and settings operations are logged to syslog at WARN level with structured targets for filtering:

- **Management operations:** `extenddb::audit::manage` — covers admin CRUD, account CRUD, IAM user/group/role/policy CRUD, access key lifecycle, permissions boundaries, assume-role, and credential import.
- **Settings operations:** `extenddb::audit::settings` — covers `extenddb settings set` changes.

Secrets (passwords, secret keys) are never included in audit log entries.

View audit entries:

```bash
journalctl -t extenddb | grep 'extenddb::audit'
```

## 10. External test suites

extenddb supports running external test suites (e.g., Java/JUnit, Python/pytest) against a running instance. Suites are registered in `external-suites.toml` at the project root and referenced by path — never copied into the repo.

### Running external suites

```bash
# Start extenddb first
./target/release/extenddb serve --config extenddb.toml

# Set GSI propagation delay to 0 for external tests.
# External suites expect synchronous GSI behavior (matching real DynamoDB's
# typical sub-millisecond propagation). The async GSI path is tested
# separately by the extenddb-specific test_gsi_async.py suite.
./target/release/extenddb settings --config extenddb.toml set gsi_propagation_delay_ms 0

# Run all registered suites
python3 devtools/run-external-tests

# Dry run — show what would execute without running
python3 devtools/run-external-tests --dry-run

# Run a specific suite by name
python3 devtools/run-external-tests \
    --suite "DynamoDB PostgreSQL Extension Functional Tests"

# Override the endpoint
python3 devtools/run-external-tests --endpoint http://localhost:9000

# Generate a JSON report
python3 devtools/run-external-tests --report results.json

# Show full test output
python3 devtools/run-external-tests --verbose
```

### Registering a new suite

Add a `[[suite]]` entry to `external-suites.toml`:

```toml
[[suite]]
name = "My Test Suite"
path = "~/source/my-test-suite"
runner = "maven"       # maven, gradle, pytest, or cargo
enabled = true

[suite.env]
DDB_ENDPOINT = "${EXTENDDB_ENDPOINT}"
```

The `${EXTENDDB_ENDPOINT}` placeholder is replaced with the `--endpoint` value at runtime.

Supported runners: `maven` (`mvn test`), `gradle` (`gradle test`), `pytest` (`python3 -m pytest`), `cargo` (`cargo test`).

### Prerequisites

Each runner requires its tools to be installed. The runner checks prerequisites before executing and skips suites with missing tools:

- **maven:** `java`, `mvn`
- **gradle:** `java`, `gradle`
- **pytest:** `python3`, `pytest`
- **cargo:** `cargo`

## 11. Check version

```bash
./target/release/extenddb version
# extenddb 0.0.2
# catalog 0.0.2
# commit abc1234
# built 2026-04-17T12:00:00Z
```

The `-V` flag prints the same output:

```bash
./target/release/extenddb -V
```

## 12. Tear down

To completely remove a deployment:

```bash
./target/release/extenddb destroy --config extenddb.toml --yes
```

The `--yes` flag is required to confirm destruction. Without it, the command exits with an error.

## Performance Tuning

### Connection pool size

The `storage.postgres.pool_size` setting (default: 20, minimum: 10) controls the maximum number of concurrent PostgreSQL connections used for DynamoDB data operations. Each in-flight request that touches the database holds one connection for the duration of its transaction. Values below 10 are clamped at startup with a warning.

The `storage.postgres.catalog_pool_size` setting controls the maximum number of concurrent connections for the management/catalog pool (authorization queries, IAM operations, console). Defaults to `pool_size` if not set, minimum: 10. With auth enabled (`provider = "builtin"`), each DynamoDB request makes concurrent authorization queries against this pool — size it to match expected concurrency. Values below 10 are clamped at startup with a warning.

**When to increase:** If you see elevated latency under concurrent load, the pool may be saturated. Requests queue at the pool level when all connections are in use. Increase `pool_size` (and `catalog_pool_size` if auth is enabled) to allow more concurrent transactions.

**Relationship to PostgreSQL `max_connections`:** The total connection footprint is `pool_size + catalog_pool_size + 1` (the extra 1 is for the log-level poller). PostgreSQL's default `max_connections` is 100. Ensure `pool_size + catalog_pool_size + 1` does not exceed your PostgreSQL `max_connections` setting.

**Example:** To support 50 concurrent data operations with auth enabled, set both pools to 50 in `extenddb.toml` and ensure PostgreSQL allows at least 101 connections.

```toml
[storage.postgres]
pool_size = 50
catalog_pool_size = 50
```

### Contention characteristics

- **Different items:** Fully concurrent up to `pool_size`. No contention.
- **Same item:** Concurrent writes to the same item serialize on PostgreSQL's row lock (`SELECT ... FOR UPDATE`). All updates succeed, but throughput for a single hot item is bounded by single-row transaction rate.
- **Reads:** GetItem and Query do not acquire row locks and proceed concurrently with writes.

### Auth/authz caching

Each DynamoDB request normally issues ~6 catalog queries during auth/authz (credential lookup, identity policies, group policies, permissions boundary, principal tags, resource tags) plus a 7th for the table's key schema. ExtendDB layers an in-memory stale-while-revalidate cache over each of these to eliminate the round-trip in steady state.

Configured via `[auth.cache]`:

```toml
[auth.cache]
enabled            = true   # master kill switch
ttl_seconds        = 60     # hard TTL — entries older than this are full misses
soft_ttl_seconds   = 30     # entries older than this serve cached AND trigger
                            #   a background refresh (request does not block)
negative_ttl_seconds = 5    # how long "not found" results are remembered
max_entries        = 10000  # per-cache LRU cap
```

**Self-induced changes** (admin API and console mutations: `CreateAccessKey`, `PutUserPolicy`, `TagResource`, `CreateTable`, etc.) propagate instantly via write-through invalidation on the local instance.

**Fanout invalidations** (`DeleteAccount`, `DeleteRole` session sweep, `DeleteGroup` member fanout) complete asynchronously; there is a brief (~ms) window between the API responding success and the eviction landing. For hard cutover, prefer single-key invalidations (`DeleteAccessKey`) over cascades.

**Off-instance changes** (multi-node deployments, or direct DB writes outside the management API) take up to `ttl_seconds` to propagate.

Cache statistics are exposed at `/management/auth-cache-metrics` (JSON, admin-authenticated): hit / stale-hit / miss counts, refresh successes and failures, current entry counts per cache, and a `pass_through` flag per cache (distinguishes "disabled" from "cold").

## Troubleshooting

See `docs/troubleshooting.md` for common errors and fixes.

## Sample Application

A complete Python sample application is included at `samples/sample_app.py`. It demonstrates the full extenddb lifecycle:

1. **Create tables** — simple PK, PK+SK with GSI, and multi-part GSI keys (tournament pattern)
2. **Poll for ACTIVE** — wait for all tables to reach ACTIVE status
3. **Load data** — PutItem and BatchWriteItem
4. **Query** — Query on base tables and GSIs (including multi-part GSI keys), Scan
5. **Update** — UpdateItem with update expressions and conditions
6. **Batch read** — BatchGetItem across multiple keys
7. **Transactions** — TransactWriteItems and TransactGetItems across tables
8. **Delete** — DeleteItem to remove individual items
9. **Drop tables** — DeleteTable to tear down all tables

### Running the sample application

```bash
# Start extenddb
./target/release/extenddb serve --config extenddb.toml

# Create an IAM user with DynamoDB access (use account ID from init)
./target/release/extenddb manage --user admin --password '<admin-password>' \
  create-user --account-id <account-id> \
  --user-name sampleuser --user-password 'SamplePass1!'

./target/release/extenddb manage --user admin --password '<admin-password>' \
  put-user-policy --account-id <account-id> \
  --user-name sampleuser --policy-name FullAccess \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}'

# Create an access key (self-service)
./target/release/extenddb manage --user <account-id>/sampleuser --password 'SamplePass1!' \
  create-access-key

# Run the sample with the access key from create-access-key output
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export EXTENDDB_ENDPOINT=https://127.0.0.1:8000
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
python3 samples/sample_app.py
```

The sample creates three tables (`SampleUsers`, `SampleOrders`, `SampleTournamentMatches`), exercises all major DynamoDB operations, and cleans up after itself.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
