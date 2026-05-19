# AWS CLI Configuration for extenddb

## 1. Purpose

The AWS CLI talks to extenddb over HTTPS using SigV4. Three values redirect it from real AWS to the extenddb endpoint: the CA bundle (to trust the self-signed cert), the endpoint URL (the extenddb host and port), and the access key pair (for SigV4 signing). A region is also required; any valid region string works, and `us-east-1` is the conventional default.

The default port is 8000 (configured via `port` in `[server]` section of `extenddb.toml`). Replace `<port>` below with the value from your config.

## 2. Option 1: Environment variables (simplest)

Copied verbatim from `docs/getting-started.md`, "Option A: Environment variables (simplest)":

```bash
export AWS_CA_BUNDLE=~/.extenddb/tls/cert.pem
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:<port>
export AWS_ACCESS_KEY_ID=<access-key-from-create-access-key>
export AWS_SECRET_ACCESS_KEY=<secret-key-from-create-access-key>
export AWS_DEFAULT_REGION=us-east-1
```

Notes:

- `AWS_ENDPOINT_URL_DYNAMODB` is DynamoDB-specific and takes precedence over the global `AWS_ENDPOINT_URL`.
- Simplest for a shell session but does not persist across shells.

## 3. Option 2: AWS config profile (persistent)

Copied verbatim from `docs/getting-started.md`, "Option B: AWS config profile".

Add to `~/.aws/config`:

```ini
[profile extenddb]
region = us-east-1
ca_bundle = ~/.extenddb/tls/cert.pem
services = extenddb-services

[services extenddb-services]
dynamodb =
  endpoint_url = https://127.0.0.1:<port>
```

Add to `~/.aws/credentials`:

```ini
[extenddb]
aws_access_key_id = <access-key-from-create-access-key>
aws_secret_access_key = <secret-key-from-create-access-key>
```

Invoke with the profile:

```bash
aws --profile extenddb dynamodb list-tables
```

Notes:

- Persistent across shell sessions.
- Easy to switch between extenddb and real AWS.

## 4. Option 3: Per-command flags (ad-hoc)

```bash
aws dynamodb list-tables \
    --endpoint-url https://127.0.0.1:<port> \
    --ca-bundle ~/.extenddb/tls/cert.pem \
    --region us-east-1
```

Notes:

- Requires the credentials to be picked up elsewhere (default profile, env vars).
- Most verbose, but unambiguous in scripts.

## 5. Quick verification

Once configured, confirm the connection works:

```bash
aws dynamodb list-tables
```

Expected output: `{"TableNames": []}`. The empty array is correct; no tables exist yet.

If the command returns a signature error, an auth error, or a network error, consult ``references/troubleshooting/01-symptom-index.md``.

## 6. SDK configuration (brief)

For Python (boto3):

```python
import boto3
client = boto3.client(
    "dynamodb",
    endpoint_url="https://127.0.0.1:<port>",
    region_name="us-east-1",
    verify="/home/<user>/.extenddb/tls/cert.pem",
)
```

The same three redirections apply: endpoint URL, region, CA bundle path.
