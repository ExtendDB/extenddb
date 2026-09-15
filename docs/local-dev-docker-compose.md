# Local Development with Docker Compose

A `docker-compose.yml` is included at the repository root. It wires the official
`extenddb/extenddb-postgres` image to a PostgreSQL container and handles
initialization automatically — no manual `extenddb init` required.

## Prerequisites

- Docker Engine 20.10+ with Compose plugin (`docker compose`)
- AWS CLI v2 (for testing)

## Start the stack

```bash
EXTENDDB_IMAGE=extenddb/extenddb-postgres:latest docker compose up -d
```

This starts four services in order:

1. `postgres` — PostgreSQL 16
2. `extenddb-volume-init` — sets up the state volume (runs once)
3. `extenddb-bootstrap` — runs `extenddb init` and applies migrations (runs once)
4. `extenddb` — the ExtendDB server

Wait for the stack to be ready:

```bash
docker compose ps
```

All services should show `healthy` or `exited` (bootstrap services exit after completing).

Verify ExtendDB is responding:

```bash
curl -sk https://127.0.0.1:18443/health
# {"status":"healthy"}
```

## Default credentials

| Setting | Value |
|---------|-------|
| Admin user | `admin` |
| Admin password | `ExtendDBLocalAdmin123` |
| Port | `18443` |

Override any value with environment variables before running `docker compose up`:

```bash
EXTENDDB_ADMIN_PASSWORD=mysecret EXTENDDB_IMAGE=extenddb/extenddb-postgres:latest docker compose up -d
```

See the comments in `docker-compose.yml` for all available variables.

## TLS certificate

ExtendDB generates a self-signed certificate on first boot. Extract it to trust
it locally:

```bash
docker compose exec extenddb cat /var/lib/extenddb/.extenddb/tls/cert.pem > /tmp/extenddb-cert.pem
export AWS_CA_BUNDLE=/tmp/extenddb-cert.pem
```

## Manage users and access keys

Use `extenddb manage` via `docker compose exec`:

```bash
# List accounts (use the account ID printed in bootstrap logs)
docker compose exec extenddb extenddb manage \
  --user admin --password 'ExtendDBLocalAdmin123' \
  list-accounts

# Create an IAM user
docker compose exec extenddb extenddb manage \
  --user admin --password 'ExtendDBLocalAdmin123' \
  create-user --account-id <account-id> --user-name myuser

# Attach a policy
docker compose exec extenddb extenddb manage \
  --user admin --password 'ExtendDBLocalAdmin123' \
  put-user-policy --account-id <account-id> --user-name myuser \
  --policy-name full-access \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}]}'

# Create an access key (shown once — save it)
docker compose exec extenddb extenddb manage \
  --user admin --password 'ExtendDBLocalAdmin123' \
  create-access-key --account-id <account-id> --user-name myuser
```

For the full management API reference, see [Admin Guide](manuals/05-admin-guide.md).

## Configure AWS CLI

```bash
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
export AWS_DEFAULT_REGION=us-east-1
export AWS_CA_BUNDLE=/tmp/extenddb-cert.pem

aws dynamodb list-tables --endpoint-url https://127.0.0.1:18443
```

For more CLI examples, see [Getting Started](getting-started.md#6-try-it-out).

## View logs

```bash
# Bootstrap logs (init output)
docker compose logs extenddb-bootstrap

# Server logs
docker compose logs -f extenddb
```

## Stop and clean up

```bash
# Stop (preserves data)
docker compose down

# Stop and delete all data
docker compose down -v
```

## ECR authentication

The PostgreSQL image is pulled from Amazon ECR Public. If the pull fails with an
authorization error, reauthenticate:

```bash
aws ecr-public get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin public.ecr.aws
```

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
