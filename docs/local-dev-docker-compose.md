---
description: "Start the extenddb/extenddb-postgres image and a PostgreSQL container with one docker compose command, then create users and access keys and make the first requests."
---

# Docker Compose with PostgreSQL

The repository's `docker-compose.yml` starts the published
`extenddb/extenddb-postgres` image together with a PostgreSQL container and runs
the initialization for you; no manual `extenddb init` is required. The result is
a server with TLS on the loopback interface, for local development and testing
against the PostgreSQL backend. For running ExtendDB on your own servers, see the
[deployment guide](manuals/11-deployment-guide.md).

## Prerequisites

- Docker Engine 20.10+ with Compose plugin (`docker compose`)
- AWS CLI v2 (for testing)

## Get the Compose file

The file lives at the root of the repository. Either clone the repository and
work from its root, or fetch the one file into an empty directory:

```bash
mkdir extenddb-compose && cd extenddb-compose
curl -fsSLO https://raw.githubusercontent.com/ExtendDB/extenddb/main/docker-compose.yml
```

Every `docker compose` command below runs from the directory that holds the file.

## Start the stack

```bash
EXTENDDB_IMAGE=extenddb/extenddb-postgres:latest docker compose up -d
```

`EXTENDDB_IMAGE` names the image to run. Without it the file falls back to the tag a
local source build produces, so keep the variable on every `up`.

This starts four services in order:

1. `postgres`: PostgreSQL 16
2. `extenddb-volume-init`: sets up the state volume (runs once)
3. `extenddb-bootstrap`: runs `extenddb init` and applies migrations (runs once)
4. `extenddb`: the ExtendDB server

Wait for the stack to be ready:

```bash
docker compose ps -a
```

The two bootstrap services show `Exited (0)` once they have run (`-a` includes
them; without it Compose lists only running containers). `extenddb` shows
`(health: starting)` for 10 to 20 seconds, then `(healthy)`.

Verify ExtendDB is responding:

```bash
curl -sk https://127.0.0.1:18443/health
# {"status":"healthy"}
```

## Default credentials and variables

| Variable | Default | What it sets |
|---|---|---|
| `EXTENDDB_IMAGE` | a local build tag | The ExtendDB image to run; use `extenddb/extenddb-postgres:latest` |
| `EXTENDDB_PORT` | `18443` | The host port published on `127.0.0.1` |
| `EXTENDDB_ADMIN_USER` | `admin` | Admin user created by the bootstrap |
| `EXTENDDB_ADMIN_PASSWORD` | `ExtendDBLocalAdmin123` | Admin password |
| `PG_ADMIN_USER` | `postgres` | PostgreSQL superuser |
| `PG_ADMIN_PASSWORD` | `ExtendDBLocalPostgres123` | PostgreSQL superuser password |
| `APP_DB_USER` | `extenddb_app` | The PostgreSQL role ExtendDB connects as |
| `APP_DB_PASSWORD` | `ExtendDBLocalApp123` | That role's password |

Set a variable before the command to override it:

```bash
EXTENDDB_ADMIN_PASSWORD=mysecret EXTENDDB_IMAGE=extenddb/extenddb-postgres:latest docker compose up -d
```

These passwords are for a local stack on the loopback interface; change them before
exposing the port anywhere else.

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

# Create an access key (shown once; save it)
docker compose exec extenddb extenddb manage \
  --user admin --password 'ExtendDBLocalAdmin123' \
  create-access-key --account-id <account-id> --user-name myuser
```

For the full management API reference, see [Administration](manuals/05-admin-guide.md).

## Configure AWS CLI

```bash
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
export AWS_DEFAULT_REGION=us-east-1
export AWS_CA_BUNDLE=/tmp/extenddb-cert.pem

aws dynamodb list-tables --endpoint-url https://127.0.0.1:18443
```

For more CLI examples, see [First requests](first-requests.md); the table at the top of that page lists the endpoint, certificate and credentials for a Compose server.

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

The PostgreSQL server image is pulled from Amazon ECR Public. If the pull fails with an
authorization error, reauthenticate:

```bash
aws ecr-public get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin public.ecr.aws
```

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is maintained by
engineers at Amazon Web Services. It is not Amazon DynamoDB and contains no DynamoDB source
code. "DynamoDB" is a trademark of Amazon.com, Inc.
