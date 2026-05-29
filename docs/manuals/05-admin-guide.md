# Admin Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Server Lifecycle

### Starting

```bash
./target/release/extenddb serve --config extenddb.toml
```

extenddb always runs as a daemon. On startup it:

1. Reads `extenddb.toml` configuration
2. Binds the TCP socket (port conflicts are reported before forking)
3. Forks to background
4. Initializes syslog logging
5. Connects to PostgreSQL (catalog + data databases)
6. Verifies catalog version matches the binary
7. Starts the HTTP server
8. Spawns background tasks (log level polling, stream cleanup, TTL expiry)

### Checking Status

```bash
./target/release/extenddb status --config extenddb.toml
# extenddb is running on port 8000 (pid 12345)
```

### Stopping

```bash
./target/release/extenddb stop --config extenddb.toml
```

Or manually:

```bash
kill <pid>
```

extenddb handles SIGTERM and SIGINT gracefully — it drains active connections for up to 5 seconds before exiting.

### Health Check

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:8000/health
# {"status":"healthy"}
```

## Configuration Reference

### extenddb.toml — Static Configuration

These settings require a server restart to take effect.

#### [server]

| Key | Default | Description |
|-----|---------|-------------|
| `bind_addr` | `127.0.0.1` | Network interface to bind |
| `port` | `8000` | HTTP port |
| `region` | `us-east-1` | AWS region for ARN generation |

#### [storage]

| Key | Default | Description |
|-----|---------|-------------|
| `backend` | `postgres` | Storage backend (only `postgres` supported) |

#### [storage.postgres]

| Key | Default | Description |
|-----|---------|-------------|
| `connection_string` | `postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog` | Catalog database connection string |
| `pool_size` | `20` | Maximum concurrent database connections (minimum: 10) |
| `catalog_pool_size` | (= `pool_size`) | Maximum connections for management/authz pool (minimum: 10) |

#### [auth]

| Key | Default | Description |
|-----|---------|-------------|
| `provider` | `builtin` | Auth provider: `builtin` (SigV4 + IAM). The server refuses to start with `"none"`. |

#### [server.tls]

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `true` | TLS is mandatory. The server refuses to start with `enabled = false`. |
| `cert_path` | `~/.extenddb/tls/cert.pem` | PEM certificate file |
| `key_path` | `~/.extenddb/tls/key.pem` | PEM private key file |

`extenddb init` generates a self-signed certificate. Replace with a CA-signed certificate for production.

#### [limits]

All defaults match real DynamoDB limits. Override only for testing edge cases.

#### [logging]

| Key | Default | Description |
|-----|---------|-------------|
| `level` | `info` | Initial log level (overridden by runtime setting) |
| `format` | `pretty` | Log format: `pretty` or `json` |

Logging always goes to syslog (facility: daemon, ident: extenddb).

### Environment Variable Overrides

Any config key can be overridden via environment variables using the `EXTENDDB__` prefix with `__` as separator:

```bash
EXTENDDB__SERVER__PORT=9000
EXTENDDB__STORAGE__POSTGRES__CONNECTION_STRING="postgresql://..."
EXTENDDB__AUTH__PROVIDER=builtin
```

Precedence: CLI flags > environment variables > config file > defaults.

### Runtime Settings

Managed via `extenddb settings set`. Changes take effect within 30 seconds without restart.

| Setting | Default | Description |
|---------|---------|-------------|
| `log_level` | `info` | Log level: trace, debug, info, warn, error |
| `control_plane_delay_seconds` | `5` | Delay for table status transitions (0 = instant) |
| `allow_credential_import` | `true` | Whether `import-access-key` is allowed |

```bash
# View current settings
./target/release/extenddb settings --config extenddb.toml get log_level

# Change a setting
./target/release/extenddb settings --config extenddb.toml set log_level debug
```

## IAM Management

### Admin Users

Admin users authenticate to the management API and web console. They have full access to all management operations.

```bash
# List admins
./target/release/extenddb manage --user admin --password <pw> list-admins

# Create admin
./target/release/extenddb manage --user admin --password <pw> \
    create-admin --admin-name ops --admin-password secret123

# Change password
./target/release/extenddb manage --user admin --password <pw> \
    change-admin-password --admin-name admin --new-password newpw

# Delete admin
./target/release/extenddb manage --user admin --password <pw> \
    delete-admin --admin-name ops
```

### Accounts

Account IDs must be 12-digit numeric strings (matching AWS format). If `--account-id` is omitted on `create-account`, a random ID is auto-generated and printed.

```bash
# Create (auto-generated account ID)
./target/release/extenddb manage --user admin --password <pw> \
    create-account --account-name dev-team

# Create (explicit account ID)
./target/release/extenddb manage --user admin --password <pw> \
    create-account --account-id 123456789012 --account-name dev-team

# List
./target/release/extenddb manage --user admin --password <pw> list-accounts

# Delete (must have no tables)
./target/release/extenddb manage --user admin --password <pw> \
    delete-account --account-id 123456789012
```

### IAM Users

```bash
# Create (with optional console password)
./target/release/extenddb manage --user admin --password <pw> \
    create-user --account-id 123456789012 \
    --user-name alice --user-password secret

# List
./target/release/extenddb manage --user admin --password <pw> \
    list-users --account-id 123456789012

# Delete (cascades: removes keys, memberships, tags, policies)
./target/release/extenddb manage --user admin --password <pw> \
    delete-user --account-id 123456789012 --user-name alice
```

### Access Keys

```bash
# Create (self-service or admin)
./target/release/extenddb manage --user 123456789012/alice --password secret \
    create-access-key --account-id 123456789012 --user-name alice

# List
./target/release/extenddb manage --user 123456789012/alice --password secret \
    list-access-keys --account-id 123456789012 --user-name alice

# Delete
./target/release/extenddb manage --user 123456789012/alice --password secret \
    delete-access-key --account-id 123456789012 \
    --user-name alice --access-key-id AKIAEXTENDDB...

# Import existing credentials
./target/release/extenddb manage --user admin --password <pw> \
    import-access-key --account-id 123456789012 --user-name alice \
    --access-key-id AKIAIOSFODNN7EXAMPLE \
    --secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY --yes
```

Access key prefixes: `AKIAEXTENDDB` (long-lived), `ASIAEXTENDDB` (temporary/AssumeRole).

### Groups

```bash
# Create
./target/release/extenddb manage --user admin --password <pw> \
    create-group --account-id 123456789012 --group-name developers

# Add member
./target/release/extenddb manage --user admin --password <pw> \
    add-group-member --account-id 123456789012 \
    --group-name developers --user-name alice

# Remove member
./target/release/extenddb manage --user admin --password <pw> \
    remove-group-member --account-id 123456789012 \
    --group-name developers --user-name alice

# Delete
./target/release/extenddb manage --user admin --password <pw> \
    delete-group --account-id 123456789012 --group-name developers
```

### Roles

```bash
# Create with trust policy
./target/release/extenddb manage --user admin --password <pw> \
    create-role --account-id 123456789012 --role-name data-reader \
    --trust-policy '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": {
          "AWS": "arn:aws:iam::123456789012:user/alice"
        },
        "Action": "sts:AssumeRole"
      }]
    }'

# Assume role (generates temporary ASIA* credentials)
./target/release/extenddb manage --user admin --password <pw> \
    assume-role --account-id 123456789012 --role-name data-reader \
    --caller-arn arn:aws:iam::123456789012:user/alice \
    --session-name test-session

# Delete
./target/release/extenddb manage --user admin --password <pw> \
    delete-role --account-id 123456789012 --role-name data-reader
```

### Policies

Inline policies can be attached to users, groups, and roles:

```bash
# User policy
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

# Group policy
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

# Role policy
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

### Permissions Boundaries

```bash
# Set boundary
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

# Get boundary
./target/release/extenddb manage --user admin --password <pw> \
    get-user-boundary --account-id 123456789012 --user-name alice

# Delete boundary
./target/release/extenddb manage --user admin --password <pw> \
    delete-user-boundary --account-id 123456789012 --user-name alice
```

### Tags

```bash
# Tag a user
./target/release/extenddb manage --user admin --password <pw> \
    tag-user --account-id 123456789012 --user-name alice \
    --tags '[{"key":"Department","value":"Engineering"}]'

# List tags
./target/release/extenddb manage --user admin --password <pw> \
    list-user-tags --account-id 123456789012 --user-name alice

# Untag
./target/release/extenddb manage --user admin --password <pw> \
    untag-user --account-id 123456789012 --user-name alice --tag-keys Department
```

## Web Console

The management web console is served at `/console/` on the same port as the DynamoDB API. It requires `auth.provider = "builtin"`.

### Features

- **Dashboard**: Account and admin user counts, version info
- **Account management**: Create, view, delete accounts
- **User management**: Create, delete users; view access keys, policies, tags, group memberships
- **Access key management**: Create and delete access keys (secret shown once)
- **Group management**: Create, delete groups; add/remove members
- **Role management**: Create, delete roles; view trust policies
- **Policy management**: Add, delete inline policies with JSON editor

### Authentication

- Admin users: enter username and password
- IAM users: enter `account_id/user_name` as username, console password as password

Sessions expire after 8 hours. Click "Logout" to end immediately.

## Monitoring

### Syslog

All server logging goes to syslog (facility: daemon, ident: extenddb).

**Linux:**

```bash
# Follow live logs
journalctl -t extenddb -f

# Last 50 lines
journalctl -t extenddb -n 50

# Plain output
journalctl -t extenddb --no-pager -o cat

# Filter by level
journalctl -t extenddb -p warning
```

**macOS:**

```bash
# Live stream
log stream --predicate 'processImagePath ENDSWITH "extenddb"' --level info

# Historical (last hour)
log show --predicate 'processImagePath ENDSWITH "extenddb"' --last 1h

# Filter by level
log show --predicate 'processImagePath ENDSWITH "extenddb" AND messageType >= 16' --last 1h
```

### Audit Logging

Management and settings operations are logged at WARN level:

```bash
# View audit entries
journalctl -t extenddb | grep 'extenddb::audit'
```

Targets: `extenddb::audit::manage` (management ops), `extenddb::audit::settings` (settings changes).

### Metrics

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:8000/metrics
```

JSON metrics endpoint with DynamoDB CloudWatch-style metric names and dimensions. The response shape is `{ metrics, buckets, segments, source }`. See `docs/design/06-component-server.md` §7.2 for the full schema and metric list.

### Health Check

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:8000/health
# {"status":"healthy"}
```

## Troubleshooting

### Server Won't Start

**Port already in use:**

```
Error: Address already in use (os error 98)
```

Another process is using the port. Find it with `ss -tlnp | grep :8000` and stop it, or change the port in `extenddb.toml`.

**Database connection failed:**

```
Error: error communicating with database
```

Check that PostgreSQL is running and the connection string in `extenddb.toml` is correct.

**Catalog version mismatch:**

```
Error: catalog version mismatch: found 1.0.0, expected 0.0.2
```

Run `extenddb migrate --config extenddb.toml` to upgrade the catalog schema.

### Authentication Errors

**UnrecognizedClientException:**

The access key ID is not found. Verify the key exists with `list-access-keys`.

**SignatureDoesNotMatch:**

The secret key does not match. Re-create the access key.

**AccessDeniedException:**

The IAM policy does not allow the operation. Check attached policies with `list-user-policies`.

### Performance

**Slow queries:**

Check PostgreSQL query performance with `EXPLAIN ANALYZE`. Ensure indexes exist on key columns.

**High connection count:**

Increase `pool_size` in `extenddb.toml` or check for connection leaks.

### Data Recovery

extenddb stores all data in PostgreSQL. Use standard PostgreSQL backup and recovery tools:

```bash
# Backup
pg_dump extenddb_catalog > catalog_backup.sql
pg_dump extenddb_catalog_data > data_backup.sql

# Restore
psql -f catalog_backup.sql extenddb_catalog
psql -f data_backup.sql extenddb_catalog_data
```

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
