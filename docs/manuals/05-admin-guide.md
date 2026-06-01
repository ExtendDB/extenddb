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
5. Connects to the configured storage backend (catalog + data databases)
6. Verifies catalog version matches the binary
7. Starts the HTTP server
8. Spawns background tasks (log level polling, backend-specific retention and metrics tasks)

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

The endpoint returns `503` with `{"status":"unhealthy"}` if the selected
storage backend cannot serve through one of the pools owned by this frontend.

## Configuration Reference

### extenddb.toml — Static Configuration

These settings require a server restart to take effect.

#### [server]

| Key | Default | Description |
|-----|---------|-------------|
| `bind_addr` | `127.0.0.1` | Network interface to bind |
| `port` | `8000` | HTTP port |
| `region` | `us-east-1` | AWS region for ARN generation |
| `throttling_enabled` | unset / `false` | PostgreSQL frontend token buckets for local capacity fidelity. Ignored by TiDB; use TiDB Resource Control/resource groups for distributed capacity governance. |

#### [storage]

| Key | Default | Description |
|-----|---------|-------------|
| `backend` | `postgres` | Storage backend (`postgres` or `tidb`; PostgreSQL is the default) |

#### [storage.postgres]

| Key | Default | Description |
|-----|---------|-------------|
| `connection_string` | `postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog` | Catalog database connection string |
| `pool_size` | `20` | Maximum concurrent database connections (minimum: 10) |
| `catalog_pool_size` | (= `pool_size`) | Maximum connections for management/authz pool (minimum: 10) |

#### [storage.tidb]

Available when the binary is built with the `tidb` feature.

| Key | Default | Description |
|-----|---------|-------------|
| `connection_string` | `mysql://extenddb:extenddb-local-dev@localhost:4000/extenddb_catalog` | Catalog database connection string |
| `pool_size` | `20` | Maximum connections for strong and default-read data pools (minimum: 10) |
| `catalog_pool_size` | (= `pool_size`) | Maximum connections for catalog metadata, control-plane, management, and authz pools (minimum: 10) |
| `resource_group` | unset | Optional TiDB Resource Control group. When set, ExtendDB binds every runtime TiDB pool session with `SET RESOURCE GROUP`. |

TiDB capacity governance is configured in TiDB, not in ExtendDB. A typical
deployment creates a TiDB resource group and binds the ExtendDB SQL user to it:

```sql
CREATE RESOURCE GROUP IF NOT EXISTS extenddb_api RU_PER_SEC = 500 BURSTABLE;
ALTER USER 'extenddb'@'%' RESOURCE GROUP extenddb_api;
```

Set `storage.tidb.resource_group = "extenddb_api"` when the server should bind
each pooled runtime session itself. This is useful when the same SQL user is
shared by multiple applications or when you want config review to show the exact
ExtendDB serving group. TiDB resource control must be enabled. If TiDB strict
resource-control mode is enabled, the SQL user needs permission to execute
`SET RESOURCE GROUP`. Existing SQL sessions keep their previous resource group
until they reconnect, so restart ExtendDB after changing the setting.

#### [storage.tidb.backup]

TiDB backup and restore uses native BR, not a logical row-copy table. Configure these fields before using `CreateBackup` with the TiDB backend.
`CreateBackup` returns after BR completes and publishes the backup as
`AVAILABLE`; incomplete native backup attempts are not exposed as durable
catalog rows.
`RestoreTableFromBackup` likewise publishes the target table only after BR
restore, physical table rename, and restored-table normalization complete; failed
or interrupted restores do not expose a durable `CREATING` table.
Table-level `RestoreTableToPointInTime` is not exposed for TiDB because the
native TiDB choices do not match DynamoDB's live new-table restore shape: BR PITR
restores into an empty or conflict-free target cluster, `FLASHBACK TABLE` covers
dropped or truncated tables, and historical reads cannot populate a current
target table as one native online operation.
`DeleteBackup` removes ExtendDB's catalog reference to the BR backup. Snapshot
files remain under the configured backup storage URI and should be retained,
archived, or deleted by the operator, TiDB Operator clean policy, or object-store
lifecycle rules. ExtendDB does not run a frontend-side file deleter for BR data.

| Key | Default | Description |
|-----|---------|-------------|
| `pd_endpoint` | unset | PD endpoint passed to BR, for example `127.0.0.1:2379` |
| `storage_uri` | unset | Base URI for BR snapshot backups (`local://`, S3, GCS, Azure Blob, or compatible storage supported by BR) |
| `log_storage_uri` | unset | Reserved for future cluster-level BR log backup orchestration; table-level PITR is not exposed by the TiDB backend |
| `binary` | `tiup` | Executable used to run BR |
| `component` | `br` | Component/subcommand after `binary`; set to `""` when `binary` is a direct `br` executable |
| `send_credentials_to_tikv` | unset | Maps to BR `--send-credentials-to-tikv`; set `false` for IAM-role based S3 access |

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
EXTENDDB__STORAGE__TIDB__CONNECTION_STRING="mysql://..."
EXTENDDB__AUTH__PROVIDER=builtin
```

Precedence: CLI flags > environment variables > config file > defaults.

### Runtime Settings

Managed via `extenddb settings set`. Changes take effect within 30 seconds without restart.

| Setting | Default | Description |
|---------|---------|-------------|
| `log_level` | `info` | Log level: trace, debug, info, warn, error |
| `control_plane_delay_seconds` | `5` | PostgreSQL delay for table status transitions (0 = instant); TiDB ignores this setting and uses native online DDL reconciliation |
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

Prometheus-compatible metrics endpoint.

### Health Check

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:8000/health
# {"status":"healthy"}
```

For TiDB, this probes the engine catalog, catalog-store/auth, strong-data, and
default-read data pools. For PostgreSQL, it probes the catalog metadata,
catalog-store/auth, and data pools.

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

Check that the configured storage backend is running and the connection string in `extenddb.toml` is correct.

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

Check the configured storage backend's query plan tools (`EXPLAIN ANALYZE` for PostgreSQL or TiDB). Ensure indexes exist on key columns.

**High connection count:**

Increase `pool_size` in `extenddb.toml` or check for connection leaks.

### Data Recovery

Use the configured storage backend's native backup and recovery path.

For PostgreSQL, use standard PostgreSQL tools:

```bash
# Backup
pg_dump extenddb_catalog > catalog_backup.sql
pg_dump extenddb_catalog_data > data_backup.sql

# Restore
psql -f catalog_backup.sql extenddb_catalog
psql -f data_backup.sql extenddb_catalog_data
```

For TiDB, configure `[storage.tidb.backup]` and use DynamoDB-compatible backup APIs backed by native TiDB BR, or operate BR directly at the cluster level for full-cluster recovery. Table-level restore from an on-demand backup publishes catalog metadata only after TiDB finishes the physical restore path. Point-in-time table restore is intentionally not emulated with frontend row replay; use TiDB cluster-level PITR into a recovery cluster for that recovery model.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
