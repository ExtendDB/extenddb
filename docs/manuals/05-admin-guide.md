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
# extenddb is running on port 18443 (pid 12345)
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

Note that `extenddb stop` reads the PID file, which a server started with
`serve --foreground` does not write by default. Stop a foreground server through
whatever supervises it (container runtime, systemd, your shell) or send it
SIGTERM directly. Alternatively start it with
`serve --foreground --write-pid-file`, which writes the PID file to the usual
`run_dir` path so `stop` and `status` work as they do in daemon mode.

### Health Check

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health
# {"status":"healthy","components":{}}
# with a backup store configured:
# {"status":"healthy","components":{"backup_store":{"status":"healthy","kind":"filesystem"}}}
# while it cannot be reached:
# {"status":"degraded","components":{"backup_store":{"status":"unhealthy","kind":"s3","reason":"unreachable"}}}
# right after startup, before an S3 store's first check has returned:
# {"status":"healthy","components":{"backup_store":{"status":"checking","kind":"s3"}}}
```

`status` is `healthy`, or `degraded` when a component is unhealthy; the HTTP
status stays 200 either way, because a component problem never takes the data
plane out of rotation. The one component is `backup_store`, present when
`[backup.store]` configures a store; its `reason` is the problem's category in
one or two words (`unreachable`, `permission denied`, `failed validation`,
`identity file unreadable`, `identity file missing`, `identity mismatch`,
`owned by another catalog`, `manifest probe failed`, `catalog unreadable`) and
nothing else: the endpoint is unauthenticated, so the ids, ARNs and table
names a problem involves are in the server's log line only. An S3 store's
first check runs after the listener is up (the next section); until it
returns the component is `checking`, with the server `healthy`. The
reconciler re-runs the store's checks every `reconcile_interval_secs`, each
run bounded (5 s for a filesystem root, 30 s for a bucket, after which the
check is abandoned and the store is `unreachable` until a later check
answers), and the component recovers on its own.

Or without `curl` — useful inside a minimal container image, and what a Docker
`HEALTHCHECK` should call:

```bash
./target/release/extenddb healthcheck --config extenddb.toml
echo $?   # 0 = healthy, 1 = not
```

`healthcheck` reads the port from the config file, or takes an explicit
`--endpoint https://host:port`. It accepts the server's self-signed certificate.

This checks liveness, not readiness. `/health` does not query the database, so
it reports healthy even when the database has become unreachable since startup. That is intentional for a liveness probe: one that
failed on a database outage would restart every replica at once and prolong the
outage. A database that is unreachable at startup stops the server from listening
at all, so that case is still caught.

There is no readiness endpoint yet, so nothing will currently drain traffic from
a replica whose backend is gone. Until one exists, gate on `/health` for restarts
only, and rely on client retries for backend failures.

## Configuration Reference

### extenddb.toml — Static Configuration

These settings require a server restart to take effect.

#### [server]

| Key | Default | Description |
|-----|---------|-------------|
| `bind_addr` | `127.0.0.1` | Network interface to bind |
| `port` | `18443` | HTTP port |
| `region` | `us-east-1` | AWS region for ARN generation. Baked into every backup ARN the server issues, so it must stay within `[a-z0-9-]` (under a value outside that class `CreateBackup` is refused with a `ValidationException` naming `server.region`, since no backup ARN can be formed; backups taken before the upgrade under such a region keep an ARN `DescribeBackup`, `DeleteBackup`, and `RestoreTableFromBackup` refuse as "not valid"; the server logs a warning at startup). A backup ARN of the caller's account in another region is resolved in the server's region first, then as written; see the upgrade manual before changing this on a deployment with backups |

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

Backup and restore jobs are bounded per server process by `[backup] max_concurrent_jobs` (default 4, below), not per account, and a request over the bound is refused at once with `LimitExceededException`. The server raises `pool_size` and `catalog_pool_size` to at least `2 * max_concurrent_jobs + 10` and logs the raise with both numbers, so every job the bound admits has a connection. Each running restore holds one connection from this pool for its whole copy, plus one dedicated connection outside the pool for its claim.

`RestoreTableFromBackup` on PostgreSQL holds a session-level advisory lock (the job's claim) for the whole copy, so ExtendDB must reach the catalog database directly or through a session-mode pooler. Behind a transaction-mode pooler (for example PgBouncer with `pool_mode = transaction`) the lock lands on a pooled server connection and is not released with the job, so later jobs can be refused as already claimed; do not run backups or restores through one.

#### [storage.sqlite]

| Key | Default | Description |
|-----|---------|-------------|
| `path` | `extenddb.sqlite` | Database file (`init --sqlite-path` writes it); `:memory:` for an ephemeral database |
| `pool_size` | `10` | Maximum connections. Writes are serialized by the engine whatever the size. A file database raises `pool_size = 1` to 2 and logs it at info (`SQLite pool_size 1 raised to 2 for a file-backed database: ...`), because a backup holds one reader connection for its whole duration while its batches write through another. Use 3 or more so a running backup does not contend with ordinary requests for connections |

#### [backup]

The knobs backup and restore jobs and the reconciler read; every key has a default, and the section may be left out.

| Key | Default | Description |
|-----|---------|-------------|
| `data_file_target_bytes` | `67108864` | Uncompressed bytes per backup data file (minimum 32 MiB); read by the store-backed backup writer |
| `heartbeat_interval_secs` | `15` | The job heartbeat timer. The in-catalog backup and restore paths of this release heartbeat every 15 s whatever this says |
| `stale_job_after_secs` | `300` | Heartbeat age after which a job's owner is dead and the reconciler fails the job and removes its restore target. Must be at least three heartbeat periods, where the period is the larger of `heartbeat_interval_secs` and the 15 s the jobs of this release heartbeat at (so 45 at the least, whatever `heartbeat_interval_secs` says); on SQLite a long write transaction delays a heartbeat, so keep a margin above that floor |
| `failed_row_retention_secs` | `604800` | How long FAILED backup rows are kept for the operator (7 days) |
| `reconcile_interval_secs` | `60` | The reconciler's cycle; it also runs once at startup |
| `max_concurrent_jobs` | `4` | Backup and restore jobs this instance runs at once (at most 1024); a request over it is `LimitExceededException`. On PostgreSQL the pool sizes are raised to `2 * max_concurrent_jobs + 10` when smaller |
| `spool_path` | (unset) | Restore download area for an S3 store, created `0700` with files `0600`, holding a `.lock` another instance on the host refuses. Required with an S3 store, except on file-backed SQLite where it defaults to `backup-spool/` beside the database file. Refused inside or containing a filesystem store root, and when it is or contains the database file's directory |

#### [backup.store]

The store CreateBackup's backups will live in, and the one restores and deletes of store-backed backups read. The section's keys, defaults and the refusals of a bad value are the backup store crate's (`crates/backup-store/src/config.rs`); the table below is what the server adds. One definition of the shape exists, so an unknown key anywhere under `[backup]` is refused at load.

| Key | Default | Description |
|-----|---------|-------------|
| `kind` | see below | `filesystem`, `s3`, or `disabled` |
| `filesystem.path` | `backups/` beside a SQLite database file | The root directory of a filesystem store. It must exist, be owned by the extenddb user with mode `0700`, and sit under directories owned by root or that user which others cannot write to; `validate` refuses anything else at startup. A relative path, like a relative `spool_path`, is resolved against the working directory the server was started from (the daemon's own working directory is `/`) |
| `s3.bucket`, `s3.prefix`, `s3.region`, `s3.endpoint`, `s3.force_path_style`, `s3.allow_insecure_endpoint`, `s3.upload_part_size_bytes`, `s3.max_concurrent_uploads` | | The S3 store; see the crate's `config` module doc. Credentials come from the AWS SDK's chain and are never in the file |

When the section is absent, the backend decides: on file-backed SQLite it is a filesystem store at `backups/` beside the database file, created `0700` if missing, whose validation or identity-check failure is a logged warning that leaves the store unconfigured rather than a startup error or an unhealthy store while the catalog has no backup rows for that root (so an upgrade never stops a server that never configured backups, and CreateBackup keeps working on one); once the catalog has rows for the default root, the root has been used and is handled as a configured one: a failure of its identity check or manifest probe is an unhealthy store (reported on `/health`, its rows' restores and deletes refused, `backup purge` refusing on it rather than reporting no store), and a refusal of its `open` or validation a startup error naming the root; on PostgreSQL, MongoDB, Cassandra and an in-memory SQLite database there is no default and the store is unconfigured, logged at startup. `kind = "disabled"` leaves it unconfigured on purpose on any backend. Without a configured store, CreateBackup writes the in-catalog backup format and logs a deprecation warning naming `[backup.store]` on each call; with one configured, this release's CreateBackup still writes the in-catalog format (the store-backed writer is a later release's) and the store serves restores and deletes of the backups it holds. Backups of either format stay listable, describable, restorable and deletable.

Startup opens the configured store and, for a filesystem store, validates it and checks its identity before the listener is up (bounded by 5 s: a mount that stops answering after the root is opened leaves the server starting with the store `checking`; a root that does not answer at startup holds the server before the listener, as the database directory would, with the port bound and silent); an S3 store's validation and identity check are requests to the endpoint, and run after the listener is up, in the reconciler's first cycle, so an endpoint that stalls never holds the data plane down (the component is `checking` until the first result, and the operations that touch the store refuse meanwhile as they do while it is unhealthy). A configuration error, every one decided without a request to the store, stops startup with the key, path or bucket in the message: a root that does not exist or fails the owner-only rule, a filesystem root another user can reach, a filesystem root that is or contains the database directory, a bucket name the SDK routes to S3 Express, a bad part size or prefix, an insecure endpoint, an S3 store without `spool_path` on a backend with no default, a `spool_path` that breaks its placement rule, that another live instance holds or that exists with a mode other than `0700`. A transport failure (no region, no credentials, an endpoint that refuses connections or does not answer within the check's bound), a missing or denied bucket, an I/O failure of a filesystem root's probe write or resolution (a full or read-only disk, a stale network handle), or a failed identity check leaves the server running with the store unhealthy: the problem is logged once at error, `/health` reports the `backup_store` component unhealthy (status `degraded`), the restore and delete of a store-backed backup are refused with a `ValidationException` that names the store's kind and the problem's category and not its root, the reconciler acts on no row of the store, and the check runs again each `reconcile_interval_secs` until it passes, at which point the server logs `is healthy again` and resumes without a restart. Each check is bounded (5 s for a filesystem root, 30 s for a bucket): a store that has stopped answering costs a reconciler cycle that bound and nothing more, the cycle's other duties run as before, and the abandoned check is logged; on a filesystem root the abandoned check's call stays parked on one thread until the mount answers, no later cycle starts a check while it is (logged each cycle, the store staying `unreachable`), and `stop` or SIGTERM ends the server within 5 s with that thread still parked. CreateBackup is not refused by an unhealthy store in this release: it writes the in-catalog format, which touches no store; the store-backed create of a later release is.

The store identity. The root carries `extenddb-store.json`: a random store id and the id of the catalog that owns the root. Startup creates it when the root has no file and the catalog has no rows for the root, and otherwise reads it and compares: a file naming another catalog, a store id that differs from the one the catalog's rows for the root carry, a root with no file while rows exist, or rows carrying more than one id each mark the store unhealthy. Instances that share a catalog must share one store root (one S3 bucket and prefix, or one shared mount): host-local roots configured at the same path give each instance different bytes under one root label, so each check also `head`s the manifest of the newest AVAILABLE backup and marks the store unhealthy when it is absent, which is what a second instance with a local copy of the directory sees. The file is left by `backup purge` and by every deletion; remove it only when retiring the root, or after `extenddb destroy`: a catalog re-created with `init` has a new id, and the old root's file names the old one, so the re-initialised server reports the store owned by another catalog until the file is removed or a new root configured (on the default SQLite root, that is the logged warning above and CreateBackup keeps the in-catalog path). While the catalog has no rows for a root, two host-local directories at one path cannot be told apart (each instance creates its own file and id); point every instance at one shared root before the first backup. The identity file is read up to 64 KiB; a larger object under its key is reported unreadable.

`extenddb destroy` removes the databases and leaves the store untouched: it is the copy that survives destroy. `extenddb backup list [--failed]` prints the catalog's rows for the configured store's root, every store id they carry, with each row's `store_root` and `store_id`; it is a read that runs beside a server (on SQLite it takes no serve lock), reads the identity file without creating one, and runs on an unhealthy store, naming the problem in its header. `extenddb backup purge --confirm` deletes every backup of the configured store, each backup's objects (its own prefix, never a whole account prefix, so a prefix the catalog has no row for survives) and its catalog row outright, leaving no tombstone, and leaves `extenddb-store.json` and the `exports/` component; it needs the store healthy. It takes the catalog-level maintenance claim first and heartbeats it for the command's whole run, waits up to `2 * stale_job_after_secs` until no store-backed CreateBackup is running on any instance (a CREATING or FAILED row with a heartbeat within `stale_job_after_secs`) and no restore is RUNNING, refuses and releases the claim if they have not finished, deletes a CREATING row the wait saw no live owner for as it deletes any other row, finishes any DeleteBackup left in progress as part of the purge (a backup a DeleteBackup finishes while the purge runs is counted as deleted, not left), and logs each deletion under the `extenddb::audit` target with the account and the ARN. While the claim is held, CreateBackup and RestoreTableFromBackup on every instance answer `LimitExceededException` naming the command, and a second maintenance command fails at once naming the holder. Without `--confirm` it prints the plan, changes nothing and exits 0. `--config` is accepted before or after the subcommand. On SQLite the command opens the database as `serve` does and so takes the serve lock: stop the server first; under that lock it first resolves what the stopped server left (a RUNNING restore is failed and its target removed, the CREATING target of a FAILED restore removed, an in-catalog CREATING backup removed, a DELETING row finished) and deletes a CREATING or FAILED row whatever its heartbeat says (the server that wrote it is stopped, and its clock is this host's), so such a job never makes it wait or refuse; the one exception is a RUNNING restore of kind `manifest`, which no release before PR 9a writes: the command leaves it for the reconciler that resumes it, waits on it as on any RUNNING restore and refuses naming the table; and a server starting on the file removes a claim a killed purge left, so no job is refused for the stale window. On the other backends it runs beside live servers, and a claim whose holder died expires after `stale_job_after_secs`. On every backend Ctrl-C or `SIGTERM` makes the command release the claim and exit: at once while it waits, and before its next backup while it deletes, so an interrupted purge leaves no half-deleted backup and no claim to wait out; run it again for the rest.

#### [auth]

| Key | Default | Description |
|-----|---------|-------------|
| `provider` | `builtin` | Auth provider: `builtin` (SigV4 + IAM). The server refuses to start with `"none"`. |

#### [auth.cache]

In-memory stale-while-revalidate caches eliminate the per-request catalog roundtrip for credentials, IAM policies, principal/resource tags, and table key info. Self-induced changes (admin API and console mutations) propagate instantly via write-through invalidation; off-instance changes take up to `ttl_seconds` to propagate.

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `true` | Master kill switch. When `false`, all caches operate in pass-through mode. |
| `ttl_seconds` | `60` | Hard TTL — entries older than this trigger a fresh, request-blocking load. |
| `soft_ttl_seconds` | `30` | Stale-but-usable threshold; older entries still serve immediately while a background task refreshes them. |
| `negative_ttl_seconds` | `5` | Negative-cache TTL for "not found" results. |
| `max_entries` | `10000` | Per-cache LRU cap. |

Statistics are exposed at `/management/auth-cache-metrics` (JSON, admin-authenticated). The per-cache `pass_through` flag distinguishes "cache disabled" from "cache cold." The report includes every authorization sub-cache, among them the two principal creation-time caches (`user_created_at`, `role_created_at`) consulted on each user and role-session request to derive `aws:userid` and `aws:TokenIssueTime`.

**Invalidation timing**:

- **Single-key invalidations** (e.g. `DeleteAccessKey`, `PutUserPolicy`) drop the cached entry immediately; the next request sees the post-mutation state.
- **Fanout invalidations** (e.g. `DeleteAccount`, `DeleteRole` session sweep, `DeleteGroup` member fanout) are **asynchronous** — there is a small window (~ms) between the API returning success and the matching entries being evicted. For hard cutover (e.g. revoking a compromised key), prefer the single-key path (`DeleteAccessKey`) over the cascade.
- **Off-instance changes** (multi-node deployments, or direct catalog writes) wait up to `ttl_seconds` to be observed locally.

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
| `control_plane_delay_seconds` | `0.25` | Delay for table status transitions, in seconds (0 = instant). PostgreSQL and SQLite write 0.25 into the catalog when it is created; MongoDB and Cassandra use 0.25 when the setting is absent. On PostgreSQL and SQLite a value below 1 makes DeleteTable remove the table in the request; on PostgreSQL, if a session holds a lock on the table's data (a reader, an open write transaction, an index build), DeleteTable returns `DELETING` instead (after at most about a second), the drop finishes in the background within about a second of that session's release, and a CreateTable of the same name is refused until then. |
| `allow_credential_import` | `true` | Whether `import-access-key` is allowed |
| `vector_backfill_batch_delay_ms` | `0` | **Test-oriented.** Milliseconds to pause between batches while a vector index backfills. Zero in production. A test sets it so a write is guaranteed to land while the index is still building. The pause is outside the batch transaction, so writes are still accepted throughout, but it does extend the per-table propagation hold: no index on that table advances while the build runs, GSIs included, and the accepted range goes up to 60 s per batch. |
| `vector_allocation_phase_delay_ms` | `0` | **Test-oriented.** Milliseconds to hold a new vector index in the resource-allocation phase (`CREATING` with `Backfilling: false`) before the scan starts. Zero in production. Without it the phase lasts only from the `UpdateTable` transaction, which inserts the row as `CREATING` with `Backfilling: false`, until the detached build task flips the flag, which is a window no client can time reliably rather than one that cannot exist. |

```bash
# View all settings
./target/release/extenddb settings --config extenddb.toml list

# View one setting
./target/release/extenddb settings --config extenddb.toml get log_level

# Change a setting
./target/release/extenddb settings --config extenddb.toml set log_level debug
```

`list` and `get` mask secret values (`encryption_key`, and any key whose name
contains `connection_string`, `password`, `secret` or `token`) as `••••••••`,
the same mask the management API and the console print. To print the stored
value of a secret setting, pass `--show-secret` to `get`:

```bash
./target/release/extenddb settings --config extenddb.toml get --show-secret encryption_key
```

The value is written to standard output in full, so keep it out of shell
history, CI logs and pasted transcripts.

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

#### What a trust-policy Deny matches

A role's trust policy decides who may assume the role. It is evaluated in IAM
order: a matching explicit `Deny` blocks assumption before any `Allow` is
considered, and with no matching `Allow` assumption is implicitly denied. The
decision is made for each STS action the call exercises: `sts:AssumeRole`
always, and `sts:TagSession` as well when the call passes session tags. Passing
session tags to a role whose trust policy does not allow `sts:TagSession` is
denied. ExtendDB does not implement source identity, so `sts:SetSourceIdentity`
is never part of the decision.

A statement matches when its `Principal` matches the caller, its `Action` (or
`NotAction`) covers the action, and all `Condition` blocks pass. `Effect` is
compared case-insensitively (`"deny"` works). A `Principal` matches on an exact
ARN, a list of ARNs, `{"AWS": "*"}`, a bare account id, or an account root ARN
(`arn:aws:iam::<account>:root`); the last two match every caller in that
account, for both `Allow` and `Deny`.

Principal ARNs are matched case-sensitively: a `Deny` naming `user/Alice` does
not block a caller asserted as `user/alice`. Assume authorizes on the trust
policy alone. The endpoint is admin-only and the admin asserts the caller ARN;
unlike IAM, ExtendDB does not also evaluate the caller's identity policy for
`sts:AssumeRole`, so a role that trusts an account root grants assumption to an
in-account caller on the strength of the trust policy by itself.

What a trust-policy `Deny` does not do:

- A statement whose `Condition` uses an operator ExtendDB does not implement
  fails closed: a `Deny` blocks assumption and an `Allow` grants nothing, rather
  than the operator being ignored. ExtendDB resolves every global key STS
  populates for an `AssumeRole` call, so no documented key fails closed:
  `aws:PrincipalTag/<key>` (tag key matched case-insensitively), `sts:ExternalId`,
  `aws:PrincipalArn`, `aws:PrincipalAccount`, `aws:ResourceAccount`,
  `aws:RequestTag/<key>`, `aws:TagKeys` (from the passed session tags),
  `aws:SecureTransport`, `aws:RequestedRegion`, `aws:CurrentTime`, `aws:EpochTime`,
  `aws:PrincipalType`, `aws:username` (for a user caller), `aws:userid`
  (`AIDA...` for a user, `<role-id>:<session-name>` for a role session),
  `aws:SourceIp` (the `AssumeRole` connection peer address), `aws:UserAgent` (the
  `User-Agent` header), `aws:ViaAWSService` and `aws:PrincipalIsAWSService`. The
  organization keys (`aws:PrincipalOrgID`, `aws:PrincipalOrgPaths`) and every
  other `aws:` global key the service does not populate for a direct caller (the
  VPC, federation, MFA and source-service keys) are absent, as on a standalone
  AWS account, and keep absent-key semantics. `IpAddress` and `NotIpAddress` are
  evaluated as IPv4 and IPv6 CIDR membership; `BinaryEquals` is accepted at role
  creation (IAM accepts it) but is not evaluated, so a statement using it fails
  closed. See the classified key list below.
- `NotPrincipal` is not allowed in a trust policy and is rejected when the role
  is created.
- A `Resource` field is not allowed in a trust policy and is rejected at create.
- An `Action` outside the STS trust set (`sts:AssumeRole`, `sts:TagSession`,
  `sts:SetSourceIdentity`), including `sts:*`, is rejected at create.
- A bare `"*"` principal string, or a wildcard inside a principal ARN
  (`user/dev-*`), is rejected at create. A principal naming a user or role that
  does not exist is accepted; the statement simply matches no caller (an `Allow`
  on it grants nothing, a `Deny` on it blocks no one).

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

#### Condition keys in identity policies

Identity-policy conditions on DynamoDB requests follow the same rule as trust
policies. A condition key the request resolves is evaluated normally. A key that
is absent follows IAM absent-key semantics, including that a negated operator
(`StringNotEquals`, `StringNotLike`, `ArnNotEquals`, `NotIpAddress`) on an
absent single-valued key matches, so a `Deny` gated on one applies when the
caller omits the key. ExtendDB resolves `aws:PrincipalArn`, `aws:PrincipalAccount`,
`aws:ResourceAccount`, `aws:PrincipalType`, `aws:username` (for a user caller),
`aws:userid`, `aws:SourceIp`, `aws:UserAgent`, `aws:SecureTransport`,
`aws:RequestedRegion`, `aws:CurrentTime`, `aws:EpochTime`, `aws:ViaAWSService`,
`aws:PrincipalIsAWSService`, `aws:PrincipalTag/<key>` and `aws:ResourceTag/<key>`
(tag keys matched case-insensitively) on a
data-plane request. On the operations that carry a tag set it also resolves
`aws:RequestTag/<key>` and `aws:TagKeys`: `TagResource` and `CreateTable` populate
both from `Tags`, `UntagResource` populates `aws:TagKeys` from `TagKeys`, and
every other operation leaves both absent. Every other `aws:` global key is
absent, which is what the service does for a key it does not populate for this
caller and for an unknown key.

#### Global condition key resolution

The complete list of AWS global condition keys, classified by how ExtendDB
resolves each for its callers (a signed IAM-user or assumed-role DynamoDB
request, or an `AssumeRole` call). ExtendDB behaves as a standalone AWS account
reached directly over its endpoint.

Resolved (evaluated against the request value):

- `aws:PrincipalArn`, `aws:PrincipalAccount`, `aws:PrincipalType`,
  `aws:username` (user caller only), `aws:userid`.
- `aws:ResourceAccount`.
- `aws:SourceIp` (connection peer address; `IpAddress` and `NotIpAddress` do
  IPv4 and IPv6 CIDR membership). This is the address of the TCP connection the
  request arrived on. Behind a reverse proxy or load balancer that is the
  proxy's address for every caller, and ExtendDB does not read `X-Forwarded-For`
  (the header is caller-supplied and spoofable), so an `aws:SourceIp` condition
  is meaningful only when clients connect to ExtendDB directly.
- `aws:UserAgent` (`User-Agent` header).
- `aws:SecureTransport`, `aws:RequestedRegion`, `aws:CurrentTime`,
  `aws:EpochTime`.
- `aws:ViaAWSService` and `aws:PrincipalIsAWSService` (both false for a direct
  caller).
- `aws:PrincipalTag/<key>`, `aws:ResourceTag/<key>`,
  `sts:ExternalId` (trust path).
- `aws:RequestTag/<key>` and `aws:TagKeys`: from the session tags on the trust
  path, and from the request's tags on a DynamoDB request that carries a tag set
  (`TagResource` and `CreateTable` populate both keys, `UntagResource` populates
  `aws:TagKeys`). Absent on every other operation.
- Role-session only (absent for an IAM user, matching the service):
  `aws:TokenIssueTime` (the session issue time) and
  `aws:MultiFactorAuthPresent` (`false` for a session assumed without MFA).

Absent (keep IAM absent-key semantics, as the service leaves them unset for
this caller):

- Organization keys: `aws:PrincipalOrgID`, `aws:PrincipalOrgPaths`,
  `aws:ResourceOrgID`, `aws:ResourceOrgPaths`, `aws:SourceOrgID`,
  `aws:SourceOrgPaths` (a standalone account has no organization). An ExtendDB
  account belongs to no organization, so a condition on an organization key
  behaves as it does for a standalone AWS account: the key is absent, a `Deny`
  that tests `aws:PrincipalOrgID` with a positive operator does not match (and
  so does not block), and a negated operator on it matches. Do not carry an
  organization-scoped `Deny` over from AWS expecting it to block on ExtendDB.
- Role-session and federation keys ExtendDB does not model:
  `aws:MultiFactorAuthAge`, `aws:FederatedProvider`,
  `aws:SourceIdentity`, `aws:AssumedRoot`, `aws:SignInSessionArn`.
  (`aws:TokenIssueTime` and `aws:MultiFactorAuthPresent` are resolved for a
  role session; see the resolved list above.)
- Network keys for VPC endpoints: `aws:SourceVpc`, `aws:SourceVpcArn`,
  `aws:SourceVpce`, `aws:VpcSourceIp`, `aws:VpceAccount`, `aws:VpceOrgID`,
  `aws:VpceOrgPaths`, `aws:Ec2InstanceSourceVpc`,
  `aws:Ec2InstanceSourcePrivateIPv4`.
- Service-principal and cross-service keys: `aws:PrincipalServiceName`,
  `aws:PrincipalServiceNamesList`, `aws:SourceAccount`, `aws:SourceArn`,
  `aws:CalledVia`, `aws:CalledViaFirst`, `aws:CalledViaLast`, `aws:referer`,
  and the service-prefixed keys (`ec2:`, `glue:`, `codebuild:`, `lambda:`,
  `ssm:`, `identitystore:`).
- Any `aws:` key AWS defines that is not in the resolved set, and any unknown
  `aws:` key, which the service also treats as absent.

Fail closed (a statement using one does not evaluate): none. The resolved and
absent sets cover every documented global key for ExtendDB's callers. The
evaluator keeps an explicit, currently empty, fail-closed list so a future
global key the service begins populating before ExtendDB resolves it can be
added rather than silently granting through a negated operator. An operator the
evaluator does not implement (for example `BinaryEquals`) still fails closed.

`aws:TokenIssueTime` is populated by the service for a role session and absent
for an IAM user. ExtendDB resolves it for a role session from the session issue
time and leaves it absent for an IAM user, matching the service. A policy that
gates on its presence or compares it with a `Date` operator behaves as it does
on AWS for a role session. This resolution is on the DynamoDB request path; in a
trust policy evaluated during `AssumeRole` the key is absent, as it is on the
service for an `AssumeRole` caller using long-term credentials.

`aws:MultiFactorAuthPresent` is populated by the service for a role session
(`false` for a session assumed without MFA) and absent for an IAM user with
long-term keys. ExtendDB resolves it to `false` for a role session and leaves it
absent for an IAM user, so the common deny-without-MFA guard
(`Deny if Bool aws:MultiFactorAuthPresent=false`) applies to a role session and
is inert for an IAM user, matching the service. `aws:MultiFactorAuthAge` stays
absent, as the service leaves it unset for a non-MFA session. Like
`aws:TokenIssueTime`, `aws:MultiFactorAuthPresent` is resolved on the DynamoDB
request path only; in a trust policy it is absent, as on the service for an
`AssumeRole` caller using long-term credentials.

### Resource-level authorization

Every operation is authorized against the specific resource ARN, matching
DynamoDB. The `Resource` element of a policy is matched field by field, and a
`*` or `?` in the resource part of the ARN spans the `:` and `/` inside it.

| Operation | Resource ARN authorized |
|-----------|-------------------------|
| Item and table operations (GetItem, PutItem, Query and Scan on the base table, CreateTable, UpdateTable, DescribeTable, and so on) | `table/<name>` |
| Query and Scan with an `IndexName` | `table/<name>/index/<index>` |
| TagResource, UntagResource, ListTagsOfResource | the `ResourceArn` (a table or stream ARN) |
| DescribeStream, GetShardIterator | the `StreamArn` |
| GetRecords | the stream the shard iterator reads |
| ListStreams | `table/<name>/stream/*` when a table is named, else `table/*/stream/*` |
| DescribeBackup, DeleteBackup | the `BackupArn` |
| CreateBackup, DescribeContinuousBackups, UpdateContinuousBackups | `table/<name>` |
| ExportTableToPointInTime | the source `table/<name>` |
| ImportTable | the new `table/<name>` |
| RestoreTableFromBackup | the backup ARN, plus Scan, Query, GetItem, PutItem, UpdateItem, DeleteItem and BatchWriteItem on the target `table/<name>` |
| RestoreTableToPointInTime | the source `table/<name>`, plus the same item actions on the target `table/<name>` |
| ListTables | `table/*` |
| DescribeLimits | `*` |
| DescribeEndpoints | not authorized (signed request, no IAM permission) |

A policy that names another partition, service, region, or account in a
resource ARN it supplies (for example on a stream or backup operation) cannot
evade a Deny written against the real resource: the resource is resolved in this
deployment's account regardless of those fields.

#### Compatibility notes

Earlier builds authorized several of these operations against
`table/*` instead of the specific resource. Review existing policies before
upgrading:

- **Index reads.** Query and Scan with an `IndexName` are authorized against
  `table/<name>/index/<index>`. A policy that granted a principal only
  `table/<name>` no longer allows reads of that table's indexes. Grant
  `table/<name>/index/<index>` or `table/<name>/index/*` as needed. This
  matches DynamoDB.
- **Fencing a whole table with a Deny.** Index reads, `ListStreams` with a
  TableName, and `ListBackups` with a TableName are authorized against a
  sub-resource ARN (`table/<name>/index/*`, `table/<name>/stream/*`,
  `table/<name>/backup/*`), not the bare `table/<name>`. A `Deny` written only
  on `table/<name>` therefore does not cover them. An operator who means to
  fence a whole table adds `table/<name>/*` to the `Deny` alongside
  `table/<name>`.
- **Index reads and resource tags.** An index Query or Scan evaluates the base
  table's resource tags (`aws:ResourceTag/*`; `dynamodb:ResourceTag/*` is
  absent for every resource),
  because a secondary index inherits the tags of its table. An ABAC `Deny` or
  `Allow` conditioned on the table's tags applies to its indexes.
- **Streams, backups, and resource tags.** An operation on a stream ARN
  (DescribeStream, GetShardIterator, GetRecords, and a tag operation on the
  stream) and an operation on a backup ARN evaluate no resource tags, as on
  DynamoDB: `aws:ResourceTag/*` and `dynamodb:ResourceTag/*` are absent. A
  stream holds a tag set of its own, separate from its table's, but neither set
  is evaluated for the stream's operations. A condition on the key evaluates as
  for an absent key: `StringEquals` and `StringLike` do not match, while a
  negated operator, an `IfExists` form, `ForAllValues`, and `Null` true do. A
  backup ARN cannot be tagged.
- **DescribeLimits and DescribeEndpoints.** DescribeLimits is authorized against
  `*`, not `table/*`, so a grant or a `Deny` scoped to `table/*` no longer
  applies to it; scope its statement to `*`. DescribeEndpoints is not evaluated
  against IAM at all (it needs a signed request but no permission), so a `Deny`
  on it does not block it, matching DynamoDB.
- **Streams.** A policy whose resource is `table/<name>/stream/*` now matches
  concrete stream ARNs (the stream label contains `:`, which the resource
  wildcard now spans). A Deny on a specific stream ARN, or on a tag, backup, or
  index ARN, now takes effect where before it was bypassed.
- **GetRecords.** GetRecords is authorized against the stream its shard iterator
  reads, so a principal needs `dynamodb:GetRecords` on that stream. A shard
  iterator issued by a build before this change does not identify its stream;
  after upgrade, GetRecords on such an iterator returns
  `ExpiredIteratorException` with the expiry message, so a stream client renews
  it by calling GetShardIterator rather than failing on a non-retryable error.
  This matters during the upgrade itself: a consumer reading when the server
  restarts holds a pre-upgrade iterator, and its iterator chain renews cleanly
  instead of stalling. A token that does not decode at all still returns
  `ValidationException` with the message `Invalid ShardIterator`, which is what
  DynamoDB returns for an iterator it did not issue. Shard iterators expire after
  15 minutes.
- **Restores.** A restore writes items into its target table, so
  RestoreTableFromBackup and RestoreTableToPointInTime require the item write
  and read actions on the target table in addition to the restore action. A
  principal that could previously restore with only the restore permission now
  also needs those actions on the target table name.
- **Denial messages.** An AccessDeniedException now ends with the clause the
  service uses: "with an explicit deny in an identity-based policy" when an
  explicit Deny matched the request, or "because no identity-based policy allows
  the <action> action" otherwise. Earlier builds returned the common prefix with
  no trailing clause and did not distinguish the two cases. The error code is
  unchanged, so a client that checks the code or the substring "is not
  authorized to perform" is unaffected; tooling that matched the full message
  text verbatim will see the new clause.

#### Backup and restore permissions

The backup operations are authorized against the resources in the table
above (measured on DynamoDB, 2026-10-20). In summary, each pair must be allowed:

| Operation | Action and resource |
|---|---|
| `CreateBackup` | `dynamodb:CreateBackup` on `arn:aws:dynamodb:<region>:<account>:table/<TableName>` |
| `DescribeBackup`, `DeleteBackup` | that action on the backup ARN, `.../table/<TableName>/backup/<id>` |
| `ListBackups` | `dynamodb:ListBackups` on `.../table/*/backup/*`, or on `.../table/<TableName>/backup/*` when `TableName` is given |
| `RestoreTableFromBackup` | `dynamodb:RestoreTableFromBackup` on the backup ARN, and `dynamodb:Scan`, `dynamodb:Query`, `dynamodb:GetItem`, `dynamodb:PutItem`, `dynamodb:UpdateItem`, `dynamodb:DeleteItem`, and `dynamodb:BatchWriteItem` on `.../table/<TargetTableName>`. `dynamodb:RestoreTableFromBackup` is not evaluated on the target table |

A `*` in a `Resource` crosses `/`, so `Resource: "arn:aws:dynamodb:*:*:table/*"`
(or `"*"`) covers tables and their backups alike. A table's own ARN,
`table/Orders`, does not cover `table/Orders/backup/...`; use
`table/Orders/backup/*` for its backups. A policy that lets a user take, list
and restore the backups of one table into a fixed set of names:

```bash
./target/release/extenddb manage --user admin --password <pw> \
    put-user-policy --account-id 123456789012 \
    --user-name backup-operator \
    --policy-name OrdersBackups \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": "dynamodb:CreateBackup",
          "Resource": "arn:aws:dynamodb:*:123456789012:table/Orders"
        },
        {
          "Effect": "Allow",
          "Action": ["dynamodb:ListBackups", "dynamodb:DescribeBackup",
                     "dynamodb:DeleteBackup", "dynamodb:RestoreTableFromBackup"],
          "Resource": "arn:aws:dynamodb:*:123456789012:table/Orders/backup/*"
        },
        {
          "Effect": "Allow",
          "Action": ["dynamodb:Scan", "dynamodb:Query", "dynamodb:UpdateItem",
                     "dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:DeleteItem",
                     "dynamodb:BatchWriteItem"],
          "Resource": "arn:aws:dynamodb:*:123456789012:table/Orders-restored-*"
        }
      ]
    }'
```

With this policy `ListBackups` must carry `TableName: Orders` (the unfiltered
list is `table/*/backup/*`, which the second statement does not cover), and a
restore into any name other than `Orders-restored-*` is refused naming the
first item action denied (`dynamodb:Scan on resource: ...table/<target>`).

A backup ARN is evaluated as the backup it names in this deployment: the
table and backup id from the request, in the server's region and the
caller's account, whatever region or account the caller wrote (see the
paragraph above the compatibility notes). A statement that names a backup ARN
should therefore spell the server's region, or use `*` in the region field as
the example does. The engine resolves a `BackupArn` of the caller's account
under the server's region first and, if that names no backup, under the ARN as
written (a backup created under an earlier `server.region`; see the upgrade
manual); both are the same backup identity, which is what was evaluated. A
`CreateBackup` whose `TableName` is the table's ARN is evaluated on the
`table/<name>` the ARN names.

The seven item-action evaluations on the restore target read the target
table's resource tags: a target that does not exist has none, so an
`aws:ResourceTag/<key>` condition on those actions does not grant the
restore; a target that exists is evaluated with its tags, and a restore the
condition then allows, and that is otherwise valid, fails with
`TableAlreadyExistsException` (`TableInUseException` for a `CREATING` restore
target). Every
DynamoDB condition key (`dynamodb:LeadingKeys`, `dynamodb:Attributes`,
`dynamodb:Select`, `dynamodb:ReturnValues`, `dynamodb:ReturnConsumedCapacity`,
`dynamodb:FullTableScan`, `dynamodb:EnclosingOperation`) is absent in these
seven evaluations, so `ForAllValues:*` and `Null: true` conditions pass and
bare `StringEquals` and `Bool` conditions fail. In particular an `Allow` of
the item actions conditioned on `ForAllValues:StringEquals
dynamodb:LeadingKeys` (the row-level pattern in the troubleshooting guide)
grants a restore of every item in the backup into any table the statement's
`Resource` covers, and an `Allow` of `dynamodb:Scan` conditioned on
`Bool dynamodb:FullTableScan: false` blocks every restore, while a `Deny` of
`dynamodb:Scan` conditioned on `Bool dynamodb:FullTableScan: true` blocks
none. Grant the item actions for restores on target names of their own, as
the example does. The keys the service sets in these evaluations were not
measured.

The request-level keys (`aws:SourceIp`, `aws:UserAgent`, `aws:SecureTransport`,
`aws:RequestedRegion`, and the principal keys above) are evaluated on every
backup and restore evaluation, as on any other request.

These resources have been evaluated since v0.1.14; the compatibility notes
above say what earlier builds evaluated. A `Deny` of
`dynamodb:RestoreTableFromBackup` on a table does not block restores into it;
deny `dynamodb:PutItem` on it instead.

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
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/metrics
```

JSON metrics endpoint with DynamoDB CloudWatch-style metric names and dimensions. The response shape is `{ metrics, buckets, segments, source }`. See `docs/design/06-component-server.md` §7.2 for the full schema and metric list.

### Health Check

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health
# {"status":"healthy","components":{}}
```

The `components` object carries the `backup_store` component when a backup
store is configured (see [Health Check](#health-check) under Server Lifecycle).

## Troubleshooting

### Vector Index Support (PostgreSQL)

Vector indexes are stored in `vector(N)` columns, a type the [pgvector](https://github.com/pgvector/pgvector) extension provides, so support is a property of the PostgreSQL server rather than of the ExtendDB build. Two things follow.

**`extenddb init` and `extenddb migrate` try to install it.** Both print one of:

```
--- Checking pgvector extension on the data database...
    pgvector available; vector indexes are supported.
```

```
--- Checking pgvector extension on the data database...
    NOTICE: could not create the pgvector extension (...). <what to do>
```

The notice is not a failure. Initialisation and migration complete either way, and every operation other than a vector one is unaffected; the server simply refuses vector indexes. The advice depends on why it failed: a missing server package (install for example `postgresql-16-pgvector`), or a role that may not create extensions (create it once as a superuser or as the database owner).

Note that the extension is installed on the **data** database, not the catalog.

**Installing pgvector on a running server needs an ExtendDB restart.** The server probes for the extension once at startup and caches the answer, so a server that started without it keeps refusing vector indexes until restarted. Confirm what a running server decided by checking its startup log:

```
pgvector 0.8.0 detected on the data database; vector index storage available
pgvector not installed on the data database; vector indexes are not supported
```

### Server Won't Start

**Port already in use:**

```
Error: Address already in use (os error 98)
```

Another process is using the port. Find it with `ss -tlnp | grep :18443` and stop it, or change the port in `extenddb.toml`.

**Database connection failed:**

```
Error: error communicating with database
```

Check that PostgreSQL is running and the connection string in `extenddb.toml` is correct.

**The catalog has pending migrations, or is ahead of the binary:**

On PostgreSQL and SQLite, `serve` reads the catalog's migration status before it opens the engine and refuses a catalog it cannot serve, printing the status and the command that resolves it. A catalog with pending migrations exits 3, for example `the catalog is at migration 3, and ExtendDB <release> needs migration 4`, followed by the pending migrations and `extenddb migrate --config extenddb.toml --yes`. A catalog that a newer release migrated with a breaking migration exits 4 and names that release's migrations; upgrade the binary rather than the catalog. The [Upgrade Manual](07-upgrade-manual.md#the-catalog-status) lists every status with its exit code and the stop / migrate / start sequence. On Cassandra and MongoDB the catalog checks are those of the 0.1.x releases; their sections of the Upgrade Manual describe them.

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
