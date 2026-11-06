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
8. Spawns background tasks (log level polling, stream cleanup, TTL expiry, bulk operation workers)

The daemon keeps the directory `serve` was started in as its working
directory, so a relative path in the configuration or in the environment
resolves against that directory, as it does with `--foreground`.
`extenddb stop` and `extenddb status` resolve a relative `run_dir` against
the directory they run in, where they look for the PID file; give `run_dir`
as an absolute path, or run them from the directory `serve` was started in.

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

extenddb handles SIGTERM and SIGINT gracefully: it drains active connections for up to 5 seconds (the TLS listener and the plain-HTTP listener of a dev-mode build alike), then waits up to 30 seconds for its running backup jobs to end (a job the stop cancels runs its failure path, which marks its row FAILED and deletes what it wrote to the store; on a large S3 prefix that takes longer than the other bounds), then up to 10 seconds for its registration supervisor to return, up to 16 seconds to stop its background workers (5 seconds for them to return, 1 second for the ones then aborted, and 10 seconds more behind a stop the supervisor was still running), up to 10 seconds to give up its serving registration (kept, to expire by itself, while a background worker or a backup job may still run), and 5 seconds for its runtime's blocking threads, and exits. With no backup job running the job wait ends at once, and in an ordinary stop the supervisor, the workers and the release each take moments. A `CreateBackup` that is still waiting in its snapshot open or row insert when the stop begins writes no row, and its client sees the connection closed by the drain (measured: 8.2 s after the request was sent): the jobs' token, which turns such a request into `ServiceUnavailable` "The server is shutting down. Retry the request.", is cancelled after the drain, so that answer reaches a client only after a registration loss, when the token is cancelled while the listener still serves. `extenddb stop` waits up to 76 seconds, the sum of these bounds, before it reports that the server did not exit.

A bulk operation running at that moment writes the rows it has staged,
checkpoints, and releases its lease, so the next process to claim it resumes
it at once. The server waits 1 second for its runs to do this. A run that
has not stopped by then, such as one inside a database or bulk store call
or one held by the write pacer, keeps its lease until the lease runs out,
and the next claim then resumes it from its last checkpoint. The rows it
committed are recorded, so the resume does not write them again.

Note that `extenddb stop` reads the PID file, which a server started with
`serve --foreground` does not write by default. Stop a foreground server through
whatever supervises it (container runtime, systemd, your shell) or send it
SIGTERM directly. Alternatively start it with
`serve --foreground --write-pid-file`, which writes the PID file to the usual
`run_dir` path so `stop` and `status` work as they do in daemon mode.

### Health Check

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health
# {"components":{},"status":"healthy"}
# (the order of the keys depends on the build: the default PostgreSQL build
# writes "status" first, the SQLite build "components" first; read the keys)
# with a backup store configured:
# {"components":{"backup_store":{"kind":"filesystem","status":"healthy"}},"status":"healthy"}
# while it cannot be reached:
# {"components":{"backup_store":{"kind":"s3","reason":"unreachable","status":"unhealthy"}},"status":"degraded"}
# right after startup, before an S3 store's first check has returned:
# {"components":{"backup_store":{"kind":"s3","status":"checking"}},"status":"healthy"}
```

`status` is `healthy`, or `degraded` when a component is unhealthy; the HTTP
status stays 200 either way, because a component problem never takes the data
plane out of rotation. The one component is `backup_store`, present whenever
the server has a store: one `[backup.store]` configures, or the default root
beside a file-backed SQLite database when the section is absent (absent when
no store is configured, when `kind = "disabled"`, and when a default root
failed its check with no rows for it); its `reason` is the problem's category in
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

Backup and restore jobs are bounded per server process by `[backup] max_concurrent_jobs` (default 4, below), not per account; a request that finds every slot held waits up to 30 s for one and is then refused with `LimitExceededException`. The server raises `pool_size` and `catalog_pool_size` to at least `2 * max_concurrent_jobs + 10` and logs the raise with both numbers, so every job the bound admits has a connection. Each running restore holds one connection from this pool for its whole copy, plus one dedicated connection outside the pool for its claim.

A `StartBulkPutItems` on PostgreSQL waits at most 200 ms at a time for its account's lock and does not wait for its table's row: when another transaction holds the row, the start returns its connection to this pool and tries again. `UpdateTable`, `DeleteTable`, `UpdateTimeToLive`, the TTL index build and the size refresh every 300 seconds each hold the row for a few statements. Starts that wait behind such a request therefore do not hold every connection it needs, do not delay the account's starts on other tables, and do not keep the request waiting. A start that has not got both locks after 30 seconds answers `ServiceUnavailable`, which SDKs retry.

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
| `data_file_target_bytes` | `67108864` | Uncompressed bytes per backup data file (minimum 32 MiB, the cap on one line): a data file closes at the first item that takes it to or past this, so it exceeds it by less than one line |
| `heartbeat_interval_secs` | `15` | The job heartbeat timer. The store-backed backup job heartbeats every `max(heartbeat_interval_secs, 15)` s and starts no store write unless a beat confirmed its claim within two periods or a beat is still in flight (on SQLite a beat waits behind any writer holding the engine write lock, a GSI backfill say, for as long as it holds it, and a backup goes on writing meanwhile; on the other backends a beat in flight does not count), the claim's insert counting as the first beat; the in-catalog backup and restore paths heartbeat every 15 s whatever this says |
| `stale_job_after_secs` | `300` | Heartbeat age after which a job's owner is dead (on PostgreSQL, once the owner's claim session has ended as well) and the reconciler resolves the job: a dead backup job with a complete manifest under its prefix is AVAILABLE, one without is FAILED and its prefix deleted; a dead restore job is FAILED and its target removed. Must be at least three heartbeat periods, where the period is the larger of `heartbeat_interval_secs` and the 15 s the in-catalog jobs heartbeat at (so 45 at the least, whatever `heartbeat_interval_secs` says); on SQLite a long write transaction delays a heartbeat, so keep a margin above that floor |
| `failed_row_retention_secs` | `604800` | How long FAILED and DELETED backup rows of the configured store are kept once their objects are gone (7 days); `extenddb backup list --failed` prints the FAILED ones with their reason. At least `stale_job_after_secs`: the reconciler deletes a stopped job's late object only under its row, so the row outlives the window in which that job can still write; a smaller value is refused at startup |
| `reconcile_interval_secs` | `60` | The reconciler's cycle; it also runs once at startup. Each cycle checks the store, resolves backup and restore jobs whose owner is dead, finishes deletions, deletes the objects of FAILED backups once their heartbeat is older than `stale_job_after_secs`, and removes tombstones past `failed_row_retention_secs` (the differences document's restore-interruption row lists the duties) |
| `max_concurrent_jobs` | `4` | Backup and restore jobs this instance runs at once (at most 1024); a request over it waits up to 30 s for a slot, then `LimitExceededException`. On PostgreSQL the pool sizes are raised to `2 * max_concurrent_jobs + 10` when smaller; on SQLite `[storage.sqlite] pool_size` is raised to `2 * (max_concurrent_jobs + 1)`, at most 34, when its snapshot slots (`pool_size / 2 - 1`, between 1 and 16) would refuse a job this knob admits. Memory: a running backup holds about 490 MiB (PostgreSQL) or 650 MiB (SQLite) at most in the pipeline's own terms for a table of items at the 32 MiB line cap, a few tens of MiB for 200-byte items (the figures are for the default `max_item_size_bytes`, 400 KB, where no stored item's line exceeds the cap; on an instance that raises it the terms are at the largest line a stored item expands to, about 68 times the item's size for the worst shape); with glibc's default allocator the process's high-water mark grows with the number of large items a job reads (measured 760 MiB over idle at 48 items of 27.7 MB on a quiet host and 1,281 MiB on a loaded one, 1.39 and 2.35 times the terms at that item size) because the items' strings are allocated on one thread and freed on another and the per-thread arenas keep that memory. Set `MALLOC_ARENA_MAX=1` in the server's environment (the deployment guide's service unit carries the line, commented out) on a host that backs up large items: the peak is then the terms' and does not grow with the table (measured 452 to 481 MiB over idle at 12, 24 and 48 items of 27.7 MB on SQLite, in two series on two hosts' loads, and 340 MiB `VmHWM` (about 200 MiB over idle) at 12 items on PostgreSQL); one arena serializes the worker threads' allocations on one lock and costs the request path a few percent (measured 3.5 to 9.2% on GetItem, Query and Scan on SQLite and about 2% on PostgreSQL (the scans, 2.1%), where the request path waits on the database on PostgreSQL). Size the host for 1.2 times the terms (about 590 MiB on PostgreSQL, 780 MiB on SQLite) times this knob under `MALLOC_ARENA_MAX=1`; without it the peak grows with the large rows a job reads and no bound was measured (1,281 MiB over idle at 48 items of 27.7 MB is the largest seen) |
| `spool_path` | (unset) | Restore download area for an S3 store, created `0700` with files `0600`, holding a `.lock` another instance on the host refuses. Required with an S3 store, except on file-backed SQLite where it defaults to `backup-spool/` beside the database file. Refused inside or containing a filesystem store root, and when it is or contains the database file's directory |

#### [backup.store]

The store CreateBackup writes to on PostgreSQL, SQLite and MongoDB, and the one restores and deletes of store-backed backups read. The section's keys, defaults and the refusals of a bad value are the backup store crate's (`crates/backup-store/src/config.rs`); the table below is what the server adds. One definition of the shape exists, so an unknown key anywhere under `[backup]` is refused at load.

| Key | Default | Description |
|-----|---------|-------------|
| `kind` | see below | `filesystem`, `s3`, or `disabled` |
| `filesystem.path` | `backups/` beside a SQLite database file | The root directory of a filesystem store. It must exist, be owned by the extenddb user with mode `0700`, and sit under directories owned by root or that user which others cannot write to; `validate` refuses anything else at startup. A relative path, like a relative `spool_path`, is resolved against the working directory the server was started from (the daemon's own working directory is `/`) |
| `s3.bucket`, `s3.prefix`, `s3.region`, `s3.endpoint`, `s3.force_path_style`, `s3.allow_insecure_endpoint`, `s3.upload_part_size_bytes`, `s3.max_concurrent_uploads` | | The S3 store; see the crate's `config` module doc. Credentials come from the AWS SDK's chain and are never in the file |

When the section is absent, the backend decides: on file-backed SQLite it is a filesystem store at `backups/` beside the database file, created `0700` if missing, whose validation or identity-check failure is a logged warning that leaves the store unconfigured rather than a startup error or an unhealthy store while the catalog has no backup rows for that root (so an upgrade never stops a server that never configured backups, and CreateBackup keeps working on one); once the catalog has rows for the default root, the root has been used and is handled as a configured one: a failure of its identity check or manifest probe is an unhealthy store (reported on `/health`, its rows' restores and deletes refused, `backup purge` refusing on it rather than reporting no store), and a refusal of its `open` or validation a startup error naming the root; on PostgreSQL, MongoDB, Cassandra and an in-memory SQLite database there is no default and the store is unconfigured, logged at startup. `kind = "disabled"` leaves it unconfigured on purpose on any backend. Without a configured store, CreateBackup writes the in-catalog backup format and logs a deprecation warning naming `[backup.store]` on each call. With one configured, CreateBackup on PostgreSQL, SQLite and MongoDB writes the backup to the store: the request takes the table's snapshot (after its checks and after any wait for a job permit, up to 30 s, so the backup is the table at that open and a write acknowledged during the wait is in it), writes the catalog row and answers `CREATING`, and a job of the server uploads gzip data files and the manifest under `<account id>/<table name>/<backup id>/` below the root and marks the row `AVAILABLE` (the job's log lines carry the ARN: `backup <arn>: started, <n> items in the table as last counted by the stats pass, key-ordered` (or `unordered`, for a table whose key shape or column collation has no format order) at the start, the stats pass's count, which is 0 on a table the pass has not reached, and `backup <arn>: AVAILABLE, <n> items, <bytes> bytes written in <s> s` at the end, with the backup's own count); Cassandra, which has no table snapshot, keeps the in-catalog create with a store configured. Backups of either format stay listable, describable and deletable, and in-catalog backups restorable; the restore of a store-backed backup is a later release's and answers `ValidationException` until then.

Startup opens the configured store and, for a filesystem store, validates it and checks its identity before the listener is up (bounded by 5 s: a mount that stops answering after the root is opened leaves the server starting with the store `checking`; a root that does not answer at startup holds the server before the listener, as the database directory would, with the port bound and silent); an S3 store's validation and identity check are requests to the endpoint, and run after the listener is up, in the reconciler's first cycle, so an endpoint that stalls never holds the data plane down (the component is `checking` until the first result, and the operations that touch the store refuse meanwhile as they do while it is unhealthy). A configuration error, every one decided without a request to the store, stops startup with the key, path or bucket in the message: a root that does not exist or fails the owner-only rule, a filesystem root another user can reach, a filesystem root that is or contains the database directory, a bucket name the SDK routes to S3 Express, a bad part size or prefix, an insecure endpoint, an S3 store without `spool_path` on a backend with no default, a `spool_path` that breaks its placement rule, that another live instance holds or that exists with a mode other than `0700`. A transport failure (no region, no credentials, an endpoint that refuses connections or does not answer within the check's bound), a missing or denied bucket, an I/O failure of a filesystem root's probe write or resolution (a full or read-only disk, a stale network handle), or a failed identity check leaves the server running with the store unhealthy: the problem is logged once at error, `/health` reports the `backup_store` component unhealthy (status `degraded`), CreateBackup (where it writes to the store) and the restore and delete of a store-backed backup are refused with a `ValidationException` that names the store's kind and the problem's category and not its root, the reconciler acts on no row of the store, and the check runs again each `reconcile_interval_secs` until it passes, at which point the server logs `is healthy again` and resumes without a restart. Each check's store calls are bounded (5 s for a filesystem root, 30 s for a bucket); the catalog's own reads (the catalog id, the rows' store ids, the newest AVAILABLE row) run beside the calls and not under the bound, so a slow catalog costs the check time and never marks the store unhealthy. A store that has stopped answering costs a reconciler cycle that bound and nothing more, the cycle's other duties run as before, and the abandoned check is logged; on a filesystem root the abandoned check's call stays parked on one thread until the mount answers, no later cycle starts a check while it is (logged each cycle, the store staying `unreachable`), a root's calls run one at a time so that calls started before the first reached its bound (two `DeleteBackup` requests together, a `DeleteBackup` beside a check) wait for it under the bound, then have the bound for their own call (a slow call behind another finishes, where one bound over both would abandon it), and park no thread of their own, and `stop` or SIGTERM still ends the server, the runtime giving that thread 5 s before the process exits without it (after the job wait of up to 30 s when a backup job is running; see Stopping). The deletions `DeleteBackup` and the reconciler run against the store go through the same bound and rule: `DeleteBackup` answers the description with `BackupStatus` `DELETED` once its DELETING write has committed, and a deletion the store does not answer within the bound, or that leaves an object under the prefix, leaves the row DELETING for the reconciler and is logged, never an error to the client. A default SQLite root whose startup checks did not finish starts `checking` with its rule kept: a first check that fails while the catalog has no rows for it is the same warning and leaves the store unconfigured. CreateBackup on Cassandra is not refused by an unhealthy store: its in-catalog create touches no store.

The store identity. The root carries `extenddb-store.json`: a random store id and the id of the catalog that owns the root. Startup creates it when the root has no file and the catalog has no rows for the root, and otherwise reads it and compares: a file naming another catalog, a store id that differs from the one the catalog's rows for the root carry, a root with no file while rows exist, or rows carrying more than one id each mark the store unhealthy. Instances that share a catalog must share one store root (one S3 bucket and prefix, or one shared mount): host-local roots configured at the same path give each instance different bytes under one root label, so each check also `head`s the manifest of the newest AVAILABLE backup and marks the store unhealthy when it is absent, which is what a second instance with a local copy of the directory sees. The file is left by `backup purge` and by every deletion; remove it only when retiring the root, or after `extenddb destroy`: a catalog re-created with `init` has a new id, and the old root's file names the old one, so the re-initialised server reports the store owned by another catalog until the file is removed or a new root configured (on the default SQLite root, that is the logged warning above and CreateBackup keeps the in-catalog path). While the catalog has no AVAILABLE backup for a root, two host-local directories at one path cannot be told apart (each instance creates its own file and id, and a file copied from one directory to the other gives both one id), and the manifest probe has nothing to probe: a backup one instance completed under its own directory looks, to the other instance's reconciler, like a dead owner's row with no manifest, which it marks FAILED, and the first instance's restart then deletes the only copy. Point every instance at one shared root before the first backup; copying `extenddb-store.json` between directories defeats the check rather than satisfying it. The identity file is read up to 64 KiB; a larger object under its key is reported unreadable.

`extenddb destroy` removes the databases and leaves the store untouched: it is the copy that survives destroy. `extenddb backup list [--failed]` prints the catalog's rows for the configured store's root, every store id they carry, with each row's `store_root` and `store_id`; it is a read that runs beside a server (on SQLite it takes no serve lock), reads the identity file without creating one, and runs on an unhealthy store, naming the problem in its header. `extenddb backup purge --confirm` deletes every backup of the configured store, each backup's objects (its own prefix, never a whole account prefix, so a prefix the catalog has no row for survives) and its catalog row outright, leaving no tombstone, and leaves `extenddb-store.json` and the `exports/` component; it needs the store healthy. It takes the catalog-level maintenance claim first and heartbeats it for the command's whole run, waits up to `2 * stale_job_after_secs` until no store-backed CreateBackup is running on any instance (a CREATING or FAILED row with a heartbeat within `stale_job_after_secs` on the catalog database's clock) and no restore is RUNNING, refuses and releases the claim if they have not finished, deletes a CREATING row the wait saw no live owner for as it deletes any other row, finishes any DeleteBackup left in progress as part of the purge (a backup a DeleteBackup finishes while the purge runs is counted as deleted, not left), and logs each deletion under the `extenddb::audit` target with the account and the ARN. While the claim is held, CreateBackup and RestoreTableFromBackup on every instance answer `LimitExceededException` saying that a backup maintenance command holds the catalog's maintenance claim (the message does not say which command; the second command's refusal and the claim's row do), and a second maintenance command fails at once naming the holder. Without `--confirm` it prints the plan, changes nothing and exits 0. `--config` is accepted before or after the subcommand. Neither command runs the startup recovery a server runs (on PostgreSQL the vector index rebuild, the control-plane recovery and the GSI queue workers; on MongoDB the key-repair pass; on Cassandra the control-plane transitions; on SQLite the interrupted-restore and index passes): that work belongs to the servers, and a command that ran it could exit with it half done. RUNNING restores are read by status, with no age, so a restore whose record was stamped this instant holds the wait too. On SQLite `purge --confirm` opens the database under the serve lock: stop the server first (while one runs on the file the command refuses with exit 6, as a second `serve` does); under that lock it first resolves what the stopped server left (a RUNNING restore is failed and its target removed, the CREATING target of a FAILED restore removed, an in-catalog CREATING backup removed, a DELETING row finished) and deletes a CREATING or FAILED row whatever its heartbeat says, a heartbeat stamped ahead of the clock by a backward clock step included (the server that wrote it is stopped, so the zero window the command passes selects by status alone), so such a job never makes it wait or refuse; the one exception is a RUNNING restore of kind `manifest`, which no release before PR 9a writes: the command leaves it for the reconciler that resumes it, waits on it as on any RUNNING restore and refuses naming the table; and a server starting on the file removes a claim a killed purge left, so no job is refused for the stale window, as a purge run again after the killed one takes its claim over. On the other backends it runs beside live servers, and a claim whose holder died expires after `stale_job_after_secs`. On PostgreSQL, MongoDB and Cassandra both commands open the catalog as a server does and hold a serving registration for their run, named after the command (`extenddb backup list`, `extenddb backup purge`: on PostgreSQL the session's `application_name`, on MongoDB the `command` field of its `extenddb_servers` document, on Cassandra the `release` column of its `extenddb_servers` row, after the release and ` as `), and release it when they exit: `extenddb migrate --yes` waits for them as for a server and names them while it waits, and while a migration runs they refuse at once (exit 6), as they do on a catalog at another migration than the binary's (exit 3, with the `extenddb migrate --yes` advice `serve` prints). On every backend Ctrl-C or `SIGTERM` makes the command release what it holds and exit: the serving registration after the open, if the signal came during it; the claim at once while it waits, and before its next backup while it deletes (the release itself is bounded by 10 s, past which the claim is left to the stale rule), so an interrupted purge leaves no half-deleted backup and no claim to wait out; run it again for the rest. The dry run on an unhealthy store lists the rows and says that `--confirm` refuses until the store's check passes.

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
| `max_entries` | `10000` | Maximum entries per cache. |

Statistics are exposed at `/management/auth-cache-metrics` (JSON, admin-authenticated). The per-cache `pass_through` flag distinguishes "cache disabled" from "cache cold." The report includes every authorization sub-cache, among them the two principal creation-time caches (`user_created_at`, `role_created_at`) consulted on each user and role-session request to derive `aws:userid` and `aws:TokenIssueTime`.

**Invalidation timing**:

- **Single-key invalidations** (e.g. `DeleteAccessKey`, `PutUserPolicy`) drop the cached entry immediately; the next request sees the post-mutation state.
- **Fanout invalidations** (e.g. `DeleteAccount`, `DeleteRole` session sweep, `DeleteGroup` member fanout) also take effect when the API returns. The account and role sweeps register a predicate with moka, and moka's `get` applies it, so matching entries are not served once the call returns; moka removes them from its map during later cache operations. The group fanout drops each member's entries in turn. If a group change cannot list the group's members, the server logs a warning and those members' entries wait for `ttl_seconds`.
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

The bulk operation keys below are the exception: they are extenddb's own,
DynamoDB exposes no equivalent, and they govern `StartBulkPutItems` and the
workers that drive it.

| Key | Default | Description |
|-----|---------|-------------|
| `max_concurrent_bulk_operations` | `50` | Non-terminal bulk operations one account may have. `StartBulkPutItems` rejects above it. |
| `max_bulk_input_bytes` | `10737418240` | Enumerated (compressed) input bytes per operation. `StartBulkPutItems` rejects above it. |
| `max_bulk_input_files` | `50000` | Enumerated input object count per operation. Above it the Start is accepted and the operation ends `FAILED` with `S3ObjectsCountLimitExceeded`, as the hosted service does at its 50,000 quota. The enumeration stores up to one object past the cap, one catalog row per object, and a checkpoint updates only the rows of the files it passes, so a lower value bounds the rows one operation holds. On the filesystem store the walk examines only the entries whose path can hold a key under the prefix, and when it examines more than four times this value of them, `StartBulkPutItems` answers `ValidationException` `Input exceeds max_bulk_input_files ({value} files)` and creates no operation. |
| `max_bulk_input_object_bytes` | `5368709120` | Size of one enumerated input object. Above it the Start is accepted and the operation ends `FAILED` with `InputObjectTooLarge`, as the hosted service does at 5 GiB. |
| `max_bulk_client_token_bytes` | `1024` | Longest accepted `ClientToken` in bytes. The API model puts no length cap on the token; this bounds what one request can store in the catalog. |
| `max_bulk_failed_starts_per_hour` | `100` | Terminal-born starts per account per hour. At or above it `StartBulkPutItems` answers `ThrottlingException`, except on a table with an operation running, which answers `ResourceInUseException`. Bounds catalog growth from requests naming absent buckets. |
| `max_bulk_line_bytes` | `4194304` | Longest accepted input line in bytes. A longer line abandons the rest of its file. |
| `max_bulk_item_count` | `100000000` | Physical lines one operation may consume, blank lines included. Exceeding it fails the operation. |
| `max_bulk_report_records` | `1000000` | Report records kept per operation. Records beyond it are dropped and the drop is logged. On SQLite the records of an operation that has ended are deleted after its terminal transition, 2,000 per statement, so other writes wait for one statement at a time. |
| `max_bulk_report_bytes` | `1073741824` | Serialized bytes of report records kept per operation. Records beyond it are dropped and the drop is logged. |
| `bulk_put_batch_size` | `25` | Rows per checkpoint. The runner persists its cursor, counters, and report records after this many consumed lines. Floored at 1 and capped at 10,000; a value outside that range is adjusted at startup and the server logs the adjustment naming the key. |
| `bulk_put_write_batch_rows` | `25` | Rows per storage transaction. The runner hands this many admitted rows to storage in one batch, so one commit covers them instead of one per row. The batch also flushes at every checkpoint, so this value is bounded by `bulk_put_batch_size`: raising it alone above the checkpoint size has no effect (the server logs the clamp at startup); raise both to widen the transaction. Capped at 100. Measured on PostgreSQL, a 100-row transaction runs about 30 to 40 percent faster than a 25-row one on plain input and about 28 percent faster on an indexed table, and it holds its row locks about four times as long: online `PutItem` to a key inside an in-flight batch waits for the batch's commit, and its p99.9 roughly doubled from 25 to 100 in measurement. On SQLite the batch holds the process write lock for its whole transaction, so every online write waits for it whatever its key; measured, the default of 25 raised online write p99 from about 1 ms to about 15 ms while a bulk load ran, and the default write pacer (80,000 units per second) is above SQLite's write capacity so it does not hold the load back. A SQLite deployment serving latency-sensitive online writes during bulk loads should lower this key (1 to 10) or `bulk_put_write_units_per_second`. |
| `bulk_operation_lease_seconds` | `60` | Lease a runner holds on its operation, renewed at every checkpoint, and every third of the lease while an input object's open or read, the output check at the first report record, a report write or the manifest write waits. After a crash, another process claims the operation once the lease expires and resumes it from the last checkpoint, and no row is written twice. A resume that finds the checkpoint where the previous claim found it logs a WARN line; the tenth in a row ends the operation without reading input and logs an ERROR line naming the outcome: `FAILED` with `InternalServerError`, `FAILED` with the deleted-table failure when the table is gone, or `CANCELLED` when a cancel is pending. On PostgreSQL the lease is read and written on the database server's clock, so a difference between the clocks of ExtendDB processes does not shorten it, and the database ends a bulk operation transaction or bulk write batch left idle for 10 seconds, so a stopped process releases the operation row, and the row that names the claim allowed to write, within that time. Floored at 1. |
| `bulk_operation_worker_concurrency` | `4` | Bulk operations one process runs at once. Floored at 1. Each running operation that reads a zstd input can hold up to 128 MiB of decoder memory, the largest decompression window a frame may declare and still be read, so the worst case is this value times 128 MiB above the process's baseline. |
| `bulk_operation_worker_per_account` | `2` | Of the concurrent runs, at most this many per account, so one account cannot hold every slot. Floored at 1. |
| `bulk_put_write_units_per_second` | `80000` | Ceiling on bulk writes in one process, in write request units per second (1 KB of item, rounded up, per unit). Shared by every operation the process runs; bulk loads are held to this rate so request traffic on the same server keeps the rest of the storage write path. The default equals `per_account_max_wcu`. Lower it on a server that also serves latency-sensitive traffic; `0` disables the ceiling. A request's own `MaxWriteRequestUnits` paces that operation on top of it. |
| `bulk_operation_retention_days` | `30` | Days a terminal operation record is kept before the retention worker purges it. Floored at 1 with a log line naming the key. A purge pass runs at startup and daily after that. The record and its ledger rows are removed; report files are not, see the reclaim note below. |

#### [import] and [export]

| Key | Default | Description |
|-----|---------|-------------|
| `paths` | (empty) | Directories files may be read from (`[import]`) or written to (`[export]`) |

Both sections are empty by default, which disables the operations that use
them: `ImportTable` and `ExportTableToPointInTime`. Files are namespaced per
account beneath each root (`<root>/<account id>/...`), every path is
canonicalized and must resolve under a configured root, and symbolic links
that escape a root are rejected.

#### AWS credentials for S3

Every S3 store the server opens, such as the bulk store with
`[bulk] store = "s3"`, signs its requests with the server's own AWS
credentials, whoever the caller is. Each store's own section lists the
permissions it needs and what its callers see when S3 refuses a request.

The server reads the credentials from the standard credential chain of the
AWS SDK for Rust, in this order: the `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` environment variables; the
shared config and credentials files (`AWS_PROFILE`, `AWS_CONFIG_FILE`,
`AWS_SHARED_CREDENTIALS_FILE`), including profiles that assume a role, sign
in through IAM Identity Center, use a login session from `aws login`, or run
a `credential_process`; web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` with
`AWS_ROLE_ARN`); container credentials
(`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or
`AWS_CONTAINER_CREDENTIALS_FULL_URI`); and the EC2 instance metadata
service, which `AWS_EC2_METADATA_DISABLED=true` turns off. The first source
that holds credentials is used. The configuration file holds no
credentials: an `access_key_id`, `secret_access_key`, or `session_token` key
in an S3 section such as `[bulk.s3]`, or the same key with an `aws_` prefix,
refuses startup with a message naming the credential chain, even when that
section's store is not selected.

A store signs for the `region` of its section, or `[server] region` when
that is unset; `AWS_REGION` and a profile's region do not change it. It
sends its requests to the `endpoint_url` of its section, or to AWS S3 for
the region when that is unset. An endpoint set in `AWS_ENDPOINT_URL`,
`AWS_ENDPOINT_URL_S3`, or a profile is ignored, so the endpoint the clients
use for ExtendDB never redirects a store.

At startup the server resolves the credentials once, waiting up to five
seconds, and logs the outcome: `S3 bulk store credentials resolved`, or a
warning that they cannot be resolved or were not resolved within five
seconds. The server starts either way, and the store's requests fail until
the credentials resolve. Keys in the environment variables or the shared
files resolve without a request to AWS, so resolving does not show whether
AWS accepts them; a role, web identity, IAM Identity Center, `aws login`, and
container or instance credentials resolve through a request. On AWS S3,
with no `endpoint_url`, the server then asks AWS STS whose credentials they
are, waiting up to five seconds without holding up startup, and logs one of
these lines:

- `S3 bulk store signs as {arn} (AWS account {account})`
- `AWS refused the S3 bulk store credentials ({code}): {message}. Its S3 requests fail until the server restarts with credentials AWS accepts`,
  with `InvalidClientTokenId` for an access key ID AWS does not know or a
  session token altered or left out, `SignatureDoesNotMatch` for a wrong
  secret key, or `ExpiredToken` for a session token past its expiry
- `S3 bulk store credentials could not be checked with AWS STS: the server clock is more than 15 minutes off ({code}): {message}`,
  when STS refuses the time the request was signed at
- `S3 bulk store credentials could not be checked with AWS STS: {cause}`,
  when STS gives no answer within five seconds, as on a network with no path
  to STS, or answers with any other code, such as a server error or a
  throttling error; the store then works as it would without the check

The STS call, GetCallerIdentity, needs no IAM permission. Against a
configured `endpoint_url` the server makes no STS call. The call goes to
STS's regional hostname for the store's region, or to the endpoint named in
`AWS_ENDPOINT_URL_STS` or in a profile's `services` entry for `sts`, such as
an STS interface endpoint. The global endpoint, `AWS_ENDPOINT_URL` or a
profile's `endpoint_url`, is never used, and an STS endpoint equal to it is
ignored. With `AWS_ENDPOINT_URL` set, the SDK reads it before a profile's
`services` section, so the profile's `sts` entry has no effect.

The AWS SDK reads the shared config and credentials files once, at startup,
so a file created or changed after that is read at the next restart. Until
the credentials resolve, the environment, web identity, container, and
instance metadata providers are asked again for each request that needs
credentials. Once they resolve, the SDK keeps them until they expire, or for
15 minutes when they carry no expiry.

The store signs with the first credentials the chain finds, so the server
must start without the ExtendDB access key the clients sign with. The chain
asks the environment variables first: an ExtendDB key exported as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, as the Getting Started guide
exports it for the AWS CLI, reaches the store even when `AWS_PROFILE` names a
profile with the server's AWS credentials. With no key in the environment, an
`AWS_PROFILE` that selects the clients' profile gives the store the key that
profile holds. S3 answers `InvalidAccessKeyId` for a key ExtendDB created, so
the store's requests fail. A key imported with
`extenddb manage import-access-key` that is active in AWS is accepted by S3,
and the store then reads and writes as the AWS identity the key belongs to.
In both cases the server logs at startup
`The S3 bulk store credentials are an access key this server also accepts from its clients. S3 refuses a key ExtendDB created, and accepts an imported AWS key as that key's identity. Start the server without the clients' AWS variables or profile`.
To start the server from such a shell, remove the clients' variables from
the server's environment. With the server's credentials in a profile:

```bash
env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
    AWS_PROFILE={server profile} ./target/release/extenddb serve --config extenddb.toml
```

When the server's credentials come from web identity, a container, or the
instance, remove `AWS_PROFILE` as well. The chain reads the `default` profile
before those sources, so keys in that profile are used first:

```bash
env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
    -u AWS_PROFILE ./target/release/extenddb serve --config extenddb.toml
```

A relative path in `AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE`, or
`AWS_WEB_IDENTITY_TOKEN_FILE` resolves against the directory `serve` was
started in, with or without `--foreground`.

The SDK renews credentials before they expire, for as long as their source
stays valid, when they come from web identity, from container or instance
credentials, or from a profile that assumes a role, signs in through IAM
Identity Center or `aws login`, or runs a `credential_process`. It does not
renew keys given in the environment variables or written into the shared
config or credentials files, such as temporary keys copied from the AWS
access portal or written by a credential tool. Once those expire, S3 answers
`ExpiredToken` and the store's requests fail until the server restarts with
new keys. Credentials that carry a session token and no expiry are such
keys, and the server warns about them at startup:
`The S3 bulk store credentials are temporary keys the AWS SDK cannot renew. When they expire, its S3 requests fail until the server restarts with new keys. Give the server a role, web identity, IAM Identity Center or a credential_process profile instead`.
Keys a `credential_process` prints without an expiry get no warning: the SDK
keeps them for 15 minutes and then runs the process again.

The AWS SDK's credential loader logs under the target `aws_config`, whose
level the server sets to `warn` whatever `log_level` is: at `info` the
loader writes the access key ID on each credential load, and for a profile
that assumes a role, the role ARN and the session name. Its warnings, such
as a credential load that failed, are still logged.

#### [bulk]

The bulk put operations read their input and write their error reports
through one store, independent of `[import]` and `[export]`: the S3 store,
which reads and writes the buckets the requests name in AWS S3 or an
S3-compatible service, or the filesystem store, one directory tree beneath a
root on the server.

| Key | Default | Description | Environment variable |
|-----|---------|-------------|----------------------|
| `store` | `filesystem` | `filesystem` or `s3`, in any letter case. | `EXTENDDB__BULK__STORE` |
| `filesystem.path` | (none) | The filesystem store's root. Required when `store = "filesystem"`. | `EXTENDDB__BULK__FILESYSTEM__PATH` |
| `s3.region` | `[server] region` | The region the S3 store signs for, and the first region a bucket is tried in. One host label: lowercase letters, digits, and hyphens, beginning and ending with a letter or a digit. | `EXTENDDB__BULK__S3__REGION` |
| `s3.endpoint_url` | AWS S3 for the region | An S3-compatible service: an absolute `http` or `https` URL with a host, an optional port, and an optional path. A URL with user information, a query, or a fragment refuses startup. So does an `http` URL whose host is not loopback, unless `s3.allow_insecure_endpoint` is `true`. | `EXTENDDB__BULK__S3__ENDPOINT_URL` |
| `s3.allow_insecure_endpoint` | `false` | Accept an `http` `endpoint_url` whose host is not loopback; the server then logs a warning at startup. | `EXTENDDB__BULK__S3__ALLOW_INSECURE_ENDPOINT` (`true` or `false`) |
| `s3.force_path_style` | `false` | Address buckets in the URL path instead of the host name. | `EXTENDDB__BULK__S3__FORCE_PATH_STYLE` (`true` or `false`) |

`store = "s3"` with no `[bulk.s3]` section takes every default. A
`[bulk.s3]` section is checked whatever `store` selects, so a credential key
or an `endpoint_url` the checks refuse stops startup beside the filesystem
store too, before a later `store = "s3"` would read it. Otherwise the
section of the store not selected is ignored, with one line in the log, so
`EXTENDDB__BULK__STORE` switches stores without an edit to the file. An
unknown key in either section refuses startup. `store` defaults to
`filesystem`, so a `[bulk.s3]` section with no `store` line and no
`[bulk.filesystem]` section refuses startup with a message that names both
fixes: set `store = "s3"` in `[bulk]`, or add `[bulk.filesystem]`.

With no `[bulk]` section the feature is disabled: `StartBulkPutItems`
returns `Bulk operations are disabled. Configure [bulk] in extenddb.toml to
enable.` and the bulk operation worker idles. An idle worker checks for an
operation to claim with one read and takes no write transaction (on SQLite,
not the write lock), with or without a `[bulk]` section. In dev mode with no `[bulk]`
section, the filesystem store is enabled at `./.extenddb/buckets`, relative
to the directory `serve` was started in; the dev-mode banner and the startup log
name the path. An explicit `[bulk]` section is honoured in dev mode,
`store = "s3"` included, and the banner then names the S3 store. The banner
prints after the `[bulk.s3]` checks pass; a section they refuse stops
startup before the banner, with a message that does not repeat the value.

An operation is read and written only through the store it started on.
After `store` changes with operations in flight, a worker that claims an
operation started on the other store ends it without reading it: `FAILED`
with `InternalServerError` when it was `IN_PROGRESS`, and `CANCELLED` when it
was `CANCELLING`. The log line names the store the operation started on.
Let the operations finish, or cancel them, before changing `store`.

On the filesystem store the layout beneath the root is
`{path}/{account id}/{S3Bucket}/...`. The
`S3Bucket` field of a request names a directory inside the caller's account
subtree, and the `S3KeyPrefix` fields name paths beneath that directory.
Every path is canonicalized and must resolve inside the account subtree, and
symbolic links that escape it are rejected. At startup the server creates
`path` if it is missing, logs its canonical form, and creates
`{path}/{account id}` for every account. If `path` names an existing regular
file the server refuses to start with `Cannot resolve bulk store path
{path}: File exists (os error 17)`. `POST
/management/accounts` creates the new account's directory, and
`StartBulkPutItems` creates it if it is missing. The caller creates the
bucket directories: a request naming a bucket directory that does not exist
fails the operation with `FailureCode` `S3NoSuchBucket`. Account deletion
leaves the directory and its files in place.

Whoever can write beneath `{path}/{account id}` can feed data into any
table that account starts a load on, so an account's subtree is as trusted
as the mechanism that populates it. The symlink defenses do not cover hard
links: a writer able to hard-link a file into its subtree on the same
filesystem gets that file's rows written into its table and echoed into its
report. Keep the root on its own filesystem or populate the subtrees
through a trusted transfer mechanism, and leave the kernel's
`fs.protected_hardlinks` setting at `1`.

The filesystem store writes report files without encryption.
`S3SseAlgorithm` and `S3SseKmsKeyId` are recorded and returned in the
description and have no effect on the files. The S3 store applies them to
every report object and the manifest.

Report files hold item values. The filesystem store creates report objects
and the manifest with mode 0600, and the directories it creates for them,
from the first `S3KeyPrefix` segment down, with mode 0700, owned by the
user the server runs as, so reading them takes that user or root. A
directory that already exists keeps its mode, and so does the bucket
directory, which the caller creates. Files that `ExportTableToPointInTime`
writes keep the modes the umask gives them.

#### S3 store

`store = "s3"` reads the input objects, and writes the reports and the
manifest, in the buckets the requests name, with the server's own AWS
credentials. No account directories exist on this store, and
`POST /management/accounts` creates none. The startup log names the store
it built, for example `Bulk store: s3 (region us-east-1, endpoint AWS S3,
virtual-hosted-style)`. A configured endpoint is named by its scheme, host,
and port, such as `endpoint https://objects.example.internal:9000`, in the
log line and in the dev-mode banner.

The store signs with the server's AWS credentials and takes its signing
region and endpoint from `[bulk.s3]`.
[AWS credentials for S3](#aws-credentials-for-s3) describes where the
credentials come from and the lines the server logs about them at startup,
which this store logs under the name `S3 bulk store`. Until the credentials
resolve, `StartBulkPutItems` answers `InternalServerError`; what a Start and
a running operation see when S3 refuses the credentials is described below.

The server's credentials need these permissions:

| Permission | Resource |
|------------|----------|
| `s3:ListBucket` | Each input bucket |
| `s3:GetObject` | The input objects, and the report keys in each output bucket for the check after a lost completion answer (see below) |
| `s3:PutObject` | The report and manifest keys in each output bucket. It covers the multipart upload calls as well as PutObject |
| `s3:AbortMultipartUpload` | The report and manifest keys in each output bucket |
| `kms:Decrypt` | The KMS keys of SSE-KMS input objects, and the key an output configuration names with `KMS` |
| `kms:GenerateDataKey` | The key an output configuration names with `KMS` |

S3 evaluates these for the server's principal, whoever the caller is. A
denial S3 returns names that principal, for example
`arn:aws:sts::{AWS account}:assumed-role/{server role}/{session}`, and its
text reaches the caller in `FailureMessage` with S3's request ids. A bucket
owner needs that ARN to write a bucket policy for the server, and every
caller who meets such a denial learns it.

S3 error codes about the server's own credentials, account, or clock are
answered as an internal error: `SignatureDoesNotMatch`,
`InvalidAccessKeyId`, `ExpiredToken`, `InvalidToken`,
`TokenRefreshRequired`, `RequestTimeTooSkewed`, `CredentialsNotSupported`,
`InvalidSecurity`, `MissingSecurityHeader`, `AccountProblem`, and
`NotSignedUp`. A Start that meets one answers `InternalServerError` and
creates no operation, and a running operation ends `FAILED` with
`InternalServerError`. The server logs an ERROR line with S3's code and
message, `S3 {call} failed: S3 refused the server's own credentials,
account or clock; the caller is answered InternalServerError`.
`AllAccessDisabled` reaches the caller as `S3AllAccessDisabled`. An S3 4xx
code the store does not map to a `FailureCode` is answered the same way,
with `InternalServerError` and `Internal server error` for the caller, and
S3's code and message appear only in the server's ERROR line,
`S3 {call} failed`.

Only two controls separate ExtendDB accounts on this store: ExtendDB
evaluates `s3:ListBucket`, `s3:GetObject`, and `s3:PutObject` against each
caller's ExtendDB policies (see the [Security Model](10-security-model.md)),
and the server's credentials reach only the buckets they are granted. The
store has no per-account namespace, and an `S3BucketOwner` names an AWS
account, which an ExtendDB account id is not. A caller whose policies allow
those actions on a bucket can load from it and write reports to it,
whatever ExtendDB account the caller is in, when the server's credentials
reach that bucket. A policy that grants the S3 actions on `*` keeps
accounts apart on the filesystem store, where each account's buckets are
directories in its own subtree, and on the S3 store reaches every bucket
the server's credentials reach. Grant each caller the S3 actions on its own
account's buckets only, for example by a bucket name prefix per account
such as `arn:aws:s3:::acct-111122223333-*`, and grant the server's
credentials the buckets the deployment serves and no others. Developer mode
evaluates no policy, so there every caller reaches every bucket the
server's credentials reach.

For an S3-compatible service, set `endpoint_url` to the service's URL, and
`force_path_style = true` for a service that does not serve buckets as host
names. Against a configured endpoint the store computes and checks
checksums only when an operation requires them, and its multipart uploads
declare no checksum algorithm, because several S3-compatible services refuse
the trailing checksums the SDK sends by default; it follows no region
redirect. A request naming another bucket owner carries
`x-amz-expected-bucket-owner` to every endpoint; whether a service other
than AWS S3 checks it, and whether its error codes and messages match AWS
S3's, depends on that service. A service that sends no `x-amz-id-2` header
leaves `null` in the extended request id of a `FailureMessage`.

An `http` endpoint sends requests, signed but not encrypted, in clear
text. The server accepts an `http` endpoint only when its host is one of
the loopback forms `127.x.y.z` (`127.0.0.0/8` in dotted-quad form), an
IPv6 address equal to `::1` (`[::1]`), or `localhost` (any case). Other
spellings of a loopback address (`127.1`, `localhost.`,
`[::ffff:127.0.0.1]`) are refused. Any other `http` endpoint refuses
startup, whatever `store` selects, unless `allow_insecure_endpoint = true`,
which accepts it and logs a warning at startup. The store's requests and
responses, which carry the input objects and the reports, then cross the
network unencrypted.

The store's endpoint comes from `endpoint_url` alone. With it unset, the
store sends its requests to AWS S3 for the region, and an endpoint named
by `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`, a profile's `endpoint_url`,
or a profile's `services` section is not used. A server started from a
shell that sets `AWS_ENDPOINT_URL` to point DynamoDB clients at ExtendDB
still sends the store's requests to AWS S3.

A service that ignores `Range` and answers a GET with the whole object is
read only when the object fits in the first range, 8 MiB. For a larger
object the operation ends `FAILED` with `InternalServerError`, and the
server logs `S3 read failed`.

On AWS S3 a bucket outside `s3.region` answers the first request with a
redirect naming its region (a 301 or 307, or a 400
`AuthorizationHeaderMalformed`, carrying `x-amz-bucket-region`). The store
sends the request once more to that region and remembers the region for the
bucket, for up to 1,024 buckets, after which it starts over. Input and
output buckets can be in different regions from each other and from the
server.

Each attempt of an S3 request has a connect timeout of 3.1 seconds and
waits up to 60 seconds for the response's status and headers. A response
body that stops arriving is ended by the AWS SDK's stalled-stream
protection once it has made no progress for 5 seconds. The SDK makes up to
three attempts. A call that still fails with a connection failure, a
timeout, HTTP 429, 500, 502, 503, or 504, or the code `SlowDown`,
`RequestTimeout`, `InternalError`, or `ServiceUnavailable` is sent again by
the store after 1 second, the wait doubling up to 30 seconds, until 120
seconds have passed since the call began. A ranged GET whose body is cut
off is requested again for the same range with the same `If-Match`, and
those requests count toward the call's 120 seconds. Each retry logs a WARN
line with its wait. A retry that starts before the 120 seconds have passed
runs all three attempts, so a call that gets no answer at all ends after
about three minutes. The S3 calls a Start makes (the bucket check, each page
of the listing, and the encryption check of an SSE-KMS output) end within
10 seconds each, retries included. A Start lists the whole input prefix
before it answers, so its time grows with the number of listing pages, and
a long listing on a slow endpoint can outlast a client's read timeout. When
a Start's call has not ended after
10 seconds, the Start answers `InternalServerError` and creates no
operation. When a running operation's call still fails after 120 seconds,
the operation stays `IN_PROGRESS` and the next claim resumes it from its
last checkpoint, under the limit of ten resumes without progress described
under `bulk_operation_lease_seconds`. In both cases the
server logs an ERROR line naming the call and the bucket. Other 4xx
answers are not sent again.

While an input object's open or read waits on S3, the runner checkpoints
every third of `bulk_operation_lease_seconds` (20 seconds by default), so
the operation keeps its lease through the wait and a cancel takes effect
within that time. When such a checkpoint ends the read (a cancel, a lost
lease, the table deleted or the caller's permission removed), the GET in
flight is dropped and its retries are not sent. The check at the first
report record, the report writes and the manifest write renew the lease the
same way while they wait on S3. A cancel or a deleted table seen during the
check at the first report record ends the check within a third of the
lease. The report and manifest writes run after the operation has read its
input, so their renewals act only on a lost lease: a cancel that arrives
during them is recorded when the operation ends, and every report and the
manifest are still written.

When the answer to a `CompleteMultipartUpload` that S3 carried out is lost,
the request is sent again and S3 answers `NoSuchUpload`. The store then
looks for the report object with a `HeadObject` and, when an object of the
size the upload sent is in place, logs a WARN line and goes on. An object
of another size at the key is not taken as the report, and the operation
ends `FAILED` with `S3NoSuchUpload`. The server's credentials need
`s3:GetObject` on the report keys for that check; without it the
operation ends `FAILED` with `S3NoSuchUpload`. A `HeadObject` that still
fails with a retryable error after 120 seconds leaves the operation to
resume, as other calls do.

A report object is a multipart upload until the reports are written at the
end of the operation, and the check at the first report record starts an
upload and aborts it. A server that stops between that check's start and
its abort leaves one empty upload, and one that stops while it writes the
reports leaves up to one upload per report object, holding the parts sent
so far, up to 200 MiB each. An abort that fails (logged as a warning) also
leaves its upload. S3 keeps the parts of an incomplete multipart upload, and
charges for their storage, until it is aborted. Give every output bucket a
lifecycle rule that aborts incomplete multipart uploads:

```json
{
  "Rules": [
    {
      "ID": "abort-incomplete-multipart-uploads",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    }
  ]
}
```

```bash
aws s3api put-bucket-lifecycle-configuration --bucket {output bucket} \
  --lifecycle-configuration file://lifecycle.json
```

The runner that holds an operation's lease writes its reports and its
manifest, and it keeps the lease while each write waits. A runner that
stops, for example a paused process, can outlast its lease during a write
and then complete a report object and write the manifest after the process
that claimed the operation next has written them and ended the operation.
Both render the same checkpointed records, so the content is the same, but
the objects' `LastModified` changes after the operation is terminal, a
versioned bucket keeps a second version of each key written twice, and the
bucket's event notifications fire twice.

#### Reclaiming bulk report files

On the filesystem store an operation that names an output bucket writes its
error report beneath the caller's subtree:

```
{path}/{account}/{S3Bucket}/{S3KeyPrefix}/AWSDynamoDB/BulkOperation/{account}:{operation id}-0/output/errors/output-00000.json.gz
{path}/{account}/{S3Bucket}/{S3KeyPrefix}/AWSDynamoDB/BulkOperation/{account}:{operation id}-0/output/condition-check-failures/output-00000.json.gz
{path}/{account}/{S3Bucket}/{S3KeyPrefix}/AWSDynamoDB/BulkOperation/{account}:{operation id}/manifest-files.json
```

A report whose compressed output passes 200 MiB continues in `output-00001.json.gz`
and so on within the same directory, so one operation can leave several objects
per kind.

The retention worker purges the operation record and its ledger rows after
`bulk_operation_retention_days`. It does not remove these files. The files
stay under the account subtree with no record the server tracks, so disk use
grows with the number of operations that produced a report. There is no
per-account or aggregate bound on the total.

Reclaim the files with a find by age under the store root. On the filesystem
store each report lives in a directory named `{account}:{operation id}-0`,
and its modification time is when the report was written:

```bash
find "{path}" -type d -path '*/AWSDynamoDB/BulkOperation/*-0' -mtime +45 -print
```

Inspect the list, then remove the directories it names once the operations
are past use. A customer can still read a report through `DescribeBulkOperation`
output paths after the operation record is purged, so set the age well beyond
`bulk_operation_retention_days` and beyond how long a customer keeps reading a
finished operation's report.

On the S3 store the reports and the manifest stay in the output bucket under
`{S3KeyPrefix}/AWSDynamoDB/BulkOperation/`, and the retention worker does not
remove them either. Reclaim them with an S3 lifecycle rule whose `Expiration`
applies to that prefix, with the age chosen on the same grounds.

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
- **Bulk operations and resource tags.** A bulk-operation resource (the
  `bulk-operation/*` wildcard and every operation ARN) carries no tags, so
  `aws:ResourceTag/*` is absent for DescribeBulkOperation,
  CancelBulkOperation, ListBulkOperations, and the
  `dynamodb:StartBulkOperation` check of StartBulkPutItems. A tag-conditioned
  `Allow` never grants them, and a `Deny` with a negated operator or `Null`
  true on a tag key always applies to them, on a tagged table too. Only the
  `dynamodb:BulkPutItems` check on the table ARN sees the table's tags.
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

- **Dashboard**: Account and admin user counts (admin sessions only; an IAM-user session gets a link to its own account), version info
- **Account management**: Create, view, delete accounts (an IAM-user session views its own account only and is redirected to `/console` from the pages of any other account)
- **User management**: Create, delete users; view access keys, policies, tags, group memberships
- **Access key management**: Create and delete access keys (secret shown once)
- **Group management**: Create, delete groups; add/remove members
- **Role management**: Create, delete roles; view trust policies
- **Policy management**: Add, delete inline policies with JSON editor
- **Metrics dashboard**: Operation metrics for every account, with charts (admin only; an IAM-user session is redirected to the console home page, `/console`)
- **Settings and Cache**: The read-only settings viewer and the auth cache invalidation form (admin only; an IAM-user session is redirected to `/console`)

### Authentication

- Admin users: enter username and password
- IAM users: enter `account_id/user_name` as username, console password as password

Sessions end 8 hours after login. Click "Logout" to end immediately. Deleting an admin or IAM user, or changing its password, ends its console sessions on every server at their next request.

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

Bulk operations publish three metrics of their own. Query them with the
`metric` parameter as with any other, for example
`?metric=BulkOperationsFinished&window=LastHour`.

| Metric | Dimensions | What the number counts |
|--------|------------|------------------------|
| `BulkOperationsStarted` | `TableName` | Bulk operations created by `StartBulkPutItems`. An idempotent replay of an existing operation is not counted again. |
| `BulkOperationsFinished` | `TableName`, `Operation` | Bulk operations that reached a terminal status, counted once per operation on the transition that held the lease. The `Operation` dimension carries the terminal status: `COMPLETED`, `COMPLETED_WITH_ERRORS`, `FAILED`, or `CANCELLED`. |
| `BulkPutItemsWritten` | `TableName` | Rows written to the table by the process whose write committed them. Rows an earlier process wrote and this process replayed after a crash are excluded, and rows whose condition evaluated false are excluded. |

The bulk operation worker and the retention worker report `WorkerLastSuccess`
and `WorkerErrorCount` each cycle, like the other background workers, under
the worker names `BulkClaim` and `BulkRetention`. A claim loop that stops on
an internal fault logs an error and stops advancing `WorkerLastSuccess` for
`BulkClaim`.

### Health Check

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health
# {"status":"healthy","components":{}}
# (the key order depends on the build; the earlier Health Check section
# shows the components a backup store adds)
```

The `components` object carries the `backup_store` component when a backup
store is configured (see [Health Check](#health-check) under Server Lifecycle).

## Bulk Operations

### Cancelling a bulk operation as the operator

There is no management endpoint or CLI subcommand that cancels a bulk
operation. The customer can cancel one with `CancelBulkOperation` under that
account's credentials. An operator who needs to stop a runaway or stuck load
without those credentials does it with one SQL statement against the catalog
database, the one that holds the `bulk_operations` table. It is not in the
data database.

List the non-terminal operations first:

```sql
SELECT account_id, operation_id, table_name, status, processed_item_count
  FROM bulk_operations
 WHERE status IN ('IN_PROGRESS', 'CANCELLING');
```

The `operation_id` is the trailing segment of the `BulkOperationArn`
(`bulk-operation/{operation id}`) and `account_id` is the ARN's account
field. Request the cancel by setting the operation to `CANCELLING`. On
PostgreSQL:

```sql
UPDATE bulk_operations
   SET cancel_requested = TRUE, status = 'CANCELLING'
 WHERE account_id = '<account id>' AND operation_id = '<operation id>'
   AND status IN ('IN_PROGRESS', 'CANCELLING');
```

On SQLite the boolean is an integer:

```sql
UPDATE bulk_operations
   SET cancel_requested = 1, status = 'CANCELLING'
 WHERE account_id = '<account id>' AND operation_id = '<operation id>'
   AND status IN ('IN_PROGRESS', 'CANCELLING');
```

The `status IN ('IN_PROGRESS', 'CANCELLING')` guard is the same one
`CancelBulkOperation` uses, so the statement never touches a terminal
operation.

What happens next depends on whether a runner holds the operation:

- A live runner notices the request at its next checkpoint boundary
  (`bulk_put_batch_size` rows) and ends the operation `CANCELLED`. The
  counters it reports are floors on the work already done; rows written
  before the cancel stay written.
- An operation with no live runner, because the `[bulk]` store is disabled
  or the operation is unclaimed, is moved from `CANCELLING` to `CANCELLED`
  by the bulk operation worker on its next pass.

Set the status to `CANCELLING` only. Do not write `CANCELLED` or any other
terminal status by hand. The lease release and the report rendering run on
the `CANCELLING` to `CANCELLED` transition, so a hand-written terminal
status bypasses both: it leaves the report unrendered and the lease still
held by whichever process last claimed the operation.

### Operations the server cannot read

A `bulk_operations` row the server cannot decode, for example one edited by
hand or written by a newer build, does not stop other operations. When a
runner claims such a row, it ends the row `FAILED` with `InternalServerError`
and the message `Internal server error`, or `CANCELLED` with the cancel pair
when a cancel was requested, deletes its report records in the same
transaction, and logs an
ERROR line with the account and operation id; the other claimed operations
run. `ListBulkOperations` skips the row with the same ERROR line, so a page
can hold fewer operations than its limit and still carry a `NextToken`.
`DescribeBulkOperation` of the row answers `InternalServerError`.

### Faults during a bulk load

A batch write that fails for a reason a later attempt can clear leaves the
operation `IN_PROGRESS`, and the next claim resumes it from its last
checkpoint. These are a lost database connection, a server shutdown, a
serialization or deadlock abort, a lock or statement timeout, and on
PostgreSQL a full disk or too many connections (SQLSTATE 53100 and 53300).
The code is read from the batch's own statements and its secondary index
and stream writes; the same fault met on a vector index write, or when the
batch opens its catalog connection, ends the operation `FAILED` with
`InternalServerError`. On SQLite a full disk ends the operation `FAILED` with
`InternalServerError`.

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

On PostgreSQL, SQLite, Cassandra and MongoDB, `serve` reads the catalog's migration status before it opens the engine and refuses a catalog it cannot serve, printing the status and the command that resolves it. A catalog with pending migrations exits 3, for example `the catalog is at migration 4, and ExtendDB <release> needs migration 5`, followed by the pending migrations and `extenddb migrate --config extenddb.toml --yes`. A catalog that a newer release migrated with a breaking migration exits 4 and names that release's migrations; upgrade the binary rather than the catalog. The [Upgrade Manual](07-upgrade-manual.md#the-catalog-status) lists every status with its exit code and the stop / migrate / start sequence.

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

A backup taken while a bulk operation is running holds whatever rows were
committed when the copy began. The catalog and data databases hold two
halves of an in-flight operation, so restore both to the same point, or
cancel every non-terminal operation before the backup. See the Upgrade
Manual, Bulk Operations During Backup and Restore.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
