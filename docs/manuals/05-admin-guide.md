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

extenddb handles SIGTERM and SIGINT gracefully: it drains active connections for up to 5 seconds (the TLS listener and the plain-HTTP listener of a dev-mode build alike), then waits up to 30 seconds for its running backup jobs to end (a job the stop cancels runs its failure path, which marks its row FAILED and deletes what it wrote to the store; on a large S3 prefix that takes longer than the other bounds), then up to 10 seconds for its registration supervisor to return, up to 16 seconds to stop its background workers (5 seconds for them to return, 1 second for the ones then aborted, and 10 seconds more behind a stop the supervisor was still running), up to 10 seconds to give up its serving registration (kept, to expire by itself, while a background worker or a backup job may still run), and 5 seconds for its runtime's blocking threads, and exits. With no backup job running the job wait ends at once, and in an ordinary stop the supervisor, the workers and the release each take moments. A `CreateBackup` that is still waiting in its snapshot open or row insert when the stop begins writes no row, and its client sees the connection closed by the drain (measured: 8.2 s after the request was sent): the jobs' token, which turns such a request into `ServiceUnavailable` "The server is shutting down. Retry the request.", is cancelled after the drain, so that answer reaches a client only after a registration loss, when the token is cancelled while the listener still serves. `extenddb stop` waits up to 76 seconds, the sum of these bounds, before it reports that the server did not exit.

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
| `region` | `us-east-1` | AWS region for ARN generation. Requests must carry a credential scope for this region; any other region is rejected with `InvalidSignatureException`, as the service rejects a scope for a region other than the endpoint's. Baked into every backup ARN the server issues, so it must stay within `[a-z0-9-]` (under a value outside that class `CreateBackup` is refused with a `ValidationException` naming `server.region`, since no backup ARN can be formed; backups taken before the upgrade under such a region keep an ARN `DescribeBackup`, `DeleteBackup`, and `RestoreTableFromBackup` refuse as "not valid"; the server logs a warning at startup). A backup ARN of the caller's account in another region is resolved in the server's region first, then as written; see the upgrade manual before changing this on a deployment with backups |
| `request_timeout_secs` | `30` | Seconds a connection may take to deliver its request headers, and seconds a request body may stall without delivering data, before the server closes it (a stalled body gets HTTP 408). A connection that is accepted, or that completes the TLS handshake, and then sends no byte is closed after the same number of seconds, as is one that sends only part of the 24-byte HTTP/2 preface. The same value bounds how long an idle keep-alive connection waits for its next request, so SDK connection pools see idle connections closed after it (they reopen transparently), and it is the HTTP/2 keep-alive PING interval and timeout, so a stalled HTTP/2 connection is closed within twice the value. Must be at least 1 and at most 86400 (a day); the server refuses a larger value at startup, because the per-connection timers add it to the current instant and a value near `u64::MAX` overflows that addition |

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

`RestoreTableFromBackup` on PostgreSQL holds a session-level advisory lock (the job's claim) for the whole copy, so ExtendDB must reach the catalog database directly or through a session-mode pooler. Behind a transaction-mode pooler (for example PgBouncer with `pool_mode = transaction`) the lock lands on a pooled server connection and is not released with the job, so later jobs can be refused as already claimed; do not run backups or restores through one.

#### [storage.sqlite]

| Key | Default | Description |
|-----|---------|-------------|
| `path` | `extenddb.sqlite` | Database file (`init --sqlite-path` writes it); `:memory:` for an ephemeral database, whose catalog is new at every start: with `[backup.store]` configured the server adopts the root an earlier in-memory catalog owned and syncs its backups into the new catalog before it serves, within 60 s (an S3 store in the reconciler's first cycle; a sync not finished in 60 s, or one that met a store or catalog error on a prefix, runs again in the next cycle). Every run lists the root from its first key, so the startup sync completes only while listing the whole root fits in 60 s; past that it stays pending, is retried every cycle, and the backups beyond what a run reaches are not served. A run holds the whole listing of the root in memory (one entry per object) until it ends, and a listing that outlasts 60 s reaches no prefix. On a filesystem store a run abandoned at the 60 s bound does not stop its listing: the walk of the root runs to its end on the blocking pool, holding the listing it builds, and no second walk starts beside it; until it ends, every check of the store is refused at once ("an abandoned startup sync's store call has not returned") and records the store `unreachable`, so CreateBackup, DeleteBackup and restores are refused and the sync does not run; the first reconciler cycle after the walk ends finds the store healthy and runs the sync again, from the first key. A root too large to list within 60 s is never synced by an in-memory server: give it a smaller root, or a durable catalog. On a filesystem store the server does not answer, `/health` included, until the startup sync ends (a liveness probe with a short timeout restarts such a server on a large root, each start syncing from the first key), and a signal then ends the process at once. Until the log line `startup sync of the in-memory catalog: N backup(s) adopted` the earlier backups answer `BackupNotFoundException`, although `/health` may already report the store healthy; a prefix that line reports as having met a store or catalog error stays so until a later cycle's line. Each run names the prefixes it skipped for a store or catalog error at WARN; until one run has listed the whole root and logged its line of counts, each run also names every other prefix it skipped at INFO (so the reasons reach the log when the first run was cut at the bound or stopped by an error), and a later run of a sync left pending logs those at DEBUG, so a root of many backups with one that fails costs one WARN line and the line of counts per cycle. A server killed inside a DeleteBackup (between the DELETING write and the manifest's deletion), or restarted after a DeleteBackup whose manifest deletion failed, serves that backup again at its next start, since the startup sync adopts the complete prefix; a prefix a stopped server had not finished writing or deleting (a killed CreateBackup, a failed backup whose prefix deletion had not finished, a deletion cut after its manifest) stays under the root with no row to drive its deletion (the next start's sync skips a prefix without its manifest); the startup sync names each such prefix in its log line `backup store startup sync: <prefix> not adopted: not a complete backup: ...`. Delete such a prefix by hand, since every `extenddb backup` command refuses an in-memory database, but only one that has no `extenddb-manifest.json` and whose backup `ListBackups` does not return: a CreateBackup the running server is still writing has no manifest until its last put either, and its row is in `ListBackups` (status CREATING). Start a new in-memory server only after the old one has exited: one started beside it adopts the root while the old one still acts on it until a later check finds it adopted, its deletion fence (the identity paragraph) excepted. A root that an in-memory server of an earlier development build wrote has an `extenddb-store.json` without `ephemeral_catalog` (no release wrote one), which reads as a durable catalog's: the server finds the root owned by another catalog, its store stays unhealthy, and no command can adopt it. Remove that `extenddb-store.json`; the next start writes a new one (a new store id) and adopts the backups under the root. Every `extenddb backup` command refuses it |
| `pool_size` | `10` | Maximum connections. Writes are serialized by the engine whatever the size. A file database raises `pool_size = 1` to 2 and logs it at info (`SQLite pool_size 1 raised to 2 for a file-backed database: ...`), because a backup holds one reader connection for its whole duration while its batches write through another. Use 3 or more so a running backup does not contend with ordinary requests for connections |

#### [backup]

The knobs backup and restore jobs and the reconciler read; every key has a default, and the section may be left out. This section and `[backup.store]` below are the reference for the keys and the `extenddb backup` commands; the [Backup and Restore manual](14-backup-and-restore.md) covers the strategy, the recovery point and time, securing the store, the restore drill and catalog-loss recovery.

| Key | Default | Description |
|-----|---------|-------------|
| `data_file_target_bytes` | `67108864` | Uncompressed bytes per backup data file (minimum 32 MiB, the cap on one line): a data file closes at the first item that takes it to or past this, so it exceeds it by less than one line |
| `heartbeat_interval_secs` | `15` | The job heartbeat timer. The store-backed backup job heartbeats every `max(heartbeat_interval_secs, 15)` s and starts no store write unless a beat confirmed its claim within two periods or a beat is still in flight (on SQLite a beat waits behind any writer holding the engine write lock, a GSI backfill say, for as long as it holds it, and a backup goes on writing meanwhile; on the other backends a beat in flight does not count), the claim's insert counting as the first beat; the in-catalog backup and restore paths heartbeat every 15 s whatever this says |
| `stale_job_after_secs` | `300` | Heartbeat age after which a job's owner is dead (on PostgreSQL, once the owner's claim session has ended as well) and the reconciler resolves the job: a dead backup job with a complete manifest under its prefix is AVAILABLE, one without is FAILED and its prefix deleted; a dead in-catalog restore job is FAILED and its target removed; a dead store-backed restore is resumed from its recorded progress (the differences document's restore-interruption row). Must be at least three heartbeat periods, where the period is the larger of `heartbeat_interval_secs` and the 15 s the in-catalog jobs heartbeat at (so 45 at the least, whatever `heartbeat_interval_secs` says); on SQLite a long write transaction delays a heartbeat, so keep a margin above that floor |
| `failed_row_retention_secs` | `604800` | How long FAILED and DELETED backup rows of the configured store are kept once their objects are gone (7 days); `extenddb backup list --failed` prints the FAILED ones with their reason. At least `stale_job_after_secs`: the reconciler deletes a stopped job's late object only under its row, so the row outlives the window in which that job can still write; a smaller value is refused at startup |
| `reconcile_interval_secs` | `60` | The reconciler's cycle; it also runs once at startup. Each cycle checks the store, resolves backup and restore jobs whose owner is dead, finishes deletions, deletes the objects of FAILED backups once their heartbeat is older than `stale_job_after_secs`, and removes tombstones past `failed_row_retention_secs` (the differences document's restore-interruption row lists the duties) |
| `max_concurrent_jobs` | `4` | Backup and restore jobs this instance runs at once (at most 1024); a request over it waits up to 30 s for a slot, then `LimitExceededException`. On PostgreSQL the pool sizes are raised to `2 * max_concurrent_jobs + 10` when smaller; on SQLite `[storage.sqlite] pool_size` is raised to `2 * (max_concurrent_jobs + 1)`, at most 34, when its snapshot slots (`pool_size / 2 - 1`, between 1 and 16) would refuse a job this knob admits. Memory: a running backup holds about 490 MiB (PostgreSQL) or 650 MiB (SQLite) at most in the pipeline's own terms for a table of items at the 32 MiB line cap, a few tens of MiB for 200-byte items (the figures are for the default `max_item_size_bytes`, 400 KB, where no stored item's line exceeds the cap; on an instance that raises it the terms are at the largest line a stored item expands to, about 68 times the item's size for the worst shape); with glibc's default allocator the process's high-water mark grows with the number of large items a job reads (measured 760 MiB over idle at 48 items of 27.7 MB on a quiet host and 1,281 MiB on a loaded one, 1.39 and 2.35 times the terms at that item size) because the items' strings are allocated on one thread and freed on another and the per-thread arenas keep that memory. Set `MALLOC_ARENA_MAX=1` in the server's environment (the deployment guide's service unit carries the line, commented out) on a host that backs up large items: the peak is then the terms' and does not grow with the table (measured 452 to 481 MiB over idle at 12, 24 and 48 items of 27.7 MB on SQLite, in two series on two hosts' loads, and 340 MiB `VmHWM` (about 200 MiB over idle) at 12 items on PostgreSQL); one arena serializes the worker threads' allocations on one lock and costs the request path a few percent (measured 3.5 to 9.2% on GetItem, Query and Scan on SQLite and about 2% on PostgreSQL (the scans, 2.1%), where the request path waits on the database on PostgreSQL). Size the host for 1.2 times the terms (about 590 MiB on PostgreSQL, 780 MiB on SQLite) times this knob under `MALLOC_ARENA_MAX=1`; without it the peak grows with the large rows a job reads and no bound was measured (1,281 MiB over idle at 48 items of 27.7 MB is the largest seen). A running restore holds the parsed manifest, one decoded data-file line and its parse, the decoded items of 32 MiB of data-file text plus one line per chunk (the chunk closes with the item that reaches the bound) at four chunk positions between the decoder and the load (two in the channel, the one being filled, the one being drained; a chunk also closes at 1,000 items, and the chunks never hold more than one data file's items), and the backend's write batch: at most 32 MiB of item text on SQLite and Cassandra (10,000 items when they are smaller), held beside the decoded items of that text; 500 items (or 4 MiB) per INSERT statement inside one transaction per data file on PostgreSQL; 1,000 documents per `insert_many` on MongoDB (the driver splits the call); times this knob. For a table of large items that is the same order as the backup, not less: measured `VmHWM` over idle from a fresh start (`docs/design/15-backup-restore-bench-pr9.md`), 24 items of 27.7 MB restore with a peak of 848 MiB on SQLite and 778 MiB on PostgreSQL under the default allocator (848 and 1,013 MiB at 12 and 48 such items on SQLite: the peak grows with the large items a restore reads, as the backup's does, and no bound was measured), and 392 MiB on SQLite and 427 MiB on PostgreSQL under `MALLOC_ARENA_MAX=1`, the same setting as above; 1,200 items of 350 KB in one data file 270 MiB on SQLite and 156 MiB on PostgreSQL; 1M items of 200 bytes under a composite key with no secondary index, the backup's lines in key order as the server's own backups are, 89 MiB on SQLite (the write connection's page cache and the final `COUNT(*)`, each bounded by `cache_size`; the count's share was 22 to 28 MiB at 1M and at 3M rows) and 16 MiB on PostgreSQL, and the same 1M items as an unordered backup (every MongoDB-sourced backup and every multi-part-key table) 144 to 164 MiB on SQLite in 66 to 90 s against 27 to 56 s key-ordered (scattered inserts dirty the whole base b-tree; the restore's buffers are the same) and 16 MiB on PostgreSQL; the same items under the same key with two full-projection GSIs 143 to 225 MiB on SQLite at 100,000 items in three runs and 165 to 172 MiB at 300,000, and 23 to 34 MiB on PostgreSQL at both: the excess on SQLite is the backend's own per-connection page cache (`cache_size`, 32 MiB) holding the index tables' b-tree pages plus the allocator's arenas (about a third less under `MALLOC_ARENA_MAX=1`), not the restore's buffers, which the 32 MiB batch bound holds whatever the indexes; it is bounded by `cache_size` and grows sub-linearly with the table. Size the host for about 450 MiB per running restore of large items under `MALLOC_ARENA_MAX=1`, times this knob, plus about 32 MiB of page cache per running restore of an indexed table on SQLite, and read the 1M figure for 200-byte items as the key-ordered shape's: an unordered backup of them restores in about 1.5 to 1.8 times that on SQLite. The page-cache term is for B-tree indexes. On SQLite a restore of items of more than a few hundred bytes, of any attribute types, holds more: its write batch (up to 10,000 items or 32 MiB of item text) and the decode chunks in front of it grow with the item's size up to that bound, and lists of numbers decode larger than strings of the same size; the [Backup and Restore manual](14-backup-and-restore.md#memory-while-a-job-runs) has the measured points |
| `spool_path` | (unset) | Restore download area for an S3 store, created `0700` with files `0600`, holding a `.lock` another instance on the host refuses. A store-backed restore from an S3 store downloads each data file here under `restore-<table id>/` (`<n>.json.gz`, `<n>` zero-padded to six digits, with a `.verified` marker holding the file's sha256, written only after the file's digests matched and trusted only while it names the digest the manifest records, plus the manifest and sidecar), reads it from there, and removes the entry when the restore ends, except when it ended because the record had become another instance's (its claim lost, a status write that matched no record), where the sweep removes it; an entry left by an earlier run of this host for a record another host then owned is removed before a new run of the same restore uses it; the reconciler sweeps the `restore-<id>` entry of a restore no longer RUNNING with this host's `spool_host`. Its disk cost is the compressed backup of the restore in flight. A filesystem store needs no spool (files are hashed in place and read again). Required with an S3 store, except on file-backed SQLite where it defaults to `backup-spool/` beside the database file. Refused inside or containing a filesystem store root, and when it is or contains the database file's directory |

#### [backup.store]

The store CreateBackup writes to on PostgreSQL, SQLite and MongoDB, and the one restores and deletes of store-backed backups read. The section's keys, defaults and the refusals of a bad value are the backup store crate's (`crates/backup-store/src/config.rs`); the table below is what the server adds. One definition of the shape exists, so an unknown key anywhere under `[backup]` is refused at load.

| Key | Default | Description |
|-----|---------|-------------|
| `kind` | see below | `filesystem`, `s3`, or `disabled` |
| `filesystem.path` | `backups/` beside a SQLite database file | The root directory of a filesystem store. It must exist, be owned by the extenddb user with mode `0700`, and sit under directories owned by root or that user which others cannot write to; `validate` refuses anything else at startup. A relative path, like a relative `spool_path`, is resolved against the working directory the server was started from (the daemon's own working directory is `/`) |
| `s3.bucket`, `s3.prefix`, `s3.region`, `s3.endpoint`, `s3.force_path_style`, `s3.allow_insecure_endpoint`, `s3.upload_part_size_bytes`, `s3.max_concurrent_uploads` | | The S3 store; see the crate's `config` module doc. Credentials come from the AWS SDK's chain and are never in the file |

When the section is absent, the backend decides: on file-backed SQLite it is a filesystem store at `backups/` beside the database file, created `0700` if missing, whose validation or identity-check failure is a logged warning that leaves the store unconfigured rather than a startup error or an unhealthy store while the catalog has no backup rows for that root (so an upgrade never stops a server that never configured backups, and CreateBackup keeps working on one); once the catalog has rows for the default root, the root has been used and is handled as a configured one: a failure of its identity check or manifest probe is an unhealthy store (reported on `/health`, its rows' restores and deletes refused, `backup purge` refusing on it rather than reporting no store), and a refusal of its `open` or validation a startup error naming the root; on PostgreSQL, MongoDB, Cassandra and an in-memory SQLite database there is no default and the store is unconfigured, logged at startup. `kind = "disabled"` leaves it unconfigured on purpose on any backend. Without a configured store, CreateBackup writes the in-catalog backup format and logs a deprecation warning naming `[backup.store]` on each call. With one configured, CreateBackup on PostgreSQL, SQLite and MongoDB writes the backup to the store: the request takes the table's snapshot (after its checks and after any wait for a job permit, up to 30 s, so the backup is the table at that open and a write acknowledged during the wait is in it), writes the catalog row and answers `CREATING`, and a job of the server uploads gzip data files and the manifest under `<account id>/<table name>/<backup id>/` below the root and marks the row `AVAILABLE` (the job's log lines carry the ARN: `backup <arn>: started, <n> items in the table as last counted by the stats pass, key-ordered` (or `unordered`, for a table whose key shape or column collation has no format order) at the start, the stats pass's count, which is 0 on a table the pass has not reached, and `backup <arn>: AVAILABLE, <n> items, <bytes> bytes written in <s> s` at the end, with the backup's own count); Cassandra, which has no table snapshot, keeps the in-catalog create with a store configured. Backups of either format stay listable, describable, deletable and restorable: a store-backed backup is restored by the restore job (the differences document's "How `RestoreTableFromBackup` runs" row; the `spool_path` row above and the `remove-target` command below are its operator surface), an in-catalog one by the previous path.

Startup opens the configured store and, for a filesystem store, validates it and checks its identity before the listener is up (bounded by 5 s: a mount that stops answering after the root is opened leaves the server starting with the store `checking`; a root that does not answer at startup holds the server before the listener, as the database directory would, with the port bound and silent); an S3 store's validation and identity check are requests to the endpoint, and run after the listener is up, in the reconciler's first cycle, so an endpoint that stalls never holds the data plane down (the component is `checking` until the first result, and the operations that touch the store refuse meanwhile as they do while it is unhealthy). A configuration error, every one decided without a request to the store, stops startup with the key, path or bucket in the message: a root that does not exist or fails the owner-only rule, a filesystem root another user can reach, a filesystem root that is or contains the database directory, a bucket name the SDK routes to S3 Express, a bad part size or prefix, an insecure endpoint, an S3 store without `spool_path` on a backend with no default, a `spool_path` that breaks its placement rule, that another live instance holds or that exists with a mode other than `0700`. A transport failure (no region, no credentials, an endpoint that refuses connections or does not answer within the check's bound), a missing or denied bucket, an I/O failure of a filesystem root's probe write or resolution (a full or read-only disk, a stale network handle), or a failed identity check leaves the server running with the store unhealthy: the problem is logged once at error, `/health` reports the `backup_store` component unhealthy (status `degraded`), CreateBackup (where it writes to the store) and the restore and delete of a store-backed backup are refused with a `ValidationException` that names the store's kind and the problem's category and not its root, the reconciler acts on no row of the store, and the check runs again each `reconcile_interval_secs` until it passes, at which point the server logs `is healthy again` and resumes without a restart. Each check's store calls are bounded (5 s for a filesystem root, 30 s for a bucket); the catalog's own reads (the catalog id, the rows' store ids, the newest AVAILABLE row) run beside the calls and not under the bound, so a slow catalog costs the check time and never marks the store unhealthy. A store that has stopped answering costs a reconciler cycle that bound and nothing more, the cycle's other duties run as before, and the abandoned check is logged; on a filesystem root the abandoned check's call stays parked on one thread until the mount answers, no later cycle starts a check while it is (logged each cycle, the store staying `unreachable`), a root's calls run one at a time so that calls started before the first reached its bound (two `DeleteBackup` requests together, a `DeleteBackup` beside a check) wait for it under the bound, then have the bound for their own call (a slow call behind another finishes, where one bound over both would abandon it), and park no thread of their own, and `stop` or SIGTERM still ends the server, the runtime giving that thread 5 s before the process exits without it (after the job wait of up to 30 s when a backup job is running; see Stopping). The deletions `DeleteBackup` and the reconciler run against the store go through the same bound and rule: `DeleteBackup` answers the description with `BackupStatus` `DELETED` once its DELETING write has committed, and a deletion the store does not answer within the bound, or that leaves an object under the prefix, leaves the row DELETING for the reconciler and is logged, never an error to the client. A default SQLite root whose startup checks did not finish starts `checking` with its rule kept: a first check that fails while the catalog has no rows for it is the same warning and leaves the store unconfigured. CreateBackup on Cassandra is not refused by an unhealthy store: its in-catalog create touches no store.

The store identity. The root carries `extenddb-store.json`: a random store id and the id of the catalog that owns the root. Startup creates it when the root has no file and the catalog has no rows for the root, and otherwise reads it and compares: a file naming another catalog, a store id that differs from the one the catalog's rows for the root carry, a root with no file while rows exist, or rows carrying more than one id each mark the store unhealthy. Instances that share a catalog must share one store root (one S3 bucket and prefix, or one shared mount): host-local roots configured at the same path give each instance different bytes under one root label, so each check also `head`s the manifest of the newest AVAILABLE backup and marks the store unhealthy when it is absent, which is what a second instance with a local copy of the directory sees. The file is left by `backup purge` and by every deletion; remove it only when retiring the root, or after `extenddb destroy`: a catalog re-created with `init` has a new id, and the old root's file names the old one, so the re-initialised server reports the store owned by another catalog until `extenddb backup sync --adopt-store` makes the new catalog its owner (below), the file is removed or a new root configured (on the default SQLite root, that is the logged warning above for the server, which keeps CreateBackup on the in-catalog path, while `backup sync` handles that root as configured, so `--adopt-store` adopts it). `destroy` leaves the config file as well, which `init` refuses; `init --overwrite` regenerates the whole file from defaults, keeping only the storage connection settings (a `port`, TLS path or limit edited in the default sections reverts too) and dropping `[backup]`, `[backup.store]` and every other added section, so copy the file aside before `init` and put the copy back whole, apart from the storage settings, before the server starts: without `[backup.store]` a PostgreSQL, MongoDB or Cassandra server has no store and `--adopt-store` exits 1, and a SQLite server takes the default root, which for a store configured elsewhere is new and empty, so `--adopt-store` adopts nothing. The default root, `backups` beside the database file, is shared by every database file in that directory: `--adopt-store` on it takes the root from whichever catalog wrote its file, so give each database its own `[backup.store]` when several share a directory. A server on an in-memory SQLite database writes the file with `ephemeral_catalog: true`; at its next start it adopts a root whose file names another catalog and carries that marker, and no other, so it never takes a durable catalog's root, and then syncs the root's backups into its new catalog (the `path` row above: until that sync's log line, the earlier backups answer `BackupNotFoundException`). A catalog whose root another catalog adopted (by `--adopt-store`, or a second in-memory server's start) keeps acting on the root until a later check that can read the catalog finds it owned by another catalog, at the earliest one reconciler interval after the cycle in progress ends (a check whose catalog read fails leaves the health as it was), with one exception, its deletion fence: right before it deletes a backup's objects (DeleteBackup, the reconciler's deletions, a failed backup job's cleanup, `purge`), removes a FAILED or DELETED row, before a CreateBackup's row (at the request) and again in the backup job right before its first object, and also before DeleteBackup and `purge` change the row (`purge` reads it once before the row change and its two deletions), a server or `purge` re-reads `extenddb-store.json`, and when the file names another catalog, names this catalog under another store id than the one the store's check settled (a root replaced by another root of this catalog), or the root has none, it deletes nothing and refuses the request. The read records on the store what the next check would record, so the store is unhealthy at once: `owned by another catalog`, `identity mismatch`, `identity file missing` for a root with no file, and, for a file the store refuses to serve or that does not parse, what the check would report (`failed validation` for a file a filesystem root may not read, such as one at mode 000; `permission denied` for an S3 denial; `identity file unreadable` for one that does not parse; `failed validation` too for an S3 server error the SDK's retries did not clear, such as a 503 `SlowDown`, which leaves the store unhealthy until a check passes (the next runs one reconciler interval later)); a CreateBackup refused there answers the unhealthy-store `ValidationException`, and one whose job meets the refusal before its first object is `FAILED` with no object written. The read has a call slot of its own on a filesystem root, so a DeleteBackup's deletion of a prefix of many objects does not hold a CreateBackup's read behind it. Only a read that fails in transport (the store unreachable, the read past the store's 5 s bound) records nothing and leaves the decision to the next check: CreateBackup then answers `InternalServerError`, and DeleteBackup takes it at the read before the row is changed as it takes a store that does not answer: it writes DELETING and answers `DELETED`, the read before the deletion refuses it, and the row stays DELETING for the reconciler, whose deletion runs the fence again. `--prune-missing`'s removal of a row is not fenced: it deletes no object. A refusal at the read before the row is changed (the file names another catalog or another store id) leaves the row as it was, so DescribeBackup still answers AVAILABLE. One at the read before the deletion (an adoption landed between the two) leaves an AVAILABLE row DELETING with nothing deleted (DeleteBackup and DescribeBackup there answer `DELETED`), which this catalog's reconciler finishes only if this catalog owns the root again, and a dead CREATING row that `purge` already removed has its prefix kept. The read narrows the window to the gap between it and the deletion; on S3 that gap is a listing and a `DeleteObjects`, each under the SDK's retries, so seconds when the store throttles. A check that read the file before the adoption and ends after a fence's refusal records the store healthy again until the next cycle's check: each deletion stays fenced in that time, but a restore is accepted. A backup job of the previous owner that fails after the adoption leaves its partial prefix (no manifest, and no row of the new owner) under the root, which no duty or command of the new owner removes. A write that has already passed that read still lands, so stop every server of the catalog a file names before adopting its root. While the catalog has no AVAILABLE backup for a root, two host-local directories at one path cannot be told apart (each instance creates its own file and id, and a file copied from one directory to the other gives both one id), and the manifest probe has nothing to probe: a backup one instance completed under its own directory looks, to the other instance's reconciler, like a dead owner's row with no manifest, which it marks FAILED, and the first instance's restart then deletes the only copy. Point every instance at one shared root before the first backup; copying `extenddb-store.json` between directories defeats the check rather than satisfying it. The identity file is read up to 64 KiB; a larger object under its key is reported unreadable.

`extenddb destroy` removes the databases and leaves the store untouched: it is the copy that survives destroy. `extenddb backup list [--failed]` prints the catalog's rows for the configured store's root, every store id they carry, with each row's `store_root` and `store_id`; it is a read that runs beside a server (on SQLite it takes no serve lock), reads the identity file without creating one, and runs on an unhealthy store, naming the problem in its header. It lists backups only, never restore jobs: a restore's record is the job's log line, not a `backup list` row. `extenddb backup purge --confirm` deletes every backup of the configured store, each backup's objects (its own prefix, never a whole account prefix, so a prefix the catalog has no row for survives) and its catalog row outright, leaving no tombstone, and leaves `extenddb-store.json` and the `exports/` component; it needs the store healthy. It takes the catalog-level maintenance claim first and heartbeats it for the command's whole run, waits up to `2 * stale_job_after_secs` until no store-backed CreateBackup is running on any instance (a CREATING or FAILED row with a heartbeat within `stale_job_after_secs` on the catalog database's clock) and no restore is RUNNING, refuses and releases the claim if they have not finished, deletes a CREATING row the wait saw no live owner for as it deletes any other row, finishes any DeleteBackup left in progress as part of the purge (a backup a DeleteBackup finishes while the purge runs is counted as deleted, not left), and logs each deletion under the `extenddb::audit` target with the account and the ARN. While the claim is held, CreateBackup and RestoreTableFromBackup on every instance answer `LimitExceededException` saying that a backup maintenance command holds the catalog's maintenance claim (the message does not say which command; the second command's refusal and the claim's row do), and a second maintenance command fails at once naming the holder. Without `--confirm` it prints the plan, changes nothing and exits 0. `--config` is accepted before or after the subcommand. None of the backup commands runs the startup recovery a server runs (on PostgreSQL the vector index rebuild, the control-plane recovery and the GSI queue workers; on MongoDB the key-repair pass; on Cassandra the control-plane transitions; on SQLite the interrupted-restore and index passes): that work belongs to the servers, and a command that ran it could exit with it half done. RUNNING restores are read by status, with no age, so a restore whose record was stamped this instant holds the wait too. On SQLite `purge --confirm` opens the database as `serve` does and so takes the serve lock: stop the server first (while one runs on the file the command refuses with exit 6, as a second `serve` does); under that lock it first resolves what the stopped server left (a RUNNING restore is failed and its target removed, the CREATING target of a FAILED restore removed, an in-catalog CREATING backup removed, a DELETING row finished) and deletes a CREATING or FAILED row whatever its heartbeat says, a heartbeat stamped ahead of the clock by a backward clock step included (the server that wrote it is stopped, so the zero window the command passes selects by status alone), so such a job never makes it wait or refuse. That includes a RUNNING restore from a backup store, which a server would resume: under the serve lock the command fails it and removes its target, as for an in-catalog restore (a resume is a job, which the maintenance claim excludes), except one that recorded no progress while its target holds items, which is failed `target_not_empty` and its target left for `extenddb backup remove-target`. A server starting on the file removes a claim a killed purge left, so no job is refused for the stale window, as a purge run again after the killed one takes its claim over. On the other backends it runs beside live servers, and a claim whose holder died expires after `stale_job_after_secs`. On PostgreSQL, MongoDB and Cassandra the backup commands (`list`, `purge`, and `sync` and `remove-target` below) open the catalog as a server does and hold a serving registration for their run, named after the command (`extenddb backup list`, `extenddb backup purge`, `extenddb backup sync`, `extenddb backup remove-target`: on PostgreSQL the session's `application_name`, on MongoDB the `command` field of its `extenddb_servers` document, on Cassandra the `release` column of its `extenddb_servers` row, after the release and ` as `), and release it when they exit: `extenddb migrate --yes` waits for them as for a server and names them while it waits, and while a migration runs they refuse at once (exit 6). On a catalog `serve` would refuse they exit with `serve`'s code and message: 3 with migrations pending (with the `extenddb migrate --yes` advice), 4 on a catalog a later release migrated with a breaking migration, and 5 on one that needs attention (the upgrade manual's [catalog status](07-upgrade-manual.md#the-catalog-status) table). On every backend Ctrl-C or `SIGTERM` makes the command release what it holds and exit: the serving registration after the open, if the signal came during it; the claim at once while it waits, and before its next backup while it deletes (the release itself is bounded by 10 s, past which the claim is left to the stale rule), so an interrupted purge leaves no half-deleted backup and no claim to wait out; run it again for the rest. The dry run on an unhealthy store lists the rows and says that `--confirm` refuses until the store's check passes.

`extenddb backup sync [--adopt-store] [--prune-missing]` rebuilds the catalog's rows from the configured store: after a lost or re-created catalog, or after a backup's prefix was copied under the root from another deployment or another backend. It reads the catalog's rows for the root (of every status and every store id they carry), then lists the whole root and handles each `<account>/<table>/<backup id>/` prefix as the listing passes it, checking it against those rows before it reads anything under it. Sync holds one prefix's object list at a time and the prefix of each manifest it saw; a filesystem store builds its listing of the whole root in memory first (about 72 bytes plus the key per object), an S3 store lists a page at a time. The report keeps an entry per prefix for the whole run, and it lists every prefix the catalog already has a row for among the skipped ones, so on a root of small backups the per-prefix term dominates: about 350 bytes per prefix at one object per prefix (62 MiB of peak RSS at 50,000 such prefixes and 112 MiB at 200,000, against 45 MiB for an empty root; about 165 bytes per object at 100 objects per prefix), measured on a SQLite debug build with a filesystem root and no rows. Of the catalog's rows it keeps each row's status, store id and table, and the key of each AVAILABLE row of the configured store, but every backend reads the rows whole before sync sees the first, so the read is the larger cost and grows with the rows of the root, tombstones within their retention included: about 22.5 KiB per row of a table with five GSIs (1,452 bytes of source table description per row; 610 MiB of peak RSS at 25,000 rows and 1,144 MiB at 50,000, against 47 MiB with none; 148 MiB at 50,000 rows whose description is `{}`), measured on a SQLite debug build on a 16-core x86_64 Linux host with glibc's default arenas. `backup list` peaks the same over those rows. A prefix with no row gets an AVAILABLE row under the configured store's identity when its manifest and sidecar verify, every data file the manifest lists is under the prefix at its recorded size, and the manifest's ARN names the prefix's account, table and backup id; an ARN of another region is adopted under this server's region and logged (requests find it under either spelling), and one of another partition than `aws` is skipped, since every request refuses such an ARN. The row carries the manifest's backup name, size, item count and source table, and the backup id's instant as its creation time. It never adopts a prefix with a row of any status (CREATING, DELETING, DELETED, FAILED or AVAILABLE), one whose manifest, sidecar or data files are missing or do not match, one whose manifest names another backup, or anything under `exports/` or a table's `changelog/`, and prints each with its reason. Before the insert it reads the row under both spellings of the ARN (this server's region and the manifest's), and the insert happens only if no row exists: a CreateBackup whose row landed after sync read the rows keeps its row, as does a row of another root, account or table holding the backup id, and sync changes no existing row. It also prints the AVAILABLE rows whose manifest the listing does not hold. It exits 0 once it ran, skipped prefixes included, and 1 when a prefix was skipped because a store read or a catalog call failed (the run is incomplete, and its summary line says `incomplete`; a second run adopts it), when the listing failed part-way (the backups adopted before the failure keep their rows), when a signal stopped it, or when `--prune-missing` left a row it may remove on a later run (a store or catalog call failed, its manifest's `head` included, or a restore is reading the backup: run the command again once the restore ends). A row `--prune-missing` leaves because its manifest is back or another writer moved it is reported, and the exit stays 0:

```
=== extenddb backup sync ===
Store:  filesystem <root> (store id <store id>)

--- Adopted (an AVAILABLE row inserted for each):
  <backup arn>

--- Skipped (not adopted):
  <account>/<table>/<backup id>/: not a complete backup: no manifest

--- AVAILABLE rows whose manifest is not in the store (the rows were AVAILABLE when the catalog was read):
  (none)

=== extenddb backup sync complete: 1 backup(s) adopted, 1 prefix(es) skipped, 0 AVAILABLE row(s) whose manifest is gone ===
```

Sync needs the store healthy: on an unhealthy store it refuses, exit 1, naming the problem and changing no row. That covers an empty root under existing rows, a root whose file carries another store id and a root without the newest AVAILABLE row's manifest, which is what an unmounted volume shows. When the newest AVAILABLE row's own manifest is gone, the store is unhealthy for the same reason, so plain sync and `--prune-missing` both refuse; put the prefix (or its manifest) back and run the command again. On PostgreSQL, MongoDB and Cassandra every sync takes the maintenance claim and heartbeats it, so no `purge` runs between its read of the rows and its insert while the claim is held. Each command that holds the claim reads it lost once no heartbeat confirmed it for two heartbeat periods, and sends one heartbeat of its own right before each step it cannot undo (sync's insert, `--prune-missing`'s removal, `--adopt-store`'s rewrite of the identity file, `purge`'s removal of a dead CREATING row and of that row's prefix), stopping when the claim is lost: on MongoDB and Cassandra a command stopped for `stale_job_after_secs` (a suspended process, a paused VM) can have its claim taken over by another, and then stops before that step. A call the command had already sent when it was stopped still lands when it resumes. Plain sync waits for no job, and while it holds the claim CreateBackup and RestoreTableFromBackup are refused, as during `purge`. A sync killed while it holds the claim (`SIGKILL`, a lost host) leaves it held until its heartbeat is `stale_job_after_secs` old (300 s by default): until then every instance refuses CreateBackup and RestoreTableFromBackup. On PostgreSQL the claim is a session's advisory lock, so the next maintenance command (the sync or `purge` run again) takes a killed command's claim over at once and ends that window; MongoDB and Cassandra take it over only once it is stale. `--prune-missing` and `--adopt-store` also wait for running jobs as `purge` does. A first Ctrl-C or `SIGTERM` stops any sync before its next prefix (or the wait at once), releases the claim it holds and exits 1; the backups adopted before the stop keep their rows. `--prune-missing` removes each AVAILABLE row whose manifest is gone outright, one line per row, and deletes nothing in the store: no DELETING or DELETED row is left for a deletion duty to act on, so on host-local roots (each instance's root at one path holding other bytes) a prune on the instance that lacks a backup's bytes removes its row for every instance of the catalog and leaves the bytes on the other host: the backup answers `BackupNotFoundException` on every instance, the one holding the bytes included, until a sync on that instance adopts them again. A row a restore is reading, or one another writer moved meanwhile, is reported and left (the first exits 1). The objects a removed row's prefix still holds stay until an operator deletes the prefix by hand: `purge` deletes only the prefixes of rows, and every later sync reports the prefix as not a complete backup. `--adopt-store` is the way a durable catalog takes a root whose `extenddb-store.json` names another catalog (plain sync refuses such a root, naming the option; a server on an in-memory database adopts, at startup, a root another in-memory catalog wrote, as the identity paragraph says): under the claim it first runs the check the server would run on the root once this catalog owns it (this catalog's rows for the root against the file's store id, and the manifest of this catalog's newest AVAILABLE backup of the root) and refuses, with nothing written and the file still naming its owner, a root that check fails (a backup of this catalog whose manifest is gone from the root, say); then it logs its intent under `extenddb::audit`, rewrites the file's catalog id and backend, keeping the store id and creation time, reads the file back and logs the outcome (adopted, or the race lost when another adopter's write landed between the two), and syncs. Each store call runs under the store's 5 s bound (the catalog's reads and the claim's heartbeat do not count against it), and a store that stops answering ends the command with exit 1 naming the step that did not finish (the first read, the rewrite, whose outcome the audit log's intent line then leaves open, or the re-read). Every sync, plain or adopting, reads the file once more at the end and fails, exit 1, naming the owner, when another catalog adopted the root during the run: the rows it inserted are then rows of a root this catalog does not own, inert until this catalog adopts the root again. A write that lands after that read is found by this catalog's next check. It refuses when this catalog's rows for the root carry another store id, and on a root this catalog already owns it says so and syncs. Stop every server of the catalog the file names before running it: the previous owner finds the store owned by another catalog only at a later check that can read its catalog, and until then its deletion fence (the identity paragraph) refuses each deletion and CreateBackup once it re-reads the file, but a write already past that read lands. A second maintenance command while one holds the claim fails at once. On SQLite every form takes the serve lock: stop the server first (beside a running server it exits 6). Plain sync there resolves nothing the stopped server left: the lock already keeps every server and command off the file, so a CREATING or DELETING backup and a RUNNING restore the server left are rows sync skips, and the next server start resolves them and resumes the restore. `--prune-missing` and `--adopt-store` take the claim and first resolve what the stopped server left, as `purge` does, which fails a store-backed restore the server left RUNNING and removes its target; each prints what it resolved. The rows of other roots are reported in part: a prefix whose backup id a row of another root holds is skipped naming that root; rows of other roots as such are not listed.

Every `extenddb backup` command refuses an in-memory SQLite database (`[storage.sqlite] path = ":memory:"`, or no path on a `sqlite-memory` build) before it opens the catalog, exit 1: it would open a new, empty catalog that ends with the command. A server on such a database syncs with its store at every start instead (the `path` row above).

`extenddb backup remove-target --account <account> <table>` removes a restore target that `DeleteTable` refuses (a `CREATING` table) together with its restore record: the target of a `FAILED` restore whatever the reason (`target_not_empty` included: the state a resume leaves when it finds no recorded progress and more items in the target than the backup holds, which a catalog restored from a dump can produce when it names a restore that finished and was written to since; an ordinary crash before the first progress record on Cassandra leaves no more items than the backup holds and is resumed, not failed; until the command runs a `target_not_empty` target keeps its backup in use, so `DeleteBackup` and a second restore of the backup are `BackupInUseException` and the name is held; `owner dead: taken over 5 times by one instance without the restore ending` is the reason of a record one server's reconciler resumed five times without the restore ending, whose target the reconciler removes itself), the target of a `RUNNING` restore whose owner is dead (no heartbeat within `stale_job_after_secs` on the database's clock, the listing the reconciler uses; on SQLite under the serve lock every owner is dead), and a legacy (in-catalog) restore's target the same way. It refuses a `RUNNING` restore with a live owner ("stop it or wait for it"), a `DONE` restore and a table no restore record names, the latter two as "not a restore target", and exits 1. It needs no store (it runs whether or not `[backup.store]` is configured, and creates neither the default root nor its identity file), takes the catalog-level maintenance claim first as `purge` does and releases it before it returns, logs the removal under `extenddb::audit` with the account, the table and the backup's ARN, and prints what it did:

```
=== extenddb backup remove-target ===
Taking the maintenance claim as "extenddb backup remove-target on <hostname> (pid <pid>)".
removed the restore target <table> (account <account>, table id <table id>) and its record: its store-backed restore from <backup arn> was FAILED (target_not_empty)
The maintenance claim is released.
```

The third line's tail names the record's state: `was FAILED (<reason>)` or `was RUNNING with a dead owner`, after `its store-backed restore from <arn>` or `its in-catalog restore from <arn>` for the record's kind. A refusal is one `Error:` line on stderr (`<table> (account <account>) is not a restore target: no restore record names it`; `... its restore from <arn> is DONE (the table is ACTIVE, or awaits its index backfill)`; `the restore into <table> (account <account>) from <arn> is RUNNING with a live owner (a heartbeat within stale_job_after_secs); stop it or wait for it`; on PostgreSQL, `the restore into <table> (account <account>) from <arn> is a CREATING restore target (table id <id>) whose owner's lock is still held: its owner's session is alive though its heartbeat is stale; wait for the owner to stop, or stop it, and run the command again`, the owner's claim session holding the target's advisory lock, a frozen process for instance; `the maintenance claim is held by "<holder>"; one maintenance command runs at a time, and a claim whose holder died expires after stale_job_after_secs (<n> s)`). On SQLite it opens the database as `serve` does and takes the serve lock: stop the server first, as for `purge`; under that lock every owner is dead, so a `RUNNING` record's target is removed too. After it, `DescribeTable` of the name is `ResourceNotFoundException`, `DeleteBackup` of the backup succeeds and a new restore into the name runs.

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

#### [logging]

| Key | Default | Description |
|-----|---------|-------------|
| `level` | `info` | Initial log level (overridden by runtime setting) |
| `format` | `pretty` | Log format: `pretty` or `json` |

The server logs to syslog (facility: daemon, ident: extenddb) when it runs as a daemon, and to stderr with `--foreground`.

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
| `allow_credential_import` | `false` | Whether `import-access-key` is allowed. Only `true` enables it |
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

# Import existing credentials (refused unless allow_credential_import is true)
./target/release/extenddb settings --config extenddb.toml set \
    allow_credential_import true
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
| RestoreTableToPointInTime | the same item actions on the target `table/<name>`, then the source `table/<name>` |
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
  also needs those actions on the target table name. RestoreTableToPointInTime
  checks the item actions on the target before the restore action, as DynamoDB
  does, so a principal with no policy is denied naming `dynamodb:Scan` on the
  target table.
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

- **Dashboard**: Account and admin user counts (admin sessions only; an IAM-user session gets a link to its own user page), version info
- **Account management**: Create, view, delete accounts (admin only; an IAM-user session is redirected to `/console` from the account list and from every account page)
- **User management**: Create, delete users; view access keys, policies, tags, group memberships (an IAM-user session opens its own user page only, which lists its access keys and leaves out its policies, groups and tags)
- **Access key management**: Create and delete access keys (secret shown once; an IAM-user session manages its own keys only)
- **Group management**: Create, delete groups; add/remove members (admin only)
- **Role management**: Create, delete roles; view trust policies (admin only)
- **Policy management**: Add, delete inline policies with JSON editor
- **Metrics dashboard**: Operation metrics for every account, with charts (admin only; an IAM-user session is redirected to the console home page, `/console`)
- **Settings and Cache**: The read-only settings viewer and the auth cache invalidation form (admin only; an IAM-user session is redirected to `/console`)

### Authentication

- Admin users: enter username and password
- IAM users: enter `account_id/user_name` as username, console password as password

Sessions end 8 hours after login. Click "Logout" to end immediately. Deleting an admin or IAM user, or changing its password, ends its console sessions on every server at their next request.

## Monitoring

### Syslog

The server logs to syslog (facility: daemon, ident: extenddb) when it runs as a daemon, and to stderr with `--foreground`.

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

The server logs some management operations, and the settings changes made through the management API, at WARN level. [Security Model: Logging](10-security-model.md#logging) lists them.

```bash
# View audit entries
journalctl -t extenddb | grep 'extenddb::audit'
```

Targets: `extenddb::audit::manage` (some management operations), `extenddb::audit::settings` (settings changes made through the management API).

### Metrics

```bash
# Requires an admin user's name and password (HTTP Basic auth); answers 401 without them
curl --cacert ~/.extenddb/tls/cert.pem -u admin:<pw> https://127.0.0.1:18443/metrics
```

JSON metrics endpoint with DynamoDB CloudWatch-style metric names and dimensions. The response shape is `{ metrics, buckets, segments, source }`. See `docs/design/06-component-server.md` §7.2 for the full schema and metric list.

### Health Check

```bash
curl --cacert ~/.extenddb/tls/cert.pem https://127.0.0.1:18443/health
# {"status":"healthy","components":{}}
# (the key order depends on the build; the earlier Health Check section
# shows the components a backup store adds)
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

On PostgreSQL, SQLite, Cassandra and MongoDB, `serve` reads the catalog's migration status before it opens the engine and refuses a catalog it cannot serve, printing the status and the command that resolves it. A catalog with pending migrations exits 3, for example `the catalog is at migration 9, and ExtendDB <release> needs migration 10`, followed by the pending migrations and `extenddb migrate --config extenddb.toml --yes`. A catalog that a newer release migrated with a breaking migration exits 4 and names that release's migrations; upgrade the binary rather than the catalog. The [Upgrade Manual](07-upgrade-manual.md#the-catalog-status) lists every status with its exit code and the stop / migrate / start sequence.

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

Table backups (`CreateBackup`) hold one table's items and definition; they do not hold the catalog's accounts, IAM users, keys, policies and settings. Back the databases up with the backend's own tools as well; the [Backup and Restore manual](14-backup-and-restore.md) covers table backups, the backup store and recovery from a lost catalog. The databases each backend uses:

| Backend | Databases |
|---------|-----------|
| PostgreSQL | The catalog database, `extenddb_catalog` unless `init --catalog-db` names another, and the data database, whose name `init` derives from the catalog's by dropping the `_catalog` suffix (`extenddb` for the default; `init --data-db` names another). A catalog name without the `_catalog` suffix needs `--data-db`, since the derived name would be the catalog's own; an `init` that failed this way leaves the catalog database behind and writes no config file, so drop that database before running `init` again with `--data-db`. The catalog records the data database's name in its `settings` table under `data_database_name` |
| SQLite | One database file (`[storage.sqlite] path`) with its `-wal` and `-shm` files beside it; stop the server before copying the three. To restore, stop the server, remove the current `-wal` and `-shm` files, and put the copied files in their place |
| MongoDB | `extenddb_catalog` and `extenddb_data`, fixed names |
| Cassandra | The catalog keyspace `<keyspace_prefix>_catalog` and one keyspace per account, `<keyspace_prefix>_account_<account id>` (`keyspace_prefix` defaults to `extenddb`) |

On PostgreSQL the catalog and data databases are one copy: dump both with every server of the catalog stopped, or, to back up without stopping the server, take a base backup (`pg_basebackup`) of the PostgreSQL server that holds both. Each `pg_dump` takes its own snapshot of one database, so two dumps taken while a server runs disagree when a table is created or deleted between them. The catalog dump holds the encryption key and the application role's password (in the `data_database_connection_string` setting), so create both files readable by their owner alone, with `umask 077`. With the default names:

```bash
# Backup, with the server stopped, with the pg_dump of the server's major version
umask 077
pg_dump extenddb_catalog > catalog_backup.sql && pg_dump extenddb > data_backup.sql \
  && test -s catalog_backup.sql && test -s data_backup.sql && echo "both dumps written"
```

Use the `pg_dump` of the PostgreSQL server's major version (for a server in a container, the container's own: `docker exec <container> pg_dump ...`). An older `pg_dump` refuses, "aborting because of server version mismatch", and exits 1, and the shell has already created its file, empty. Go on only when both commands exited 0 and neither file is empty: `psql -v ON_ERROR_STOP=1` restores an empty file without an error, and the server then refuses to start ("the catalog at ... is not initialized").

Restore into new, empty databases owned by the application role (`extenddb` unless `init --extenddb-user` named another), created by the PostgreSQL admin role, with the server stopped. Restoring over the existing databases restores nothing: the dump's statements fail on the objects already there (`psql` prints an `ERROR` line for each and exits 0), and the tables keep their current items. Rename the old databases aside first, and drop them only once the restored server has read every table, so that the old copy stays until the new one is proven. The owner matters: on PostgreSQL 15 and later the `public` schema belongs to the database's owner and the dump does not carry it, so in a database the admin role owns the server cannot create tables (`CreateTable` fails and the server logs "permission denied for schema public"). `-v ON_ERROR_STOP=1` makes `psql` stop at the first error, with exit 3:

```bash
# Restore, as the PostgreSQL admin role, with the server stopped
psql -c 'ALTER DATABASE extenddb_catalog RENAME TO extenddb_catalog_before_restore' postgres
psql -c 'ALTER DATABASE extenddb RENAME TO extenddb_before_restore' postgres
createdb -O extenddb extenddb_catalog
createdb -O extenddb extenddb
psql -v ON_ERROR_STOP=1 -f catalog_backup.sql extenddb_catalog
psql -v ON_ERROR_STOP=1 -f data_backup.sql extenddb
# Start the server and read every table, then
psql -c 'DROP DATABASE extenddb_catalog_before_restore' -c 'DROP DATABASE extenddb_before_restore' postgres
```

On a new PostgreSQL server (the old one lost with its databases) create the application role first: a dump holds no roles, and without it `createdb -O extenddb` fails, `role "extenddb" does not exist`. Give it the password that the catalog dump's `data_database_connection_string` row carries (`postgresql://extenddb:<password>@<host>:<port>/extenddb`), the one the config file's `[storage.postgres] connection_string` names as well: `psql -c "CREATE USER extenddb WITH PASSWORD '<password>'" postgres`. That row also records the data database's host and port, and the server connects to the data database there. On a server at another address, point the config file's `connection_string` at it and, after the restore and before the server starts, set the row: `psql -c "UPDATE settings SET value = 'postgresql://extenddb:<password>@<new host>:<new port>/extenddb' WHERE key = 'data_database_connection_string'" extenddb_catalog`. Without that the server connects to the old address: it fails to start once nothing answers there, and while the old server still answers, the tables created then keep their items in the old server's data database while the new catalog lists them.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
