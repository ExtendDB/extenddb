---
description: "Every key of extenddb.toml, its default and what it controls, generated from extenddb.sample.toml."
custom_edit_url: https://github.com/ExtendDB/extenddb/edit/main/extenddb.sample.toml
---

# Configuration

This page is generated from [`extenddb.sample.toml`](https://github.com/ExtendDB/extenddb/blob/main/extenddb.sample.toml) by `npm run gen:config` in `website/`; edit the sample, not this file. Tests in the `extenddb-config` crate and in each storage backend crate compare the sample with the configuration structs, so a key listed here exists in the code with the default shown.

The server reads `extenddb.toml` (or the file named by `--config`) at startup. Every key can also be set through an environment variable: the name is `EXTENDDB__` followed by the key path in upper case with `__` between levels, for example `EXTENDDB__SERVER__PORT=9000` or `EXTENDDB__STORAGE__POSTGRES__CONNECTION_STRING="postgresql://..."`. A list key (`import.paths`, `export.paths`, `storage.cassandra.contact_points`) takes one comma-delimited string, `EXTENDDB__IMPORT__PATHS="/data/in,/data/shared"`; items are trimmed and empty items dropped, and an item that itself contains a comma can only be written as an array in the file. Precedence, highest first: environment variables, the configuration file, built-in defaults; `extenddb serve --port` overrides `server.port` on top of all three.

The keys below are static: a change takes effect at the next server start. Settings that change on a running server (log level, control plane delay, throttling at runtime) are set with `extenddb settings set` and are listed under [Runtime Settings](./manuals/05-admin-guide.md#runtime-settings) in the administration guide.

## Top-level keys

These keys sit at the top level and must appear before the first [section] header of the file. A top-level key placed after a header lands inside that section, and every section refuses to start the server with "unknown field" and the key named.

| Key | Default | Description |
|-----|---------|-------------|
| `docs_dir` | none | Path to the rendered documentation directory (HTML and PDF files) produced by `python3 docs/build-docs.py`. When set, the web console serves the documentation at /console/docs; when absent, those routes show a "not configured" message. `extenddb init` discovers this directory when it exists. Example: `docs_dir = "/path/to/docs/rendered"` |
| `max_import_bytes` | none | Maximum import file size in bytes. When set, it replaces `limits.max_import_file_bytes`, which carries the 10 GB default. Example: `max_import_bytes = 10737418240` |
| `import_export_root` | none | Deprecated: a single root for both import and export. It fills whichever of the [import] and [export] lists is empty: with both sections present it does nothing, with one present it supplies the other, and with neither present it makes the import and export roots the same directory. The server logs which of those happened at startup. Prefer the sections; this key will be removed. Example: `import_export_root = "/var/lib/extenddb/data"` |

## `[server]`

The listening socket and the per-server behavior that is fixed at startup. The management web console is served at /console/ on the same address and port as the DynamoDB API and needs no configuration of its own.

| Key | Default | Description |
|-----|---------|-------------|
| `bind_addr` | `"127.0.0.1"` | Network interface to bind. The default listens on loopback only; bind 0.0.0.0 to accept connections from other hosts. A dev-mode build refuses a non-loopback address. |
| `port` | `18443` | Listening port for the DynamoDB API, the management API and the console. |
| `region` | `"us-east-1"` | AWS region used when generating Amazon Resource Names (ARNs). Requests must carry a credential scope for this region; any other region is rejected with `InvalidSignatureException`, as the service rejects a scope for a region other than the endpoint's. Every backup ARN the server issues carries it, so the value must stay within [a-z0-9-]. Under a value outside that class the server logs a warning at startup and refuses `CreateBackup` with a `ValidationException` naming `server.region`, since no backup ARN can be formed; a backup taken earlier under such a region keeps an ARN that `DescribeBackup`, `DeleteBackup` and `RestoreTableFromBackup` refuse as not valid. See the [upgrade manual](./manuals/07-upgrade-manual.md) before changing it on a deployment that has backups. |
| `run_dir` | `"~/.extenddb/run"` | Directory for runtime files (the process ID file). A leading ~ expands to $HOME. When HOME is unset the default is /tmp. |
| `throttling_enabled` | none | Provisioned throughput throttling via in-memory token buckets. When true, tables with provisioned capacity are rate limited to their configured read capacity units (RCU) and write capacity units (WCU), on-demand tables use the default ceilings (12,000 RCU and 4,000 WCU), and the account-level aggregate limits in [limits] apply. This key sets the value the server starts with; once the `throttling_enabled` runtime setting has been written with `extenddb settings set`, the runtime setting takes precedence. Unset means throttling is off. Example: `throttling_enabled = false` |
| `request_timeout_secs` | `30` | Seconds a connection may take to deliver its request headers, and seconds a request body may stall without delivering data, before the server closes the connection (a stalled body gets HTTP 408). Bounds how long a stalled or malicious client can hold a connection open. A connection that is accepted, or that completes the TLS handshake, and then sends no byte is closed after the same number of seconds, as is one that sends only part of the 24-byte HTTP/2 preface. The same value bounds how long an idle keep-alive connection waits for its next request, so SDK connection pools see idle connections closed after it (they reopen transparently), and it is the HTTP/2 keep-alive PING interval and timeout, so a stalled HTTP/2 connection is closed within twice the value. Must be at least 1 and at most 86400 (a day); the server refuses a larger value at startup, because the per-connection timers add it to the current instant and a value near `u64::MAX` overflows that addition. |

## `[server.tls]`

Transport Layer Security (TLS) is mandatory. `extenddb init` generates a self-signed certificate; replace it with one signed by a certificate authority for production use.

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `true` | Accepted for backward compatibility. The server refuses to start when this is set to false. A dev-mode build serves plain HTTP on loopback and ignores this key. |
| `cert_path` | `"~/.extenddb/tls/cert.pem"` | Path to the certificate file in Privacy Enhanced Mail (PEM) format. A leading ~ expands to $HOME. When HOME is unset the default is /tmp/extenddb-cert.pem. |
| `key_path` | `"~/.extenddb/tls/key.pem"` | Path to the private key file in PEM format. A leading ~ expands to $HOME. When HOME is unset the default is /tmp/extenddb-key.pem. |

## `[storage]`

Each ExtendDB binary contains exactly one storage backend and reads the [storage.&lt;backend&gt;] section with that backend's name. The other [storage.\*] sections are ignored, so one file can carry all four. ExtendDB owns the collections, tables and indexes of its catalog and data databases. A change to them, such as an added index, is not supported and can make a later migration fail.

| Key | Default | Description |
|-----|---------|-------------|
| `backend` | none | Name of the storage backend this file is written for: "postgres", "sqlite", "mongodb" or "cassandra". The key is optional; when present it is checked against the binary's compiled-in backend and the server refuses to start on a mismatch. Example: `backend = "postgres"` |

## `[storage.postgres]`

The connection string points to the catalog database, which stores table metadata and the connection information for the data database. Several independent catalogs can coexist on one PostgreSQL instance.

Connection budget: `pool_size` sizes two pools, one against the catalog database and one against the data database, which `extenddb init` creates separately. Total PostgreSQL connections used: 2 \* `pool_size` + `catalog_pool_size` + `[backup] max_concurrent_jobs` (one claim connection per running backup or restore job), plus one per vector index build running on this server and one while a schema migration runs. PostgreSQL's max_connections is per cluster, not per database, so both pools draw on the same budget. The defaults need 64 of PostgreSQL's default 100. The claim connections, the index build session and the migration connection sit outside every pool. A build's ownership lock is session scoped, so the build holds a session that ends with it; a pooled connection would return to the pool still holding the lock. A job's claim is a session-level advisory lock held the same way. The migration lock is transaction scoped and runs in one explicit transaction, so it needs a connection pinned to one PostgreSQL backend until COMMIT; a transaction-pooling proxy could otherwise move it between backends. The administration guide's section on the [connection budget and restores](./manuals/05-admin-guide.md#postgresql-connection-budget-and-restores) describes how backup and restore jobs draw on this pool.

| Key | Default | Description |
|-----|---------|-------------|
| `connection_string` | see description | Connection string for the catalog database. For production add `?sslmode=require` and a password of your own. Default: `connection_string = "postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog"` |
| `pool_size` | `20` | Maximum concurrent connections in each of the two data-operation pools. Minimum 10: smaller values are raised to 10 with a startup warning, and a value under 2 \* `[backup] max_concurrent_jobs` + 10 is raised to that with a warning naming both numbers. Increase for higher concurrency and raise PostgreSQL's max_connections to at least 2 \* `pool_size` + `catalog_pool_size` + `max_concurrent_jobs` + the number of vector index builds you expect to overlap. |
| `catalog_pool_size` | none | Maximum connections for the management and catalog pool (authorization, Identity and Access Management (IAM), console). Minimum 10, raised with a startup warning. With auth enabled each DynamoDB request makes concurrent authorization queries, so size this to match the expected request concurrency. Falls back to `pool_size` when absent. Example: `catalog_pool_size = 20` |

## `[storage.sqlite]`

Developer mode backend. Select it with `extenddb init --backend sqlite`. The database path can be given at init time (`extenddb init --sqlite-path <path>`, written into this file) or set here; at serve time the `EXTENDDB__STORAGE__SQLITE__PATH` environment variable overrides it.

| Key | Default | Description |
|-----|---------|-------------|
| `path` | `"extenddb.sqlite"` | Database file. A relative path is resolved against the working directory when the configuration loads, before `serve` daemonizes. ":memory:" selects an ephemeral in-memory database whose data is lost on shutdown and whose catalog is new at every start: with [backup.store] configured the server adopts the root an earlier in-memory catalog owned and syncs its backups into the new catalog before it serves, within 60 s (an S3 store in the reconciler's first cycle; a sync not finished in 60 s, or one that met a store or catalog error on a prefix, runs again in the next cycle). Every run lists the root from its first key, so the startup sync completes only while listing the whole root fits in 60 s; past that it stays pending, is retried every cycle, and the backups beyond what a run reaches are not served. A run holds the whole listing of the root in memory (one entry per object) until it ends, and a listing that outlasts 60 s reaches no prefix. On a filesystem store a run abandoned at the 60 s bound does not stop its listing: the walk of the root runs to its end on the blocking pool, holding the listing it builds, and no second walk starts beside it; until it ends, every check of the store is refused at once ("an abandoned startup sync's store call has not returned") and records the store `unreachable`, so CreateBackup, DeleteBackup and restores are refused and the sync does not run; the first reconciler cycle after the walk ends finds the store healthy and runs the sync again, from the first key. A root too large to list within 60 s is never synced by an in-memory server: give it a smaller root, or a durable catalog. On a filesystem store the server does not answer, `/health` included, until the startup sync ends (a liveness probe with a short timeout restarts such a server on a large root, each start syncing from the first key), and a signal then ends the process at once. Until the log line `startup sync of the in-memory catalog: N backup(s) adopted` the earlier backups answer `BackupNotFoundException`, although `/health` may already report the store healthy; a prefix that line reports as having met a store or catalog error stays so until a later cycle's line. Each run names the prefixes it skipped for a store or catalog error at WARN; until one run has listed the whole root and logged its line of counts, each run also names every other prefix it skipped at INFO (so the reasons reach the log when the first run was cut at the bound or stopped by an error), and a later run of a sync left pending logs those at DEBUG, so a root of many backups with one that fails costs one WARN line and the line of counts per cycle. A server killed inside a DeleteBackup (between the DELETING write and the manifest's deletion), or restarted after a DeleteBackup whose manifest deletion failed, serves that backup again at its next start, since the startup sync adopts the complete prefix; a prefix a stopped server had not finished writing or deleting (a killed CreateBackup, a failed backup whose prefix deletion had not finished, a deletion cut after its manifest) stays under the root with no row to drive its deletion (the next start's sync skips a prefix without its manifest); the startup sync names each such prefix in its log line `backup store startup sync: <prefix> not adopted: not a complete backup: ...`. Delete such a prefix by hand, since every `extenddb backup` command refuses an in-memory database, but only one that has no `extenddb-manifest.json` and whose backup `ListBackups` does not return: a CreateBackup the running server is still writing has no manifest until its last put either, and its row is in `ListBackups` (status CREATING). Start a new in-memory server only after the old one has exited: one started beside it adopts the root while the old one still acts on it until a later check finds it adopted, its deletion fence (the store identity paragraph of the administration guide's backup store section) excepted. A root that an in-memory server of an earlier development build wrote has an `extenddb-store.json` without `ephemeral_catalog` (no release wrote one), which reads as a durable catalog's: the server finds the root owned by another catalog, its store stays unhealthy, and no command can adopt it. Remove that `extenddb-store.json`; the next start writes a new one (a new store id) and adopts the backups under the root. Every `extenddb backup` command refuses an in-memory database. |
| `pool_size` | `10` | Connection pool size. Writes are serialized by the engine whatever the size. A file database raises a value of 1 to 2 and logs it at info, because a backup holds one reader connection for its whole duration while its batches write through another; use 3 or more so a running backup does not contend with ordinary requests. Besides the `[backup] max_concurrent_jobs` bound, the process runs at most `pool_size` / 2 - 1 backups at once (between 1 and 16; 4 with the default) and refuses a CreateBackup that finds no slot free at once with LimitExceededException, without waiting; a file database raises a smaller `pool_size` to 2 \* (`max_concurrent_jobs` + 1), at most 34, so that bound refuses no job `max_concurrent_jobs` admits. |

## `[storage.mongodb]`

MongoDB requires a replica set, even a single-node one, because conditional writes and transactions use multi-document transactions.

| Key | Default | Description |
|-----|---------|-------------|
| `connection_string` | none | MongoDB connection string. Required. Example: `connection_string = "mongodb://localhost:27017/?replicaSet=rs0"` |
| `max_connections` | none | Maximum concurrent connections for data operations. Unset means 50. Example: `max_connections = 50` |
| `max_catalog_connections` | none | Shared catalog and authorization pool for management and authorization traffic; size it for their combined concurrency. Unset means 20. Example: `max_catalog_connections = 20` |
| `transaction_read_concern` | `"snapshot"` | Read concern for the multi-document transactions behind conditional writes, TransactWriteItems, TransactGetItems and idempotency tokens. Accepted values: "snapshot", "majority" and "local". "snapshot" keeps the single point-in-time guarantee. Some MongoDB-wire-compatible servers reject "snapshot" transactions with CommandNotSupported (code 115); set "majority" or "local" only when required. Both lose the point-in-time guarantee and "local" may read data that is later rolled back after a failover. The server logs a startup warning for either. |

## `[storage.cassandra]`

Cassandra backend. Keyspaces are created with NetworkTopologyStrategy in the configured datacenter. One connection pool serves catalog and data traffic.

| Key | Default | Description |
|-----|---------|-------------|
| `contact_points` | none | Contact points as host:port strings. Required. Example: `contact_points = ["localhost:9042"]` |
| `username` | none | Username for authentication. Omit for an unauthenticated cluster. Example: `username = "cassandra"` |
| `password` | none | Password for authentication. Omit for an unauthenticated cluster. Example: `password = "cassandra"` |
| `keyspace_prefix` | `"extenddb"` | Prefix of the keyspaces the backend creates. |
| `replication_factor` | `3` | Replication factor for the keyspaces the backend creates. |
| `datacenter` | `"datacenter1"` | Datacenter name used in NetworkTopologyStrategy. |
| `max_connections` | `10` | Maximum connections per host. |

## `[auth]`

Authentication is mandatory. Run `extenddb init` to create the encryption key and the admin user before the first start.

| Key | Default | Description |
|-----|---------|-------------|
| `provider` | `"builtin"` | Auth provider. "builtin" verifies Signature Version 4 (SigV4) signatures against the local credential store and evaluates IAM policies. The server refuses to start with "none". |

## `[auth.cache]`

In-memory stale-while-revalidate caches sit between the request path and the catalog database. Without them each request issues about six catalog queries during authentication and authorization; with them the steady state is close to none. Changes made through this server's admin API propagate at once by write-through invalidation. Changes made elsewhere (other nodes of a multi-node deployment, or direct database writes) take up to `ttl_seconds` to become visible. The server refuses to start when `soft_ttl_seconds` or `negative_ttl_seconds` exceeds `ttl_seconds`, or when `max_entries` is 0. The administration guide's section on [cache statistics and invalidation timing](./manuals/05-admin-guide.md#auth-cache-statistics-and-invalidation-timing) covers the metrics endpoint.

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `true` | Kill switch. When false, the cache wrappers skip lookup, insert and eviction and forward every request to the catalog. Use it during incident response. |
| `ttl_seconds` | `60` | Hard time to live in seconds. Entries older than this trigger a fresh load that the request waits for. |
| `soft_ttl_seconds` | `30` | Stale-but-usable threshold in seconds. Entries older than this are returned at once while a background task refreshes them. Set it equal to `ttl_seconds` to disable refresh-ahead. |
| `negative_ttl_seconds` | `5` | Time to live in seconds for negative results ("not found"). Short so newly created entities become usable quickly. |
| `max_entries` | `10000` | Maximum entries per cache; least recently used entries are evicted beyond it. |

## `[limits]`

The defaults match the DynamoDB service quotas. Override them only to test edge cases.

| Key | Default | Description |
|-----|---------|-------------|
| `max_item_size_bytes` | `409600` | Maximum size of one item in bytes, attribute names and values included. |
| `max_partition_key_size_bytes` | `2048` | Maximum size of a partition key value in bytes. |
| `max_sort_key_size_bytes` | `1024` | Maximum size of a sort key value in bytes. |
| `max_tables_per_account` | `2500` | Shown on the console settings page. Not enforced: CreateTable does not count the account's tables. |
| `max_gsis_per_table` | `20` | Maximum number of global secondary indexes per table. |
| `max_lsis_per_table` | `5` | Maximum number of local secondary indexes per table. |
| `list_tables_max_per_page` | `100` | Largest Limit value ListTables accepts, and its page size when Limit is absent. |
| `max_table_name_length` | `255` | Maximum length of a table name in characters. |
| `min_table_name_length` | `3` | Minimum length of a table name in characters. |
| `per_table_max_rcu` | `40000` | Value DescribeLimits reports as TableMaxReadCapacityUnits. CreateTable and UpdateTable do not enforce it. |
| `per_table_max_wcu` | `40000` | Value DescribeLimits reports as TableMaxWriteCapacityUnits. CreateTable and UpdateTable do not enforce it. |
| `per_account_max_rcu` | `80000` | Maximum aggregate read capacity units across the account, enforced when throttling is enabled and reported by DescribeLimits. |
| `per_account_max_wcu` | `80000` | Maximum aggregate write capacity units across the account, enforced when throttling is enabled and reported by DescribeLimits. |
| `allow_multipart_table_keys` | `false` | Preview extension: allow multi-part (composite) keys on base tables. When false, base tables follow the DynamoDB rule of one HASH key and at most one RANGE key. Global secondary indexes allow multi-part keys regardless. |
| `max_attribute_name_bytes` | `65535` | Maximum attribute name size in bytes. |
| `max_expression_tokens` | `4096` | Maximum number of tokens in one expression (condition, update, projection or key condition). |
| `max_expression_length_bytes` | `4096` | Maximum size of one expression string in bytes, measured on the text before #name and :value substitution. |
| `max_expression_depth` | `150` | Maximum nesting depth in condition expressions (parentheses, NOT, AND and OR). |
| `max_policy_document_bytes` | `6144` | Accepted but not read. The policy document size limit is fixed at 6,144 bytes, measured before JSON parsing. |
| `max_import_file_bytes` | `10737418240` | Maximum import file size in bytes (10 GB). The top-level `max_import_bytes` key replaces this value when set. |
| `max_import_item_count` | `10000000` | Maximum number of items in one import. |
| `max_export_item_count` | `10000000` | Maximum number of items in one export. |
| `enforce_reserved_keywords` | `true` | Reject bare reserved keywords in expressions, as the service does. |

## `[logging]`

In daemon mode output goes to syslog (facility daemon, ident `extenddb`); read it with `journalctl -t extenddb`. With `extenddb serve --foreground` output goes to stderr for the process supervisor. The level set here is the one the server starts with; the `log_level` runtime setting changes it without a restart.

| Key | Default | Description |
|-----|---------|-------------|
| `level` | `"info"` | Log level: "trace", "debug", "info", "warn" or "error". |
| `format` | `"pretty"` | Log format: "pretty" for humans or "json" for structured output. |

## `[backup]`

The knobs backup and restore jobs and the reconciler read. Every key has a default, so the section may be left out. The server refuses to start when `stale_job_after_secs` is under three heartbeat periods, when `data_file_target_bytes` is under 32 MiB, when `max_concurrent_jobs` is 0 or over 1024, when `heartbeat_interval_secs` or `reconcile_interval_secs` is 0, or when `failed_row_retention_secs` is under `stale_job_after_secs`. This section and [backup.store] below are the reference for the keys; the [Backup and Restore manual](./manuals/14-backup-and-restore.md) covers the strategy, the recovery point and time, securing the store, the restore drill and catalog-loss recovery, and the administration guide's section on the backup store covers the `extenddb backup` commands.

| Key | Default | Description |
|-----|---------|-------------|
| `data_file_target_bytes` | `67108864` | Uncompressed bytes per backup data file, read by the store-backed backup writer. Minimum 33554432 (32 MiB), the cap on one line of a data file: a data file closes at the first item that takes it to or past this size, so it exceeds the size by less than one line. |
| `heartbeat_interval_secs` | `15` | The job heartbeat timer, in seconds. The store-backed backup job heartbeats every max(`heartbeat_interval_secs`, 15) s and starts no store write unless a beat confirmed its claim within two periods or a beat is still in flight (on SQLite a beat waits behind any writer holding the engine write lock, a GSI backfill say, for as long as it holds it, and a backup goes on writing meanwhile; on the other backends a beat in flight does not count), the claim's insert counting as the first beat; the in-catalog backup and restore paths heartbeat every 15 s whatever this says. The staleness floor below is computed from the larger of the two. |
| `stale_job_after_secs` | `300` | Heartbeat age in seconds after which a job's owner is dead (on PostgreSQL, once the owner's claim session has ended as well) and the reconciler resolves the job: a dead backup job with a complete manifest under its prefix is AVAILABLE, one without is FAILED and its prefix deleted; a dead in-catalog restore is FAILED and its target removed; a dead store-backed restore is resumed from its recorded progress (the differences document's restore-interruption row). After the same age a maintenance command that died (killed, or its catalog connection lost) no longer holds the maintenance claim, on PostgreSQL too (until then the next one fails at once, naming the holder). Must be at least three heartbeat periods, where the period is the larger of `heartbeat_interval_secs` and the 15 s the in-catalog jobs heartbeat at, so 45 at the least. On SQLite a heartbeat waits for the engine write lock like any writer, so a value near the floor can declare a live job dead on a busy file; keep a margin above the floor. |
| `failed_row_retention_secs` | `604800` | How long FAILED and DELETED backup rows of the configured store are kept once their objects are gone, in seconds (7 days); `extenddb backup list --failed` prints the FAILED ones with their reason. At least `stale_job_after_secs`: the reconciler deletes a stopped job's late object only under its row, so the row outlives the window in which that job can still write. |
| `reconcile_interval_secs` | `60` | The reconciler's cycle, in seconds; it also runs once at startup. Each cycle checks the store, resolves backup and restore jobs whose owner is dead, finishes deletions, deletes the objects of FAILED backups once their heartbeat is older than `stale_job_after_secs`, and removes tombstones past `failed_row_retention_secs` (the differences document's restore-interruption row lists the duties). |
| `max_concurrent_jobs` | `4` | Backup and restore jobs this instance runs at once, the in-catalog creates and restores included; at most 1024. A request that finds every slot held waits up to 30 s for one and is then refused with LimitExceededException. On PostgreSQL each running job holds one claim connection outside the pools, and `pool_size` and `catalog_pool_size` are raised to at least 2 \* `max_concurrent_jobs` + 10 when smaller; on SQLite `[storage.sqlite] pool_size` is raised to 2 \* (`max_concurrent_jobs` + 1), at most 34, when its snapshot slots (`pool_size / 2 - 1`, between 1 and 16) would refuse a job this knob admits. Memory: a running backup holds about 490 MiB (PostgreSQL) or 650 MiB (SQLite) at most in the pipeline's own terms for a table of items at the 32 MiB line cap, a few tens of MiB for 200-byte items (the figures are for the default `max_item_size_bytes`, 400 KB, where no stored item's line exceeds the cap; on an instance that raises it the terms are at the largest line a stored item expands to, about 68 times the item's size for the worst shape); with glibc's default allocator the process's high-water mark grows with the number of large items a job reads (measured 760 MiB over idle at 48 items of 27.7 MB on a quiet host and 1,281 MiB on a loaded one, 1.39 and 2.35 times the terms at that item size) because the items' strings are allocated on one thread and freed on another and the per-thread arenas keep that memory. Set `MALLOC_ARENA_MAX=1` in the server's environment (the deployment guide's service unit carries the line, commented out) on a host that backs up large items: the peak is then the terms' and does not grow with the table (measured 452 to 481 MiB over idle at 12, 24 and 48 items of 27.7 MB on SQLite, in two series on two hosts' loads, and 340 MiB `VmHWM` (about 200 MiB over idle) at 12 items on PostgreSQL); one arena serializes the worker threads' allocations on one lock and costs the request path a few percent (measured 3.5 to 9.2% on GetItem, Query and Scan on SQLite and about 2% on PostgreSQL (the scans, 2.1%), where the request path waits on the database on PostgreSQL). Size the host for 1.2 times the terms (about 590 MiB on PostgreSQL, 780 MiB on SQLite) times this knob under `MALLOC_ARENA_MAX=1`; without it the peak grows with the large rows a job reads and no bound was measured (1,281 MiB over idle at 48 items of 27.7 MB is the largest seen). A running restore holds the parsed manifest, one decoded data-file line and its parse, the decoded items of 32 MiB of data-file text plus one line per chunk (the chunk closes with the item that reaches the bound) at four chunk positions between the decoder and the load (two in the channel, the one being filled, the one being drained; a chunk also closes at 1,000 items, and the chunks never hold more than one data file's items), and the backend's write batch: at most 32 MiB of item text on SQLite and Cassandra (10,000 items when they are smaller), held beside the decoded items of that text; 500 items (or 4 MiB) per INSERT statement inside one transaction per data file on PostgreSQL; 1,000 documents per `insert_many` on MongoDB (the driver splits the call); times this knob. For a table of large items that is the same order as the backup, not less: measured `VmHWM` over idle from a fresh start (`docs/design/15-backup-restore-bench-pr9.md`), 24 items of 27.7 MB restore with a peak of 848 MiB on SQLite and 778 MiB on PostgreSQL under the default allocator (848 and 1,013 MiB at 12 and 48 such items on SQLite: the peak grows with the large items a restore reads, as the backup's does, and no bound was measured), and 392 MiB on SQLite and 427 MiB on PostgreSQL under `MALLOC_ARENA_MAX=1`, the same setting as above; 1,200 items of 350 KB in one data file 270 MiB on SQLite and 156 MiB on PostgreSQL; 1M items of 200 bytes under a composite key with no secondary index, the backup's lines in key order as the server's own backups are, 89 MiB on SQLite (the write connection's page cache and the final `COUNT(*)`, each bounded by `cache_size`; the count's share was 22 to 28 MiB at 1M and at 3M rows) and 16 MiB on PostgreSQL, and the same 1M items as an unordered backup (every MongoDB-sourced backup and every multi-part-key table) 144 to 164 MiB on SQLite in 66 to 90 s against 27 to 56 s key-ordered (scattered inserts dirty the whole base b-tree; the restore's buffers are the same) and 16 MiB on PostgreSQL; the same items under the same key with two full-projection GSIs 143 to 225 MiB on SQLite at 100,000 items in three runs and 165 to 172 MiB at 300,000, and 23 to 34 MiB on PostgreSQL at both: the excess on SQLite is the backend's own per-connection page cache (`cache_size`, 32 MiB) holding the index tables' b-tree pages plus the allocator's arenas (about a third less under `MALLOC_ARENA_MAX=1`), not the restore's buffers, which the 32 MiB batch bound holds whatever the indexes; it is bounded by `cache_size` and grows sub-linearly with the table. Size the host for about 450 MiB per running restore of large items under `MALLOC_ARENA_MAX=1`, times this knob, plus about 32 MiB of page cache per running restore of an indexed table on SQLite, and read the 1M figure for 200-byte items as the key-ordered shape's: an unordered backup of them restores in about 1.5 to 1.8 times that on SQLite. The page-cache term is for B-tree indexes. On SQLite a restore of items of more than a few hundred bytes, of any attribute types, holds more: its write batch (up to 10,000 items or 32 MiB of item text) and the decode chunks in front of it grow with the item's size up to that bound, and lists of numbers decode larger than strings of the same size; the [Backup and Restore manual](./manuals/14-backup-and-restore.md#memory-while-a-job-runs) has the measured points. |
| `spool_path` | none | Restore download area for an S3 backup store, created 0700 with files 0600 and holding a `.lock` another instance on the host refuses. A store-backed restore from an S3 store downloads each data file here under `restore-<table id>/` (`<n>.json.gz`, `<n>` zero-padded to six digits, with a `.verified` marker holding the file's sha256, written only after the file's digests matched and trusted only while it names the digest the manifest records, plus the manifest and sidecar), reads it from there, and removes the entry when the restore ends, except when it ended because the record had become another instance's (its claim lost, a status write that matched no record), where the sweep removes it; an entry left by an earlier run of this host for a record another host then owned is removed before a new run of the same restore uses it; the reconciler sweeps the `restore-<id>` entry of a restore no longer RUNNING with this host's `spool_host`. Its disk cost is the compressed backup of the restore in flight. A filesystem store needs no spool (files are hashed in place and read again). Required with an S3 store, except on file-backed SQLite where it defaults to `backup-spool/` beside the database file. Refused inside or containing a filesystem store root, and when it is or contains the database file's directory. Example: `spool_path = "/var/lib/extenddb/spool"` |

## `[backup.store]`

Where CreateBackup writes its backups on PostgreSQL, SQLite and MongoDB (CreateBackup answers CREATING and a job of the server uploads; Cassandra writes in-catalog backups with or without a store), and where restores and deletes of store-backed backups read. The section's keys, defaults and the refusals of a bad value are the backup store crate's (`crates/backup-store/src/config.rs`). Absent on file-backed SQLite: a filesystem store at `backups/` beside the database file. Absent on PostgreSQL, MongoDB, Cassandra and an in-memory SQLite database: no store, and CreateBackup writes in-catalog backups with a deprecation warning. Instances that share a catalog must share one store root (an S3 bucket and prefix, or a shared mount). The administration guide's section on the backup store covers startup validation, the store identity file and the `extenddb backup` commands.

| Key | Default | Description |
|-----|---------|-------------|
| `kind` | `"filesystem"` | Which store the section selects: "filesystem", "s3" or "disabled". "disabled" opts out of the per-backend default on any backend. |

## `[backup.store.filesystem]`

Read when `kind = "filesystem"`.

| Key | Default | Description |
|-----|---------|-------------|
| `path` | none | Root directory for backup objects. It must exist, be owned by the extenddb user with mode 0700, and sit under directories owned by root or by that user which others cannot write to; the server refuses anything else at startup. A relative path, like a relative `spool_path`, is resolved against the working directory the server was started from (the daemon's own working directory is `/`). Example: `path = "/var/lib/extenddb/backups"` |

## `[backup.store.s3]`

Read when `kind = "s3"`. Credentials are not in the file: they come from the AWS SDK's credential chain.

| Key | Default | Description |
|-----|---------|-------------|
| `bucket` | none | Destination bucket. Example: `bucket = "my-extenddb-backups"` |
| `prefix` | `""` | Key prefix inside the bucket; empty means the bucket root. |
| `region` | none | Region override. When absent the SDK chain resolves the region, querying the instance metadata service when the environment and the profile name none. Example: `region = "us-east-1"` |
| `endpoint` | none | Endpoint override for an S3-compatible store. Example: `endpoint = "http://127.0.0.1:9000"` |
| `allow_insecure_endpoint` | `false` | Accept an `http://` endpoint whose host is not a loopback address, or an S3 endpoint without `force_path_style`, for the S3 client, the credential providers' clients and the instance metadata client alike. Off by default: such an endpoint can carry requests, credentials and objects in clear text across the network. |
| `force_path_style` | `false` | Use path-style addressing; required by most S3-compatible stores, and by an `http://` endpoint unless `allow_insecure_endpoint` is set. |
| `upload_part_size_bytes` | `8388608` | Multipart part size in bytes (8 MiB). The enforced range is 5 MiB to 5 GiB, the smallest and largest part S3 accepts. Each part must upload within the 60 s attempt timeout, so keep it at most about the link's bytes per second times 60. |
| `max_concurrent_uploads` | `4` | Maximum concurrently in-flight part uploads per put. |

## `[import]`

Import and export file operations are disabled until a directory is listed here. Every file path is canonicalized and must resolve under one of the configured roots; a symlink that escapes a root is rejected. Files are namespaced per account beneath each root: a caller in account 111122223333 may only read under &lt;root&gt;/111122223333, and a path that resolves elsewhere is rejected, so tenants sharing an instance cannot read each other's files.

On Unix, export files are created with mode 0600, and the directories the server creates to hold import and export files are created with mode 0700: a missing import or export root, created at startup, and the account directory an export creates under each export root. Missing parent directories of a root are created 0755. A directory that already exists keeps its mode. The umask can only narrow these modes. At startup the server warns about a root with a parent directory that group or others can write to without the sticky bit, since another user could replace the root through it.

| Key | Default | Description |
|-----|---------|-------------|
| `paths` | `[]` | Directories imports may read from, for example /var/lib/extenddb/imports. Empty means imports are denied. |

## `[export]`

Same rules as [import]. Export refuses to overwrite an existing file.

| Key | Default | Description |
|-----|---------|-------------|
| `paths` | `[]` | Directories exports may write to, for example /var/lib/extenddb/exports. Empty means exports are denied. |
