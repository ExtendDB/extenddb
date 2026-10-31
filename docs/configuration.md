---
description: "Every key of extenddb.toml, its default and what it controls, generated from extenddb.sample.toml."
custom_edit_url: https://github.com/ExtendDB/extenddb/edit/main/extenddb.sample.toml
---

# Configuration

This page is generated from [`extenddb.sample.toml`](https://github.com/ExtendDB/extenddb/blob/main/extenddb.sample.toml) by `npm run gen:config` in `website/`; edit the sample, not this file. Tests in the `extenddb-config` crate and in each storage backend crate compare the sample with the configuration structs, so a key listed here exists in the code with the default shown.

The server reads `extenddb.toml` (or the file named by `--config`) at startup. Scalar keys can also be set through an environment variable: the name is `EXTENDDB__` followed by the key path in upper case with `__` between levels, for example `EXTENDDB__SERVER__PORT=9000` or `EXTENDDB__STORAGE__POSTGRES__CONNECTION_STRING="postgresql://..."`. The list keys (`import.paths`, `export.paths`, `storage.cassandra.contact_points`) and the numeric keys of `[storage.sqlite]` and `[storage.cassandra]` are file only. Precedence, highest first: environment variables, the configuration file, built-in defaults; `extenddb serve --port` overrides `server.port` on top of all three.

The keys below are static: a change takes effect at the next server start. Settings that change on a running server (log level, control plane delay, throttling at runtime) are set with `extenddb settings set` and are listed under [Runtime Settings](./manuals/05-admin-guide.md#runtime-settings) in the administration guide.

## Top-level keys

These keys sit at the top level and must appear before the first [section] header of the file. A top-level key placed after a header lands inside that section, and every section except [storage.cassandra] refuses to start the server with "unknown field".

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
| `region` | `"us-east-1"` | AWS region used when generating Amazon Resource Names (ARNs). Every backup ARN the server issues carries it, so the value must stay within [a-z0-9-]; the server logs a warning at startup for a value outside that class. See the [upgrade manual](./manuals/07-upgrade-manual.md) before changing it on a deployment that has backups. |
| `run_dir` | `"~/.extenddb/run"` | Directory for runtime files (the process ID file). A leading ~ expands to $HOME. When HOME is unset the default is /tmp. |
| `throttling_enabled` | none | Provisioned throughput throttling via in-memory token buckets. When true, tables with provisioned capacity are rate limited to their configured read capacity units (RCU) and write capacity units (WCU), on-demand tables use the default ceilings (12,000 RCU and 4,000 WCU), and the account-level aggregate limits in [limits] apply. This key sets the value the server starts with; once the `throttling_enabled` runtime setting has been written with `extenddb settings set`, the runtime setting takes precedence. Unset means throttling is off. Example: `throttling_enabled = false` |

## `[server.tls]`

Transport Layer Security (TLS) is mandatory. `extenddb init` generates a self-signed certificate; replace it with one signed by a certificate authority for production use.

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `true` | Accepted for backward compatibility. The server refuses to start when this is set to false. A dev-mode build serves plain HTTP on loopback and ignores this key. |
| `cert_path` | `"~/.extenddb/tls/cert.pem"` | Path to the certificate file in Privacy Enhanced Mail (PEM) format. A leading ~ expands to $HOME. When HOME is unset the default is /tmp/extenddb-cert.pem. |
| `key_path` | `"~/.extenddb/tls/key.pem"` | Path to the private key file in PEM format. A leading ~ expands to $HOME. When HOME is unset the default is /tmp/extenddb-key.pem. |

## `[storage]`

Each ExtendDB binary contains exactly one storage backend and reads the [storage.&lt;backend&gt;] section with that backend's name. The other [storage.\*] sections are ignored, so one file can carry all four.

| Key | Default | Description |
|-----|---------|-------------|
| `backend` | none | Name of the storage backend this file is written for: "postgres", "sqlite", "mongodb" or "cassandra". The key is optional; when present it is checked against the binary's compiled-in backend and the server refuses to start on a mismatch. Example: `backend = "postgres"` |

## `[storage.postgres]`

The connection string points to the catalog database, which stores table metadata and the connection information for the data database. Several independent catalogs can coexist on one PostgreSQL instance.

Connection budget: `pool_size` sizes two pools, one against the catalog database and one against the data database, which `extenddb init` creates separately. Total PostgreSQL connections used: 2 \* `pool_size` + `catalog_pool_size`, plus one per vector index build running on this server, one per running restore (its lock session), and one while a schema migration runs. PostgreSQL's max_connections is per cluster, not per database, so both pools draw on the same budget. The defaults need 60 of PostgreSQL's default 100. The index build session, the restore lock session and the migration connection sit outside every pool. A build's ownership lock is session scoped, so the build holds a session that ends with it; a pooled connection would return to the pool still holding the lock. A restore's lock is session scoped in the same way. The migration lock is transaction scoped and runs in one explicit transaction, so it needs a connection pinned to one PostgreSQL backend until COMMIT; a transaction-pooling proxy could otherwise move it between backends. The administration guide's section on the [connection budget and restores](./manuals/05-admin-guide.md#postgresql-connection-budget-and-restores) describes how restores draw on this pool.

| Key | Default | Description |
|-----|---------|-------------|
| `connection_string` | see description | Connection string for the catalog database. For production add `?sslmode=require` and a password of your own. Default: `connection_string = "postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog"` |
| `pool_size` | `20` | Maximum concurrent connections in each of the two data-operation pools. Minimum 10: smaller values are raised to 10 with a startup warning. Increase for higher concurrency and raise PostgreSQL's max_connections to at least 2 \* `pool_size` + `catalog_pool_size` + the number of vector index builds and restores you expect to overlap. |
| `catalog_pool_size` | none | Maximum connections for the management and catalog pool (authorization, Identity and Access Management (IAM), console). Minimum 10, raised with a startup warning. With auth enabled each DynamoDB request makes concurrent authorization queries, so size this to match the expected request concurrency. Falls back to `pool_size` when absent. Example: `catalog_pool_size = 20` |

## `[storage.sqlite]`

Developer mode backend. Select it with `extenddb init --backend sqlite`. The database path can be given at init time (`extenddb init --sqlite-path <path>`, written into this file) or set here; at serve time the `EXTENDDB__STORAGE__SQLITE__PATH` environment variable overrides it.

| Key | Default | Description |
|-----|---------|-------------|
| `path` | `"extenddb.sqlite"` | Database file. A relative path is resolved against the working directory when the configuration loads, before `serve` daemonizes. ":memory:" selects an ephemeral in-memory database whose data is lost on shutdown. |
| `pool_size` | `10` | Connection pool size. Writes are serialized by the engine whatever the size. A file database raises a value of 1 to 2 and logs it at info, because a backup holds one reader connection for its whole duration while its batches write through another; use 3 or more so a running backup does not contend with ordinary requests. The process runs at most `pool_size` / 2 - 1 backups at once (between 1 and 16; 4 with the default) and answers LimitExceededException to a CreateBackup that waited 30 seconds for a slot. |

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

## `[import]`

Import and export file operations are disabled until a directory is listed here. Every file path is canonicalized and must resolve under one of the configured roots; a symlink that escapes a root is rejected. Files are namespaced per account beneath each root: a caller in account 111122223333 may only read under &lt;root&gt;/111122223333, and a path that resolves elsewhere is rejected, so tenants sharing an instance cannot read each other's files.

| Key | Default | Description |
|-----|---------|-------------|
| `paths` | `[]` | Directories imports may read from, for example /var/lib/extenddb/imports. Empty means imports are denied. |

## `[export]`

Same rules as [import]. Export refuses to overwrite an existing file.

| Key | Default | Description |
|-----|---------|-------------|
| `paths` | `[]` | Directories exports may write to, for example /var/lib/extenddb/exports. Empty means exports are denied. |
