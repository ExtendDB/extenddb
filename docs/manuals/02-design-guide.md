# Design Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Storage Schema

### Catalog and Data Separation

ExtendDB manages two kinds of stored state in every storage backend. The
examples below name the PostgreSQL databases; other backends map the same two
roles onto their own structures (see backend-specific documentation).

- **Catalog** (e.g., the `extenddb_catalog` PostgreSQL database): All metadata — table definitions, indexes, accounts, IAM entities, settings, stream metadata, and schema history.
- **Data** (the `extenddb` PostgreSQL database for the default catalog `extenddb_catalog`): User item data. Each DynamoDB table maps to a PostgreSQL table. GSI and LSI data are stored in separate PostgreSQL tables.

On PostgreSQL, the data database connection string is stored in the catalog's `settings` table under the key `data_database_connection_string`, and its name under `data_database_name`. This allows the catalog and data databases to live on different PostgreSQL instances.

### Catalog Tables

| Table | Purpose |
|-------|---------|
| `accounts` | Multi-account support. `account_id` (12-digit string) is the primary key. |
| `tables` | DynamoDB table metadata. Composite PK: `(account_id, table_name)`. Stores key schema, attribute definitions, billing mode, stream spec, status, ARN, and TTL config. |
| `indexes` | GSI/LSI metadata. FK on `table_id` (UUID) with CASCADE delete. |
| `tags` | Resource tags. PK: `(resource_arn, tag_key)`. |
| `settings` | Key-value store for catalog version, data DB URL, the AES-256-GCM key that encrypts access key secrets (`encryption_key`), and runtime settings. |
| `schema_history` | Migration tracking. Records which SQL files have been applied. |
| `admin_users` | Admin credentials (bcrypt-hashed passwords). |
| `iam_users` | IAM users scoped to accounts. Optional console password. |
| `iam_groups` | IAM groups scoped to accounts. |
| `iam_group_members` | User-to-group membership. |
| `iam_roles` | IAM roles with trust policies. |
| `iam_user_policies` | Inline policies attached to users. |
| `iam_group_policies` | Inline policies attached to groups. |
| `iam_role_policies` | Inline policies attached to roles. |
| `access_keys` | Access key ID + AES-256-GCM encrypted secret key. |
| `iam_user_tags` | Tags on IAM users. |
| `iam_role_tags` | Tags on IAM roles. |
| `permissions_boundaries` | Permissions boundaries for users and roles. |
| `stream_shards` | Stream shard metadata (parent shard, sequence range). |
| `stream_records` | Stream change records with 24-hour retention. |
| `stream_pending` | Stream records that wait for their sequence number. A write stores its record here, and the record moves to `stream_records` once the write has committed. |

### Data Tables

Each DynamoDB table `T` in account `A` maps to a PostgreSQL table named `t_{table_id}` in the data database. The table has:

- `pk` column: Partition key value (stored as JSONB)
- `sk` column: Sort key value (JSONB, nullable for hash-only tables)
- `item` column: Full item as JSONB
- Primary key: `(pk)` or `(pk, sk)`

GSI tables are named `gsi_{table_id}_{index_name}` with the GSI key columns and a copy of projected attributes. LSI tables are named `lsi_{table_id}_{index_name}`.

### Schema Conventions

- Cross-table foreign keys use `table_id` (UUID), not `table_name`, to support future table rename operations.
- All IAM entities are scoped to `account_id` via foreign keys to the `accounts` table.
- CASCADE deletes ensure that deleting an account removes all its IAM entities, and deleting a table removes its indexes.

## Expression Evaluation

The expression engine lives in `core` and operates on in-memory `AttributeValue` types. It handles five expression types:

### ConditionExpression

Evaluated before writes to enforce preconditions. Supports:

- Comparisons: `=`, `<>`, `<`, `<=`, `>`, `>=`
- Functions: `attribute_exists`, `attribute_not_exists`, `attribute_type`, `begins_with`, `contains`, `size`
- Logical: `AND`, `OR`, `NOT`
- `BETWEEN` and `IN` operators
- Nested attribute paths with dot notation and array indexing

Condition evaluation happens inside the storage transaction (after `SELECT FOR UPDATE`) to prevent TOCTOU races.

### FilterExpression

Applied after reads (Query/Scan) to exclude non-matching items. Same syntax as ConditionExpression. Filter expressions do not reduce consumed capacity — all scanned items count toward RCU.

### UpdateExpression

Applied during UpdateItem to modify attributes. Four clauses:

- `SET`: Assign values, with `if_not_exists()` and `list_append()` functions
- `REMOVE`: Delete attributes or list elements
- `ADD`: Numeric addition or set union
- `DELETE`: Set subtraction

Update expressions are applied inside the storage transaction after condition evaluation.

### ProjectionExpression

Applied after reads to return only requested attributes. Supports nested paths. If omitted, all attributes are returned.

### KeyConditionExpression

Parsed by the engine and translated to SQL WHERE clauses by the storage backend. Supports partition key equality and sort key conditions (equality, range, `begins_with`, `between`).

## Authentication Model

### Built-in Auth (`auth.provider = "builtin"`)

extenddb uses SigV4 signature verification with a local IAM credential store. This is the only supported authentication mode.

Full SigV4 signature verification:

1. Extract `Authorization` header components (credential, signed headers, signature)
2. Look up access key in the credential store (cached in memory, database on a miss; encryption key cached at startup)
3. Reconstruct the canonical request and string-to-sign
4. Derive the signing key: `HMAC-SHA256(HMAC-SHA256(HMAC-SHA256(HMAC-SHA256("AWS4" + secret, date), region), service), "aws4_request")`
5. Compare computed signature with the provided signature (constant-time comparison)
6. Return `AuthIdentity::User` or `AuthIdentity::RoleSession` with account context

### IAM Policy Evaluation

After authentication, the authorization layer evaluates IAM policies using a 5-phase algorithm:

1. **Explicit Deny** — scan all policies (identity, permissions boundary, session). Any matching Deny → access denied.
2. **Permissions Boundary** — if set, must contain a matching Allow → else denied.
3. **Session Policy** — if set (AssumeRole), must contain a matching Allow → else denied.
4. **Identity Allow** — scan identity policies (user, group, role). Any matching Allow → access granted.
5. **Implicit Deny** — no matching Allow → access denied.

Policy conditions support all IAM condition operators: `StringEquals`, `StringNotEquals`, `StringEqualsIgnoreCase`, `StringLike`, `StringNotLike`, `NumericEquals`, `NumericNotEquals`, `NumericLessThan`, `NumericLessThanEquals`, `NumericGreaterThan`, `NumericGreaterThanEquals`, `DateEquals`, `DateNotEquals`, `DateLessThan`, `DateLessThanEquals`, `DateGreaterThan`, `DateGreaterThanEquals`, `Bool`, `Null`, `ArnEquals`, `ArnNotEquals`, `ArnLike`, `ArnNotLike`, plus `ForAllValues`, `ForAnyValue`, and `IfExists` modifiers. Supported condition keys include `aws:PrincipalTag/*`, `aws:ResourceTag/*`, `dynamodb:LeadingKeys`, `dynamodb:Attributes`, `dynamodb:Select`, `dynamodb:ReturnValues`, `dynamodb:ReturnConsumedCapacity`, `dynamodb:FullTableScan`, and `dynamodb:EnclosingOperation`.

### Credential Storage

Access key secrets are encrypted at rest using AES-256-GCM. The encryption key is generated during `extenddb init` and stored in the `settings` table under the key `encryption_key`. Each access key record stores the encrypted secret and a unique nonce.

Credential lookups (access key ID → decrypted secret) go through each server's in-memory credential cache; a miss reads the database. See Credential and Policy Caching in the Security Model. The server reads the encryption key that decrypts secrets once, at startup. extenddb never changes the key. A change made by hand in the `settings` table takes effect at the next restart. See Credential Storage in the Security Model, and Caching Design below.

## DynamoDB Streams Internals

### Record Capture

Stream records are captured atomically with data writes. The engine constructs a `StreamCapture` struct with metadata (stream ARN, view type, shard ID, sequence number, keys). The storage backend persists the stream record in the same PostgreSQL transaction as the data write.

On PostgreSQL the record waits in `stream_pending`, with no sequence number, until the write commits. GetRecords and a `LATEST` shard iterator first number one batch of the committed records of the shard they read, in the order the writes stored them: 1,000 records, or fewer once the records hold 1 MiB of JSON text. Every 250 ms a background worker numbers up to 10,000 committed records of each shard that has some, and skips a shard that another transaction numbers, so the records of a shard that nobody reads are numbered too. A number drawn inside the write's transaction could commit after a higher number that a consumer had already read past, and that consumer would never receive the record. A read returns the records whose writes committed before it started, except in three cases: more records of the shard wait for their numbers than one batch takes, another transaction numbers the shard for longer than one second, or the read's numbering fails, which the server logs as a warning. The records left waiting get higher numbers than any the read returned, from a later read or the worker, and a later read returns them. In the same three cases a `LATEST` iterator can return records whose writes committed before it was taken. The order needs `stream_seq` and `stream_pending_id_seq` to keep a cache of one value per session, which migration 7 sets; a larger cache lets a record take a number below one a consumer has read.

For UpdateItem, the `new_image` is not known until after `apply_update` runs inside the transaction, so the storage backend constructs the full `StreamRecord` after the update.

### Shard Model

Each stream has a fixed set of shards (currently 4 shards per stream). On PostgreSQL and SQLite, shard IDs embed the table's id, a UUID assigned at creation (`shardId-<table_id>-0000000000000000` through `shardId-<table_id>-0000000000000003`), so a new shard ID is 61 characters, inside the AWS SDKs' 28-to-65-character bounds for `ShardId`. The table name is not used because it is reused by delete-and-recreate and by every account that picks it, while shard IDs must be unique across the data database. Shards created by earlier versions keep their `shardId-<table>-<n>` IDs. Sequence numbers are monotonically increasing integers. On PostgreSQL, DeleteTable leaves a table's shards and records in place; the hourly retention sweep trims records older than 24 hours and then removes the shards of a table that no longer exists once none of its records remain (SQLite removes both with the table). A stream ARN carries the table name and a millisecond label, and resolves to the table currently holding that name and label, so a table deleted and recreated within one millisecond would give the old ARN the new stream; that window is tracked, not closed.

### Iterator Types

- `TRIM_HORIZON`: Start from the oldest available record
- `LATEST`: Start from the most recent record
- `AT_SEQUENCE_NUMBER`: Start at a specific sequence number
- `AFTER_SEQUENCE_NUMBER`: Start after a specific sequence number

Iterators expire after 15 minutes of inactivity.

### Retention

Stream records are retained for 24 hours. A background task runs hourly to delete expired records.

## Architecture Decision Records

### SQL Injection Defense

All user-supplied strings are validated at the engine layer before reaching storage. The storage layer uses parameterized queries exclusively — no dynamic SQL construction with user input. See `docs/adr/sql-injection-defense.md`.

### BoxFuture vs async_trait

Storage traits use `BoxFuture` for object safety, allowing dynamic dispatch of storage backends. Auth traits use `#[async_trait]` for the same reason. The per-request allocation cost is negligible compared to I/O and crypto operations.

### Condition Evaluation Inside Transactions

Condition expressions are evaluated inside the storage transaction (after `SELECT FOR UPDATE`) rather than in the engine layer. This prevents TOCTOU races where another request could modify the item between condition check and write.

## Capacity Calculation

extenddb calculates consumed capacity matching real DynamoDB:

- **Read capacity**: Item size rounded up to 4 KB. Eventually consistent reads cost 0.5 RCU per 4 KB. Strongly consistent reads cost 1.0 RCU per 4 KB. Transactional reads cost 2.0 RCU per 4 KB.
- **Write capacity**: Item size rounded up to 1 KB. Standard writes cost 1.0 WCU per 1 KB. Transactional writes cost 2.0 WCU per 1 KB.
- **Table-level and index-level**: When `ReturnConsumedCapacity` is `INDEXES`, capacity is broken down per table and per index.

Item size includes attribute names and values, matching DynamoDB's size calculation rules.

## Caching Design

extenddb caches a small set of operational settings in memory to avoid per-request database queries on hot paths; this section covers those settings and the risks of caching catalog state. Credentials, policies, boundaries, tags and table key definitions are cached separately by the auth/authz caches (see Credential and Policy Caching in the Security Model, and `docs/design/12-auth-authz-cache.md`).

### What Is Cached

| Setting | Mechanism | Refresh | Justification |
|---------|-----------|---------|---------------|
| `index_propagation_delay_ms` | `AtomicU64` | Background poller every 30s | Write-path hot path; briefly-stale value only affects GSI propagation timing |
| `encryption_key` | `Arc<str>` loaded at startup | Never: read at startup, so a change made by hand takes a restart | Decryption key for access key secrets. extenddb generates it once and never changes it |
| `log_level` / `log_destination` | Tracing filter reload | Background poller every 30s | Observability tuning; stale value only delays log level changes |
| `throttling_enabled` | `AtomicBool` | Background poller every 30s | Capacity management toggle; briefly-stale is safe |

All cached values are operational tuning knobs where a briefly-stale value does not affect correctness.

### Risks of Caching Catalog State

The settings cache above holds no catalog state. The auth/authz caches hold some (credentials, IAM policies, boundaries, tags, and table key definitions), and a stale copy can be wrong in the ways listed below. On the server that makes a change, they drop the affected entries before the call returns. For a change made elsewhere, the TTL (`auth.cache.ttl_seconds`) bounds how long a stale entry can be used (see `docs/design/12-auth-authz-cache.md`).

- **Table metadata** (table ID, key schema, attribute definitions, indexes, stream specification): A stale cache could serve the wrong key schema after a table is deleted and recreated with the same name but different schema. The new table has a different `table_id`, different key schema, and different indexes — stale cache serves wrong schema, writes corrupt data, reads return garbage.
- **IAM policies and credentials**: A revoked Deny policy still cached as absent creates a security gap. A deleted access key still cached as valid allows unauthorized access.
- **Tags**: Tag-based authorization (`aws:ResourceTag/*`) requires current tag values.
- **GSI definitions**: Stale GSI metadata could route writes to wrong index tables.

### The Table-Name-Reuse Problem

The main risk in caching catalog state:

1. Client calls `DeleteTable("Orders")`
2. Client immediately calls `CreateTable("Orders")` with a different key schema
3. New table gets a new `table_id`, new key schema, new indexes
4. A stale cache still maps "Orders" → old `table_id` with old key schema
5. Writes use wrong column layout → data corruption
6. Reads return items with wrong attribute interpretation → garbage

On the server that runs the table operation, `CreateTable`, `UpdateTable`, `DeleteTable`, `RestoreTableFromBackup` and `ImportTable` drop the table's cached key and index definitions before the call returns, and a cache refresh that started before the change discards its result (the epoch counter in `crates/cache/src/swr.rs`). Across servers no TTL is safe, because a delete and recreate can happen within milliseconds: after a table is recreated through one server, another server keeps the old `table_id` for up to `ttl_seconds`. Cross-instance invalidation (for example PostgreSQL LISTEN/NOTIFY, Appendix B of `docs/design/12-auth-authz-cache.md`) is a prerequisite for running several servers on one catalog.

### Multi-Instance Considerations

On SQLite, `extenddb serve` enforces one instance per database file with an exclusive lock on `<database>.lock`. On PostgreSQL and MongoDB nothing enforces it, and running several instances against one catalog is not supported: the credential and policy caches are per instance with no cross-instance invalidation, and every instance runs every background worker (see the deployment guide).

### Future Considerations

Caching of operational settings is currently unconditional. If issues arise (e.g., a setting change must take effect immediately for safety reasons), a runtime toggle (`extenddb settings set caching_enabled false`) should be added. Running several servers on one catalog needs the cross-instance invalidation in Appendix B of `docs/design/12-auth-authz-cache.md` first.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc.
