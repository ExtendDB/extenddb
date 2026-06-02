# Design Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Storage Schema

### Dual-Database Architecture

extenddb uses a catalog/data database topology per deployment:

- **Catalog database** (e.g., `extenddb_catalog`): All metadata — table definitions, indexes, accounts, IAM entities, settings, stream metadata, and schema history.
- **Data database** (e.g., `extenddb_catalog_data`): User item data plus backend-specific secondary-index state. PostgreSQL stores base and secondary-index data in physical companion tables. TiDB stores item rows once and uses generated columns plus native secondary indexes.

The data database connection string is stored in the catalog's `settings` table under the key `data_database_connection_string`. PostgreSQL deployments may place the catalog and data databases on different PostgreSQL instances. TiDB deployments must keep both databases in the same TiDB cluster so snapshot timestamps, online DDL, native TTL, and BR backup/restore all refer to one global timeline. At startup, the TiDB backend compares `information_schema.cluster_info` from the catalog and data pools when TiDB exposes that native topology table. On TiDB editions that hide cluster topology metadata, ExtendDB accepts the split only when both databases use the same SQL endpoint and user; separate endpoints or users require visible TiDB topology metadata so the backend can prove they belong to one cluster.

### Catalog Tables

| Table | Purpose |
|-------|---------|
| `accounts` | Multi-account support. `account_id` (12-digit string) is the primary key. |
| `tables` | DynamoDB table metadata. Composite PK: `(account_id, table_name)`. Stores key schema, attribute definitions, billing mode, stream spec, status, ARN, and TTL config. |
| `indexes` | GSI/LSI metadata. FK on `table_id` (UUID) with CASCADE delete. |
| `tags` | Resource tags. PK: `(resource_arn, tag_key)`. |
| `settings` | Key-value store for catalog version, data DB connection string, and runtime settings. |
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
| `encryption_keys` | AES-256-GCM key used to encrypt access key secrets. |
| `stream_records` | Stream change records with 24-hour native TTL retention. TiDB derives fixed shard IDs from table metadata instead of storing shard rows. |

### Data Tables

Each DynamoDB table `T` in account `A` maps to backend-owned physical storage in
the data database. The logical shape is a partition-key slot, an optional
sort-key slot, the complete item document, and a backend-native primary key over
the key slots. Physical column types are backend-specific:

- PostgreSQL stores key slots in PostgreSQL-typed columns and the complete item
  in `item_data JSONB`.
- TiDB stores the hash-key slot as raw `VARBINARY(2048)`, sort-key slots as
  typed `VARBINARY(1024)` or `DECIMAL(65, 30)` columns, and the complete item in
  `item_data JSON`. Fresh TiDB data tables are `PARTITION BY KEY(pk)` so TiDB
  distributes rows by the DynamoDB HASH key while preserving the raw key bytes
  used for point reads, Query, transaction locks, and stream shard assignment.

PostgreSQL companion index tables are named `_ddb_<index_id>` and store the
index key columns plus projected attributes. TiDB represents every DynamoDB
secondary index definition as generated key columns plus a native secondary
index on the base data table; GSI versus LSI is API metadata, not a separate
TiDB physical path. The native index contains the DynamoDB index key columns
only because TiDB already carries the clustered row handle in secondary-index
entries. On fresh partitioned TiDB tables those native secondary indexes are
declared `GLOBAL`, so an `IndexName` read uses one global TiDB index range
instead of probing every partition. Initial indexes are included in the
physical TiDB `CREATE TABLE`; replay repairs an already-existing physical
table with TiDB online `IF NOT EXISTS` DDL before activation, and later GSI
changes use TiDB online DDL.

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
2. Look up access key in the credential store (database-backed, credential lookup per request; encryption key cached at startup)
3. Reconstruct the canonical request and string-to-sign
4. Derive the signing key: `HMAC-SHA256(HMAC-SHA256(HMAC-SHA256(HMAC-SHA256("AWS4" + secret, date), region), service), "aws4_request")`
5. Compare computed signature with the provided signature (constant-time comparison)
6. Return `AuthIdentity::User` or `AuthIdentity::RoleSession` with account context; role sessions include the access key so authorization fetches the exact session row

### IAM Policy Evaluation

After authentication, the authorization layer evaluates IAM policies using a 5-phase algorithm:

1. **Explicit Deny** — scan all policies (identity, permissions boundary, session). Any matching Deny → access denied.
2. **Permissions Boundary** — if set, must contain a matching Allow → else denied.
3. **Session Policy** — if set (AssumeRole), must contain a matching Allow → else denied.
4. **Identity Allow** — scan identity policies (user, group, role). Any matching Allow → access granted.
5. **Implicit Deny** — no matching Allow → access denied.

Policy conditions support all IAM condition operators: `StringEquals`, `StringNotEquals`, `StringEqualsIgnoreCase`, `StringLike`, `StringNotLike`, `NumericEquals`, `NumericNotEquals`, `NumericLessThan`, `NumericLessThanEquals`, `NumericGreaterThan`, `NumericGreaterThanEquals`, `DateEquals`, `DateNotEquals`, `DateLessThan`, `DateLessThanEquals`, `DateGreaterThan`, `DateGreaterThanEquals`, `Bool`, `Null`, `ArnEquals`, `ArnNotEquals`, `ArnLike`, `ArnNotLike`, plus `ForAllValues`, `ForAnyValue`, and `IfExists` modifiers. Supported condition keys include `aws:PrincipalTag/*`, `dynamodb:ResourceTag/*`, `dynamodb:LeadingKeys`, `dynamodb:Attributes`, `dynamodb:Select`, `dynamodb:ReturnValues`, `dynamodb:ReturnConsumedCapacity`, `dynamodb:FullTableScan`, and `dynamodb:EnclosingOperation`.

### Credential Storage

Access key secrets are encrypted at rest using AES-256-GCM. The encryption key is generated during `extenddb init` and stored in the `encryption_keys` table. Each access key record stores the encrypted secret and a unique nonce.

Credential lookups (access key → encrypted secret) read directly from the database on every request — there is no in-process cache for credentials. The encryption key used to decrypt secrets is cached at startup because it is immutable after `extenddb init` (see Caching Design below).

## DynamoDB Streams Internals

### Record Capture

Stream records are captured atomically with data writes. The engine constructs a `StreamCapture` struct with stream ARN, view type, and region metadata. The storage backend assigns the shard and sequence number and persists the stream record in the same backend transaction as the data write.

For UpdateItem, the `new_image` is not known until after `apply_update` runs inside the transaction, so the storage backend constructs the full `StreamRecord` after the update.

### Shard Model

Each stream has a fixed set of shards. TiDB uses 16 deterministic shards per
stream and puts the shard bucket before the table id in the shard key
(`shardId-000000000000-<table>` through `shardId-000000000015-<table>`) so
the shared stream commit-sequence index can be pre-split by bucket prefix.
TiDB schemas store stream rows under an `AUTO_RANDOM` clustered
`record_id`, so highly concurrent stream inserts are scattered by TiDB instead
of appending inside one shard key range. Sequence numbers are monotonically
increasing, sortable strings within a shard; TiDB derives them from native MVCC
commit timestamps with a per-transaction ordinal suffix.

### Iterator Types

- `TRIM_HORIZON`: Start from the oldest available record
- `LATEST`: Start from the most recent record
- `AT_SEQUENCE_NUMBER`: Start at a specific sequence number
- `AFTER_SEQUENCE_NUMBER`: Start after a specific sequence number

Iterators expire after 15 minutes of inactivity.

### Retention

Stream records are retained for 24 hours. TiDB uses native table TTL for
retention; backends without native TTL use a background cleanup task.

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

Capacity enforcement is backend-aware. PostgreSQL can use ExtendDB's
process-local token buckets for local fidelity tests. TiDB disables those
frontend buckets and relies on TiDB Resource Control/resource groups for
distributed flow control and scheduling, so multiple ExtendDB frontends share
one storage-owned quota instead of each admitting its own local burst.

## Caching Design

extenddb caches a small set of operational settings in memory to avoid per-request database queries on hot paths. Catalog state (table metadata, auth policies, tags, GSI definitions) is never cached.

### What Is Cached

| Setting | Mechanism | Refresh | Justification |
|---------|-----------|---------|---------------|
| `encryption_key` | `Arc<str>` loaded at startup | Never (immutable after `extenddb init`) | Decryption key for access key secrets; generated once, never changes |
| `log_level` / `log_destination` | Tracing filter reload | Background poller every 30s | Observability tuning; stale value only delays log level changes |
| `throttling_enabled` | `AtomicBool` | Background poller every 30s | PostgreSQL frontend capacity toggle; rejected by TiDB because capacity control is storage-native |

All cached values are operational tuning knobs where a briefly-stale value does not affect correctness.

### What Is NOT Cached (and Why)

Catalog state is never cached because correctness requires every request to see the current state:

- **Table metadata** (key schema, attribute definitions, status, billing mode): A stale cache could serve the wrong key schema after a table is deleted and recreated with the same name but different schema. The new table has a different `table_id`, different key schema, and different indexes — stale cache serves wrong schema, writes corrupt data, reads return garbage.
- **IAM policies and credentials**: A revoked Deny policy still cached as absent creates a security gap. A deleted access key still cached as valid allows unauthorized access.
- **Tags**: Tag-based authorization (`dynamodb:ResourceTag/*`) requires current tag values.
- **GSI definitions**: Stale GSI metadata could route reads or writes through the wrong backend-specific index shape.

### The Table-Name-Reuse Problem

The fundamental reason catalog state cannot be cached safely:

1. Client calls `DeleteTable("Orders")`
2. Client immediately calls `CreateTable("Orders")` with a different key schema
3. New table gets a new `table_id`, new key schema, new indexes
4. A stale cache still maps "Orders" → old `table_id` with old key schema
5. Writes use wrong column layout → data corruption
6. Reads return items with wrong attribute interpretation → garbage

No safe TTL exists because delete-recreate can happen within milliseconds. Cross-instance invalidation through backend-native change notifications would be a prerequisite for any future catalog caching.

### Multi-Instance Considerations

extenddb does not enforce single-instance-per-catalog. Multiple extenddb instances may share the same catalog. Any in-process cache of catalog state would be invisible to other instances. Backend buffer pools provide memory-resident access to hot rows, making application-level caching unnecessary for most workloads.

### Future Considerations

Caching of operational settings is currently unconditional. If issues arise (e.g., a setting change must take effect immediately for safety reasons), a runtime toggle (`extenddb settings set caching_enabled false`) should be added. Catalog caching remains prohibited without a cross-instance invalidation design and explicit human approval.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
