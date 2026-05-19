# Design Guide

> See [NOTICE](../NOTICE.md) for important disclaimers.

## Storage Schema

### Dual-Database Architecture

extenddb uses two PostgreSQL databases per deployment:

- **Catalog database** (e.g., `extenddb_catalog`): All metadata — table definitions, indexes, accounts, IAM entities, settings, stream metadata, and schema history.
- **Data database** (e.g., `extenddb_catalog_data`): User item data. Each DynamoDB table maps to a PostgreSQL table. GSI and LSI data are stored in separate PostgreSQL tables.

The data database connection string is stored in the catalog's `settings` table under the key `data_database_url`. This allows the catalog and data databases to live on different PostgreSQL instances.

### Catalog Tables

| Table | Purpose |
|-------|---------|
| `accounts` | Multi-account support. `account_id` (12-digit string) is the primary key. |
| `tables` | DynamoDB table metadata. Composite PK: `(account_id, table_name)`. Stores key schema, attribute definitions, billing mode, stream spec, status, ARN, and TTL config. |
| `indexes` | GSI/LSI metadata. FK on `table_id` (UUID) with CASCADE delete. |
| `tags` | Resource tags. PK: `(resource_arn, tag_key)`. |
| `settings` | Key-value store for catalog version, data DB URL, and runtime settings. |
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
| `stream_shards` | Stream shard metadata (parent shard, sequence range). |
| `stream_records` | Stream change records with 24-hour retention. |

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
2. Look up access key in the credential store (database-backed, credential lookup per request; encryption key cached at startup)
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

Policy conditions support all IAM condition operators: `StringEquals`, `StringNotEquals`, `StringEqualsIgnoreCase`, `StringLike`, `StringNotLike`, `NumericEquals`, `NumericNotEquals`, `NumericLessThan`, `NumericLessThanEquals`, `NumericGreaterThan`, `NumericGreaterThanEquals`, `DateEquals`, `DateNotEquals`, `DateLessThan`, `DateLessThanEquals`, `DateGreaterThan`, `DateGreaterThanEquals`, `Bool`, `Null`, `ArnEquals`, `ArnNotEquals`, `ArnLike`, `ArnNotLike`, plus `ForAllValues`, `ForAnyValue`, and `IfExists` modifiers. Supported condition keys include `aws:PrincipalTag/*`, `dynamodb:ResourceTag/*`, `dynamodb:LeadingKeys`, `dynamodb:Attributes`, `dynamodb:Select`, `dynamodb:ReturnValues`, `dynamodb:ReturnConsumedCapacity`, `dynamodb:FullTableScan`, and `dynamodb:EnclosingOperation`.

### Credential Storage

Access key secrets are encrypted at rest using AES-256-GCM. The encryption key is generated during `extenddb init` and stored in the `encryption_keys` table. Each access key record stores the encrypted secret and a unique nonce.

Credential lookups (access key → encrypted secret) read directly from the database on every request — there is no in-process cache for credentials. The encryption key used to decrypt secrets is cached at startup because it is immutable after `extenddb init` (see Caching Design below).

## DynamoDB Streams Internals

### Record Capture

Stream records are captured atomically with data writes. The engine constructs a `StreamCapture` struct with metadata (stream ARN, view type, shard ID, sequence number, keys). The storage backend persists the stream record in the same PostgreSQL transaction as the data write.

For UpdateItem, the `new_image` is not known until after `apply_update` runs inside the transaction, so the storage backend constructs the full `StreamRecord` after the update.

### Shard Model

Each stream has a fixed set of shards (currently 4 shards per stream). Shard IDs are deterministic (`shardId-<table>-000000000000` through `shardId-<table>-000000000003`). Sequence numbers are monotonically increasing integers.

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

extenddb caches a small set of operational settings in memory to avoid per-request database queries on hot paths. Catalog state (table metadata, auth policies, tags, GSI definitions) is never cached.

### What Is Cached

| Setting | Mechanism | Refresh | Justification |
|---------|-----------|---------|---------------|
| `gsi_propagation_delay_ms` | `AtomicU64` | Background poller every 30s | Write-path hot path; briefly-stale value only affects GSI propagation timing |
| `encryption_key` | `Arc<str>` loaded at startup | Never (immutable after `extenddb init`) | Decryption key for access key secrets; generated once, never changes |
| `log_level` / `log_destination` | Tracing filter reload | Background poller every 30s | Observability tuning; stale value only delays log level changes |
| `throttling_enabled` | `AtomicBool` | Background poller every 30s | Capacity management toggle; briefly-stale is safe |

All cached values are operational tuning knobs where a briefly-stale value does not affect correctness.

### What Is NOT Cached (and Why)

Catalog state is never cached because correctness requires every request to see the current state:

- **Table metadata** (key schema, attribute definitions, status, billing mode): A stale cache could serve the wrong key schema after a table is deleted and recreated with the same name but different schema. The new table has a different `table_id`, different key schema, and different indexes — stale cache serves wrong schema, writes corrupt data, reads return garbage.
- **IAM policies and credentials**: A revoked Deny policy still cached as absent creates a security gap. A deleted access key still cached as valid allows unauthorized access.
- **Tags**: Tag-based authorization (`dynamodb:ResourceTag/*`) requires current tag values.
- **GSI definitions**: Stale GSI metadata could route writes to wrong index tables.

### The Table-Name-Reuse Problem

The fundamental reason catalog state cannot be cached safely:

1. Client calls `DeleteTable("Orders")`
2. Client immediately calls `CreateTable("Orders")` with a different key schema
3. New table gets a new `table_id`, new key schema, new indexes
4. A stale cache still maps "Orders" → old `table_id` with old key schema
5. Writes use wrong column layout → data corruption
6. Reads return items with wrong attribute interpretation → garbage

No safe TTL exists because delete-recreate can happen within milliseconds. Cross-instance invalidation (e.g., PostgreSQL LISTEN/NOTIFY) would be a prerequisite for any future catalog caching.

### Multi-Instance Considerations

extenddb does not enforce single-instance-per-catalog. Multiple extenddb instances may share the same PostgreSQL catalog. Any in-process cache of catalog state would be invisible to other instances. PostgreSQL's own buffer pool provides memory-resident access to hot rows, making application-level caching unnecessary for most workloads.

### Future Considerations

Caching of operational settings is currently unconditional. If issues arise (e.g., a setting change must take effect immediately for safety reasons), a runtime toggle (`extenddb settings set caching_enabled false`) should be added. Catalog caching remains prohibited without a cross-instance invalidation design and explicit human approval.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
