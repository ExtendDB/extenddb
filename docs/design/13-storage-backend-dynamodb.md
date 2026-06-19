# DynamoDB Storage Backend — Design

**Date:** 2026-06-17
**Status:** Approved (design); pending implementation plan
**Backend name:** `dynamodb` · **Crate:** `extenddb-storage-dynamodb` · **Cargo feature:** `dynamodb`

## Premise

ExtendDB speaks the DynamoDB wire protocol. This backend stores its data in
*actual DynamoDB*. The point is not the encoding — there is barely any — it is
the deployment posture: run ExtendDB yourself, pointed at DynamoDB, and you are
technically "self-hosted / on-prem," so the execs stop asking you to get off the
cloud. "We have DynamoDB at home."

It is the third entry in the satirical-but-functional backend series (after the
Route 53 and S3 Object Annotations backends), but it diverges from them in two
ways the others could not:

1. **It actually works.** The data plane forwards to real DynamoDB rather than
   returning a porting-map stub.
2. **The encoding is the anti-joke.** The other backends cram items into a
   primitive never meant to hold them. DynamoDB is already a key/value database,
   so the encoding is the *identity function* — and the comedy lives in deadpan
   documentation of how little there is to do.

## Approach: Hybrid composition (data → DynamoDB, catalog → Postgres)

A fully-functional ExtendDB backend must satisfy a large contract: a data plane
(`TableEngine`, `DataEngine`, `MetadataEngine`, `StreamEngine`, `BackupEngine`,
`WorkerStore`) **and** a catalog/management plane (`ManagementStore`,
`AuthorizationStore`, `AdminStore`, `SettingsStore`, `MetricsStore`,
`RateLimitStore`, `DiagnosticsStore`), plus six `inventory` registrations.

Most of the catalog plane (IAM accounts, users, roles, policies, settings,
metrics, rate-limits, admin users) has **no natural home in DynamoDB** —
DynamoDB has no such concepts. Rather than reimplement ExtendDB's entire
relational-shaped catalog on top of DynamoDB's query model (the rejected
"Approach A"), this backend is a **hybrid composer**:

- **Data plane → real DynamoDB**, via `aws-sdk-dynamodb`.
- **Catalog plane → the existing Postgres `CatalogStore`, reused wholesale.**

Deadpan framing: *your data is on-prem; the bureaucracy that proves you're
on-prem still needs a real database.* This is a more honest punchline than
pretending DynamoDB can be a relational catalog, and it sidesteps the part of a
pure-DynamoDB approach that is all toil and no comedy.

### Approaches considered

- **A — Pure DynamoDB (data + catalog both on DynamoDB).** Zero non-DynamoDB
  dependency; purest on-prem claim. Rejected: reimplements ExtendDB's whole
  catalog/IAM layer against DynamoDB's no-joins, GSI-limited model — months of
  engineering, and the punchline gets buried.
- **B — Hybrid (chosen).** Data forwards to DynamoDB; catalog delegates to
  Postgres. Working `serve --backend dynamodb`, tractable scope, best joke-to-
  effort ratio.
- **C — Data-plane MVP, catalog stays stubbed.** Smallest, but `serve` does not
  fully come up, so the "I'm really running it on-prem" gag is weaker.

## Crate layout

New crate `crates/storage-dynamodb`, depending on `extenddb-storage`,
`extenddb-storage-postgres` (for the reused catalog/auth), `aws-sdk-dynamodb`,
and `aws-config` (the workspace's first AWS SDK dependencies).

```
src/
  lib.rs            // DynamoEngine struct, inventory registrations, ServerComponents factory
  config.rs         // DynamoStorageConfig (region, endpoint_url, table_prefix, catalog_connection_string)
  encoding.rs       // REAL, round-trip-tested Item <-> AttributeValue marshalling (near-identity)
  errors.rs         // aws-sdk SdkError / operation errors -> StorageError (wire-protocol fidelity)
  bootstrapper.rs   // CreateTable-based provisioning; delegates catalog bootstrap to postgres
  operations.rs     // OperationsEngine (delegates catalog_version to postgres)
  table_engine.rs   // CreateTable/DeleteTable/DescribeTable/ListTables/UpdateTable with account-id namespacing
  data_engine.rs    // PutItem/GetItem/UpdateItem/DeleteItem/Query/Scan/Transact*
  metadata_engine.rs// native TTL, tags, table size
  worker_store.rs   // control-plane state via DescribeTable polling
  catalog_delegate.rs // constructs the postgres CatalogStore + auth for ServerComponents
```

## Component design

### ServerComponents (the composition point)

`ServerComponentsRegistration` for `"dynamodb"` returns:

- `engine: Arc<DynamoEngine>` — data/table/metadata/worker traits forward to DynamoDB.
- `catalog_store: Arc<dyn CatalogStore>` — constructed from the existing Postgres
  implementation using `catalog_connection_string`.
- `auth_provider` — the Postgres-backed provider, reused.
- `runtime_hooks` — minimal: a control-plane poller. **No TTL worker** (DynamoDB
  performs TTL deletion itself); **no GSI queue** (DynamoDB owns GSI lifecycle).

### Encoding module — the real, tested piece (near-identity)

`encoding.rs` maps ExtendDB's internal `Item`/key representation ↔
`aws-sdk-dynamodb::types::AttributeValue`. Because ExtendDB already speaks
DynamoDB's type system, this is structurally the identity function; the file is
non-empty only because ExtendDB's in-memory Rust type and the SDK's
`AttributeValue` enum are distinct Rust types holding the same data. It is
round-trip tested across every type (S/N/B/M/L/SS/NS/BS/NULL/BOOL).

It also houses:

- key extraction (partition/sort key) and the
  `exclusive_start_key ↔ ExclusiveStartKey` / `LastEvaluatedKey` conversions;
- the **account-id → physical table name** namespacer. ExtendDB is multi-tenant;
  DynamoDB tables are flat per AWS account. Physical names are
  `<table_prefix><account_id>_<table>`, default prefix `athome_`.

### Data-plane mapping (v1 functional surface)

| ExtendDB trait method | DynamoDB call | Notes |
|---|---|---|
| `put_item` (+condition) | `PutItem` + `ConditionExpression` | translate parsed condition AST → expression string |
| `get_item` | `GetItem` | direct |
| `delete_item` (+condition) | `DeleteItem` | direct |
| `update_item` (actions, condition) | `UpdateItem` | translate update AST → `UpdateExpression` |
| `query` | `Query` | key-condition AST → `KeyConditionExpression`; pagination tokens map 1:1 |
| `scan` (segment/total) | `Scan` | `Segment`/`TotalSegments` direct |
| `transact_write_items(ops, token)` | `TransactWriteItems` | `token` → `ClientRequestToken` (idempotency maps perfectly) |
| `transact_get_items` | `TransactGetItems` | direct |
| `cleanup_expired_idempotency_tokens` | no-op | DynamoDB manages its own idempotency window |
| `create/delete/describe/list/update_table` | `CreateTable` / `DeleteTable` / `DescribeTable` / `ListTables` / `UpdateTable` | account-namespaced physical names |
| `describe_ttl` / `update_ttl` | `DescribeTimeToLive` / `UpdateTimeToLive` | native; ExtendDB TTL worker becomes a no-op |
| tags | `TagResource` / `UntagResource` / `ListTagsOfResource` | direct |
| table size | `DescribeTable` | `TableSizeBytes` / `ItemCount` |
| `process_control_plane_transitions` | `DescribeTable` poll | report CREATING→ACTIVE from AWS's real state |

### Honestly stubbed in v1 (named, not silent)

`StreamEngine` and `BackupEngine` are honest stubs in v1 — every method errors
naming the DynamoDB API it maps to (`DescribeStream`/`GetRecords`/
`GetShardIterator`; `CreateBackup`/`RestoreTableFromBackup`/
`UpdateContinuousBackups`/`DescribeContinuousBackups`). Reason: ExtendDB's stream
model assumes ExtendDB synthesizes records on write, but a passthrough must
instead read DynamoDB's own stream — a real architectural reconciliation not
worth rushing. Both map cleanly and are flagged as fast follow-ups. The gap is
documented in `docs/differences-from-dynamodb.md`.

### Error handling

`errors.rs` maps `SdkError` and DynamoDB operation errors → `StorageError`,
preserving wire-protocol fidelity so SDK clients hitting ExtendDB see the errors
they would expect from DynamoDB:

- `ConditionalCheckFailedException` → ExtendDB's condition-failed error
- `ResourceNotFoundException`, `ResourceInUseException`
- `ProvisionedThroughputExceededException`
- `TransactionCanceledException`, `TransactionConflictException`
- `ItemCollectionSizeLimitExceededException`, `RequestLimitExceeded`, throttling

### Configuration

```toml
[storage]
backend = "dynamodb"

[storage.dynamodb]
region = "us-east-1"
endpoint_url = "https://dynamodb.us-east-1.amazonaws.com"  # may point at ANOTHER ExtendDB
table_prefix = "athome_"
catalog_connection_string = "postgresql://user:pass@localhost/extenddb_catalog"
# AWS credentials resolve via the standard provider chain unless overridden here.
```

`DynamoStorageConfig` implements the `StorageConfig` trait and is registered via
`StorageConfigRegistration` for parsing the `[storage.dynamodb]` section.

### Registration & build wiring

Six `inventory::submit!` registrations under name `"dynamodb"`:
`BackendRegistration`, `OperationsEngineRegistration`,
`StorageConfigRegistration`, `SettingsStoreRegistration`,
`DiagnosticsStoreRegistration`, `ServerComponentsRegistration`. The latter three
delegate to the Postgres catalog. Build wiring:

- `crates/bin/Cargo.toml`: add `dynamodb = ["extenddb-storage-dynamodb"]` feature.
- `crates/bin/src/main.rs`: add `#[cfg(feature = "dynamodb")] extern crate extenddb_storage_dynamodb;` to force linker inclusion of the inventory submissions.
- `crates/bin/src/cmd_serve.rs`: extend the feature-validation arm to accept `dynamodb`.

### Catalog version

`catalog_version()` / `OperationsEngine::catalog_version` delegate to the Postgres
implementation, since the catalog schema lives in Postgres. No separate DynamoDB
catalog version exists.

## Recursion is a feature

`endpoint_url` may point at another ExtendDB endpoint. Documented as a legitimate
use case in deadpan voice ("compliance calls this defense in depth; we call it
on-prem"). **No loop guard** — the near-identity encoding is exactly what makes
the stack composable, and it also enables elegant integration testing
(ExtendDB-on-ExtendDB, or ExtendDB-on-DynamoDB-Local).

## Testing

- **Unit:** `encoding` round-trip across every `AttributeValue` type; the
  error-mapping table with mocked SDK responses; condition/key/update AST →
  expression-string translation.
- **Integration:** point the existing external DynamoDB test suite at
  `serve --backend dynamodb` backed by **DynamoDB Local**, with the catalog on a
  throwaway Postgres. Use the recursion property for an ExtendDB-on-ExtendDB
  smoke test.

## Open questions (resolve during planning; not blockers)

1. Whether ExtendDB's parsed condition/update ASTs can be losslessly
   re-serialized to DynamoDB expression strings, or whether the data-plane traits
   need access to the original wire expressions. Affects how much translation
   `data_engine.rs` performs.
2. Which Postgres catalog/auth constructors are sufficiently `pub` to reuse
   directly, versus needing a small public factory added to
   `extenddb-storage-postgres`.

## Out of scope (v1)

- Streams and Backups/PITR functional implementations (honest stubs in v1).
- Import/Export.
- A pure-DynamoDB catalog (Approach A).
