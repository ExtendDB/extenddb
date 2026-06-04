# Technical Debt Tracker

Last updated: 2026-05-04 (P112)

## Categories

- **Fidelity**: Behavior that differs from real DynamoDB
- **Cleanup**: Code quality, performance, or maintainability improvements
- **Security**: Security hardening items
- **Testing**: Missing or inadequate test coverage

## Fidelity

| # | Item | Location | Priority | Origin |
|---|------|----------|----------|--------|
| F-1 | `AttributesToGet` (legacy API) not supported | `core/types/batch.rs:27` | Low | P6 |
| F-2 | `ConsistentRead=false` not routed to read replica | `core/types/item.rs:166` | Low | P5 |
| F-3 | Import/export: full Ion parser not implemented (JSON subset only) | `engine/import_export.rs:339` | Medium | P24 |
| F-4 | Import/export: full Ion writer not implemented (JSON subset only) | `engine/import_export.rs:433` | Medium | P24 |
| F-5 | `PutItem` returns `None` for `Item` field instead of omitting it | `server/lib.rs:235` | Low | P2 |
| F-6 | `extract_key_from_item` returns alphabetically first key, not necessarily the correct one for multi-key tables | `server/lib.rs:256` | Low | P7 |
| F-7 | `MissingAuthenticationToken` returned regardless of auth provider state | `server/lib.rs:190` | Low | P12 |
| F-8 | IAM policies have no FK on `principal_name` — can reference nonexistent principals | `server/management/iam_policy.rs:144` | Medium | P12c |
| F-9 | Permissions boundary has no FK enforcement on principal existence | `server/management/permissions_boundary.rs:110` | Medium | P12c |
| F-10 | `ACTIVE_WINDOW` hardcoded to 10s; real DynamoDB varies | `bin/cmd_serve.rs:346` | Low | P1 |
| F-11 | `describe_table` + `list_tags` not in a transaction — concurrent race possible | `storage-postgres/lib.rs:1073` | Low | P9 |
| F-12 | ~~Tagging operations don't validate resource existence (real DynamoDB returns `ResourceNotFoundException`)~~ | ~~`engine/tagging.rs`~~ | ~~Medium~~ | P26 |
| F-13 | HTTP 500 returned for pool exhaustion instead of 503 | `server/lib.rs` | Medium | P25 |
| F-14 | POSIX syslog single-identity limitation prevents separate `extenddb-sqlx` syslog identity | `bin/cmd_serve.rs` | Low | P25 |
| F-15 | ~~TTL worker bypasses stream capture — expired item deletions don't generate REMOVE stream records~~ | `bin/cmd_serve.rs:ttl_cleanup_worker` | ~~High~~ | P26 |
| F-16 | `transact_write_items.rs` passes `None` for `old_item` in stream capture — `OldImage` always `None` for transaction-originated stream records | `engine/transact_write_items.rs` | Medium | P27 |
| F-17 | `validate_attribute_name_sizes` only checks top-level attribute names — nested map keys not validated | `core/validation/mod.rs` | Low | P30 |

## Cleanup

| # | Item | Location | Priority | Origin |
|---|------|----------|----------|--------|
| C-1 | `--catalog-db` should be `Optional<String>` for `init` without full config | `bin/cmd_init.rs:113` | Low | P1 |
| C-2 | Storage backend config field unused (always "postgres") | `bin/config.rs:55` | Low | P1 |
| C-3 | ~~GSI error matching via English substring `"does not exist"`~~ — now uses SQLSTATE `42P01` | `storage-postgres/gsi_queue.rs` | ~~Medium~~ | P25 |
| C-4 | BigDecimal parsed on every comparison in expression evaluator | `core/expression/evaluator.rs:112` | Low | P4 |
| C-5 | Stream shard list not cached per table (extra SQL round-trip per write) | `storage-postgres/lib.rs:1844` | Low | P10 |
| C-6 | Inactive auth keys return `Err(DynamoDbError)` instead of a typed error | `auth/lib.rs:83` | Low | P12 |
| C-7 | Console account pages: concurrent `CreateTable` race at READ COMMITTED | `server/console/pages/account_pages.rs:370` | Low | P12j |
| C-8 | Console routing could be cleaner | `server/console/mod.rs:19` | Low | P12j |
| C-9 | `poll_log_level` function name doesn't reflect dual-level responsibility | `bin/cmd_serve.rs` | Low | P25 |
| C-10 | Windows installer script not implemented — Linux and macOS only | — | Medium | P32 |

## Security

| # | Item | Location | Priority | Origin |
|---|------|----------|----------|--------|
| S-1 | Multi-instance safety: no lease table prevents multiple extenddb instances sharing one PostgreSQL database | — | High | P25 review |
| S-2 | `--password` flag visible in process listings (`ps aux`) | `bin/cmd_manage.rs` | Low | P12b |
| S-3 | Release tarballs not GPG-signed — blocked on key ownership, public key distribution, CI secrets management | `devtools/build-release` | Medium | P28 |

## Testing

| # | Item | Location | Priority | Origin |
|---|------|----------|----------|--------|
| T-1 | `test_disable_ttl` flaky due to TTL modification cooldown | `tests/test_ttl.py` | Medium | P23 |
| T-2 | ~~No code coverage tooling configured (Rust or Python)~~ | — | ~~Medium~~ | P25 review |
| T-3 | External Java tests lack `waitForGSI` helpers — 5 GSI tests fail intermittently due to propagation timing | `tests/external/` | Medium | P26 |

## Architecture

| # | Item | Location | Priority | Origin |
|---|------|----------|----------|--------|
| A-1 | Catalog/data database separation not implemented (REQ-CAT-001/002) | `storage-postgres/src/lib.rs` | High | P40 |

### A-1: Catalog/Data Database Separation

**Design requirement:** Two databases — catalog (`extenddb`) for metadata, data (`extenddb_data`) for user items (REQ-CAT-001, REQ-CAT-002).

**Current state:** `extenddb init` correctly creates both databases and stores the data connection string in the settings table. However, the runtime (`PostgresEngine`) only opens one connection pool to the catalog database. All `_ddb_*` item tables are created in the catalog database. The `extenddb_data` database exists but sits empty. The settings table has `data_database_connection_string` and `data_database_name` but the code never reads them at runtime.

**What needs to change:**
1. Open a second connection pool for the data database at startup
2. Route item storage operations (`_ddb_*` tables) to the data pool
3. Update transaction boundaries — catalog metadata and data writes may need coordinated commits
4. Update GSI queue to use the data pool for item data
5. Update `extenddb destroy` to drop both databases

## Unenforced DynamoDB Limits

See `docs/dynamodb-limits.md` for the full catalog. The following are the highest-priority unenforced limits:

| # | Limit | DynamoDB Value | Priority | Origin |
|---|-------|---------------|----------|--------|
| L-1 | Projected attributes across all indexes | 100 | Medium | P42 |
| L-2 | Expression size limits (condition/filter/projection) | 4 KB each | Low | P42 |
| L-3 | Batch/transaction aggregate request size | 4–16 MB | Low | P42 |
| L-4 | GetRecords max per call | 1,000 records | Low | P42 |
| L-5 | Shard iterator lifetime | 15 minutes | Medium | P42 |
| L-6 | Tag count per resource | 50 | Low | P42 |
| L-7 | Tag key/value length limits | 128/256 chars | Low | P42 |
| L-8 | LSI item collection size | 10 GB | Low | P42 |
| L-9 | Provisioned capacity decrease limit | 27/day | Low | P42 |

## File Size Overages (>500 lines)

Tracked per review checklist hard gate. Splits happen opportunistically when files are modified, or as cleanup when bandwidth allows. No dedicated phase.

Last verified: P112 (v0.0.113)

| # | File | Lines | Origin |
|---|------|-------|--------|
| FS-4 | `core/src/validation/mod.rs` | 890 | P4 |
| FS-5 | `auth/src/policy/condition.rs` | 720 | P15c |
| FS-6 | `core/src/expression/key_condition.rs` | 702 | P17 |
| FS-7 | `auth/src/policy/evaluator.rs` | 675 | P15c |
| FS-13 | `storage-postgres/src/backup_engine.rs` | 581 | P97 |
| FS-14 | `core/src/throttle.rs` | 561 | P56 |
| FS-10 | `core/src/expression/update_evaluator.rs` | 561 | P4 |
| FS-12 | `auth/src/policy/document.rs` | 552 | P15c |
| FS-11 | `core/src/types/table.rs` | 546 | P1 |
| FS-15 | `storage/src/lib.rs` | 510 | P69 |
| FS-8 | `bin/src/cmd_serve.rs` | 506 | P1 |

Resolved (split in P94–P96):
- ~~FS-1: `storage-postgres/src/data.rs` (2809)~~ — split into 11 modules
- ~~FS-2: `storage-postgres/src/lib.rs` (1980)~~ — split into focused modules (now 216 lines)
- ~~FS-3: `bin/src/cmd_manage.rs` (1117)~~ — refactored (now 44 lines)
- ~~FS-9: `engine/src/transact_write_items.rs` (611)~~ — split into helpers (now 317 lines)

## Resolved in P30

- ~~F-12: Tagging operations don't validate resource existence~~ (fixed: `validate_resource_arn()` checks table existence via `table_key_info()`, returns `ResourceNotFoundException` for missing tables)

## Resolved in P27

- ~~F-15: TTL worker bypasses stream capture~~ (fixed: TTL deletions now route through engine stream capture with `userIdentity: {type: "Service", principalId: "dynamodb.amazonaws.com"}`)
- ~~T-2: No code coverage tooling~~ (fixed: `cargo-llvm-cov` for Rust, `pytest-cov` for Python; `devtools/run-coverage` script added)

## Resolved in P26

- ~~Streams shard iterator doesn't advance past consumed records~~ (fixed: `streams.rs:244`)
- ~~`DefaultHasher` instability in GSI queue partitioning~~ (fixed: replaced with `crc32fast`)
- ~~LATEST iterator returns no records~~ (fixed: resolved to current max sequence at creation time)
- ~~Zero Python test coverage for TagResource, UntagResource, ListTagsOfResource, DescribeEndpoints, DescribeLimits, ListStreams~~ (added `test_misc_operations.py`, `test_streams.py`)
- ~~Streams tests only covered record creation, not consumption~~ (added full polling protocol tests)
- ~~TTL+streams test used wrong client type~~ (fixed: uses `dynamodbstreams` client)
- ~~`demos/stream_demo.py` not in `samples/`~~ (moved to `samples/stream_consumer.py`)
- ~~Streams docs missing SDK client note and polling pattern~~ (added to `getting-started.md` and `usage-guide.md`)

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
