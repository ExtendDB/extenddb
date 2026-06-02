# Backlog

Refreshed: v0.0.118 (P115)

## Fidelity Bugs

- ⬜ **Backup `TableNotFoundException`** — extenddb returns `ResourceNotFoundException` for backup operations on nonexistent tables; real DynamoDB returns `TableNotFoundException`. Requires adding a new error variant. (P114 follow-up)
- ⬜ **Tagging rate limiting** — extenddb should implement `LimitExceededException` for rapid tag operations to match real DynamoDB behavior. 5 tagging tests fail against real DynamoDB due to this. (P114 follow-up)
- ⬜ **Key-vs-item size gap** — batch/transact delete/update WCU uses key size, not old item size. Minor fidelity gap.

## Test Gaps

- ⬜ **CLI lifecycle tests** — 9 tests exist but require `EXTENDDB_TEST_PG_CONNECTION_STRING` (separate from standard suite). Currently produce 1 failure + 9 errors in pytest output. Not run by `run-tests --pytest`.
- ⬜ **Cross-restart metrics test** — 12 metrics tests exist but none verify metrics survive a server restart.

## Code Quality Debt

- ⬜ **8 files over 500 lines** — `validation/mod.rs` (969), `policy/condition.rs` (720), `key_condition.rs` (702), `policy/evaluator.rs` (675), `backup_engine.rs` (581), `throttle.rs` (561), `update_evaluator.rs` (561), `policy/document.rs` (552). Human deferred to after testing is complete. P114 recommends splitting validation/mod.rs into `validation/table.rs`, `validation/item.rs`, `validation/key.rs`.
- ⬜ Handler boilerplate consolidation
- ⬜ AST cache for expressions
- ⬜ Benchmarking gate
- ⬜ Dockerfile `entrypoint.sh` graceful failure handling (deferred from P47 N-4)
- ⬜ Dockerfile example missing `extenddb init` step (deferred from P47 N-5)
- ⬜ **HTTP→HTTPS redirect path preservation** — redirect goes to `https://{addr}/` regardless of original request path (P84 S2)
- ⬜ **docs_page category order** — hardcoded category list; should derive from manifest (P84 P-S1)

## Feature Backlog (no phase assigned)

- ⬜ **Real PITR implementation** — PostgreSQL would need a temporal/history-table design. TiDB should not use frontend row replay; only expose DynamoDB table-level PITR if TiDB provides a native set-based online restore into a new table. The engine now delegates `RestoreTableToPointInTime` to storage, and backends return explicit unsupported errors when they cannot implement it faithfully.
- ⬜ **Ion parser** — `InputFormat::Ion` falls through to DynamoDB JSON reader. Full Ion support needed for import/export.
- ⬜ **Key-vs-item size gap** — batch/transact delete/update WCU uses key size, not old item size. Minor fidelity gap.
- ⬜ **PostgreSQL single-frontend-per-catalog enforcement** — the PostgreSQL backend still needs an HA-safe worker coordination design before shared-catalog multi-frontend deployment. TiDB is not blocked by this item because the TiDB backend uses idempotent catalog reconciliation and TiDB-native online DDL scheduling.
- ⬜ **C/C++ test suite** — human has not confirmed whether this is desired. Rust + Python + Java suites are complete.

## Standing Items (need human decision)

- ⬜ **22 unapproved license dependencies** — Unicode-3.0, CDLA-Permissive-2.0, MPL-2.0. All pre-existing. Human approved as-is (P99 session).

## Recently Completed

### P115 — TTL Redesign (v0.0.118)
- ✅ Indexed TTL sweep — partial B-tree expression index created on TTL enable, sweeper uses index-ordered scan
- ✅ Backend-owned TTL cleanup — PostgreSQL uses the indexed sweeper; TiDB uses native TTL instead of a frontend deletion target
- ✅ Staleness metric — `TtlDeletionStaleness` records deletion lag (sum/count/min/max)
- ✅ PostgreSQL file split — extracted `ttl_worker.rs` from `workers.rs` (both under 500 lines)
- ✅ SQL injection fix — `validate_ttl_attribute_name()` at engine layer for DDL safety
- ✅ Migration 011 consolidated into 001_schema.sql, catalog version 0.0.2
- ✅ Clippy improvement: 272 (down from 273 baseline)

### P114 — Fidelity Fixes (v0.0.117)
- ✅ `RestoreTableToPointInTime` returns `ValidationException` (unsupported) instead of faking restore
- ✅ GSI `ProvisionedThroughput` on `PayPerRequest` tables returns `ValidationException`
- ✅ Real DynamoDB test compatibility: tagging ARNs, raw HTTP, backup retry, throttling skip, TTL cooldown
- ✅ External Java tests: 346/346 (100% pass rate, up from 345/346)
- ✅ Identified 2 new fidelity follow-ups: `TableNotFoundException` for backups, tagging rate limiting

### P113 — Real DynamoDB Test Infrastructure (v0.0.116)
- ✅ Rust integration tests can run against real DynamoDB (conditional endpoint + credential chain)
- ✅ Removed dummy-key/dummy-secret fallbacks
- ✅ Fixed GSI ProvisionedThroughput on PayPerRequest tables in test helpers (206 test failures resolved)
- ✅ Real DynamoDB run: 115/346 passed (pre-GSI-fix), ~300+/346 expected post-fix
- ✅ Identified 5 categories of real DynamoDB test failures for follow-up

### P112 — Documentation Refresh, UNSIGNED-PAYLOAD Fix (v0.0.113)
- ✅ Reject UNSIGNED-PAYLOAD in SigV4 verification (fidelity fix)
- ✅ Updated differences-from-dynamodb.md (backup/restore, throttling, runtime settings)
- ✅ Updated getting-started.md (throttling docs, version references)
- ✅ Refreshed backlog.md and todo-index.md

### P102–P111 — Test Infrastructure + Rust Integration Suite (v0.0.103–v0.0.113)
- ✅ Eliminated all pytest skips (D1)
- ✅ Credential validation in run-tests (D2)
- ✅ Target echo in run-tests (D3)
- ✅ run-tests script lives in code repos (D4)
- ✅ Error prefix fidelity fix (UnknownOperationException)
- ✅ Rust integration test suite: 346 tests, 100% Java parity, all passing (D6)
- ✅ Fixed 4 failing Rust integration tests (throttling + table name validation)

### P94–P100 — File Splits, External Test Fixes, Docs (v0.0.99–v0.0.103)
- ✅ Split storage-postgres lib.rs, data/mod.rs, management_store.rs
- ✅ External tests: 346/346 passing
- ✅ Throttling as runtime setting
- ✅ 130 new Rust unit tests (187 → 317)
- ✅ 9 CLI lifecycle pytest tests
- ✅ 124 comprehensive Python tests (296 total)

### P82–P93: Auth, Streams, Import/Export, Console, Docs ✅
### P69–P81: Storage Abstraction, Interactive Prompts, Refactoring ✅
### P56–P68: Throttling, Metrics, Security, UX ✅
### P44–P55: Metrics, TLS, Security, Operational Hardening ✅

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
