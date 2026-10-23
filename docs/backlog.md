# Backlog

Refreshed: v0.0.118 (P115)

## Fidelity Bugs

- ⬜ **Backup `TableNotFoundException`** — extenddb returns `ResourceNotFoundException` for backup operations on nonexistent tables; real DynamoDB returns `TableNotFoundException`. Requires adding a new error variant. (P114 follow-up)
- ⬜ **Tagging rate limiting** — extenddb should implement `LimitExceededException` for rapid tag operations to match real DynamoDB behavior. 5 tagging tests fail against real DynamoDB due to this. (P114 follow-up)
- ⬜ **Key-vs-item size gap** — batch/transact delete/update WCU uses key size, not old item size. Minor fidelity gap.
- ⬜ **`f64` equality in `NumericEquals`/`NumericNotEquals`** — `policy/condition.rs` compares parsed numeric condition values with `f64` equality, violating the numeric-safety rule (and clippy `float_cmp`). Verify against AWS numeric-condition semantics (AWS treats these as arbitrary-precision decimals) and switch to `BigDecimal`/integer comparison. (BR-7085 follow-up)
- ⬜ **Non-`2012-10-17` policy `Version` accepted with warning** — extenddb accepts policy documents with an unrecognized `Version` string and logs a warning; real AWS rejects them with a `MalformedPolicyDocument` error. Confirm intended behavior and align. (BR-7085 follow-up)

## Test Gaps

- ⬜ **CLI lifecycle tests** — 9 tests exist but require `EXTENDDB_TEST_PG_CONNECTION_STRING` (separate from standard suite). Currently produce 1 failure + 9 errors in pytest output. Not run by `run-tests --pytest`. The connection-string role (`extenddb`) needs `CREATEDB` in the local test PostgreSQL for these to pass; grant it as part of local test setup.
- ⬜ **Cross-restart metrics test** — 12 metrics tests exist but none verify metrics survive a server restart.
- ⬜ **Rust integration tests leak tables** — `tests/rust/src/batch_transact_authz.rs::tables()` creates `BtaAllowed_*`/`BtaSecret_*` tables with no teardown. Under `run-tests --all` (rust-integration before pytest) these pollute the shared server and cause `test_table_operations.py::test_list_tables_pagination` to fail. Add teardown to the Rust suite and add `Bta`/`BtaSecret` to `devtools/cleanup-test-tables` `TEST_PREFIXES`. (BR-7085 test-run finding)
- ⬜ **`test_list_tables_pagination` isolation sensitivity** — passes in isolation, fails when the shared server carries leaked tables from other suites. Consider scoping the assertion to a unique table-name prefix. (BR-7085 test-run finding)
- ⬜ **OnDemandThroughput pytest cases need modern boto3** — `test_config_fields.py::TestOnDemandThroughput` fails with `botocore ParamValidationError` on the pinned Python 3.7 / pre-2023 botocore (no `OnDemandThroughput` param). Pre-existing (present in the a2f734c baseline). Upgrade the test venv to a botocore that models `OnDemandThroughput`. (BR-7085 test-run finding)

## Code Quality Debt

- ⬜ **6 files over 500 lines** — `validation/mod.rs` (969), `key_condition.rs` (702), `backup_engine.rs` (581), `throttle.rs` (561), `update_evaluator.rs` (561), `policy/document.rs` (552). Human deferred to after testing is complete. P114 recommends splitting validation/mod.rs into `validation/table.rs`, `validation/item.rs`, `validation/key.rs`. (`policy/condition.rs` and `policy/evaluator.rs` split into nested `tests/` modules under 500 lines each in BR-7085.)
- ⬜ Handler boilerplate consolidation
- ⬜ AST cache for expressions
- ⬜ Benchmarking gate
- ⬜ Dockerfile `entrypoint.sh` graceful failure handling (deferred from P47 N-4)
- ⬜ Dockerfile example missing `extenddb init` step (deferred from P47 N-5)
- ⬜ **HTTP→HTTPS redirect path preservation** — redirect goes to `https://{addr}/` regardless of original request path (P84 S2)
- ⬜ **docs_page category order** — hardcoded category list; should derive from manifest (P84 P-S1)

## Feature Backlog (no phase assigned)

- ⬜ **Real PITR implementation** — PostgreSQL temporal/history table approach: `item_history` table capturing every mutation, `DISTINCT ON` query to reconstruct state at time T, 35-day retention via background pruning. Deferred until `RestoreTableToPointInTime` unsupported error is in place. (P113 human session design direction)
- ⬜ **Ion parser** — `InputFormat::Ion` falls through to DynamoDB JSON reader. Full Ion support needed for import/export.
- ⬜ **Key-vs-item size gap** — batch/transact delete/update WCU uses key size, not old item size. Minor fidelity gap.
- ⬜ **Single-frontend-per-catalog enforcement** — no advisory lock or multi-instance coordination. Per steering, caching is prohibited until this is resolved.
- ⬜ **C/C++ test suite** — human has not confirmed whether this is desired. Rust + Python + Java suites are complete.

## Standing Items (need human decision)

- ⬜ **22 unapproved license dependencies** — Unicode-3.0, CDLA-Permissive-2.0, MPL-2.0. All pre-existing. Human approved as-is (P99 session).
- ⬜ **Policy-variable expansion spec/code/AWS divergence** — `05-component-auth.md` §6.5 and `01-requirements.md` (REQ-ABAC-006) state variable substitution is deferred/literal, but `condition.rs::expand_policy_variables` **is** implemented and applied to Condition values only, not Resource ARNs (`evaluator.rs::resource_matches`/`arn_match` do not expand). AWS expands in both. Three-way inconsistent (spec vs code vs AWS). Fail-closed (a `${var}` in a Resource ARN silently fails to match), so not a leak. Resolve by either (a) documenting the implemented condition-value expansion and marking resource-ARN expansion deferred, or (b) completing REQ-ABAC-006. Needs human direction. (BR-7085)


## Recently Completed

### P115 — TTL Redesign (v0.0.118)
- ✅ Indexed TTL sweep — partial B-tree expression index created on TTL enable, sweeper uses index-ordered scan
- ✅ Configurable deletion target — `ttl_deletion_target_seconds` runtime setting (default 300)
- ✅ Staleness metric — `TtlDeletionStaleness` records deletion lag (sum/count/min/max)
- ✅ File split — extracted `ttl_worker.rs` from `workers.rs` (both under 500 lines)
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
