<!-- Copyright 2026 ExtendDB contributors -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Comprehensive Test Suite

Multi-SDK test suite for extenddb. Runs identically against real DynamoDB
and extenddb — failures always fail the suite.

## Directory Structure

```
tests/
  python/          ← Primary suite (boto3 + pytest)
  java/            ← Targeted Java coverage (JUnit 5 + Maven) — P90
  rust/            ← Targeted Rust coverage (tokio::test) — P91
  shared/          ← Shared test data and configuration
```

## Quick Start

### Against extenddb

```bash
# Ensure extenddb is running with TLS + auth
export DYNAMODB_ENDPOINT=https://127.0.0.1:18443
export AWS_ACCESS_KEY_ID=<test-key>
export AWS_SECRET_ACCESS_KEY=<test-secret>
export AWS_DEFAULT_REGION=us-east-1

cd tests/python
python3 -m pytest -v
```

### Against real DynamoDB (validation mode)

```bash
# Ensure AWS credentials are configured
export EXTENDDB_VALIDATION_MODE=true
cd tests/python
python3 -m pytest -v
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DYNAMODB_ENDPOINT` | extenddb endpoint URL. Omit for real DynamoDB. | (none) |
| `AWS_ACCESS_KEY_ID` | AWS access key | (from AWS config) |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | (from AWS config) |
| `AWS_DEFAULT_REGION` | AWS region | `us-east-1` |
| `EXTENDDB_CA_CERT` | Path to CA cert for self-signed TLS | (none — disables verification) |
| `EXTENDDB_VALIDATION_MODE` | When `true`, skips `extenddb_only` tests | `false` |

## Design Principles

1. **Failures always fail.** No expected failures, no xfail, no conditional skips
   based on target. If a test fails against real DynamoDB, the test is wrong.
   If it fails against extenddb, extenddb is wrong.

2. **No target-specific branching.** Tests assert the same behavior regardless
   of target. The only exception: `extenddb_only` tests (CLI lifecycle, multi-instance)
   are skipped in validation mode.

3. **Exact error fidelity.** Error codes, HTTP status, and error messages are
   tested exactly as DynamoDB returns them.

4. **Isolation.** Each test creates its own tables with unique names. Tests
   never depend on state from other tests.

5. **Cleanup.** All fixtures delete tables after use, with `wait_for_deleted()`
   to prevent teardown races.

## Test Categories

- `test_tables.py` — CreateTable, DescribeTable, ListTables, UpdateTable, DeleteTable
- `test_items.py` — PutItem, GetItem, DeleteItem, UpdateItem (all expression types)
- `test_query_scan.py` — Query, Scan (key conditions, filters, pagination, parallel scan)
- (P87) Batch, transactions, GSI, TTL, streams, tagging, expressions
- (P88) Auth/permissions, import/export, error fidelity
- (P89) CLI lifecycle, multi-instance isolation
