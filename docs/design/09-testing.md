---
description: "How tests are organized, how golden files work and how multi-language suites check SDK compatibility."
version: "1.0"
date: "2026-04-06"
status: "Draft"
---

# Test strategy

## 1. Purpose

This document defines extenddb's test strategy: how tests are organized, what reference suites inform coverage, how golden files work, and how multi-language test suites validate SDK compatibility. It is the authoritative reference for anyone writing or running extenddb tests.

## 2. Tenets (for testing)

1. **Real DynamoDB is the oracle**: every expected behavior is captured by running against real DynamoDB, never assumed or copied from reference suites.
2. **Reference suites tell us what to test, not how to answer**: we extract scenario names and API coverage from reference suites; we independently discover correct responses via golden files.
3. **Python carries the fidelity burden**: the Python suite is the primary test suite with full golden file coverage. Other languages validate SDK integration.
4. **Coverage grows with the code**: test coverage and coverage maps expand incrementally as phases progress, never front-loaded as prerequisites that gate implementation.
5. **Same code, two targets**: every test runs against both real DynamoDB and extenddb with zero code changes, controlled by environment variables.
6. **Standard operations only**: the test suite validates fidelity mode exclusively. Preview extensions have their own tests and never appear in the fidelity test suite.

## 3. Reference Suites

### 3.1 Reference Strategy

extenddb uses test suites as a coverage map — a source of *what to test*, not *how to implement*. The test suites are not included here.

### 3.2 Arm's-Length Boundary (MF-1)

The boundary between safe reference and proprietary content is defined by a single rule:

> Reference suites tell us **what behavior to test**. Real DynamoDB tells us **what the correct answer is**.

Concretely:

**Safe to reference:**
- Test class names and method names (they describe what is tested)
- The DynamoDB API operation exercised
- The category of behavior (error path, edge case, limit, expression type)
- The test organization structure (how tests are grouped)

**Never referenced:**
- Specific assertion values, expected error messages, or HTTP status codes from any suite
- Internal DynamoDB implementation details revealed by test setup
- Test infrastructure code (base classes, helpers, utilities)
- Any code referencing internal-only APIs or features

**Example:** A method named `testCreateTableWith6GSIsFails_LimitExceeded` tells us to test "GSI count limit enforcement on CreateTable." We independently discover the exact error type, message, and HTTP status by running the scenario against real DynamoDB and capturing the golden file.

### 3.3 Tracking Upstream Changes

The reference suite's analyzed git hash is stored in `tests/reference/`:

```
tests/reference/
├── suite-hash.txt           # e.g. 1a2b3c4d1a2b3c4d1a2b3c4d1a2b3c4d1a2b3c4d
├── check-upstream.sh        # Diff script
└── coverage-map.md          # Test scenario inventory
```

`check-upstream.sh` compares the stored hash against current HEAD of the reference repo, diffs the file list, and reports new/modified/deleted test files. New files are flagged as high-priority (likely test behaviors extenddb must support).

When new tests appear upstream, a developer reviews them for new scenarios to add to the coverage map. This is a manual review process, not automated ingestion.

### 3.4 Coverage Map (MF-2)

The coverage map (`tests/reference/coverage-map.md`) tracks which reference suite scenarios have corresponding extenddb tests. Every test method maps to an extenddb test scenario at per-method granularity.

The coverage map is a living document, not a prerequisite — it is updated alongside development and does not block Phase 1 implementation.

## 4. Multi-Language Test Suites

### 4.1 Why Multiple Languages

Different SDKs serialize requests differently (field ordering, default values, header formatting), handle error responses differently (some parse `__type`, some parse `Code`), and implement SigV4 with subtle variations. If extenddb only passes Python tests, it may fail with the Java SDK. Real DynamoDB users use all four SDKs — extenddb must work with all of them.

### 4.2 Language Roles (SF-1)

| Language | SDK | Role | When |
|----------|-----|------|------|
| Python | boto3 | Primary test suite. Full fidelity: golden files, error message matching, edge cases. Every phase starts here. | Phase 1+ |
| Java | aws-sdk-java-v2 | Elevated integration. Covers more scenarios than Rust/C++ because the PostgreSQL extension suite (our primary reference) is Java. Tests scenarios that have a direct PostgreSQL extension suite counterpart. | Phase 2+ |
| Rust | aws-sdk-rust | SDK integration validation. Core smoke tests proving the Rust SDK works against extenddb. | Phase 2+ |
| C/C++ | aws-sdk-cpp | SDK integration validation. Exercises a fundamentally different SDK architecture. | Phase 7+ (Query/Scan gives a broad enough API surface — CRUD alone is too narrow to exercise C++'s distinct serialization and memory patterns meaningfully) |

**Phase exit criteria:**
- Python tests: mandatory
- Java tests: mandatory for scenarios with a direct PostgreSQL extension suite counterpart
- Rust/C++ tests: best-effort, tracked but non-blocking

### 4.3 What Non-Python Suites Cover

The Python suite carries the full fidelity burden. Rust, Java, and C++ suites cover:

1. A core smoke test (CRUD + Query + Scan) proving the SDK works
2. SDK-specific serialization edge cases (e.g., Java's handling of empty strings, C++'s handling of binary data)
3. SigV4 signing differences across SDKs

Java additionally covers scenarios that map directly to the PostgreSQL extension suite's test methods, since that suite uses the same SDK.

### 4.4 Directory Structure (N-1)

```
tests/
├── golden/                    # Shared golden files (all languages read, Python captures)
├── comparison_rules/          # Shared comparison rules (all languages read)
├── python/                    # Primary test suite (pytest + boto3)
│   ├── conftest.py           # Shared fixtures, endpoint config, capture workflow
│   ├── test_phase01_table_crud.py
│   ├── test_phase02_put_get.py
│   └── ...
├── rust/                      # Rust SDK integration tests
│   ├── Cargo.toml            # Separate crate, depends on aws-sdk-dynamodb
│   ├── src/
│   │   └── lib.rs            # Shared helpers
│   └── tests/                # Integration tests (idiomatic Rust layout)
│       ├── smoke_crud.rs
│       └── ...
├── java/                      # Java SDK integration tests
│   ├── pom.xml               # Maven project, depends on aws-sdk-java-v2
│   └── src/test/java/
├── cpp/                       # C++ SDK integration tests
│   ├── CMakeLists.txt
│   └── src/
├── reference/                 # Coverage tracking
│   ├── suite-hash.txt
│   ├── check-upstream.sh
│   └── coverage-map.md
└── README.md                 # How to run tests, add tests, capture golden files
```

## 5. Golden Files

### 5.1 Format (SF-2)

Golden files are shared across all language suites and live in a top-level shared directory (`tests/golden/`), not under any language-specific directory. The Python suite owns the capture workflow (via `CAPTURE_GOLDEN=1` in conftest.py), but the output goes to the shared location so all languages read from the same source. The format is language-agnostic: raw HTTP response bodies (JSON) plus HTTP status codes and relevant headers.

```
tests/golden/
├── create_table/
│   ├── basic.json
│   ├── with_gsi.json
│   └── duplicate_table_error.json
├── put_item/
│   ├── basic.json
│   └── item_too_large_error.json
└── ...
```

Each golden file contains:

```json
{
  "request": {
    "operation": "CreateTable",
    "headers": { "Authorization": "REDACTED", "X-Amz-Security-Token": "REDACTED" },
    "body": { "TableName": "...", "KeySchema": [...], ... }
  },
  "response": {
    "status": 200,
    "headers": {
      "x-amzn-RequestId": "example-uuid",
      "x-amz-crc32": "12345"
    },
    "body": { "TableDescription": { ... } }
  }
}
```

The `request` is included so the test harness can replay it. The `response` is the expected result. Each language's test harness deserializes the golden file and compares against its SDK's parsed response. Note: headers like `x-amzn-RequestId` are captured for completeness but are not meaningful for exact comparison — they are validated by format only (see §5.3).

### 5.2 Capturing Golden Files (N-2)

To capture a new golden file:

1. Write the test scenario in Python against real DynamoDB (`us-east-1`)
2. Run with `CAPTURE_GOLDEN=1` to record the raw HTTP response
3. Strip credentials from the captured request before saving:
   - `Authorization` header (SigV4 signature and credential scope)
   - `X-Amz-Security-Token` header (session token)
   - Any `Credential=` or `Signature=` values in query strings
   - Lowercase variants (`x-amz-credential`, `x-amz-signature`)
   - Replace stripped values with `"REDACTED"`
4. Save to `tests/golden/<operation>/<scenario>.json`
5. Verify the captured response is deterministic (run twice, diff)
6. Commit the golden file — it must never contain real credentials

The `tests/README.md` documents this workflow.

### 5.3 Field Comparison Rules

Not all fields can be compared exactly between AWS and extenddb. Per-operation YAML files in `tests/comparison_rules/` specify field handling:

```yaml
# comparison_rules/create_table.yaml
exact:
  - TableDescription.TableName
  - TableDescription.KeySchema
  - TableDescription.AttributeDefinitions
  - TableDescription.BillingModeSummary.BillingMode
  - TableDescription.DeletionProtectionEnabled
  - TableDescription.TableSizeBytes
  - TableDescription.ItemCount
format_only:
  - x-amzn-RequestId    # UUID regex
  - x-amz-crc32         # valid integer
ignore:
  - TableDescription.CreationDateTime
  - TableDescription.TableArn        # account ID differs
  - TableDescription.TableId         # UUID differs
  - TableDescription.TableStatus     # CREATING vs ACTIVE
normalize:
  - TableDescription.ProvisionedThroughput.LastIncreaseDateTime
  - TableDescription.ProvisionedThroughput.LastDecreaseDateTime
  - TableDescription.BillingModeSummary.LastUpdateToPayPerRequestDateTime
partial:
  - TableDescription.ProvisionedThroughput  # extenddb may omit NumberOfDecreasesToday initially
```

Five comparison categories:

| Category | Behavior |
|----------|----------|
| `exact` | Field must match the golden file value exactly |
| `format_only` | Field must be present and match a format (e.g., UUID regex) but the value is not compared |
| `ignore` | Field is skipped entirely during comparison |
| `normalize` | Field is transformed before comparison (e.g., timestamps normalized to epoch) |
| `partial` | extenddb's response for this object is a strict subset of the golden file. Fields present in the golden file but absent from extenddb are allowed (extenddb is incomplete). Fields present in extenddb but absent from the golden file are test failures (extenddb must not return data that real DynamoDB does not). Fields present in both are compared according to their own comparison category. This is a transitional mode: as extenddb matures, fields move from `partial` to `exact`. |

The comparison rules live in a top-level shared directory (`tests/comparison_rules/`) so all language suites can read them. The Python test harness reads these rules and applies them during comparison. Other language suites use the same YAML files.

## 6. Test Harness Design

Each language's test harness:

- Reads endpoint configuration from environment variables (`AWS_ENDPOINT_URL_DYNAMODB`, `AWS_REGION`, etc.)
- Runs against both real DynamoDB and extenddb with zero code changes
- Handles table creation wait times — both real DynamoDB and extenddb use async control plane transitions. The harness polls `DescribeTable` until `ACTIVE`
- Cleans up test tables using the `EXTENDDB_TABLE_PREFIX` convention — deletes all matching tables at the start of each run (idempotent cleanup)
- Reports results in JUnit XML format for CI integration

### 6.1 Test Infrastructure Pattern (Q-7)

All language suites adopt the same architectural pattern independently (not copied from reference suites):

- A base class/fixture with table setup and teardown
- Assertion helpers for item comparison and attribute value comparison
- Endpoint configuration from environment variables
- Table name prefix (`EXTENDDB_TABLE_PREFIX`, defaults to `extenddb_test_`) for cleanup isolation — configurable to support parallel test runs

This pattern is standard for DynamoDB test suites and proven by the PostgreSQL extension suite's architecture.

### 6.2 Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `AWS_ENDPOINT_URL_DYNAMODB` | extenddb endpoint override | (none — uses real DynamoDB) |
| `AWS_REGION` | Region for requests | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | Credentials | (from AWS config) |
| `AWS_SECRET_ACCESS_KEY` | Credentials | (from AWS config) |
| `CAPTURE_GOLDEN` | Enable golden file capture mode | `0` |
| `EXTENDDB_TABLE_PREFIX` | Table name prefix for test isolation and cleanup | `extenddb_test_` |

## 7. Phase Integration

### 7.1 Test Scope Per Phase

| Phase | Python | Java | Rust | C++ |
|-------|--------|------|------|-----|
| 1 (Server + table CRUD) | Full: table lifecycle, error paths, raw HTTP edge cases, health/metrics | none | none | none |
| 2 (PutItem + GetItem) | Full: all attribute types, size limits, ReturnValues | Smoke: basic put/get | Smoke: basic put/get | none |
| 3 (SigV4) | Full: valid/invalid credentials, clock skew, missing headers | Smoke: SigV4 with Java SDK | Smoke: SigV4 with Rust SDK | none |
| 4 (Policy engine) | Full: policy enforcement, ABAC, management API | none | none | none |
| 5 (DeleteItem + UpdateItem) | Full: CRUD + expressions | Java scenarios from PostgreSQL ext suite | none | none |
| 6 (Full expressions) | Full: all operators, functions, Expected | none | none | none |
| 7 (Query + Scan) | Full: key conditions, pagination, 1MB limit | Java scenarios from PostgreSQL ext suite | Smoke: basic query/scan | Smoke: basic CRUD + query |
| 8+ | Python mandatory, Java for PostgreSQL ext counterparts, Rust/C++ extended smoke | | | |

### 7.2 Health and Metrics Endpoints (Q-6)

Phase 1 includes `/health` and `/metrics` endpoints. They are trivial to implement (already designed in 06-component-server.md §7), useful for operational validation from day one, and tested by the PostgreSQL extension suite's `RawHttpTests`.

### 7.3 New Test Categories from PostgreSQL Extension Suite

The PostgreSQL extension suite identified test categories the original implementation plan did not enumerate:

| Category | Source | Phase |
|----------|--------|-------|
| Empty value handling (empty strings, binary, sets) | `EmptyValueTests` | 2 |
| Unicode and special characters (emoji, single quotes) | `UnicodeTests` | 2 |
| Raw HTTP edge cases (invalid JSON, empty body, missing target, GET rejection) | `RawHttpTests` | 1 |
| Health and metrics endpoints | `RawHttpTests` | 1 |
| Capacity reporting (`ReturnConsumedCapacity`) | `CapacityThrottlingTests` | 12 |
| Throttling behavior | `CapacityThrottlingTests` | 12 |

## 8. Out of Scope

### 8.1 PartiQL (Q-5, N-3)

PartiQL is deferred to post-v1 (Phase 15+). It requires a separate parser and execution engine.

### 8.2 CI Infrastructure (Q-4)

CI design is deferred to a separate discussion. This document defines *what* runs; CI defines *where* and *when*. The test strategy is not blocked on CI decisions.

## 9. Phase Exit Criteria (Testing Addendum)

Phase exit criteria are defined in the implementation plan. This document adds the following testing requirements:

1. For phases with PostgreSQL extension suite counterparts: all mandatory Java tests pass
2. Rust and C++ test failures are tracked but do not block phase completion

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](https://github.com/ExtendDB/extenddb/blob/main/LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is maintained by
engineers at Amazon Web Services. It is not Amazon DynamoDB and contains no DynamoDB source
code. "DynamoDB" is a trademark of Amazon.com, Inc.
