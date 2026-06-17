# ADR: SQL Injection Defense

## Status

Accepted — 2026-04-07. Revised — 2026-04-21 (P30: reflect actual design).

## Context

extenddb stores DynamoDB table metadata and item data in PostgreSQL. User-supplied strings — table names, attribute names, key values, filter expressions — flow from HTTP requests through the engine layer into SQL queries. A SQL injection vulnerability here would allow arbitrary database access.

## Decision

### Two-tier defense

**Tier 1: Engine-layer validation.** All user-supplied strings are validated at the engine layer before reaching storage. The storage layer trusts that inputs have been validated.

- Table names: validated against DynamoDB's regex (`[a-zA-Z0-9_.-]+`, 3–255 chars) in `validate_table_name()`
- Index names: validated against the same character set
- Account IDs: validated in `PostgresEngine::validate_account_id()` — rejects `"`, `\0`, and non-ASCII
- Attribute names: validated in expression parsing
- Key values: type-checked via `AttributeValue` deserialization

**Tier 2: Parameterized queries for user values.** All user-supplied *values* (key data, item data, filter parameters) use bind parameters (`$1`, `$2`, ...) via sqlx. No user-supplied value is ever interpolated into a SQL string.

```rust
// Correct — parameterized value
sqlx::query("SELECT item_data FROM some_table WHERE pk = $1")
    .bind(&pk_value)
    .fetch_one(&pool)
    .await?;
```

### SQL identifier construction (validated interpolation)

Per-table data storage uses dynamically named PostgreSQL tables. Table names are constructed from validated components via `data_table_name()` and `index_table_name()` in `storage-postgres/src/data.rs`:

```rust
// Table name built from validated account_id + table_name
fn data_table_name(account_id: &str, table_name: &str) -> String {
    format!("\"_ddb_{account_id}_{table_name}\"")
}
```

These identifiers are interpolated into SQL strings via `format!`. This is safe because:

1. `account_id` is validated by `validate_account_id()` — rejects `"`, `\0`, non-ASCII
2. `table_name` is validated by `validate_table_name()` — only `[a-zA-Z0-9_.-]`
3. The result is double-quoted, preventing interpretation as SQL keywords
4. No character in the validated set can escape a double-quoted identifier

This is **not** the same as raw string interpolation of user input. The validation happens at the engine layer before the storage layer ever sees the value.

### What is prohibited

- `format!` with raw, unvalidated user input in SQL strings
- Any SQL construction path that bypasses the engine-layer validation

## Consequences

- Every new SQL query that uses bind parameters for values needs no special review beyond normal correctness.
- Any new SQL identifier interpolation must go through `data_table_name()` / `index_table_name()` or an equivalent validated path. Direct `format!` with user-supplied identifiers is a review blocker.
- The engine layer is the single point of input validation. Adding a new user-facing field requires adding validation before it reaches storage.
- `validate_account_id()` is the defense-in-depth gate for account IDs used in SQL identifiers.

## Audit Trail

- 2026-04-07: Initial ADR. All SQL used parameterized queries.
- 2026-04-07: `ListTables.ExclusiveStartTableName` gap closed — `validate_table_name_chars()` added.
- 2026-04-21: ADR rewritten to accurately describe the two-tier design. The original ADR claimed "no dynamic SQL" which was misleading — validated identifier interpolation via `format!` is used extensively for per-table storage tables. The defense is validation + quoting, not absence of interpolation.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
