# ADR-0004: Natural UPSERT Semantics

- Status: Accepted
- Date: 2026-06-04
- Deciders: ExtendDB Cassandra plugin contributors

## Context

PostgreSQL requires explicit `ON CONFLICT ... DO UPDATE` syntax to achieve UPSERT (insert-or-update) behavior. Without this clause, inserting a duplicate primary key results in a unique constraint violation error.

Cassandra has different semantics: A simple `INSERT` statement will automatically overwrite any existing row with the same primary key. This "last write wins" behavior is fundamental to Cassandra's conflict resolution model.

Operations like `put_policy` must be idempotent: Calling the operation multiple times with the same policy name should result in the policy being stored, not an error. PostgreSQL achieves this with `ON CONFLICT`, but Cassandra can use a plain `INSERT`.

## Options Considered

1. **Natural INSERT behavior** — Use plain `INSERT` statements and rely on Cassandra's built-in overwrite semantics. Simpler code, same idempotent result.

2. **Explicit UPDATE with conditional INSERT** — Check if row exists via SELECT, then execute UPDATE or INSERT accordingly. Mimics PostgreSQL logic exactly but adds unnecessary complexity.

3. **Lightweight transaction with IF EXISTS** — Use `UPDATE ... IF EXISTS` to make the upsert explicit. Adds LWT overhead for no behavioral benefit.

## Decision

Use natural INSERT behavior for operations requiring UPSERT semantics.

## Rationale

- Cassandra's INSERT is already idempotent: Inserting the same primary key twice produces the same end state as inserting once. No need to emulate PostgreSQL's explicit `ON CONFLICT` syntax.
- Simpler code: Single `INSERT` statement instead of `SELECT` + conditional branching + `UPDATE`/`INSERT`.
- Better performance: Plain INSERT is faster than LWT. No consensus round-trip required.
- Idiomatic Cassandra: "Last write wins" is the expected conflict resolution model. Fighting it with conditional logic adds complexity for no gain.
- Same observable behavior: From the caller's perspective, `put_policy` is idempotent regardless of whether the backend uses `ON CONFLICT` or natural overwrite.

## Consequences

- Operations like `put_policy_impl` use plain INSERT:
  ```rust
  let query = format!(
      "INSERT INTO {}.iam_policies (account_id, principal_type, principal_name, 
       policy_name, policy_document, created_at) VALUES (?, ?, ?, ?, ?, toTimestamp(now()))",
      catalog_keyspace
  );
  ```
- No `IF NOT EXISTS` check needed: INSERT will succeed regardless of whether the policy already exists.
- Timestamp behavior: `created_at` is overwritten on every INSERT. This differs from PostgreSQL, where `created_at` is preserved on UPDATE. Mitigation: IAM catalog queries do not rely on `created_at` for critical logic. It's informational only.
- Simpler error handling: No need to check `[applied]` column or handle LWT failure cases.
- Divergence from PostgreSQL: Code structure is simpler in Cassandra plugin. This is acceptable because both backends provide the same idempotent guarantees to the caller.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
