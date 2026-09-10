# ADR-0002: Transaction and Atomicity Patterns

- Status: Accepted
- Date: 2026-06-04
- Deciders: ExtendDB Cassandra plugin contributors

## Context

PostgreSQL implements multi-statement ACID transactions via `BEGIN`/`COMMIT` blocks. The PostgreSQL storage layer uses this for operations like `create_user`, which must atomically insert both an `iam_users` row and a `iam_policies` row (for the self-service policy). If either insert fails, the entire transaction rolls back.

Cassandra does not support multi-statement transactions. Logged batches provide atomicity only when all statements target the same partition key. Lightweight transactions (LWT) with `IF NOT EXISTS` can detect duplicates but cannot be combined with batches. This creates a challenge for operations requiring both duplicate detection and atomicity.

## Options Considered

1. **Check-then-batch pattern** — Execute a SELECT to check for duplicates, then use a logged BATCH for the atomic writes. Accept a small race condition window between check and write.

2. **Sequential LWTs** — Execute each insert with `IF NOT EXISTS` sequentially, rolling back manually on failure. Not truly atomic.

3. **Compensation pattern** — Allow writes to proceed without duplicate checks, then clean up on failure. Leaves temporary inconsistent state.

4. **Single LWT** — Combine both operations into a single denormalized record. Breaks schema compatibility with PostgreSQL.

## Decision

Check-then-batch pattern for operations requiring both duplicate detection and atomicity.

## Rationale

- IAM operations naturally partition by `account_id`: Both `iam_users` and `iam_policies` use `account_id` as their partition key. This enables true atomic batches in Cassandra.
- Race condition window is acceptable: The window between SELECT and BATCH is milliseconds. For infrequent IAM operations, the probability of collision is negligible.
- Sequential LWTs are not atomic: If the second LWT fails, the first write has already committed. Manual compensation requires additional complexity and still leaves a window of inconsistency.
- Compensation pattern introduces eventual consistency: Temporary invalid states violate ExtendDB's IAM consistency expectations.
- Single LWT with denormalization breaks PostgreSQL schema compatibility and complicates queries.
- Idiomatic Cassandra: Check-then-batch is the recommended pattern for operations requiring both validation and atomicity when strict ACID guarantees are not required.

## Consequences

- All multi-entity IAM operations follow this pattern:
  1. Pre-check foreign keys (via ADR-0001)
  2. SELECT to check for duplicate primary keys
  3. Logged BATCH to perform atomic writes
- Small race window: Another concurrent operation could insert the same entity between steps 2 and 3. Mitigation: IAM operations are admin-driven and infrequent. Application-layer retry with exponential backoff handles the rare collision.
- Simpler code than compensation: No cleanup logic needed. Either the batch succeeds atomically or it fails atomically.
- Consistency model: Operations are immediately consistent once the batch commits. No eventual consistency delay.
- Cannot use LWT inside batches: Cassandra limitation. Batches are atomic but not isolated, and LWT requires isolation. This is an acceptable tradeoff given IAM operation frequency.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
