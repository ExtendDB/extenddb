# ADR-0020: Optimistic Concurrency Control for UpdateItem and PutItem

- Status: Accepted
- Date: 2026-08-31
- Deciders: ExtendDB Cassandra plugin contributors

## Context

`update_item_impl` and the conditional path of `put_item_impl` use a read-check-write pattern: read `item_data`, mutate in Rust, write back. This is not atomic. Under concurrent writes to the same item, the last writer wins and intermediate updates are silently lost. This affects all `UpdateItem` calls — not just conditional ones. The root cause is the non-atomic read-modify-write cycle itself.

## Options Considered

1. **Version-based OCC via LWT** — Add a `version bigint` column to each data table. Every successful write increments it. The read-modify-write cycle is made atomic by conditioning the write on `version = <read_version> AND prepared_txn_id = NULL`. A `[applied] = false` response means another writer won the race; retry from the read.

2. **Serialized writes via a per-item lock service** — Acquire an external distributed lock per item key before each write. Rejected: adds infrastructure dependency and defeats Cassandra's distributed architecture.

3. **Accept last-writer-wins** — Document the behavior and leave it to callers to use conditional expressions. Rejected: DynamoDB's `UpdateItem` is defined to be atomic; silent data loss is not acceptable.

## Decision

Version-based OCC via LWT (option 1).

## Rationale

- Closes the race atomically at the storage layer with no external dependency.
- The LWT condition must include both `version = ?` and `prepared_txn_id = NULL`. Checking `version` alone is insufficient: a transaction could PREPARE the row (setting `prepared_txn_id`) without changing `version`, and a concurrent non-transactional writer's `IF version = ?` would then succeed and overwrite a prepared item, corrupting the in-flight transaction.
- Existing rows with `version = NULL` are treated as version 0; the first write uses `IF version = NULL AND prepared_txn_id = NULL`.
- Transaction COMMIT must increment `version` (`version = version + 1`) so that a non-transactional writer that read a pre-transaction version cannot overwrite committed transaction data. The `IF prepared_txn_id = ?` guard on COMMIT ensures the blind increment is safe.
- Transaction PREPARE and ROLLBACK do not touch `version`; PREPARE sets `prepared_txn_id` (which blocks non-transactional writers via their LWT condition), and ROLLBACK leaves `version` at the pre-PREPARE value since the item was not modified.

## Retry policy

- **Max retries:** 20
- **Base delay:** 2 ms
- **Backoff:** full jitter — `sleep = random(0, base * 2^min(attempt, 3))`, capped at a 16 ms window to bound tail latency
- **Retry on:** `[applied] = false` (clean lost race) only
- **Do not retry on:** Cassandra errors (timeout, unavailable) — propagate immediately

## Consequences

- A `version bigint` column is added to all data tables via migration.
- `update_item_impl` and the conditional `put_item_impl` path use an OCC retry loop.
- `commit_put_or_update` in the transaction system adds `version = version + 1` to the COMMIT UPDATE.
- Under high contention (many concurrent writers to the same item), tail latency increases due to retries. The retry ceiling (20) and bounded backoff (max 16 ms window) prevent unbounded latency growth.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
