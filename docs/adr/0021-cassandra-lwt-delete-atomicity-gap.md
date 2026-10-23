# ADR-0021: Cassandra LWT Delete Atomicity Gap and Required Worker Pattern

- Status: Accepted
- Date: 2026-10-01
- Deciders: ExtendDB Cassandra plugin contributors

## Context

Cassandra's LWT (Lightweight Transaction) mechanism — `INSERT ... IF NOT EXISTS`,
`UPDATE ... IF ...`, `DELETE ... IF EXISTS` — uses Paxos under the hood. A
fundamental Cassandra constraint is that **LWT statements cannot appear inside a
logged batch**. Attempting to do so is rejected by the coordinator at runtime.

This creates an atomicity gap for any operation that must:

1. Delete (or update) rows that were created with LWT, **and**
2. Span more than one row or partition.

ADR-0012 documents the check-then-batch pattern for *creation* operations. That
pattern works because plain `INSERT` statements (without `IF NOT EXISTS`) can be
batched. The symmetric delete case is different: if a row was created with
`INSERT ... IF NOT EXISTS`, any subsequent plain `DELETE` on that row corrupts
Cassandra's `system.paxos` table (the LWT/non-LWT mixing problem). The delete
must therefore use `DELETE ... IF EXISTS`, which cannot be batched.

### Concrete examples

- `delete_user` — must cascade to `iam_users`, `iam_user_tags`, `access_keys`,
  `iam_policies`, and `iam_group_members`. Each table uses LWT for creation.
- `delete_group` — must cascade to `iam_groups` and `iam_group_members`.
- `delete_account` — must cascade to `accounts`, `backups_by_arn`,
  `backups_by_table`, `backups_by_account`, and `continuous_backups`.

### Current state (as of this ADR)

All of the above are implemented as sequential `apply_lwt` calls (one per row).
This is correct from a paxos-safety standpoint but **not atomic**: a process
crash between any two calls leaves orphaned rows in the catalog keyspace. Those
orphans are invisible to normal reads (the parent row is gone) but consume space
and can confuse low-level diagnostics.

## Decision

Accept the orphan risk in the short term. Record here that the Cassandra backend
requires a **worker/saga pattern** for all complex multi-row operations that
cannot be expressed as a single logged batch.

## What "worker/saga pattern" means here

A saga is a sequence of individually committed steps with a corresponding
compensating action for each step. For Cassandra, the practical shape is:

1. **Write intent first.** Before beginning a multi-step delete (or any
   multi-step mutation), write a durable "pending operation" record to a
   dedicated catalog table (e.g., `pending_ops`) using LWT. This record names
   the operation type and its arguments.

2. **Execute steps idempotently.** Each step uses `IF EXISTS` / `IF ...`
   so that re-running a step that already completed is a no-op.

3. **Mark complete.** Delete the `pending_ops` record (also with LWT) once all
   steps succeed.

4. **Background worker.** A dedicated worker (analogous to the TTL sweep worker
   in ADR-0010) periodically scans `pending_ops` for records older than a
   threshold and re-drives them to completion. This is the "nanny" that handles
   crash recovery.

This pattern requires one new worker per operation class (or a single generic
worker that dispatches by operation type). It is non-trivial but well-understood.

## Why this matters

Without this pattern, the Cassandra backend has a class of operations that are
**not crash-safe**. For an admin-facing management store where operations are
infrequent and the operator can manually clean up, this is tolerable. For
higher-frequency or user-facing operations it is not.

Any future work that adds multi-row LWT-managed operations to the Cassandra
backend **must** either:

- Express the entire operation as a single logged batch of plain statements
  (only possible if none of the rows involved use LWT for creation), or
- Implement the worker/saga pattern described above.

Adding a plain multi-row delete against LWT-managed rows is not an acceptable
shortcut — it causes `system.paxos` corruption that accumulates across
operations and eventually stalls the entire node (observed: 29-minute test
hangs, GC pauses of 4–11 seconds, `system.paxos` growing without bound).

## Consequences

- **Short term:** Cascading deletes (`delete_user`, `delete_group`,
  `delete_account`, etc.) are best-effort. Orphaned rows are possible on crash.
  Acceptable given operation frequency and operator visibility.

- **Medium term:** Implement `pending_ops` table and a background reconciliation
  worker before the Cassandra backend is considered production-ready for
  environments where crash-safe admin operations are required.

- **Long term:** All new multi-row operations on LWT-managed tables must go
  through the saga pattern. This should be enforced in code review.

## Related ADRs

- ADR-0012: Transaction and Atomicity Patterns (creation-side check-then-batch)
- ADR-0010: Cassandra TTL Expiration Queue (existing worker pattern to model on)

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
