# Architecture Decision Records

ADRs record decisions that have been made. They are short, immutable once
accepted, and numbered sequentially.

## When to write an ADR

Write an ADR when you make a decision that:

- A future contributor would otherwise have to reverse-engineer from code
- Has tradeoffs that should be visible (rejected options, constraints)
- Affects more than one component or cuts across crates

ADRs are *records*, not proposals. If you are still soliciting input, write an
[RFC](../rfcs/README.md) instead.

## Process

1. Copy `0000-template.md` to `NNNN-short-title.md`, where `NNNN` is the next
   unused number.
2. Fill in Context, Options Considered, Decision, Rationale, Consequences.
3. Open a PR. Discussion happens inline.
4. On merge, the ADR is Accepted. Subsequent decisions that override it should
   create a new ADR and update the original's Status to "Superseded by ADR-NNNN".

ADRs are never edited after acceptance except to update Status. To change a
decision, write a new ADR.

## Index

| # | Title | Status |
|---|-------|--------|
| [0001](0001-documentation-format.md) | Documentation format — Markdown over LaTeX | Accepted |
| [0002](0002-sql-injection-defense.md) | SQL injection defense | Accepted |
| [0003](0003-catalog-migration-mechanism.md) | Adopt sqlx::migrate for PostgreSQL catalog and data schema migrations | Accepted |
| [0004](0004-vector-search-exact-scan.md) | Vector search is an exact scan over one row per vector | Accepted |
| [0005](0005-index-build-lifecycle-ownership.md) | Index-build lifecycle stays in the backend until a second backend needs it | Accepted |
| [0006](0006-pgvector-storage-and-scoring.md) | Vector storage on PostgreSQL uses pgvector's type, and the engine decides what it cannot compute | Accepted |
| [0010](0010-cassandra-ttl-expiration-queue.md) | Durable sharded expiration queue for Cassandra TTL | Accepted |