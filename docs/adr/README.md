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
