# ADR-0006: Vector storage on PostgreSQL uses pgvector's type, and the engine decides what it cannot compute

- Status: Accepted
- Date: 2026-08-20
- Deciders: @yesyayen

## Context

The PostgreSQL backend had to serve the vector surface the engine already
defines: a `vector(N)` payload per indexed item, a `SearchVectors` operation with
a top-k ordered by one of three distance functions, and the build lifecycle
shared with the SQLite backend. The decisions below are the ones a reader will
otherwise have to reconstruct from the code, and three of them were reversed or
narrowed by measurement during the work.

The measurements referenced here were taken against Amazon DynamoDB (August
2026) and against pgvector 0.8.0 on PostgreSQL 15.18.

## Decision

**1. Embeddings are stored in pgvector's `vector(N)` column, not as `BYTEA`.**

The alternative was a byte-packed blob decoded in Rust, which is what the SQLite
backend does because SQLite has no vector type. On PostgreSQL that would put the
distance computation in the process rather than in the database, so every
candidate row would cross the wire for every search, and the index types pgvector
provides could never be used. The cost of the choice is that the arithmetic is no
longer ours (see decision 4).

**2. Vector support is detected at runtime and fails closed.**

Whether an ExtendDB build can serve vector indexes is a property of the
PostgreSQL server it is pointed at, not of the binary. The engine probes for the
extension once at startup and caches the answer, and a server without it refuses
every vector operation with a message naming the extension rather than failing
somewhere inside a query. The consequence, which is documented in the admin
guide: installing pgvector under a running server needs an ExtendDB restart,
because the probe is not re-run.

Failing closed rather than degrading is the same reasoning as the restore
refusal. A silent downgrade produces a table that looks like it has an index and
answers every search with nothing.

**3. Search is an exact scan first; the approximate index is a follow-up.**

ADR-0004 decided this for the SQLite backend and it holds here for a different
reason: correctness of the scan is a precondition for measuring recall against
it. An HNSW index changes which rows are considered, so landing it before the
exact path is verified would make a recall regression indistinguishable from a
scan defect. The partition predicate and the inline filters are evaluated in SQL
so that `LIMIT` applies after filtering rather than before.

**4. A score that PostgreSQL cannot compute is bounded in SQL, not in Rust.**

pgvector accumulates distances in single precision, so for vectors of ordinary
finite `f32` components the operators return values that cannot be serialised:
Euclidean overflows to infinity above about 9.2e18 (its difference is doubled
before squaring, so it goes first), dot product returns negative infinity above
about 1.8e19, and cosine returns NaN at both ends. Every one of those reaches a client as `"Score": null`
on a 200 response, because that is how a non-finite double serialises.

The scoring expression therefore bounds each metric at the end its own
accumulator overflows towards, and substitutes the measured zero-vector answer
for a cosine NaN.

The location is the decision, and it is decided by the cosine case alone. Being
specific matters, because the uniform version of this argument is false for two of
the three metrics.

The two magnitude bounds could equally run in Rust. `LEAST(x, 1e308)` is monotone
non-decreasing, and every finite value the operator can return is at most about
3.4e38, far below the bound, so the top-k set, its order and the reported values
are identical wherever the clamp is applied. The same holds for the floor on the
negated inner product, including after the score contract negates it. They are in
SQL for consistency with the third case, not because Rust would break them.

Cosine is different because its repair is a substitution rather than a clamp.
PostgreSQL sorts NaN as greater than every other value, so a NaN row sorts last
and may be cut by `LIMIT`. Reporting that row as 1.0 in Rust would place a value
of 1.0 after values of 2.0, and would keep or drop the wrong rows at the cut,
because both the order and the truncation were already decided by the unrepaired
value. So the substitution has to happen before the cut, which means in the
expression.

## Consequences

The bounds are not measured service answers, and the reported distance at the
underflow end is not the true one: with a query vector whose `f32` squares
underflow, pgvector's own norms collapse and the reported cosine distance takes
one of three values following the sign of the inner product. Ranking still
separates nearer-than-orthogonal from farther and loses resolution within each
half. Both facts are recorded in `docs/differences-from-dynamodb.md` rather than
left for a user to discover.

The SQLite backend does not need any of this. It owns its arithmetic, computes in
`f64`, and reports the true value across the whole domain, which is why the
differences row is scoped to PostgreSQL. Where the two backends can differ in a
reported number, the doc says so.

Removing the difference would mean computing distances outside pgvector's
operators, which costs a per-row unpack of every candidate and gives up any index
on the column. That trade is worse than the documented asymmetry for an input
class no real embedding produces.

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
