# ADR-0004: Vector search is an exact scan over one row per vector

- Status: Accepted
- Date: 2026-08-06
- Deciders: @LeeroyHannigan

## Context

DynamoDB vector indexes let a table be searched by vector similarity. Supporting
them means two separate decisions: how a backend finds nearest neighbours, and how
the vectors are stored.

Constraints that narrow the field, all of them properties of this project rather
than preferences:

- **The SQLite deployment target is a statically linked musl binary in a
  `FROM scratch` container** (`docs/design/01-requirements.md`, REQ-DEPLOY-006). A
  static binary cannot `dlopen`, so a runtime-loadable SQLite extension is
  unusable there.
- **The project is Apache-2.0.** A dependency under a non-OSI source-available
  licence cannot be carried.
- **The index must live in the database file.** The SQLite backend has single-file
  persistent and `:memory:` ephemeral modes and an existing backup and restore
  path; an index in a sidecar file would not be captured by any of them.
- **Vector search is partition-scoped.** A search supplies an equality on the
  index's `HASH` element, so the candidate set is normally one partition rather
  than the table. The `HASH` element is optional, though, so an unscoped index
  spans everything.
- **Three distance functions are required**: `COSINE`, `EUCLIDEAN` and
  `DOT_PRODUCT`, because the service supports all three.

## Options Considered

1. **`sqlite-vec`** (asg017) — dual Apache-2.0/MIT, index in in-database shadow
   tables, native partition-key columns that pre-filter. The closest fit.
2. **`sqlite-vss`** (asg017) — Faiss-backed, real ANN.
3. **`sqliteai/sqlite-vector`** — SIMD, works on existing table schemas.
4. **`vectorlite`** — real HNSW via hnswlib.
5. **`usearch`'s SQLite extension** — from the usearch project.
6. **`sqlite-muninn`** — HNSW, loadable extension.
7. **libSQL / Turso native vector search** — LM-DiskANN built into a SQLite fork.
8. **A Rust index crate persisted in the database** — `hnsw_rs`,
   `instant-distance`, `usearch` as a library, `arroy`, `hannoy`.
9. **An exact scan written in Rust**, with vectors stored as ordinary rows.

## Decision

Exact scan in Rust, with one row per vector in a per-index data table. No
third-party vector dependency.

## Rationale

- **Every extension is eliminated by a hard constraint, not by preference.**
  `sqlite-vss` is deprecated by its own maintainer, ships only as a loadable object
  with BLAS/LAPACK/OpenMP, and supports no filtering on a KNN query.
  `sqliteai/sqlite-vector` is Elastic License 2.0, which is not OSI-approved and
  carries a managed-service restriction, and it has no Rust binding.
  `vectorlite` holds its HNSW index **in memory with a sidecar `.bin` file**, does
  not support transactions, and filters by rowid only. `usearch`'s SQLite surface
  is distance functions with no index at all; its HNSW exists only in the library.
  `sqlite-muninn` is four stars with no tagged release. libSQL has a genuine
  in-file LM-DiskANN index but would require replacing sqlx entirely, offers no
  dot-product metric, cannot pre-filter a partition (its `vector_top_k` takes only
  index, vector and k, so scoping is a post-filter that can under-return), is in
  maintenance mode, and has an open index-corruption-on-delete bug.

- **The one viable extension would not have bought an index.** `sqlite-vec` is
  brute force in every stable release; its ANN work exists only in a `v0.1.10`
  alpha line that had a DELETE data-loss bug. Adopting it meant taking on a C
  toolchain, an unverified static-musl build and a pre-1.0 dependency in exchange
  for a constant-factor speedup on the same asymptotic scan. It also has no
  dot-product metric, and dot-product ranking cannot be recovered from an L2
  top-k, because the ordering depends on each candidate's own norm.

- **Row per vector, because a packed blob makes writes O(partition).** Measured:
  a contiguous blob per partition reads 2 to 4x faster at 256 dimensions and 1.3
  to 2.5x at 1024, and is *slower* at 4096. But inserting or deleting one vector
  rewrites the whole blob, which is 390 MB for a 100k-vector partition at 1024
  dimensions, and vector indexes are maintained on every write touching an indexed
  attribute. No read gain justifies that.

- **Row per vector also inherits the existing machinery.** It streams, so a scan
  allocates nothing proportional to the partition; it has no per-partition blob
  ceiling; and it follows the per-index data table pattern the GSI and LSI paths
  already use, so it reuses their transaction, backup and cleanup behaviour.

## Measured evidence

Single core, warm page cache, release build, real SQLite via sqlx with WAL.
Harness and raw output recorded with the benchmark; summary:

| dimensions | vectors/sec | inside 10 ms | inside 100 ms |
|---|---|---|---|
| 256 | 213k to 334k | ~2,100 to 3,300 | ~21k to 33k |
| 1024 | 94k to 103k | ~940 to 1,030 | ~9.4k to 10.3k |
| 4096 | 39k to 43k | ~390 to 430 | ~3.9k to 4.3k |

Two findings from the benchmark matter more than the headline numbers.

**An earlier modelled estimate was wrong by 10 to 30x.** It assumed `N x D x 4`
bytes streaming at ~15 GB/s and predicted ~36,000 vectors at 1024 dimensions
inside 10 ms. Measured effective throughput is 0.2 to 1.2 GB/s, so the scan is not
memory-bandwidth bound and the model was unusable. Any figure derived from it is
void.

**The cost is the SQLite read path, not the arithmetic.** A variant that
reinterpreted the stored blob as `&[f32]`, so the dot product could vectorise
instead of decoding each element, measured **no faster** (1024d/100k: 766 ms
packed versus 850 ms zero-copy). The time goes on overflow-page assembly and the
copy sqlx must make. Optimising the distance loop would be wasted effort until
that changes.

## Consequences

**Easier.** No third-party vector dependency, so no licence question, no C or C++
toolchain, no static-musl uncertainty, and nothing pre-1.0 in the dependency tree.
This was the only option with zero unconfirmed constraints, precisely because
there is nothing external to be uncertain about. All three distance functions cost
the same to support: they are the same loop over the same bytes.

**Harder.** Search cost is linear in partition size, and the crossover is roughly
an order of magnitude lower than the earlier estimate claimed. A partition stays
inside a 10 ms budget up to about 1,000 vectors at 1024 dimensions and 100 ms up
to about 10,000.

**The trigger for revisiting, stated as a measurement rather than a judgement.** A
vector index declared with no `HASH` element searches the whole table, so its
corpus is not bounded by a partition. Past roughly the figures above, that leaves
an interactive budget. If that case becomes real, the answer is an index crate
persisted inside the database file, and on the research the candidates are
`usearch` (verified `save_to_buffer`, `remove` and `filtered_search`, but a C++17
core to check against static musl) with `hnsw_rs` as the pure-Rust fallback. Not
now, and gated on a measured need rather than on anticipation.

**Not measured.** Cold cache, concurrency beyond one core, and real embedding
distributions. Cold reads can only be worse; the other two do not change the
layout decision.
