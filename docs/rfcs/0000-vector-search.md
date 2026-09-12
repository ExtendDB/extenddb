# RFC-NNNN: Vector search support across storage backends

- Status: Draft
- Author: @LeeroyHannigan
- Created: 2026-08-05
- FCP ends: (set when entering Final Comment Period)
- Tracking issue: #NNN

## Summary

DynamoDB became generally available with native vector search on 2026-08-05. It
adds a new index type created over an attribute holding an embedding, and a new
`SearchVectors` operation that returns the nearest neighbours to a query vector,
ranked by a distance function, with optional exact-match filtering. This RFC
proposes adding that surface to ExtendDB.

The proposal is deliberately split in two. The wire surface, validation,
consistency model and index lifecycle are defined once in the engine and are
identical for every backend. Approximate nearest neighbour (ANN) execution is
declared as a per-backend capability, because the underlying stores differ
sharply in what they can do, and requiring ANN everywhere would either block
backends from being accepted or force us to ship something that silently returns
wrong results.

## Motivation

ExtendDB's value is that an application written against DynamoDB runs unchanged
against a backend the operator chooses. Vector search is not a peripheral
addition to DynamoDB's surface: it changes what a table is for. The launch
targets agentic memory, retrieval augmented generation, recommendations and
anomaly detection, and the pitch is explicitly that you no longer copy
operational data into a separate vector store.

That has a direct consequence for us. An application that adopts vector search
in DynamoDB and then points at ExtendDB does not degrade gracefully: it fails at
`CreateTable` on an unknown index type, or at `SearchVectors` on an unknown
operation. Local development, CI, and any offline or edge deployment that
depends on ExtendDB stops being able to exercise the code path at all. The gap
is not a missing optimisation, it is a missing feature that silently pushes
users back onto a real endpoint for the one workload they most want to test
cheaply and repeatedly.

Evidence the surface is real and stable enough to target: it is GA in all
commercial Regions plus GovCloud (US), it is documented in the DynamoDB
Developer Guide, and it is exposed in the console, CLI, SDKs and
CloudFormation. This is not a preview we would be chasing.

## Detailed design

### Data model: no new attribute type

Embeddings are stored in the existing `L` (list) type, where each element is an
`N` (number) holding one float. There is no new attribute type and no schema
change to store a vector.

This is the single most important fact for our implementation, and it is good
news. The `AttributeValue` codec, the item serialisation path, the 400KB item
limit check, and every existing validation rule stay exactly as they are. A
vector is an ordinary attribute that an index happens to interpret numerically.
Writes go through unchanged `PutItem` and `UpdateItem`; there is no vector write
path to add.

One interaction worth stating explicitly: a 4096 dimension embedding is 4096
numbers inside one item, so vectors consume the existing 400KB item budget. The
existing limit check applies unchanged and we should not special case it.

### Index configuration

A vector index is created on a table and carries:

- the vector attribute name
- the number of dimensions, up to 4096
- a distance function: `Euclidean`, `Cosine`, or `DotProduct`
- an optional partition key attribute
- optional inline filter attributes
- an attribute projection

The partition key deserves attention because it changes query semantics rather
than only physical layout. When a vector index declares a partition key, each
search is scoped to a single value of it. A search is therefore not global over
the index; it is nearest neighbours within one partition. Our implementation
must treat the partition key as part of the query contract, not as a
distribution hint we are free to ignore.

Inline filters accept exact-match conditions only. Range conditions such as
`BETWEEN` and `BEGINS_WITH` are not supported and must be rejected. We should
reject them with the same error class DynamoDB uses rather than silently
evaluating them, since an accepted-but-ignored filter is the worst outcome for a
caller.

### `SearchVectors`

The operation takes a query vector, a result count (top K, maximum 100), and
optional filter conditions, and returns items ranked by similarity score
alongside their projected attributes.

**Score semantics are asymmetric and must be exact.** For `Cosine` and
`Euclidean`, a lower score means more similar, and 0 means identical. For
`DotProduct`, a higher score means more similar. The score is a distance for two
of the three functions and a similarity for the third.

This is a parity trap worth calling out in the design rather than discovering in
implementation. The natural instinct is to normalise everything into a single
"similarity" number so ordering is uniform, and doing so inverts the meaning for
two of the three functions. I have already seen this exact defect in a
first-party client library against the real service, where a distance was
surfaced as a similarity and the ranking silently reversed. Our conformance
tests should assert the sign convention per distance function directly, not just
assert that the expected item set comes back.

### Consistency model: reuse the GSI path

A vector index is eventually consistent, the same model as a global secondary
index. A write is acknowledged before the index reflects it.

We already have machinery for exactly this. The asynchronous index maintenance
path, the pending index queue, and the `gsi_propagation_delay_ms` runtime
setting exist to model GSI propagation, including crash safety across restarts.
The proposal is to extend that path to carry vector index maintenance rather
than to introduce a second, parallel asynchronous mechanism. One queue, one
recovery story, one setting operators already understand.

Two lifecycle requirements follow, and the second is a bug we should prevent by
design rather than fix later:

1. Index creation is asynchronous and the index reports `CREATING` until the
   backfill over existing items completes, then `ACTIVE`.
2. The index must not report `ACTIVE` while its backfill is still running. A
   client that waits for `ACTIVE` and then searches must not observe a partially
   populated index. Any status transition must be causally ordered after the
   backfill drains, not scheduled on a wall clock alongside it.

### Backend capability variance

This is the crux of the RFC. Vector similarity execution is not uniformly
available across the stores we support or intend to support:

- **PostgreSQL** has `pgvector`, with HNSW and IVFFlat, if the operator installs
  the extension. Without it, no ANN.
- **SQLite** has `sqlite-vec` as a loadable extension, otherwise nothing.
- **MongoDB** has vector search in Atlas, not in community `mongod`, so a
  self-hosted deployment has no ANN operator available.

Two of those depend on an extension the operator may not have installed, and one
depends on a managed offering rather than the engine itself. Mandating ANN would
mean either rejecting backends that cannot provide it or shipping a fallback
while claiming ANN performance.

The proposal is a declared capability with three levels, surfaced through the
existing backend declaration:

- **`Ann`**: the backend executes nearest neighbour search using a real vector
  index. Sub-linear, and the only level that should be described as vector
  search in performance terms.
- **`ExactScan`**: the backend computes distances by brute force over the
  candidate set, applying the partition key scope and inline filters first to
  narrow it. Correct and fully conformant on results, linear in candidates.
  Appropriate for local development and CI, which is a large fraction of how
  ExtendDB is actually used, and explicitly not appropriate for production
  vector workloads.
- **`Unsupported`**: the backend rejects vector index creation with a typed
  error at `CreateTable`, so the failure is immediate and legible rather than
  surfacing later as an empty or wrong result set.

`ExactScan` is not a consolation prize. For the local-development and
integration-test use case it is indistinguishable from ANN in observable
behaviour and is in fact *more* correct, because it has perfect recall. The
distinction is throughput and dataset size, and it should be documented in those
terms.

This bears on the backend acceptance criteria under discussion separately.
Recommendation: vector search should be **optional** for backend acceptance. A
backend that declares `Unsupported` should remain acceptable, provided it does
so explicitly and fails closed.

### Testing: recall is not assertable by equality

ANN is approximate by construction, so a conformance test cannot assert an exact
result ordering against the real service and expect it to hold. This changes how
we test:

- `ExactScan` is the oracle. It has perfect recall, so its output is the ground
  truth for a given dataset, distance function and filter set.
- ANN backends are asserted against a **recall threshold** relative to that
  oracle, not against exact equality. The service targets 99%+ recall; we should
  pick a threshold and state it rather than leave it implied.
- Sign convention, top K truncation, partition key scoping, filter rejection and
  index lifecycle are all deterministic and should be asserted exactly. Only
  neighbour selection is probabilistic.

### Limits to enforce for parity

- dimensions at most 4096
- top K at most 100
- inline filter conditions exact-match only
- search scoped to exactly one partition key value when the index declares one

### Implementation sketch

1. `crates/core`: index configuration and `SearchVectors` request and response
   types, dimension and top K validation, distance function enum.
2. `crates/storage`: extend `TableEngine` for vector index create, describe and
   delete; add the search entry point to `DataEngine`; add the capability
   declaration to `Backend`. This is the breaking trait change and should land
   first and alone, so backends adapt against a stable signature.
3. `crates/engine`: the `SearchVectors` handler, authorization, validation, and
   the `ExactScan` reference implementation, which is backend-agnostic and can
   live above the storage layer where the candidate set is already available.
4. `crates/storage-postgres`: `pgvector` detection at startup, capability
   declaration derived from what is actually installed rather than assumed, ANN
   execution, and vector index maintenance folded into the existing pending
   index queue.
5. Conformance tests: the oracle harness, sign convention assertions per
   distance function, lifecycle assertions, and the recall comparison.

Ordering matters here. The trait change in step 2 conflicts with any in-flight
backend work, so it should be sequenced deliberately rather than landing
alongside a backend PR.

## Drawbacks

- **Extension dependency.** The only credible ANN path for our reference backend
  is `pgvector`, which the operator must install. That is a new external
  dependency in a project that has so far kept its backend requirements to a
  stock database, and it makes capability depend on deployment rather than on
  our build.
- **A correct-but-slow mode invites misreading.** `ExactScan` will be measured
  by somebody as though it were ANN, and we will be compared unfavourably. The
  mitigation is documentation and an explicit capability in `DescribeTable`, not
  code.
- **Recall testing is genuinely harder** than the equality-based conformance
  testing the project relies on everywhere else, and it introduces the first
  probabilistic assertion into the suite.
- **Trait churn.** This touches `TableEngine`, `DataEngine` and `Backend` at a
  time when several backends are mid-flight.
- **Surface area.** Vector search brings its own validation matrix, its own
  index lifecycle and its own error taxonomy, all of which we then owe parity on
  indefinitely.

## Alternatives

- **Do nothing.** Applications adopting vector search cannot use ExtendDB for
  the workload at all. This is the status quo and it degrades over time as
  adoption grows.
- **Mandate ANN for every backend.** Blocks MongoDB on community `mongod`
  outright and makes PostgreSQL support conditional on an operator installing an
  extension. Rejected as it converts a feature gap into a backend gap.
- **`ExactScan` only, no ANN anywhere.** Simpler, no extension dependency, and
  sufficient for local development. Rejected because it forecloses the
  production use case permanently and misrepresents what the feature is.
- **A new attribute type for vectors.** Rejected because it diverges from the
  service, which reuses `L` of `N`. Divergence here would break the codec
  contract for no gain.
- **Synchronous index maintenance.** Simpler to reason about and would make
  tests deterministic, but it is the wrong consistency model. It would let code
  pass against ExtendDB that then fails against the real service, which is the
  one failure mode this project exists to prevent.
- **A sidecar vector process.** Rejected: it reintroduces exactly the separate
  vector store and synchronisation pipeline the feature exists to remove.

## Unresolved questions

Several fine-grained behaviours are not stated in the public launch material and
I do not want to guess at them in a design document. Each needs verifying
against the real service before the corresponding code is written, and I am
happy to run these:

1. What error does a `PutItem` receive when the vector attribute's length does
   not match the index's declared dimensions? Is the write rejected, or accepted
   and skipped by the index?
2. What happens when an item simply lacks the vector attribute? Presumably it is
   absent from the index, matching GSI sparse-index behaviour, but this should be
   confirmed rather than assumed.
3. Can a vector index be created on a table that is not `ACTIVE`, and can more
   than one be created concurrently?
4. Is there a per-table limit on vector indexes, as GSIs have?
5. Exact error codes and messages for exceeding 4096 dimensions, exceeding top K
   of 100, and supplying a range condition as an inline filter.

Design questions for reviewers specifically:

6. Should vector search be optional for backend acceptance? I propose yes.
7. What recall threshold should ANN backends be held to in conformance, and
   should it be a hard gate or a reported metric?
8. Should `ExactScan` be allowed in a non-development build at all, or should it
   be gated so it cannot be reached in a production configuration by accident?

## Prior art

- **DynamoDB vector search**, the compatibility target: a new index type over an
  ordinary list attribute, `SearchVectors`, three distance functions, inline
  exact-match filters, partition-key-scoped search, eventual consistency.
  Notably it does not introduce a vector data type, which is the design decision
  we are copying most directly.
- **pgvector**: HNSW and IVFFlat over PostgreSQL. Demonstrates that a relational
  engine can host ANN credibly, and is the most likely execution path for our
  reference backend.
- **sqlite-vec**: brute force and ANN in a loadable SQLite extension. Relevant
  as the embedded analogue.
- **MongoDB Atlas Vector Search**: instructive as a negative example for us. The
  capability exists in the managed product but not in the community engine, which
  is precisely the situation that motivates a declared capability rather than an
  assumed one.
- **OpenSearch and MemoryDB** both expose ANN with explicit index build
  parameters. Their surface is more tunable than DynamoDB's deliberately opaque
  one, which is a reminder that our job is to match DynamoDB's abstraction level
  and not to expose the knobs of whatever engine we happen to sit on.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
