# ADR-0024: Read consistency reaches storage as a `ReadOptions` parameter

- Status: Accepted
- Date: 2026-10-25
- Deciders: @lhnng

## Context

A DynamoDB client sets `ConsistentRead` on `GetItem`, `Query`, `Scan` and,
per table, on `BatchGetItem`. Before this decision the engine read the flag
for capacity accounting (a strongly consistent read costs twice the read
capacity units) and dropped it; `DataEngine::get_item`, `query` and `scan`
had no parameter for it, so no backend could act on it.

Three in-tree backends sit on databases that keep more than one copy of the
data. A MongoDB replica set can serve a read from a secondary under a
`secondaryPreferred` read preference; a PostgreSQL deployment can serve a
read from a streaming replica; a Cassandra keyspace replicates every row
(replication factor 3 by default) and serves a read at the consistency level
the statement names. A follower copy lags the primary, and a Cassandra read
at `ONE` may miss a write another replica holds, which is the behaviour
`ConsistentRead: false` permits and `ConsistentRead: true` forbids. Without
the flag at the storage layer, a backend cannot tell the two reads apart,
and an application that depends on `ConsistentRead: true` cannot be shown
the difference between the two modes against ExtendDB.

The high availability design (`docs/design/11-high-availability.md`,
section 8 and Stage 1) proposed the plumbing as a bare `consistent_read:
bool` parameter with an internal `ConsistencyLevel` enum. The storage design
(`docs/design/04-component-storage.md`, section 5.3) describes PostgreSQL
read replica routing in terms of the same flag.

## Options Considered

1. A `consistent_read: bool` parameter on the three methods, as the high
   availability design proposed. One more positional `bool` on `scan`, which
   has six parameters already; every later per-read option (a projection
   hint, a request identifier for a slow-query log) widens the three
   signatures again and breaks every backend each time.
2. A `ReadOptions` struct with a `consistent_read` field, `#[non_exhaustive]`,
   `Default`, and a `const fn new(consistent_read: bool)`, passed by value as
   the last parameter. The `VectorSearch` parameter struct is the precedent.
   A field added later is read by a backend that wants it and ignored by one
   that does not; a backend outside this workspace builds the value through
   `new` or `default` and is not broken by the addition, as long as the added
   field is `Copy` and owns its data.
3. A `ConsistencyLevel` enum (`Eventually`, `Strong`) as the parameter. It
   names two states the service exposes as one boolean, and it does not carry
   a second option any better than a `bool` does.
4. No trait change; the handler publishes the flag in an ambient per-request
   context (a task-local) that a backend reads. A `BatchGetItem` needs a
   per-table value inside one request, which an ambient value cannot hold,
   and a backend author has to know the context exists to find it.

## Decision

Option 2. `extenddb_storage::ReadOptions { consistent_read: bool }` is
`#[non_exhaustive]` with `Default` and `const fn new`; `DataEngine::get_item`,
`query` and `scan` take it by value as their last parameter.
`transact_get_items` is unchanged: the service has no consistency flag there
and serves the read serializable.

The engine passes the client's value on `GetItem`, `Query` and `Scan`
(`false` when the member is absent), each table's own
`KeysAndAttributes.ConsistentRead` on `BatchGetItem`, and `true` on the
export scan, because an export reflects every write acknowledged before the
export time. A read through a global secondary index reaches storage with
`false`: the engine refuses `ConsistentRead: true` on a GSI before any
storage call, as the service does. A local secondary index read carries the
client's value.

The four in-tree backends accept the value and do not route on it. Each
parameter's doc comment states what that backend does today and what a
routing change would have to do.

## Rationale

- One parameter now, none later. The struct absorbs the next per-read option
  without a second signature change across every backend.
- A backend author reads a named field, not a second positional `bool` on
  `query` beside `forward`.
- `#[non_exhaustive]` keeps the addition of a field a non-breaking change for
  a backend outside the workspace, which is the case RFC-0002 anticipates as
  external backends grow.
- Passing by value costs nothing: the struct is `Copy` and one byte.

## Consequences

- This is a breaking change to `DataEngine` for any backend outside the
  workspace: three methods gain a required parameter. The migration is one
  line per method (`opts: ReadOptions` as the last parameter; a backend with
  one copy of the data ignores it). RFC-0002 records that the ExtendDB team
  maintains all backends today and that `Storage` trait changes land across
  all backends at once; the high availability design records that no
  out-of-tree implementation exists. This change lands across all four.
- Fields added to `ReadOptions` later must be `Copy` and own their data. A
  field that cannot be (a borrowed projection list) needs its own parameter
  or a major version of `extenddb-storage`.
- The high availability design's section 8 and Stage 1 now describe the
  shape that landed; Stage 2 (replica routing in the PostgreSQL backend)
  builds on it.
- The Cassandra backend serves `ConsistentRead: true` at the driver's
  default consistency `ONE`, and its OCC and put-if-absent lightweight
  transactions, plain delete and transaction commit run at `ONE` (the
  conditional delete and the batched secondary writes run at
  `LOCAL_QUORUM`), so the flag is not honoured there when the replication
  factor is above 1. That
  predates this decision and is tracked separately; the parameter is what a
  fix routes on.

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
