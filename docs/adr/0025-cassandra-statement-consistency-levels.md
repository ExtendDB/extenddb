# ADR-0025: Cassandra: every data-path statement states its consistency level

- Status: Accepted
- Date: 2026-10-26
- Deciders: @LeeroyHannigan

## Context

The Cassandra driver runs a statement with no stated level at `ONE`: one
replica acknowledges a write, one replica answers a read. Before this decision
the data path (`crates/storage-cassandra/src/data/`) ran 105 statements that
way: the reads behind GetItem, Query and Scan, the reads whose result a
following write conditions on, the OCC and put-if-absent lightweight
transactions (their Paxos round at the server default `SERIAL`, their commit
at `ONE`), the plain delete, the transaction prepare and commit statements and
the transaction ledger. The conditional delete, the TTL claims and the
secondary-effects batches already ran at `LOCAL_QUORUM`.

Each keyspace the backend creates replicates with `NetworkTopologyStrategy`
to the one datacenter named by `datacenter`, with `replication_factor` 3 by
default. A read at `ONE` against a write acknowledged at `ONE` can answer
from a replica the write has not reached, so `ConsistentRead: true` was not
honoured above replication factor 1 (issue #560). ADR-0024 carries the flag
to the backend as `ReadOptions::consistent_read`; this decision is what the
Cassandra backend does with it, and with every other statement it runs.

## Options Considered

1. Raise the read level alone: `true` reads at `QUORUM` or `LOCAL_QUORUM`,
   writes unchanged at `ONE`. A quorum read and a one-replica write do not
   intersect, so the read can still miss the write. Rejected by issue #560
   and here.
2. `SERIAL` reads for `true`. A serial read runs a Paxos round per read; it
   answers the same question as a quorum read at several times the cost and
   only on tables whose writes are all lightweight transactions. Rejected.
3. `QUORUM` and `SERIAL` for reads, writes and Paxos. The keyspace lives in
   one datacenter, where `QUORUM` and `LOCAL_QUORUM` count the same replicas;
   the `LOCAL_` forms keep the contract per datacenter if an operator ever
   replicates a keyspace to a second one. Rejected in favour of the local
   forms.
4. Reads at the level the request names, every write at `LOCAL_QUORUM`,
   every lightweight transaction at `LOCAL_QUORUM` with `LOCAL_SERIAL`, and
   a lint that refuses a statement at the driver default. Chosen.

For the guard: per-call `#[allow]` outside `data/` on each of the 225 calls
that run at the default there (catalog, metadata, backup, bootstrap) was
rejected for one crate-level allow with the lint turned back on for `data/`.

## Decision

Every statement under `crates/storage-cassandra/src/data/` states its level
through one of four constructors in `cassandra_util`:

- `read_params(consistent_read, values)`: `LOCAL_QUORUM` for `true`,
  `LOCAL_ONE` for `false`. GetItem, Query and Scan pass the request's flag.
  A read whose result decides a write (the OCC reads before put, update and
  delete, the transaction reads, the idempotency token reads, the ledger
  reads the recovery worker acts on) and a table walk (the export scan, the
  metadata walks, the global secondary index backfill) pass `true`. Catalog
  and queue metadata reads pass `false`.
- `write_params(values)`: `LOCAL_QUORUM`, for every statement without an
  `IF` clause.
- `lwt_params(values)`: `LOCAL_QUORUM` with serial `LOCAL_SERIAL`, for every
  statement with an `IF` clause.
- `batch_builder()`: a `BatchQueryBuilder` at `LOCAL_QUORUM`, for every
  batch.

The four schema statements in `ddl.rs` stay at the driver default, each with
a statement-level allow. `clippy.toml` disallows `Session::query`,
`query_with_values`, `exec`, `exec_with_values` and `paged`,
`StatementParamsBuilder::new`, `BatchQueryBuilder::new` and the four
`cassandra_util` helpers that run at the default; the crate allows the lint
outside `data/` and `data/mod.rs` turns it back on. A test in
`data/level_pairing_tests.rs` reads the statement sites in `data/` whose CQL
and level it can resolve (121 sites on the tree this record describes) and
checks that a `SELECT` carries a read level, a plain write `write_params` and
an `IF` statement `lwt_params`, and that no `BatchQueryBuilder::new`,
`BatchQueryBuilder::default`, `Default::default()` or `.with_consistency(`
appears under `data/`, so a batch is built by `batch_builder()` or not at all;
the lint cannot see which constructor a call passes.

## Rationale

With base-row writes acknowledged by a local quorum, a `LOCAL_QUORUM` read
reaches a majority of the same `replication_factor` replicas the write
reached, and two majorities of one replica set share a replica, so a `true`
read reflects every write acknowledged before it started. `LOCAL_ONE` may
miss a recently acknowledged write, which is the contract for `false`. The
reads that decide a write take the quorum level because the condition is
evaluated against that image before the lightweight transaction runs; a
stale image would make the transaction apply against the wrong state. A
Paxos round at `LOCAL_SERIAL` and a commit at `LOCAL_QUORUM` keep a
lightweight transaction's linearizability within the datacenter.

## Consequences

- A `ConsistentRead: true` read, a plain write and a lightweight transaction
  need `replication_factor / 2 + 1` replicas up in `datacenter`: two of three
  at the default. At replication factor 1 every statement needs the one
  replica, as before. The lightweight transactions already needed a Paxos
  majority; their commit now waits for a local quorum instead of one replica.
  A `ConsistentRead: false` read needs one replica.
- A request that cannot reach its level fails as a storage fault (HTTP 500
  `InternalServerError`; the server log carries the driver's text). The
  driver sends statements to every node it discovers, so a coordinator in a
  datacenter that holds no replica of the keyspace refuses `LOCAL_QUORUM` and
  `LOCAL_ONE` statements; ExtendDB is run against nodes in `datacenter`.
  Routing coordinators to `datacenter` and classifying the driver's
  unavailable error as transient are tracked separately.
- Reads of catalog and queue metadata stay at one replica; the four of them
  that feed a write or a cleanup are tracked separately.
- Before this decision the reads, the OCC and put-if-absent lightweight
  transactions, the plain delete and the transaction statements ran at the
  driver default `ONE`; the conditional delete, the TTL claims and the
  secondary-effects batches already ran at `LOCAL_QUORUM`.
- ADR-0024's Consequences, written before this decision, describe the
  Cassandra backend as serving `true` at `ONE`; that sentence is history, and
  this record is the current statement.

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License,
Version 2.0. See [LICENSE](../../LICENSE) for the full text.
