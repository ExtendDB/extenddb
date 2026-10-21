# ADR-0023: Cassandra Index Rows Carry the Base-Row Version as Their Write Timestamp

- Status: Accepted
- Date: 2026-10-21
- Deciders: ExtendDB Cassandra plugin contributors
- Amends: ADR-0020

## Context

Every write to a Cassandra data table commits the base row with a lightweight
transaction (ADR-0020) and then writes the item's secondary-index rows in a
separate logged batch. Cassandra does not allow a conditional statement in a
batch, so the two steps cannot be one. The index batch is an ordinary write:
each cell takes the batch's write timestamp, and when two writes touch one cell
Cassandra keeps the one with the larger timestamp, with a tombstone winning a
tie.

Three defects followed from leaving those timestamps to the coordinator.

1. Two writers to one item commit their base rows in Paxos order, but their
   index batches reach the index table in arrival order. When the later version
   deletes the index row the earlier version inserted, and the earlier batch
   arrives last, the deleted row comes back. `test_put_item_create_race` on the
   LSI table failed this way in CI (issue #414).
2. A write that keeps the index key emitted a DELETE and an INSERT of the same
   index row in one batch. Both carried the batch's timestamp, the tombstone won,
   and every `UpdateItem` of a non-key attribute, `PutItem` over an item with the
   same keys, and transactional Update of the same shape removed the item from
   all of its indexes.
3. Transaction COMMIT set `version = version + 1` under an `IF prepared_txn_id`
   condition. Cassandra 4.1 accepts that arithmetic on a non-counter `bigint`
   only inside a conditional update (the unconditional form is rejected), and
   on a row PREPARE had created, where `version` was null, it left `version`
   null. The ordinary write paths read a null as `0` and fenced on
   `IF version = 0`, which a null column never matches, so an item a
   transaction created could not be updated, overwritten or conditionally
   deleted until the OCC retry budget ran out. The transactional Delete also
   never removed the item's index rows, and a transactional Put over an existing
   item never removed the rows of the image it replaced.

## Decision

The `version` column becomes `max(previous + 1, now)` in microseconds since the
epoch, computed by the writer and bound as a value in every statement that
stores it. It is still the OCC token: the fence compares it for equality with
the value the writer read, and a row whose `version` is null fences on
`IF version = null`.

Every index-row batch that a base-row mutation produces is stamped with that
version: the post-LWT batch of `put_item` and `update_item`, the secondary
effects batch of `delete_item`, the transaction commit's index batch, and the
`gsi_pending` entry an asynchronous GSI queues, whose context carries the stamp
for the worker to apply. Within the batch, an index row whose primary key is
unchanged between the old and new image is rewritten by the INSERT alone; the
DELETE is emitted only when the row moves.

Ordinary deletes without TTL go through the same `IF version = ?` fence as
writes, so a delete and a concurrent write to one key commit in one order and
the index batches carry that order. A delete that loses the fence is retried
from a fresh read, as a lost write is.

## Rationale

- With timestamps that increase with the version, a later version's index
  mutation beats an earlier version's whatever order the batches arrive in, and
  a tie between a tombstone and a cell can only occur within one batch, which
  the same-row rule removes.
- `max(previous + 1, now)` keeps the value strictly increasing across the row's
  history by construction and keeps it in the domain of the timestamps Cassandra
  assigns to writes that carry none (index backfill, TTL expiration, rows written
  before this change), so those writes keep their relative order against it.
- Binding the version as a value gives a transaction-created row a version and
  lets the commit know the stamp for its index batch without a second read.
  Reading `version` as an optional value and binding `IF version = null` for a
  row that has none is what lets the ordinary write paths take over a row
  written before this change.
- The TTL-claimed write path keeps the mutation timestamp it pins after taking
  the claim as its batch timestamp, because the claim cells that batch clears
  must not land above a later claim's Paxos timestamp; its version is computed
  with that timestamp as the clock reading, so the stored version is never
  below the cells the batch wrote. On a TTL table the index rows are therefore
  ordered by the pinned wall-clock timestamps, and the claim is what serializes
  the writers; a clock that steps backwards between two claimed writes can
  leave an older index row, the same class as the delete-then-create case
  below.
- Recovery of a COMMITTING transaction stamps its index batch with the version
  it stored and syncs only the rows whose commit fence it applied itself. A row
  the commit already wrote before the crash, or that a later writer has since
  taken, is left to that writer: its index batch carries a higher stamp, and
  re-syncing the ledger's image would place stale rows above it.

## Consequences

- A delete without a condition on a table without TTL costs a Paxos round it did
  not before. A delete of an absent item writes nothing.
- A row created before this change carries a small counter or null in
  `version`; its next write moves it into the microsecond domain. Index rows
  written before this change carry coordinator timestamps, which the next
  write's stamp exceeds. During a rolling upgrade a node still running the old
  binary accepts the microsecond versions but keeps dropping the index rows of
  the items it writes (defect 2 above); the next write from an upgraded node
  restores them.
- The index rows a later creator writes after a delete are ordered against the
  delete's tombstones by the clock, as they were before: the deleted row leaves
  no version behind. A clock that runs backwards between the delete and the
  create by more than the gap between them can shadow the new rows, and so can
  a create in the same microsecond as a delete whose image was itself created
  in that microsecond (the tombstone is stamped one above). Each write is a
  Paxos round trip, so neither case was produced in 200 back-to-back
  delete-and-create iterations over the wire; a later write to the item
  rewrites the row.
- Recovery of a COMMITTING transaction syncs the index rows from the two images
  the ledger holds, which it did not do before. A ledger written before the
  pre-image field existed carries no old image, so its recovery is insert-only
  and a key the transaction moved leaves its old index row behind; such ledgers
  exist only for transactions in flight across the upgrade.
- The asynchronous GSI worker applies entries in version order on the index
  table. Under `extenddb serve` the workers still stop at startup (issue #407);
  the worker path is exercised by the crate's direct integration tests, which
  hold the worker guard.

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
