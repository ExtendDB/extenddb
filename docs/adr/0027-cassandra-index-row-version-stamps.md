# ADR-0027: Cassandra Index Rows Carry the Base-Row Version as Their Write Timestamp

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
epoch, with `now` read from the ExtendDB host's clock, computed by the writer
and bound as a value in every statement that stores it. It is still the OCC
token: the fence compares it for equality with the value the writer read, and a
row whose `version` is null fences on `IF version = null`. A write to a row
whose `version` is the largest `bigint` is refused, because its next version
would equal the current one.

Every index row that a base-row mutation writes carries that version as its
write timestamp: the index statements of the post-LWT batch of `put_item` and
`update_item`, of the secondary-effects batch of `delete_item`, and of the
transaction commit's batch each name it with `USING TIMESTAMP`, and the
`gsi_pending` entry an asynchronous GSI queues carries it in its context for the
worker to apply. The queue rows and stream records in the same batches keep
Cassandra's timestamp. Within a batch, an index row whose primary key is
unchanged between the old and new image is rewritten by the INSERT alone; the
DELETE is emitted only when the row moves.

Ordinary deletes without TTL go through the same `IF version = ?` fence as
writes, so a delete and a concurrent write to one key commit in one order. A
delete that loses the fence is retried from a fresh read, as a lost write is.
A delete stores no version, and its index tombstones carry the deleted row's
version plus one, with no clock reading. A row whose `version` is null or a
counter from before this change (below 10^15, which is 2001-09-09 in
microseconds) keeps `max(previous + 1, now)`, because its index rows carry
Cassandra's timestamps. The transactional Delete and the recovery of one
follow the same rule.

A GSI backfill gives each index row it writes the base row's version as its
write timestamp, and Cassandra's timestamp for a row with a null or counter
version.

The transaction commit reads the row it holds under its PREPARE fence at
`LOCAL_SERIAL`, binds a concrete version, and syncs index rows for Put, Update
and Delete. An unconditional delete whose read finds no item reads again at
`LOCAL_SERIAL` before it answers without writing, and retries if that read
finds an item a write stored before the delete began.

Recovery of a COMMITTING transaction syncs index rows from the ledger's two
images, stamped with the row's version, for each Put, Update or Delete whose
commit fence it applies itself, and for each Put or Update whose live commit
had already landed: the row then holds this transaction's commit timestamp and
the ledger's image.

## Rationale

- With timestamps that increase with the version, a later version's index
  mutation beats an earlier version's whatever order the batches arrive in, and
  a tie between a tombstone and a cell can only occur within one batch, which
  the same-row rule removes.
- `max(previous + 1, now)` keeps the value strictly increasing across the row's
  history by construction and keeps it in the domain of the timestamps Cassandra
  assigns to writes that carry none (TTL expiration, rows written before this
  change), so those writes keep their relative order against it.
- A writer that creates an item reads its clock before its LWT, and a delete of
  the item can commit between that reading and the LWT. A tombstone stamped
  with a clock reading taken after the delete's fence could then sit above the
  creator's stamp and hide the new item's index rows, whichever index the two
  share. On one Cassandra 4.1 node, a delete racing an `attribute_not_exists`
  create with the same key and index key hid the new item in 71 of 200 rounds
  through `DeleteItem` and 77 of 200 through a `TransactWriteItems` Delete, and
  in 0 of 200 for each once the tombstones carried the version plus one. The deleted
  row's version plus one is above every index row that image wrote, and below
  the clock reading of any creator that started after the deleted image was
  written.
- A row with a null or counter version was last written before this change, so
  its index rows carry Cassandra's timestamps, which the version plus one does
  not exceed; those deletes keep the clock reading.
- A backfill runs after the items it indexes were written. With Cassandra's
  timestamps its rows sat above the items' versions, and a delete's
  version-plus-one tombstone would leave them in the index. Stamped with the
  version, a backfilled row is also superseded by any later write of the item,
  whichever of the two reaches the index table first.
- The GSI worker deletes a queue entry it has applied with Cassandra's
  timestamp. An entry written with a version ahead of Cassandra's clock (a host
  clock ahead of the nodes', or a version carried forward from such a host)
  would outlive that delete and be applied on every poll, so the queue rows and
  the stream records keep Cassandra's timestamp.
- Binding the version as a value gives a transaction-created row a version and
  lets the commit know the stamp for its index batch without a second read.
  Reading `version` as an optional value and binding `IF version = null` for a
  row that has none is what lets the ordinary write paths take over a row
  written before this change.
- A read at the driver's default `ONE` can reach a replica that missed the
  latest write. The commit phase uses its read as the old image whose index
  rows it removes and as the version it advances, and its own fence does not
  check either, so it reads at `LOCAL_SERIAL`. The unconditional delete's fence
  catches a stale image but not a stale absence, so the delete confirms an
  absence at `LOCAL_SERIAL`. An item that read finds with a version taken
  after the delete began belongs to a write that started after the delete,
  which the delete is ordered before. Retrying into it deleted a concurrent
  create's item and widened the three-writer case below.
- The TTL-claimed write path keeps the mutation timestamp it pins after taking
  the claim as its batch timestamp, because the claim cells that batch clears
  must not land above a later claim's Paxos timestamp; its version is computed
  with that timestamp as the clock reading, so the stored version is never
  below the cells the batch wrote. On a TTL table the index rows are therefore
  ordered by the pinned wall-clock timestamps, and the claim is what serializes
  the writers.
- Recovery that applies a commit fence stamps its index batch with the version
  it stored. When the fence does not apply, the row was either committed by the
  live path before the crash or taken by a later writer. A row that holds this
  transaction's commit timestamp and the ledger's image is the live commit's,
  and re-syncing it at its current version writes the rows the live batch
  would have; a later writer's batch carries a higher stamp and still wins.

## Consequences

- An unconditional delete on a table without TTL costs a Paxos round it did not
  before. On one Cassandra 4.1 node that added 10 to 20 percent at the median,
  put the 99th percentile under 16 concurrent writers at about 1.5 to 2 times
  the plain DELETE's, and a delete on a hot key can now answer
  `TransactionConflictException` after its nine attempts, where the plain DELETE
  never failed. Each Paxos round makes four round trips from the coordinator to
  a quorum, so on a multi-node cluster the cost grows with the distance between
  replicas. A delete of an absent item writes nothing and reads once more at
  `LOCAL_SERIAL`.
- The transaction commit's read of each row, one read per operation, is a
  `LOCAL_SERIAL` read, which is a Paxos round of its own.
- A delete and a create of the same key and index row now leave the index
  agreeing with the base table when two writers race. Three writers can still
  hide a create: a creator reads its clock, a second writer creates the item,
  the delete removes the second writer's item with a tombstone one above that
  item's version, and the first creator's LWT lands, with index rows stamped
  below the tombstone. The item is then in the base table and missing from the
  index row it shares with the deleted image until its next write. On one node,
  a conditional create, an unconditional create and a delete released together
  on an absent key produced this in 22 of 900 rounds.
  Closing it needs the delete to leave a version that the next create fences on.
- The ExtendDB hosts' clocks must be synchronized with each other. A version
  written from a host whose clock runs ahead stays ahead for that row, since
  `previous + 1` carries it forward, and a delete's tombstone one above it can
  hide a later create from a host whose clock reads lower, until that host's
  clock passes the version or the item is written again. A write that finds a
  version more than one second ahead of its host's clock logs a warning, at most
  once a minute.
- A row created before this change carries a small counter or null in
  `version`; its next write moves it into the microsecond domain. Index rows
  written before this change carry coordinator timestamps, which the next
  write's stamp exceeds.
- During a rolling upgrade a node still running the old binary accepts the
  microsecond versions but keeps dropping the index rows of the items it writes
  (defect 2 above); the next write from an upgraded node restores them. The old
  binary also stores `previous + 1` over a microsecond version with index rows
  at Cassandra's timestamps, and a delete of that item from an upgraded node
  stamps its tombstones one above that version, below those rows, which then
  stay in the index after the item is gone. Rows the old binary created carry a
  counter and are deleted with the clock reading.
- Recovery of a COMMITTING transaction syncs the index rows from the two images
  the ledger holds, which it did not do before. It does not repair a
  transactional Delete whose base delete landed before the crash and whose
  index batch did not: the row is gone, so recovery cannot read the version the
  tombstones must exceed, and those index rows stay. A ledger written before
  the pre-image field existed carries no old image, so its recovery is
  insert-only and a key the transaction moved leaves its old index row behind;
  such ledgers exist only for transactions in flight across the upgrade.
- A TTL expiration that recovery replays restores the index rows of an item
  written again in the meantime at a timestamp above the replayed tombstones,
  which can exceed the item's version. If TTL is then disabled and the item is
  deleted before its next write, the delete's tombstones sit below those rows.
- The asynchronous GSI worker applies entries in version order on the index
  table. Under `extenddb serve` the workers still stop at startup (issue #407);
  the worker path is exercised by the crate's direct integration tests, which
  hold the worker guard.

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project managed by Amazon Web Services. It is not Amazon DynamoDB or an AWS
service. "DynamoDB" is a trademark of Amazon.com, Inc.
