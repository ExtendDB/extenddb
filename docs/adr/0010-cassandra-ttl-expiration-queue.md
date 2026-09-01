# ADR-0010: Durable sharded expiration queue for Cassandra TTL

**Status:** Accepted
**Date:** 2026-09-01

## Context

DynamoDB TTL is more than storage expiry. Removing an expired item must also remove its secondary-index rows and emit a Streams `REMOVE` record whose identity is the DynamoDB service. Cassandra native TTL deletes a base row internally, bypassing both effects, and an item stored as JSON cannot be efficiently searched by an arbitrary configured TTL attribute.

Cassandra also cannot atomically combine a Paxos condition on the base item with queue, index, and stream mutations in other partitions. The PostgreSQL backend is therefore a semantic reference, not an implementation whose ACID internals can be reproduced exactly.

## Decision

Use a generation-fenced, durable TTL-specific state machine.

* Maintain `ttl_expiration_buckets` and `ttl_expirations` in every account keyspace. Queue partitions use table ID, TTL-enable generation, day bucket, and one of 64 deterministic key shards. Entries are ordered by expiration time and key.
* Accept only positive integral DynamoDB `N` values as epoch seconds. Missing, non-numeric, fractional, zero, and negative values are not queued.
* Allocate a fresh generation on every enable. Backfill the active generation synchronously and publish `ttl_index_ready` only after reconciliation. Disable, re-enable, cleanup, and sweeps are fenced by the exact attribute and generation.
* Record ordinary Put/Update reconciliation in a durable key-only `ttl_reconcile_pending` outbox in the same logged batch as the base mutation. The worker point-reads the committed item and idempotently inserts its current queue entry. A conflict with claimed TTL work remains retryable; the outbox is removed only after reconciliation succeeds.
* Reconcile transaction Put/Update results before deleting their recovery ledger. COMMITTING recovery repeats reconciliation from the persisted ledger payload.
* Represent deletion work as `PENDING`, `CLAIMED`, and `EFFECTS_APPLIED`. A claim persists the old item image, a stable delete timestamp, a work UUID, and an optional deterministic stream plan.
* Serialize TTL deletion with ordinary and transactional writes through the base row's `prepared_txn_id` LWT field at `LOCAL_QUORUM`/`LOCAL_SERIAL`. The worker adopts a persistent exact work owner; ordinary TTL-table claims expire and their complete logged batch uses a request-start timestamp so a request resuming after claim expiry cannot overwrite newer transaction state. Successful requests conditionally release only their exact owner.
* Apply synchronous index deletion and the deterministic stream write while the exact base owner blocks writers. Transition the queue row to `EFFECTS_APPLIED`, then delete the base row with an LWT requiring the exact work UUID and item image, and finally conditionally complete the queue work.
* Persist stream event ID, sequence number, timestamp, region, and view type. Retrying side effects overwrites the same stream row rather than creating a second externally visible `REMOVE` event.
* If a stale queue snapshot differs from the current item, conditionally retire that exact work before reconciling the current image. If an old-timestamp delete leaves no item but the permanent work owner survives, recovery conditionally releases that exact UUID before retiring or completing the queue row.
* Use a generation-bound table sweep lease and renew it while processing. TTL lifecycle changes are blocked while a sweep owns the generation. Generation cleanup is retryable and refuses to remove in-flight durable work.
* Reject TTL enable when a table has a GSI whose effective propagation delay is nonzero. The current asynchronous GSI queue has no version-conditional replay fence, so admitting that combination could let an old TTL delete overtake a recreated item's insert. Base tables, LSIs, and synchronous GSIs are supported.

## Consequences

### Positive

* Expiration lookup uses complete Cassandra partition keys and an ordered clustering range; it does not require `ALLOW FILTERING`.
* Renewed or recreated items cannot be deleted by stale TTL work.
* Worker crashes recover from durable queue state, including crashes after side effects but before the exact base delete.
* Stream retries retain one externally visible service-identified `REMOVE` record.
* Synchronous index rows are removed before final queue completion.
* Queue rebuilds, lifecycle changes, and cleanup are isolated by TTL-enable generation.

### Negative

* This is practical DynamoDB TTL behavior, not strict PostgreSQL-style ACID equivalence. Index and stream effects become durable immediately before the final exact base delete, so a brief internal visibility gap is possible while writers remain claim-blocked.
* TTL cannot currently be enabled on a table with asynchronously propagated GSIs. Supporting that combination requires versioned, conditionally applied GSI mutations or an equivalent causal replay protocol.
* Enable performs a synchronous table scan. Very large tables will require a durable background backfill before this path is suitable at scale.
* The worker processes bounded batches, so large expiration backlogs drain gradually.
* LWT claims and lifecycle leases add Cassandra coordination cost to TTL-enabled tables.
