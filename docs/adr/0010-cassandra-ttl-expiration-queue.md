# ADR-0010: Durable sharded expiration queue for Cassandra TTL

**Status:** Accepted
**Date:** 2026-09-01

## Context

DynamoDB TTL is more than storage expiry. Removing an expired item must also remove its secondary-index rows and emit a Streams `REMOVE` record whose identity is the DynamoDB service. Cassandra native TTL deletes a base row internally, bypassing both effects, and an item stored as JSON cannot be efficiently searched by an arbitrary configured TTL attribute.

Cassandra also cannot atomically combine a Paxos condition on the base item with queue, index, and stream mutations in other partitions: a batch may carry conditions for at most one partition. The PostgreSQL backend is therefore a semantic reference, not an implementation whose ACID internals can be reproduced exactly.

## Options considered

### Cassandra native TTL

Operationally simple, but Cassandra would remove the base row without cleaning ExtendDB's secondary-index tables or writing the DynamoDB-compatible service `REMOVE` stream record. It does not meet the external contract.

### A backend-wide durable mutation protocol

Every Put, Update, Delete, transaction, index mutation, and stream mutation could be represented as one durable operation with a commit token and replay state. That is the most complete answer to Cassandra's cross-partition atomicity limits, and it would also improve non-TTL failure handling.

It is also a much larger storage-engine redesign. It changes every write path, adds permanent journal traffic and recovery machinery to tables that do not use TTL, and needs its own migration, capacity, compaction, and operational model. Shipping that redesign as part of TTL would make the first Cassandra TTL release harder to review and risk unrelated behavior that already works.

### A TTL-specific durable state machine

This keeps the durable protocol at the boundary that needs it: TTL queue reconciliation and expiration deletion. Once TTL is enabled, ordinary writes take the same exact row claim used by deletion and commit the base row, TTL queue/outbox, synchronous index changes, and stream record in one logged batch. The deletion worker persists its own retry phases and deterministic stream identity.

This does not solve every Cassandra write-path limitation. In particular, a write that started before TTL was enabled cannot be retroactively joined to the new TTL generation. Enabling TTL on a table that is already serving writes therefore requires those writes to be briefly quiesced until enable/backfill completes. New tables can enable TTL before accepting traffic and do not have this transition window.

We chose this option because it gives the supported TTL lifecycle a bounded, testable recovery protocol without pretending to solve the whole backend. It leaves the broader mutation journal as a separate design that can be evaluated on its own merits.

## Decision

Use a generation-fenced, durable TTL-specific state machine.

* Maintain `ttl_expiration_buckets` and `ttl_expirations` in every account keyspace. Queue partitions use table ID, TTL-enable generation, day bucket, and one of 64 deterministic key shards. Entries are ordered by expiration time and key.
* Accept only positive integral DynamoDB `N` values as epoch seconds. Missing, non-numeric, fractional, zero, and negative values are not queued.
* Allocate a fresh generation on every enable. Backfill the active generation synchronously and publish `ttl_index_ready` only after reconciliation. Disable, re-enable, cleanup, and sweeps are fenced by the exact attribute and generation.
* Before enabling TTL on a table that is already accepting writes, quiesce writes that began under the disabled generation until enable and synchronous backfill return. Once enabled, all ordinary writes enter the exact-claim logged-batch path. Enabling TTL before opening a new table to traffic avoids this operational step.
* Record ordinary Put/Update reconciliation in a durable key-only `ttl_reconcile_pending` outbox in the same logged batch as the base mutation. The worker point-reads the committed item and idempotently inserts its current queue entry. A conflict with claimed TTL work remains retryable; the outbox is removed only after reconciliation succeeds.
* Reconcile transaction Put/Update/Delete results against the pre-commit image before deleting the recovery ledger, so a transactional write retires the entry the item had before it as well as registering the new one. COMMITTING recovery repeats reconciliation from the persisted ledger payload, which holds only the committed image (see *Known gaps*).
* Represent deletion work as `PENDING`, `CLAIMED`, and `EFFECTS_APPLIED`. A claim persists the old item image, a stable delete timestamp, a work UUID, and an optional deterministic stream plan.
* Serialize TTL deletion with ordinary and transactional writes through the base row's `prepared_txn_id` LWT field at `LOCAL_QUORUM`/`LOCAL_SERIAL`. Both the worker's claim and an ordinary request's claim are time-bounded, and a request's logged batch is stamped with a timestamp pinned immediately after its claim, so a request that resumes after its claim expired loses to any owner that committed in the meantime rather than overwriting it. Successful requests conditionally release only their exact owner.
* Treat contention for that claim as contention, not as a client error. A blocked writer retries against a freshly read image on a jittered backoff, and only reports `TransactionConflictException` once those retries are exhausted, per RFC-0003 §4.3. Ordinary concurrent writes to one key therefore remain last-writer-wins.
* Apply synchronous index deletion and the deterministic stream write while the exact base owner blocks writers. Transition the queue row to `EFFECTS_APPLIED`, then delete the base row with an LWT requiring the exact work UUID and item image, and finally conditionally complete the queue work.
* Persist stream event ID, sequence number, timestamp, region, and view type. Retrying side effects overwrites the same stream row rather than creating a second externally visible `REMOVE` event.
* If a stale queue snapshot differs from the current item, conditionally retire that exact work before reconciling the current image. If an old-timestamp delete leaves no item but the permanent work owner survives, recovery conditionally releases that exact UUID before retiring or completing the queue row.
* Use a generation-bound table sweep lease and renew it while processing. TTL lifecycle changes and index creation that could invalidate a live sweep take the same lease, retry briefly when a sweep holds it, and report `ResourceInUseException` only after that.
* Retire a generation by *draining* it, never by deleting its rows outright. Cleanup removes only `PENDING` rows; a `CLAIMED` row has its claim released and its work abandoned, and an `EFFECTS_APPLIED` row has its base delete completed, because its index and stream effects are already durable. The `ttl_cleanup_generation` marker is cleared only when nothing is left, so a partial pass is retried.
* Retire a `(day bucket, shard)` registration once its partition is observed empty and its day is fully past, using a delete stamped with a timestamp taken before that observation so that any concurrent queue insert wins. Bound the number of registry partitions one sweep cycle visits.
* Reject TTL enable when a table has a GSI whose effective propagation delay is nonzero. The current asynchronous GSI queue has no version-conditional replay fence, so admitting that combination could let an old TTL delete overtake a recreated item's insert. Base tables, LSIs, and synchronous GSIs are supported.

## Recovery model

Each phase is idempotent and is entered only from durable state. `work_id` is the exact owner UUID; every transition is conditional on it.

| Crash point | Durable state | Recovery action |
| --- | --- | --- |
| Before the queue row is claimed | `PENDING` | Re-read the item. Expired and unchanged: claim it. Renewed, changed, or gone: retire this exact entry and re-register the current image. |
| After `CLAIMED`, before effects | `CLAIMED` + old image + stream plan | Re-read. Image matches: re-take the claim and apply effects. Image differs or item gone: release the exact claim and abandon the work; nothing externally visible happened. |
| After effects, before `EFFECTS_APPLIED` | `CLAIMED` | Re-apply effects. Index deletes are idempotent and the stream write reuses the persisted event identity, so no second `REMOVE` becomes visible. |
| After `EFFECTS_APPLIED`, before the base delete | `EFFECTS_APPLIED` | Complete the base delete under the exact owner and image. This is the one phase that must go forward: index rows are already removed, so abandoning it would leave a live item with a missing index. Generation cleanup honours the same rule. |
| After the base delete, before completion | Base row gone, `EFFECTS_APPLIED` | Complete the queue row conditionally on the exact owner. |
| Ordinary request suspended past its claim | Base row unchanged | The claim expires and the resumed batch, stamped before the suspension, loses to any newer owner. |
| TTL disabled mid-flight | `ttl_cleanup_generation` set | Drain the generation per the rule above, then remove its `PENDING` rows; retry until empty. |

## Consequences

### Positive

* Expiration lookup uses complete Cassandra partition keys and an ordered clustering range; it does not require `ALLOW FILTERING`.
* Renewed or recreated items cannot be deleted by stale TTL work.
* Worker crashes recover from durable queue state, including crashes after side effects but before the exact base delete.
* Stream retries retain one externally visible service-identified `REMOVE` record.
* Synchronous index rows are removed before final queue completion.
* Queue rebuilds, lifecycle changes, and cleanup are isolated by TTL-enable generation, and no lifecycle change can strand claimed work.
* No claim is unbounded, so no failure leaves a key permanently unwritable.

### Negative

* This is practical DynamoDB TTL behavior, not strict PostgreSQL-style ACID equivalence. Index and stream effects become durable immediately before the final exact base delete, so a brief internal visibility gap is possible while writers remain claim-blocked.
* TTL-enabled writes cost more than non-TTL writes: one extra catalog read, one claim LWT, one release LWT, and a logged batch instead of a single statement. Non-TTL tables keep the existing fast paths.
* TTL cannot currently be enabled on a table with asynchronously propagated GSIs. Supporting that combination requires versioned, conditionally applied GSI mutations or an equivalent causal replay protocol.
* Enable performs a synchronous table scan under the table's control lease, and the `UpdateTimeToLive` call awaits it. Very large tables will require a durable, checkpointed background backfill before this path is suitable at scale.
* Expiration throughput is bounded by design: one exclusive sweep lease per table, one bounded batch per scan interval. See *Operating envelope* for the resulting rate.
* LWT claims and lifecycle leases add Cassandra coordination cost to TTL-enabled tables.
* Cassandra coordinators and ExtendDB hosts must have synchronized clocks, and data-plane requests must have deadlines well below the request claim lifetime.
* Claims and the exact base delete condition on `item_data` string equality, so the persisted JSON encoding of an item is part of the protocol. A unit test pins that encoding as canonical; a version column would be a more robust fence and is a candidate follow-up.

### Operating envelope

This version is suitable for production when the deployment stays inside the supported envelope:

* enable TTL before opening a new table to writes, or briefly quiesce an existing table while enable/backfill completes;
* use no GSI with a nonzero propagation delay on a TTL-enabled table;
* serve writes for a given table from one Cassandra datacenter, because all claims and lifecycle LWTs use `LOCAL_QUORUM`/`LOCAL_SERIAL` and are therefore linearizable only within a datacenter;
* stay under roughly 100 expirations per table per scan interval — one host wins each table's sweep lease per cycle and processes one bounded batch, so cluster-wide steady-state expiration is about 100 items per table per minute regardless of fleet size, and a table expiring faster than that accumulates backlog;
* size and monitor the expiration backlog, Cassandra LWT latency, and worker health;
* keep Cassandra coordinators and ExtendDB hosts time-synchronized, with request deadlines well below the request claim lifetime; and
* accept DynamoDB-style eventual expiration plus the documented brief internal effects-before-delete window.

Within that envelope, item renewal, crash recovery, generation changes, synchronous index cleanup, transaction reconciliation, ordinary write contention, and deterministic stream removal are covered by durable state and real-Cassandra tests. This is not yet a claim of unrestricted DynamoDB parity for every Cassandra table configuration.

### Known gaps

Deliberately out of scope for this change, in rough priority order:

* **Expiration throughput does not scale horizontally.** The sweep lease is per table. Sharding it, so several hosts drain disjoint key shards of one table concurrently, is the natural next step.
* **The backfill has no durable cursor.** A failure restarts the scan from the beginning, and the `UpdateTimeToLive` call blocks for its duration instead of reporting `ENABLING`.
* **`DescribeTimeToLive` has no `ENABLING`/`DISABLING` state.** A table reports `ENABLED` from the moment the attribute is set, including while its queue is still being backfilled.
* **No five-year cutoff.** DynamoDB ignores a TTL timestamp more than five years in the past; here any positive timestamp is queued and expired.
* **Transaction recovery reconciles insert-only.** The ledger does not persist the pre-commit image, so a transaction that crashes between COMMIT and reconciliation can leave the item's previous queue entry behind until it comes due. It is inert — the worker revalidates the item before deleting anything — but it is queue garbage.
* **`item_data` equality as the fence.** A monotonic version column would remove the dependency on a stable JSON encoding and stop shipping whole items as LWT condition values.
* **Catalog reads are uncached.** TTL configuration is read from the catalog on every write, on top of the index read the write path already performs.
