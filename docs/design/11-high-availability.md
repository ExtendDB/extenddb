# extenddb — High Availability Design

**Version:** 0.5 (Draft — Reviewer feedback round 3)
**Date:** 2026-05-08
**Status:** Draft — awaiting reviewer and principal reviewer deliberation
**Phase:** P122
**Applies to:** Both `extenddb` (ExtendDB) and `extenddb` (ExtendDB) with name substitution.

## 1. Problem Statement

extenddb currently operates as a single-process server backed by one configured storage backend. The steering documents note a `TODO(architecture)` about enforcing single-frontend-per-catalog vs. designing for multi-instance topology. This design addresses that open question and defines how extenddb scales from a single Raspberry Pi to a petabyte-scale cloud deployment with multiple replicas.

### Goals

1. Multiple extenddb instances sharing the same backing database (horizontal frontend scaling).
2. Pluggable storage layers with varying native replication capabilities.
3. A notion of leadership that enables correct strongly-consistent vs. eventually-consistent read semantics.
4. Deployment models spanning single-node to multi-region.
5. Staged delivery — each stage is independently useful and testable.
6. All existing features continue to work at every stage.
7. Extensible to strongly consistent GSIs — the design must support zero-propagation-delay indexes where the GSI write is transactional with the data write, and a consistent read on the GSI reflects the latest committed base table state.

### Non-Goals (This Document)

- Implementing a full Paxos/Raft consensus protocol within extenddb itself.
- Multi-region active-active writes (deferred to a future phase).
- Automatic resharding of DynamoDB partitions (extenddb does not emulate DynamoDB's internal partition management).

## 2. Terminology

| Term | Definition |
|------|-----------|
| **Frontend** | A extenddb process that accepts DynamoDB API requests. Stateless except for in-flight request state. |
| **Catalog** | The backing database that stores table metadata, items, streams, and auth data. |
| **Deployment** | One or more frontends sharing a single logical catalog. All frontends in a deployment serve the same set of accounts and tables. |
| **Replica set** | Multiple catalog nodes providing data redundancy. |
| **Leader** | The frontend (or catalog node) that handles writes and strongly-consistent reads for a given scope. |
| **Follower** | A frontend (or catalog node) that handles eventually-consistent reads. |

## 3. DynamoDB Consistency Model (What We Must Emulate)

DynamoDB offers two read consistency levels per request:

- **Eventually consistent reads** (default): May return stale data. Consumes half a read capacity unit per 4KB.
- **Strongly consistent reads**: Returns the most recent write. Consumes 1 read capacity unit per 4KB. May have higher latency and is unavailable during network partitions.

All writes are strongly consistent (acknowledged only after durable commit).

**Key insight:** DynamoDB's consistency model is per-request, not per-table or per-partition. A client chooses consistency on every read call. extenddb must honor this choice.

## 4. Architecture Overview

### 4.1 Layered Approach

```
                    ┌─────────────────────────────────────────┐
                    │           Load Balancer / DNS            │
                    └────────────┬───────────┬────────────────┘
                                 │           │
              ┌──────────────────┴──┐   ┌────┴──────────────────┐
              │   Frontend A        │   │   Frontend B          │
              │   (extenddb process)    │   │   (extenddb process)      │
              │   ┌──────────────┐  │   │   ┌──────────────┐    │
              │   │ Engine       │  │   │   │ Engine       │    │
              │   │ Auth         │  │   │   │ Auth         │    │
              │   │ Consistency  │  │   │   │ Consistency  │    │
              │   │   Routing    │  │   │   │   Routing    │    │
              │   └──────┬───────┘  │   │   └──────┬───────┘    │
              └──────────┼──────────┘   └──────────┼────────────┘
                         │                         │
              ┌──────────┴─────────────────────────┴────────────┐
              │              Storage Adapter Layer               │
              │  (implements TableEngine, DataEngine, etc.)      │
              └──────────┬─────────────────────────┬────────────┘
                         │                         │
              ┌──────────┴──────────┐   ┌──────────┴──────────┐
              │  Primary Catalog    │   │  Replica Catalog    │
              │  (writes + strong   │   │  (eventually        │
              │   consistent reads) │   │   consistent reads) │
              └─────────────────────┘   └─────────────────────┘
```

### 4.2 Core Principle: Consistency Routing

The key architectural addition is **consistency routing** within the storage adapter. For every read request:

1. The engine layer passes `consistent_read` (from the DynamoDB request) to the storage method.
2. If `consistent_read = true` → storage adapter routes to the primary catalog connection.
3. If `consistent_read = false` → storage adapter routes to any available catalog connection (primary or replica).

For writes: always route to the primary catalog connection.

This is the minimal mechanism needed to honor DynamoDB's consistency model. It works regardless of whether the catalog provides native replication.

## 5. Deployment Models

### Model 1: Single Frontend, Single Catalog (Current)

```
[Frontend] → [Storage Backend]
```

- No HA. Single point of failure.
- Suitable for: development, testing, single Raspberry Pi.

### Model 2: Multiple Frontends, Single Catalog

```
[Frontend A] ─┐
              ├→ [Storage Backend]
[Frontend B] ─┘
```

- Frontend HA via load balancer.
- Catalog is still a SPOF.
- All reads are strongly consistent (single catalog node).
- Suitable for: increased request throughput, frontend redundancy.

### Model 3: Multiple Frontends, Replicated Catalog

```
[Frontend A] ─┐     ┌→ [Storage Primary] (writes + strong reads)
              ├─────┤
[Frontend B] ─┘     └→ [Storage Replica] (eventually consistent reads)
```

- Full HA for both frontend and catalog.
- Consistency routing directs reads appropriately.
- Suitable for: production deployments requiring high availability.

### Model 4: Multiple Frontends, Natively-Clustered Catalog

```
[Frontend A] ─┐     ┌→ [TiDB SQL Node]
              ├─────┼→ [TiDB SQL Node]
[Frontend B] ─┘     └→ [TiKV / PD Cluster]
```

- Storage layer maps DynamoDB consistency to backend-native semantics.
  - `ConsistentRead = true` → route through the backend's strongly consistent read path
  - `ConsistentRead = false` → route through any backend-supported eventually consistent path, or the same strong path if the backend is globally consistent
- No separate primary/replica distinction — the storage adapter handles it.
- Suitable for: large-scale deployments, multi-datacenter.

### Model 5: Multi-Region (Future)

- Multiple deployments with cross-region replication.
- Maps to DynamoDB Global Tables semantics.
- Out of scope for initial implementation.

## 6. Design Decisions

### D1: No In-Process State (Preserved)

The existing "No Caching Rule" is preserved and strengthened. Frontends remain stateless. This is what makes horizontal frontend scaling trivial — any frontend can serve any request because all state lives in the catalog.

**Rationale:** Multiple frontends sharing a catalog would have stale caches. The No Caching Rule already anticipated this.

### D2: Consistency Routing Lives in the Storage Adapter

The storage adapter (e.g., `storage-postgres`) is responsible for routing reads to the appropriate connection based on the consistency requirement. The engine passes a `ConsistencyLevel` parameter; the storage adapter decides which connection to use.

**Rationale:** Different backends implement replication differently. The storage adapter is the right place to abstract this.

### D3: Leadership Is Per-Catalog, Not Per-Frontend

In deployment models 2 and 3, there is no "leader frontend." All frontends are equal. Leadership (for writes and strong reads) is a property of the catalog node, not the extenddb process.

**Rationale:** DynamoDB's leadership is per-partition, but extenddb doesn't emulate internal partition management. Since all frontends talk to the same catalog, the catalog's primary node is the effective leader. This avoids the complexity of distributed consensus among frontends.

**Known divergence:** DynamoDB's per-partition leadership means a single partition failure doesn't affect other partitions. In extenddb, a catalog primary failure affects all tables. This is an acceptable limitation — extenddb delegates HA to the catalog's native replication, which provides node-level (not partition-level) failover.

**Exception:** If a future storage backend requires frontend-level coordination (e.g., a storage layer with no native replication where extenddb must implement its own replication), a frontend leader election mechanism would be needed. This is deferred.

### D4: Heterogeneous Storage Is Illegal

A single deployment must use a single storage backend type. You cannot mix different backend storage types in one deployment.

**Rationale:** Different backends have different data models, consistency semantics, and transaction capabilities. Mixing them would create an untestable matrix of behaviors. Each deployment is homogeneous.

### D5: Configuration Declares Topology

The `extenddb.toml` configuration file declares the deployment topology:

```toml
[storage]
backend = "postgres"

[storage.postgres]
# Primary connection (writes + strongly consistent reads)
primary = "postgresql://primary:5432/extenddb"

# Replica connections (eventually consistent reads)
# If empty, all reads go to primary (Model 2 behavior)
replicas = [
    "postgresql://replica1:5432/extenddb",
    "postgresql://replica2:5432/extenddb",
]
```

### D6: Health Checks and Connection Failover

Each frontend maintains health checks against its catalog connections. If a replica becomes unavailable, eventually-consistent reads fall back to the primary. If the primary becomes unavailable, writes and strongly-consistent reads return `InternalServerError` (matching DynamoDB behavior during partition leader failover).

### D7: Connection Pool Sizing

With N frontends each maintaining pools to 1 primary + M replicas, total connection count is N × (primary_pool_size + M × replica_pool_size). Backend connection limits can be exhausted quickly. Guidance:

- **Small deployments (1-3 frontends):** Direct connections with pool size 5-10 per target. Total: 15-60 connections.
- **Medium deployments (4-10 frontends):** Use the backend's recommended connection pooler between frontends and catalog. Pool size per frontend: 3-5 per target.
- **Large deployments (10+ frontends):** External connection pooling is required for backends with strict connection limits. Document backend-specific connection tuning.

The `extenddb.toml` configuration accepts `pool_size` per connection target. The design does not mandate a specific connection pooler; operators should follow backend-specific best practices for deployments with more than 3 frontends.

## 7. Alternatives Considered

### A1: Raft/Paxos Among Frontends

**Approach:** Implement a consensus protocol among extenddb frontends to elect a leader for writes.

**Rejected because:**
- Adds enormous complexity (Raft implementation, membership management, log replication).
- Unnecessary when the catalog already provides durability and consistency.
- extenddb frontends are stateless by design — adding state contradicts the architecture.
- Many databases already solve this problem at the storage layer.

### A2: Shared-Nothing Architecture (Each Frontend Owns a Partition)

**Approach:** Partition the key space across frontends. Each frontend owns a subset of partitions and is the exclusive writer for those keys.

**Rejected because:**
- Requires a partition map and routing layer (adds latency and complexity).
- Partition rebalancing on frontend add/remove is complex.
- DynamoDB's API doesn't expose partitioning to clients — any frontend must be able to serve any request.
- Contradicts the "equally comfortable on a Raspberry Pi" requirement.

### A3: Frontend-Level Read Replicas (Cached Reads)

**Approach:** Frontends cache recent reads and serve eventually-consistent reads from cache.

**Rejected because:**
- Violates the No Caching Rule.
- Cache invalidation across frontends is the exact problem the No Caching Rule was designed to avoid.
- Backend buffer pools already provide memory-resident access to hot data.

### A4: Single-Writer with Read Replicas at Frontend Level

**Approach:** Designate one frontend as the writer; others are read-only replicas.

**Rejected because:**
- Requires leader election among frontends (back to A1).
- A client sending a write to a read-only frontend would need request forwarding (adds latency, complexity).
- No benefit over Model 3 where the catalog handles write routing.

## 8. Storage Adapter Interface Changes

### 8.1 ConsistencyLevel Parameter

Add a `ConsistencyLevel` enum to the storage crate. This enum is **internal to the storage adapter** — the engine layer passes a raw `consistent_read: bool` and the storage adapter converts it. This avoids adding a storage-crate type dependency to the engine:

```rust
/// Read consistency level, matching DynamoDB's per-request semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ConsistencyLevel {
    /// Eventually consistent read. May return stale data.
    #[default]
    Eventually,
    /// Strongly consistent read. Returns the most recent write.
    Strong,
}
```

The `#[non_exhaustive]` attribute allows future extension without a breaking change.

### 8.2 Consistency Routing via Trait Parameter (Option A)

The existing `DataEngine` trait methods receive decomposed parameters — not input structs. The `consistent_read` field from the DynamoDB request is consumed by the engine layer for capacity metering but is **never passed to storage**. To enable consistency routing, the storage layer must receive this information.

**Chosen approach: Option A — add a `consistent_read: bool` parameter to read methods.**

This is the simplest approach. The trait is internal (no out-of-tree implementations exist), so the change is mechanical. The affected methods are:

```rust
// Before:
fn get_item(&self, key_info: &TableKeyInfo, key: &Item)
    -> impl Future<Output = Result<Option<Item>, StorageError>> + Send;

// After:
fn get_item(&self, key_info: &TableKeyInfo, key: &Item, consistent_read: bool)
    -> impl Future<Output = Result<Option<Item>, StorageError>> + Send;
```

Methods that gain the `consistent_read` parameter:
- `get_item`
- `query`
- `scan`

Methods that do NOT need it:
- `transact_get_items` — always strongly consistent (DynamoDB requires `ConsistentRead = true`)
- `put_item`, `delete_item`, `update_item`, `transact_write_items` — writes always go to primary

**`BatchGetItem` routing:** The engine handles `BatchGetItem` by calling `get_item` per key in a loop. `BatchGetItem` has per-table `ConsistentRead` — different tables in the same batch can specify different consistency levels. When Stage 1 adds `consistent_read: bool` to `get_item`, the `batch_get_item` engine handler must pass `ka.consistent_read.unwrap_or(false)` to each `get_item` call. This means a single `BatchGetItem` request may route some reads to the primary and others to replicas, depending on per-table settings. This is correct behavior — it matches DynamoDB's semantics where each table in a batch independently honors its `ConsistentRead` setting.

The storage adapter maps the parameter internally:

```rust
impl ConsistencyLevel {
    pub fn from_consistent_read(consistent_read: bool) -> Self {
        if consistent_read { Self::Strong } else { Self::Eventually }
    }
}
```

**TransactGetItems:** DynamoDB requires `ConsistentRead = true` for all items in a `TransactGetItems` request. The operation is always strongly consistent. The storage adapter unconditionally routes `TransactGetItems` to the primary connection. This is not configurable — it is a DynamoDB API constraint.

**Breaking change note:** This is a breaking change to the internal `DataEngine` trait. Since the trait is internal and current implementations live in-tree (`storage-postgres`, `storage-tidb`), no external migration path is needed. The change is mechanical: add the parameter to the trait, the implementations, and all call sites in the engine.

**Alternatives considered and rejected:**

- **Option B (request-scoped context):** Thread `ConsistencyLevel` through a `RequestContext` struct passed to all storage methods. More extensible but more invasive — every method signature changes, not just reads. Overkill for a single boolean.
- **Option C (dual DataEngine instances):** The engine selects "primary pool" or "replica pool" before calling storage methods. Requires the engine to understand storage topology, violating the abstraction boundary.

### 8.2.1 Paginated Scan Consistency

A paginated scan with `ConsistentRead = true` makes multiple round-trips to the primary. Between pages, writes may occur. Each page is individually strongly consistent, but the full scan is NOT transactionally isolated across pages. This matches DynamoDB's behavior — a strongly consistent scan guarantees each page reflects the latest writes at the time that page was read, not a point-in-time snapshot of the entire table.

### 8.2.2 Locking Reads

Any SQL statement that acquires locks (`SELECT ... FOR UPDATE`) routes to primary regardless of the `consistent_read` parameter. This applies to condition expressions in write paths and `transact_write_items`. Since these are write-path operations that already route to primary, no special handling is needed.

### 8.2.3 Strongly Consistent GSIs

**Requirement:** In the near future, extenddb will support strongly consistent GSIs. A strongly consistent GSI has zero propagation delay — the write to the index commits atomically with the base table write. A strongly consistent read on such a GSI returns data current with the base table.

**Current state:** The storage layer supports backend-specific synchronous GSI maintenance. PostgreSQL can keep companion GSI tables in the write transaction when `propagation_delay_ms = Some(0)` (or when the system default is 0). TiDB stores each item once and relies on native secondary indexes generated from the base table row.

**Interaction with HA consistency routing:**

1. **Writes:** A write to a table with a strongly consistent GSI commits the base row and secondary-index state atomically on the primary. PostgreSQL does this with companion-table writes; TiDB does it through native secondary indexes. No change needed — writes always route to primary.

2. **Strongly consistent GSI reads (`ConsistentRead = true` on a GSI query/scan):** Today, DynamoDB rejects `ConsistentRead = true` on GSI queries with `ValidationException`: "Consistent reads are not supported on global secondary indexes." extenddb faithfully reproduces this rejection (tenet 1). When strongly consistent GSIs are introduced as a extenddb extension, `ConsistentRead = true` on a strongly consistent GSI query routes to the primary — same as any strongly consistent read. The routing logic in §8.2 handles this without modification.

3. **Eventually consistent GSI reads (`ConsistentRead = false` on a GSI query/scan):** Routes to a replica. The replica may have replication lag, so the GSI data on the replica may be slightly behind the primary. This is acceptable — it matches the semantics of eventually consistent reads (the caller explicitly opted into potentially stale data). The GSI data on the replica is guaranteed to be consistent *with itself* (the base row and GSI row committed atomically on the primary, so they replicate together).

4. **Replica consistency guarantee:** Because the base table write and secondary-index maintenance commit atomically, they appear on replicas atomically. PostgreSQL streaming replication replays WAL records in commit order, so a single transaction's effects are visible atomically on replicas. TiDB secondary indexes are part of the table's native replicated state.

   **Important qualification:** For PostgreSQL companion tables, this atomicity guarantee requires **physical streaming replication** (the default for PostgreSQL HA, and what Aurora PostgreSQL uses internally). Logical replication configurations must ensure that base table and GSI tables are replicated through the same subscription with `streaming = on` (not `parallel`). If the subscription uses `streaming = parallel`, transactions may be applied out of order. If the user has multiple subscriptions covering different tables, atomicity across subscriptions is not guaranteed. The design does not support split-subscription logical replication for tables with strongly consistent GSIs.

**Design implications:**

- **No new routing logic needed.** The existing `consistent_read: bool` parameter on `get_item`, `query`, `scan` handles strongly consistent GSI reads identically to base table reads. The storage adapter routes to primary or replica based on the parameter, regardless of whether the target is a base table or a GSI.

- **The `propagation_delay_ms` setting determines GSI consistency class.** A GSI with `propagation_delay_ms = 0` (or a future explicit `strongly_consistent = true` flag) commits synchronously. The HA design does not need to distinguish between "regular" and "strongly consistent" GSIs for routing purposes — the distinction is in the write path (sync vs. async commit), not the read path.

- **Async GSIs (non-zero propagation delay) have weaker replica guarantees.** An async GSI update is enqueued after the base table transaction commits. The GSI row is written in a separate transaction (by the GSI worker). On a replica, the base row and the async GSI row may appear at different times (different transactions, different WAL positions). A strongly consistent read on an async GSI would need to route to primary AND wait for the GSI worker to process the queue — which is impractical. Therefore: **strongly consistent reads are only supported on strongly consistent GSIs (zero propagation delay).** Attempting a strongly consistent read on an async GSI returns a `ValidationException` with message "Strongly consistent reads are not supported on eventually consistent indexes." This matches DynamoDB's approach of rejecting invalid consistency requests at the API layer rather than silently degrading. The caller explicitly asked for strong consistency; silently returning stale data would violate the principle of least surprise.

- **Stage 1 compatibility:** The `consistent_read: bool` parameter added in Stage 1 is sufficient. No additional parameters are needed for strongly consistent GSI support. The storage adapter's routing decision is the same: `true` → primary, `false` → replica.

**Alternatives considered:**

- **A: Separate routing for GSI vs. base table reads.** Rejected — unnecessary complexity. The routing decision is the same (`consistent_read` → primary). The only difference is in the write path (sync vs. async), which is orthogonal to read routing.

- **B: Require all GSIs to be strongly consistent in HA deployments.** Rejected — removes a useful performance optimization. Async GSIs with eventual consistency are appropriate for workloads that tolerate propagation delay (e.g., analytics indexes queried with `ConsistentRead = false`).

- **C: Implement read-your-writes consistency for async GSIs via version vectors.** Rejected — enormous complexity for marginal benefit. If a caller needs current GSI data, they should use a strongly consistent GSI (zero delay) and `ConsistentRead = true`.

### 8.3 StorageTopology (Extension of Storage Lifecycle)

Topology awareness is added as a default-implemented method on the existing storage initialization/lifecycle trait rather than introducing a new trait. Backends that support replicas override the default:

```rust
/// Storage topology information for health checks and monitoring.
/// Default implementation returns single-node healthy status.
pub struct TopologyStatus {
    /// Whether the primary is reachable and accepting writes.
    pub primary_healthy: bool,
    /// Number of healthy replicas available for eventually-consistent reads.
    pub healthy_replicas: usize,
    /// Total configured replicas.
    pub total_replicas: usize,
}

// Added to the existing StorageInit trait (or equivalent lifecycle trait):
// fn topology_status(&self) -> impl Future<Output = TopologyStatus> + Send {
//     async { TopologyStatus { primary_healthy: true, healthy_replicas: 0, total_replicas: 0 } }
// }
```

This avoids adding yet another trait that every storage backend must implement. Single-node backends get the correct default behavior for free.

## 9. Staged Implementation Plan

### Stage 1: Consistency Parameter Plumbing (No Behavioral Change)

**Deliverable:** Add `consistent_read: bool` parameter to `get_item`, `query`, and `scan` in the `DataEngine` trait. Thread it from the engine layer call sites. The PostgreSQL backend accepts the parameter but ignores it (all reads go to the single connection). All existing tests pass unchanged.

**Value:** Establishes the interface contract. Future stages add behavior without changing the API.

**Scope:**
- Add `ConsistencyLevel` enum to `extenddb-storage` (internal to storage adapter).
- Add `consistent_read: bool` parameter to `get_item`, `query`, `scan` in `DataEngine` trait.
- Update `PostgresEngine` implementation to accept the parameter (no routing change — single connection).
- Update all engine call sites to pass `input.consistent_read.unwrap_or(false)`.
- Update `batch_get_item` handler to pass per-table `ka.consistent_read.unwrap_or(false)` to each `get_item` call.
- All existing tests pass unchanged (no behavioral change, only signature change).

### Stage 2: Multi-Connection PostgreSQL Adapter

**Deliverable:** The PostgreSQL storage adapter accepts a primary + replica configuration. Reads with `ConsistencyLevel::Eventually` are routed to a replica connection pool. Reads with `ConsistencyLevel::Strong` and all writes go to the primary.

**Value:** Enables deployment Model 3 with PostgreSQL streaming replication. Customers with a PostgreSQL primary + read replica get read scaling immediately.

**Scope:**
- `storage-postgres` accepts `replicas` config.
- Connection pool per replica with health checks.
- Round-robin or least-connections routing among healthy replicas.
- Fallback to primary when no replicas are healthy.
- Health check endpoint reports topology status.

### Stage 3: Multi-Frontend Coordination

**Deliverable:** Multiple extenddb frontends can safely share the same catalog without coordination issues. Document the deployment model and provide operational tooling.

**Value:** Enables deployment Model 2 and 3 with multiple frontends behind a load balancer.

**Scope:**
- Verify all operations are safe under concurrent multi-frontend access (the No Caching Rule already ensures this, but explicit verification is needed for: control-plane transitions, TTL worker, GSI backfill worker, stream shard assignment).
- Add distributed locking for background workers (only one frontend runs TTL cleanup, GSI backfill, etc. at a time) using backend-native advisory/session locks or an equivalent lease.
- Document load balancer configuration (sticky sessions not required since frontends are stateless).
- Add instance-id to metrics and logs for multi-frontend debugging.

## 10. Background Worker Coordination

### Problem

extenddb runs background workers for:
- Control-plane transitions (CREATING → ACTIVE)
- TTL item expiration
- GSI backfill
- Stream record cleanup
- Table size refresh

With multiple frontends, these workers must not run concurrently on multiple instances (double-processing, race conditions).

### Solution: Distributed Worker Locks (Global Granularity)

Use backend-native advisory/session locks or an equivalent lease to ensure only one frontend runs each worker type at a time. **Lock granularity is global (one lock per worker type), not per-table.**

**Rationale for global locks:**
- The current TTL worker iterates all tables with TTL enabled. Per-table locking would require restructuring the worker loop.
- Per-table locking adds N advisory locks (one per table), which is operationally complex and creates a thundering-herd problem (N frontends × M tables = N×M lock attempts per tick).
- Global locking is simpler and sufficient until profiling shows TTL processing is a bottleneck.
- Per-table locking can be added as a future optimization if needed.

```rust
/// Attempt to acquire a distributed lock for a worker type.
/// Returns true if the lock was acquired (this instance should run the worker).
/// Non-blocking: returns false immediately if another instance holds the lock.
pub trait WorkerLock: Send + Sync {
    fn try_acquire_worker_lock(
        &self,
        worker_type: WorkerType,
    ) -> impl Future<Output = Result<bool, StorageError>> + Send;

    fn release_worker_lock(
        &self,
        worker_type: WorkerType,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;
}

/// Worker types use a namespace prefix (0x_EXTENDDB_0000 + discriminant) to avoid
/// collisions with application advisory locks on the same database.
/// The namespace value 0xEXTENDDB0000 = 3,720,937,472 in decimal — operators
/// debugging advisory locks in pg_locks will see values starting from this base.
pub enum WorkerType {
    ControlPlaneTransitions,
    TtlExpiration,
    GsiBackfill,
    StreamCleanup,
    TableSizeRefresh,
    MetricsAggregation,
}

impl WorkerType {
    /// Advisory lock ID with namespace prefix to avoid collisions.
    pub fn lock_id(&self) -> i64 {
        const NAMESPACE: i64 = 0x_EXTENDDB_0000;
        NAMESPACE + *self as i64
    }
}
```

The `WorkerLock` trait follows the same pattern as `DataEngine` and `MetadataEngine` — defined in the `extenddb-storage` crate and implemented by each backend engine. Since the current architecture uses concrete backend types behind trait objects, `WorkerLock` is simply another trait that backend engines implement.

Each frontend attempts to acquire the lock on its worker tick interval. If it gets the lock, it runs the worker. If not, it skips.

**Lock lifecycle:** Prefer backend-native session locks that are automatically released when the storage connection drops. PostgreSQL can use `pg_try_advisory_lock(worker_type_id)`. TiDB can use a backend-native/session-scoped equivalent or fall back to a catalog lease when a session lock does not satisfy the worker's failure semantics. This means:
- No explicit TTL/lease mechanism is needed when the backend provides crash-released session locks.
- A crashed frontend's locks are released when the backend cleans up the dead connection.
- The instance registry heartbeat (§13) is for **observability only**, not for lock management.

## 11. Failure Modes and Recovery

| Failure | Impact | Recovery |
|---------|--------|----------|
| Frontend crash | Requests to that frontend fail. Load balancer routes to others. | Restart frontend. No data loss. |
| Replica catalog unavailable | Eventually-consistent reads fall back to primary. | Repair/replace replica. |
| Primary catalog unavailable | Writes and strongly-consistent reads fail with 500. Eventually-consistent reads continue from replicas. | Promote replica to primary using the backend's failover process. |
| Network partition (frontend ↔ catalog) | Affected frontend returns 500. Others continue. | Resolve network issue. |
| Split brain (two primaries) | Prevented by catalog's own replication protocol. extenddb does not manage catalog failover. | N/A — delegated to catalog HA. |

### 11.1 Replica Failover Strategy (Stage 2 Implementation Detail)

**Detection:** An unhealthy replica is detected via connection pool health checks. The pool marks a replica unhealthy after `replica_health_check_failures` consecutive failed checks (default: 3, at 10-second intervals = 30 seconds to detect).

**Fallback behavior:** Per-request. Each read request checks the set of healthy replicas at dispatch time. If no replicas are healthy, the request falls back to primary for that single request. When the replica recovers (health check succeeds), subsequent requests resume routing to it.

**In-flight requests:** A request that fails mid-query due to a replica going down receives a connection error from the pool. The storage adapter retries the request once against the primary before returning an error to the engine. This retry is transparent to the caller.

**Connection string rotation (cloud deployments):** Aurora's writer endpoint (`*.cluster-*.rds.amazonaws.com`) and reader endpoint (`*.cluster-ro-*.rds.amazonaws.com`) handle DNS-based failover transparently. For Aurora deployments, the configuration may specify a single reader endpoint that the infrastructure load-balances across replicas, rather than listing individual replica hosts. The storage adapter treats this as a single "replica pool" — Aurora handles the distribution.

## 12. What Leadership Means Per Backend

| Backend | Write Leader | Strong Read Leader | Eventually Consistent Read |
|---------|-------------|-------------------|---------------------------|
| PostgreSQL (streaming replication) | Primary node | Primary node | Any replica |
| TiDB | TiDB transaction coordinator / region leaders | TiDB cluster | TiDB cluster |
| Cassandra | Coordinator (any node) | QUORUM nodes | ONE node |
| MongoDB (replica set) | Primary member | Primary member | Secondary preferred (falls back to primary if no secondaries available) |
| Single PostgreSQL (no replicas) | The single node | The single node | The single node (no distinction) |

**Key insight:** extenddb never needs to implement leader election itself. The catalog's native replication protocol determines leadership. extenddb's job is to route requests to the right catalog node based on the consistency requirement.

## 13. Configuration Validation

### Legal Configurations

- 1 frontend, 1 catalog node (Model 1) ✓
- N frontends, 1 catalog node (Model 2) ✓
- N frontends, 1 primary + M replicas (Model 3) ✓
- N frontends, K-node cluster (Model 4) ✓

### Illegal Configurations

- Mixed storage backends in one deployment ✗
- Frontend configured with replicas but no primary ✗
- Multiple independent write leaders for one catalog ✗ (use the storage backend's own failover/consensus)

### Startup Validation

On startup, each frontend:
1. Connects to the primary catalog and verifies schema version.
2. Connects to each configured replica, if configured, and verifies it belongs to the same backend topology.
3. Registers itself in a `extenddb_instances` table (instance_id, hostname, started_at, last_heartbeat).
4. Begins heartbeat updates (every 30s).

**Instance registry purpose:** The `extenddb_instances` table is for **observability and operational tooling only**. It answers "which frontends are running?" for operators. It is NOT used for lock management or coordination when the backend provides session-scoped locks; otherwise a dedicated backend lease table owns correctness. Dead entries (heartbeat older than 5 minutes, configurable via `extenddb settings set instance_heartbeat_timeout_seconds`) are cleaned up periodically but their presence has no correctness impact.

## 14. Observability

### Instance Identification

Each frontend gets a unique instance ID (UUID generated at startup). All log messages and metrics include this ID.

### Health Endpoint

`GET /health` returns topology and worker status. **This endpoint requires authentication** (management API credentials) because it exposes internal topology details (replica endpoints, replication lag, worker status). It is not available to unauthenticated callers.

Response:
```json
{
  "status": "healthy",
  "instance_id": "550e8400-e29b-41d4-a716-446655440000",
  "catalog": {
    "primary": "healthy",
    "replicas": [
      {"endpoint": "replica1:5432", "status": "healthy", "replication_lag_ms": 12},
      {"endpoint": "replica2:5432", "status": "unhealthy", "error": "connection refused"}
    ]
  },
  "workers": {
    "control_plane": "active",
    "ttl_expiration": "standby",
    "stream_cleanup": "active"
  }
}
```

### Metrics

New metrics for HA monitoring:
- `extenddb_replica_lag_seconds` (gauge, per replica)
- `extenddb_replica_health` (gauge, 0/1 per replica)
- `extenddb_worker_lock_held` (gauge, 0/1 per worker type)
- `extenddb_consistency_routing_total` (counter, labels: level=strong|eventual, target=primary|replica)
- `extenddb_failover_to_primary_total` (counter, when replica unavailable)

## 15. Impact on Existing Features

| Feature | Impact | Notes |
|---------|--------|-------|
| DynamoDB API operations | None (Stage 1) / Consistency-aware routing (Stage 2+) | All operations continue to work. |
| Streams | None | Stream records are written to primary in the same transaction as data. |
| TTL | Worker lock needed (Stage 3) | Only one frontend runs TTL worker. |
| Auth/IAM | None | Auth data is in the catalog, read on every request (No Caching Rule). |
| Management console | None | Console reads from catalog like any other request. |
| Metrics | Instance-scoped (Stage 3) | Each frontend reports its own metrics. The `extenddb_metrics` table uses `INSERT` (append-only), so concurrent writes from multiple frontends do not contend. Aggregation queries sum across instance IDs. |
| Import/Export | None | File I/O is local to the frontend that received the request. |
| Strongly consistent GSIs | Compatible | Zero-delay GSI writes already commit atomically with base table writes. Consistency routing handles GSI reads identically to base table reads (§8.2.3). |
| Async GSIs (non-zero delay) | Worker lock needed (Stage 3) | GSI backfill worker uses distributed lock. Async GSI reads on replicas may lag behind primary (acceptable for eventually consistent reads). |

## 16. Success Criteria

1. **Stage 1:** All existing tests pass with `consistent_read` parameter added. No behavioral change.
2. **Stage 2:** A deployment with 1 frontend + 1 primary + 1 replica correctly routes eventually-consistent reads to the replica and strongly-consistent reads to the primary. Verified by query logs. Strongly consistent GSI reads (`ConsistentRead = true` on a zero-delay GSI) route to primary. Eventually consistent GSI reads route to replica.
3. **Stage 3:** Two frontends sharing a catalog can serve concurrent requests without data corruption. Background workers run on exactly one frontend at a time. Verified by concurrent load test.
4. **Stages 4-5:** Storage backend passes the full test suite with the same pass rate as PostgreSQL.
5. **GSI atomicity invariant:** On any replica, a base table row and its corresponding strongly consistent secondary-index state are always visible atomically (never partially). Verified by a test that writes to a table with a strongly consistent GSI, then reads with `ConsistentRead = false` (which routes to a replica), confirming either both the base row and index entry/state are visible or neither is. The test must use eventually consistent reads to target the replica — a strongly consistent read would route to primary and not test replica atomicity.

## 17. Open Questions for Reviewer Deliberation

1. **Replication lag visibility:** Should extenddb expose replication lag to clients (e.g., via a response header)? DynamoDB doesn't, but it could be useful for debugging. **Proposed answer:** No — fidelity tenet applies. Expose only on the authenticated health/management endpoint.

2. **Stale read tolerance:** Should there be a configurable maximum replication lag beyond which eventually-consistent reads fall back to primary? **Proposed answer:** Yes, as a runtime setting (`extenddb settings set max_replica_lag_ms 5000`). Default: no limit (trust the replica). This is an operational safety net, not a fidelity feature.

3. ~~**Worker lock granularity:**~~ **Decided:** Global locks for Stage 3 (see §10). Per-table is a future optimization if profiling shows need.

4. **Instance registry cleanup:** How long before a stale heartbeat entry is considered dead? **Proposed answer:** 5 minutes, configurable via settings. Dead entries are informational only (advisory locks handle real coordination).

5. **Strongly consistent GSIs on non-PostgreSQL backends:** PostgreSQL guarantees that a single transaction's effects replicate atomically (WAL replay). TiDB's native secondary indexes are part of the base table's transactional state. Other databases may have different transaction semantics. **Proposed answer:** Each storage backend must guarantee that a base table write and its corresponding strongly consistent secondary-index state are atomic. PostgreSQL: single transaction. TiDB: native secondary indexes in the same transaction. If a backend cannot provide this guarantee, strongly consistent GSIs are not supported on that backend — this must be surfaced at `CreateTable` time (reject the request), not discovered at write time.

6. **GSI write amplification under HA:** A table with N strongly consistent GSIs increases write work on the primary. PostgreSQL writes companion index rows in the same transaction. TiDB maintains native secondary indexes from the base row, so amplification is handled by TiDB's index maintenance path rather than ExtendDB item replay. **Proposed answer:** This is an operational consideration, not a design change. Document that strongly consistent GSIs increase primary write load proportionally to the number of indexes, with the exact physical cost owned by the backend.

### Resolved Questions

5. **TransactGetItems consistency:** Resolved in §8.2. DynamoDB requires `ConsistentRead = true` for all items in `TransactGetItems`. The operation is always strongly consistent and always routes to primary. Not an open question.

6. **Strongly consistent GSI read routing:** Resolved in §8.2.3. A strongly consistent read on a zero-delay GSI routes to primary (same as any strongly consistent read). No special routing logic needed — the existing `consistent_read: bool` parameter handles it.

7. **Async GSI + strongly consistent read:** Resolved in §8.2.3. Strongly consistent reads are only meaningful on strongly consistent GSIs (zero propagation delay). Attempting a strongly consistent read on an async GSI returns a `ValidationException` with message "Strongly consistent reads are not supported on eventually consistent indexes." This matches DynamoDB's approach of rejecting invalid consistency requests at the API layer.

## 18. References

- [DynamoDB Read Consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)
- [PostgreSQL Streaming Replication](https://www.postgresql.org/docs/current/warm-standby.html)
- [CockroachDB Architecture](https://www.cockroachlabs.com/docs/stable/architecture/overview.html) — inspiration for per-range leadership
- [Cassandra Consistency Levels](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html)
- [MongoDB Read Preference](https://www.mongodb.com/docs/manual/core/read-preference/)
- [FoundationDB Layer Concept](https://apple.github.io/foundationdb/layer-concept.html) — inspiration for storage-agnostic design
