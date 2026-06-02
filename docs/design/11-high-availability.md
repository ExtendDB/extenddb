# extenddb — High Availability Design

**Version:** 0.5 (Draft — Reviewer feedback round 3)
**Date:** 2026-05-08
**Status:** Draft — awaiting reviewer and principal reviewer deliberation
**Phase:** P122
**Applies to:** Both `extenddb` (ExtendDB) and `extenddb` (ExtendDB) with name substitution.

## 1. Problem Statement

extenddb runs as stateless frontend instances backed by one configured storage backend. This design defines how multiple frontends share one durable catalog/data topology, from a single-node deployment to a distributed TiDB-backed deployment.

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
2. If `consistent_read = true` → storage adapter uses the backend's strong read path.
3. If `consistent_read = false` → storage adapter uses the backend's default read path.

For writes: always use the backend's transactional write path.

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

The storage adapter is responsible for mapping the DynamoDB `ConsistentRead` flag to the backend's native read path. The engine passes the raw `consistent_read: bool` to `get_item`, `query`, and `scan`; it does not know whether a backend uses one pool, a replica endpoint, TiDB follower reads, or a clustered SQL gateway.

**Rationale:** Different backends implement replication differently. The storage adapter is the right place to abstract this. TiDB maps default reads to a default-read pool configured with `tidb_replica_read = 'closest-adaptive'` and maps writes plus `ConsistentRead=true` reads to the strong data pool. PostgreSQL currently uses its configured primary pool for both values.

### D3: Leadership Is Per-Catalog, Not Per-Frontend

In deployment models 2 and 3, there is no "leader frontend." All frontends are equal. Leadership (for writes and strong reads) is a property of the catalog node, not the extenddb process.

**Rationale:** DynamoDB's leadership is per-partition, but extenddb doesn't emulate internal partition management. Since all frontends talk to the same catalog, the catalog's primary node is the effective leader. This avoids the complexity of distributed consensus among frontends.

**Known divergence:** DynamoDB's per-partition leadership means a single partition failure doesn't affect other partitions. In extenddb, a catalog primary failure affects all tables. This is an acceptable limitation — extenddb delegates HA to the catalog's native replication, which provides node-level (not partition-level) failover.

**Exception:** If a future storage backend requires frontend-level coordination (e.g., a storage layer with no native replication where extenddb must implement its own replication), a frontend leader election mechanism would be needed. This is deferred.

### D4: Heterogeneous Storage Is Illegal

A single deployment must use a single storage backend type. You cannot mix different backend storage types in one deployment.

**Rationale:** Different backends have different data models, consistency semantics, and transaction capabilities. Mixing them would create an untestable matrix of behaviors. Each deployment is homogeneous.

### D5: Configuration Declares Backend, Backend Owns Topology

The `extenddb.toml` configuration chooses one storage backend and gives that backend its native connection information:

```toml
[storage]
backend = "tidb"

[storage.tidb]
connection_string = "mysql://root@127.0.0.1:4000/extenddb_catalog"
pool_size = 20
catalog_pool_size = 20
```

For TiDB, topology is owned by TiDB itself: SQL nodes, PD, TiKV region leaders, follower reads, online DDL, TTL, and BR are native TiDB capabilities. ExtendDB keeps independently sized engine-catalog, catalog-store/auth, strong-data, and default-read pools, but it does not configure per-replica endpoints or implement storage leadership. The catalog and data pools must resolve to the same TiDB cluster; startup compares TiDB's native `information_schema.cluster_info` topology view when available, and falls back to requiring one shared SQL endpoint and user only on TiDB editions that hide cluster topology metadata.

For PostgreSQL, the current configuration is a single connection string. Any future PostgreSQL topology configuration must be explicit in config and implemented in `storage-postgres`; docs must not imply hidden default-read routing.

### D6: Health Checks and Connection Failover

Each frontend checks the pools that its configured backend actually owns. TiDB health is cluster-oriented: the SQL endpoint must accept catalog, catalog-store/auth, strong-data, and default-read data sessions, and the backend relies on TiDB/PD/TiKV for leader movement, follower availability, online DDL progress, TTL jobs, and BR status. PostgreSQL health checks its catalog metadata, catalog-store/auth, and data pools.

### D7: Connection Pool Sizing

With N frontends, connection count is N times the pools created by the selected backend. TiDB creates one catalog metadata/control-plane pool, one catalog store/authz pool, one strong data pool, and one default-read data pool, and reports metrics across all four pools. Operators should size TiDB SQL nodes for roughly `N * (2 * catalog_pool_size + 2 * pool_size)` ExtendDB sessions before considering other clients. PostgreSQL creates engine catalog and data pools from `pool_size` plus a catalog-store/auth pool from `catalog_pool_size`.

The design does not mandate a specific connection pooler. Operators should follow backend-specific best practices as frontend count grows.

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

### 8.1 Read Consistency Parameter

The `DataEngine` trait methods receive decomposed parameters, not DynamoDB input structs. Read operations therefore carry the DynamoDB `ConsistentRead` value explicitly as `consistent_read: bool`.

This keeps topology out of the engine and avoids adding a storage-crate enum whose only current values duplicate the DynamoDB boolean. The affected methods are:

```rust
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

**`BatchGetItem` routing:** The engine handles `BatchGetItem` by calling `get_item` per key in a loop. `BatchGetItem` has per-table `ConsistentRead` — different tables in the same batch can specify different consistency levels. The `batch_get_item` engine handler passes `ka.consistent_read.unwrap_or(false)` to each `get_item` call. This means a single `BatchGetItem` request may route some reads through the backend's strong path and others through the backend's default-read path, depending on per-table settings. This is correct behavior because each table in a DynamoDB batch independently honors its `ConsistentRead` setting.

For TiDB, `consistent_read = false` selects the read-only data pool configured with `tidb_replica_read = 'closest-adaptive'`; `consistent_read = true` selects the strong data pool. For PostgreSQL, both values currently select the same configured primary data pool.

**TransactGetItems:** DynamoDB requires `ConsistentRead = true` for all items in a `TransactGetItems` request. The operation is always strongly consistent. TiDB runs plain reads inside one TiDB transaction to get a native snapshot without acquiring application-level locks. PostgreSQL uses its normal transaction path.

**Breaking change note:** This is a breaking change to the internal `DataEngine` trait. Since the trait is internal and current implementations live in-tree (`storage-postgres`, `storage-tidb`), no external migration path is needed. The change is mechanical: add the parameter to the trait, the implementations, and all call sites in the engine.

**Alternatives considered and rejected:**

- **Request-scoped context:** Thread a context struct through all storage methods. More extensible but more invasive — every method signature changes, not just reads. Overkill for one DynamoDB flag.
- **Storage enum:** Add a custom read-consistency enum to the storage crate. This adds an abstraction without current semantic gain because DynamoDB exposes exactly one boolean choice.
- **Dual `DataEngine` instances:** The engine selects a strong or default-read backend before calling storage methods. Requires the engine to understand storage topology, violating the abstraction boundary.

### 8.2.1 Paginated Scan Consistency

A paginated scan with `ConsistentRead = true` makes multiple round-trips to the primary. Between pages, writes may occur. Each page is individually strongly consistent, but the full scan is NOT transactionally isolated across pages. This matches DynamoDB's behavior — a strongly consistent scan guarantees each page reflects the latest writes at the time that page was read, not a point-in-time snapshot of the entire table.

### 8.2.2 Locking Reads

Any SQL statement that acquires locks (`SELECT ... FOR UPDATE`) routes to primary regardless of the `consistent_read` parameter. This applies to condition expressions in write paths and `transact_write_items`. Since these are write-path operations that already route to primary, no special handling is needed.

### 8.2.3 Secondary Index Consistency

ExtendDB preserves DynamoDB API behavior: `ConsistentRead=true` on a GSI query or scan is rejected at the engine layer. Storage therefore only receives `consistent_read=true` for base-table reads and LSI reads.

Backend write paths still must keep index state atomic with base-table writes:

1. **TiDB:** GSI and LSI are API metadata only. Physically, TiDB uses generated columns plus native secondary indexes on the same table, and TiDB maintains those indexes transactionally with the base row. There is no local-index compatibility layer, no GSI worker, and no application-level index propagation delay.

2. **PostgreSQL:** The backend owns its companion index-table writes in the same data transaction as the base-row write. Its current read path uses the primary data pool for both strong and default reads.

**Design implications:**

- No special GSI routing exists in storage. The engine rejects unsupported strong-GSI requests before storage sees them.
- TiDB does not need a separate GSI consistency class. Its secondary indexes are native global TiDB indexes.
- TiDB query and scan SQL for an explicit `IndexName` forces the matching native
  secondary index. DynamoDB already made index choice part of the request, so
  ExtendDB should not let stale TiDB statistics choose a different access path.
- A backend that cannot keep index state atomic with base-row writes must reject the corresponding index feature at table-creation or index-update time.

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

## 9. Implementation State

### Implemented: Consistency Parameter Plumbing

The `DataEngine` read methods accept `consistent_read: bool`, and the engine threads the DynamoDB request flag through GetItem, Query, Scan, BatchGetItem, and export scans. PostgreSQL accepts the parameter and currently uses its configured primary data pool for both values.

### Implemented: TiDB-Native Distributed Paths

TiDB uses backend-native primitives instead of ExtendDB ownership layers:

- Default reads use a read-only data pool configured with `tidb_replica_read = 'closest-adaptive'`.
- Strong reads and writes use the strong data pool.
- `TransactGetItems` uses one TiDB transaction snapshot with plain reads.
- GSI and LSI are represented with generated columns plus native TiDB secondary indexes.
- Schema changes rely on TiDB online DDL and idempotent catalog publication.
- TTL uses TiDB native table TTL.
- On-demand backup and restore use TiDB BR metadata instead of row-copy backup payloads.
- `ExportTableToPointInTime` uses TiDB native `AS OF TIMESTAMP` snapshot reads.
- Table-level PITR restore is not emulated; TiDB-native PITR remains a cluster recovery path.
- Capacity governance uses TiDB Resource Control/resource groups rather than
  per-frontend token buckets, so quotas and scheduling are cluster-owned. If
  `storage.tidb.resource_group` is configured, every runtime pool session binds
  to that TiDB resource group.

### Deferred: PostgreSQL Replica Topology

PostgreSQL separate default-read routing is not part of the current configuration. If added later, it must be an explicit `storage-postgres` feature with its own config, health checks, and verification; it must not be implied by generic HA docs.

### Multi-Frontend Coordination Contract

Multiple ExtendDB frontends can share the same TiDB catalog because frontends are stateless, catalog transitions are durable and idempotent, and physical distributed work is delegated to TiDB. Load balancer sticky sessions are not required. Metrics and logs should include instance identity for operational debugging.

## 10. Background Worker Coordination

### Problem

extenddb may run background workers for:
- Control-plane transitions (CREATING → ACTIVE)
- TTL item expiration
- GSI backfill
- Stream record cleanup
- Table size refresh

With multiple frontends, a worker must either be backend-native, idempotent under
concurrent execution, or protected by a backend-native coordination primitive.
TiDB should use the first two options whenever possible: TiDB online DDL owns
distributed schema changes and backfill, and TiDB native TTL owns expiration.

### Solution: Native Coordination First

The best design is to remove the worker-specific coordination problem:

- TiDB control-plane transitions are durable catalog intents. Any frontend may replay them; idempotent `IF EXISTS` / `IF NOT EXISTS` DDL plus conditional catalog publication converges on the TiDB-owned schema state. The whole per-table replay plan is retried on TiDB write conflicts, schema-version races, lock waits, and deadlocks, so transient multi-frontend races re-enter from the catalog intent instead of relying on a partially completed step. A table in `UPDATING` is not protected by an ExtendDB DDL owner: additional compatible GSI, TTL, billing, stream, or delete intent can be appended under a short catalog row transaction while TiDB schedules the physical online DDL.
- TiDB GSI creation uses native secondary indexes. The reconciler batches
  generated-column additions per table, and TiDB online DDL performs distributed
  backfill before maintaining each index transactionally with the base table.
- TiDB TTL uses table-level native TTL. ExtendDB does not run a per-table TTL deletion worker for TiDB user data; the catalog stores explicit TTL transition state so any frontend can complete an interrupted enable or disable using TiDB online DDL, and startup repair re-enables native TTL jobs if TiDB recovery tooling left `TTL_ENABLE = 'OFF'`.
- TiDB diagnostics also stay schema-job-aware. `catalog-check` does not treat
  an old `CREATING`, `UPDATING`, or `DELETING` row as stuck while TiDB reports a
  progressing native DDL job for the physical `_ddb_*` table in
  `information_schema.ddl_jobs`; paused, failed, or missing TiDB DDL progress is
  surfaced as the problem.

For a backend that still needs an application worker, use backend-native
advisory/session locks or an equivalent lease. **Lock granularity should be
global by worker type until profiling proves a finer grain is necessary.**

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

If a backend still needs a residual application worker, a `WorkerLock` trait can follow the same pattern as `DataEngine` and `MetadataEngine`: define the contract in `extenddb-storage` and implement it in each backend that needs non-native coordination. Frontends would attempt to acquire the lock on each worker tick; the holder runs the worker and non-holders skip.

**Lock lifecycle:** Prefer backend-native session locks that are automatically released when the storage connection drops. PostgreSQL can use `pg_try_advisory_lock(worker_type_id)`. TiDB control-plane DDL is different: ExtendDB should persist desired state, let any frontend replay idempotent `IF [NOT] EXISTS` DDL, and rely on TiDB's DDL owner and online schema-job scheduler for distributed ordering and backfill. Only a truly non-idempotent residual worker should use a backend-native/session-scoped equivalent or a catalog lease when a session lock does not satisfy the worker's failure semantics. This means:
- No explicit TTL/lease mechanism is needed when the backend provides crash-released session locks.
- A crashed frontend's locks are released when the backend cleans up the dead connection.
- The instance registry heartbeat (§13) is for **observability only**, not for lock management.

## 11. Failure Modes and Recovery

| Failure | Impact | Recovery |
|---------|--------|----------|
| Frontend crash | Requests to that frontend fail. Load balancer routes to others. | Restart frontend. No data loss. |
| TiDB SQL node unavailable | Affected frontend connections fail until TiDB or the operator-provided SQL endpoint routes to a healthy SQL node. | Repair SQL node or endpoint routing. |
| TiKV/PD disruption | Writes, strong reads, online DDL, TTL, or BR may fail according to TiDB cluster health. | Recover the TiDB cluster using TiDB operational procedures. |
| PostgreSQL primary unavailable | Current PostgreSQL backend reads and writes fail until the configured connection string points at a healthy primary. | Promote/repair PostgreSQL using the operator's HA process, then reconnect. |
| Network partition (frontend ↔ catalog) | Affected frontend returns 500. Others continue. | Resolve network issue. |
| Split brain (two primaries) | Prevented by catalog's own replication protocol. extenddb does not manage catalog failover. | N/A — delegated to catalog HA. |

### 11.1 Backend Failover Strategy

ExtendDB does not run a catalog failover protocol. It opens pools to the configured backend endpoint and lets the backend's native HA layer own failover:

- TiDB: SQL-node load balancing, PD leader movement, TiKV region leadership, online DDL ownership, TTL scheduling, and BR recovery stay inside TiDB.
- PostgreSQL: the current backend follows the configured connection string. A future PostgreSQL replica feature must add explicit topology config and health checks in `storage-postgres`.

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
- N frontends, backend-native HA topology (Model 3/4) ✓
- N frontends, K-node cluster (Model 4) ✓

### Illegal Configurations

- Mixed storage backends in one deployment ✗
- Backend topology options that are documented but not implemented by that backend ✗
- Multiple independent write leaders for one catalog ✗ (use the storage backend's own failover/consensus)

### Startup Validation

On startup, each frontend:
1. Connects to the configured catalog endpoint and verifies schema version.
2. Connects to the backend-owned data pools that the selected backend creates.
3. Lets the backend validate any native topology it owns.
4. Registers frontend identity only for observability when that feature is enabled.

**Instance registry purpose:** The `extenddb_instances` table is for **observability and operational tooling only**. It answers "which frontends are running?" for operators. It is NOT used for lock management or coordination when the backend provides session-scoped locks; otherwise a dedicated backend lease table owns correctness. Dead entries (heartbeat older than 5 minutes, configurable via `extenddb settings set instance_heartbeat_timeout_seconds`) are cleaned up periodically but their presence has no correctness impact.

## 14. Observability

### Instance Identification

Each frontend gets a unique instance ID (UUID generated at startup). All log messages and metrics include this ID.

### Health Endpoint

The current unauthenticated `GET /health` endpoint returns a minimal liveness payload. A future authenticated management endpoint can expose backend topology and worker status because those details may include internal endpoints, native job state, and operational errors.

Response:
```json
{
  "status": "healthy",
  "instance_id": "550e8400-e29b-41d4-a716-446655440000",
  "catalog": {
    "endpoint": "healthy"
  },
  "tidb": {
    "online_ddl": "healthy",
    "ttl": "healthy",
    "br": "available"
  },
  "workers": {
    "control_plane": "active",
    "ttl_expiration": "standby",
    "stream_cleanup": "active"
  }
}
```

### Metrics

Useful metrics for HA monitoring:
- Backend pool active/idle connection counts per pool.
- Native backend job health where available (TiDB online DDL, TTL, BR).
- `extenddb_consistency_routing_total` (counter, labels: level=strong|default, backend_path=backend-defined)
- Worker-lock metrics only for backends that still need non-native worker coordination.

## 15. Impact on Existing Features

| Feature | Impact | Notes |
|---------|--------|-------|
| DynamoDB API operations | Consistency-aware storage routing | All operations continue to work. |
| Streams | TiDB-native scattered writes | Stream records are written in the same transaction as data. TiDB schemas use an `AUTO_RANDOM` clustered `stream_records.record_id` for write distribution, while the `(shard_id, commit_sequence_number)` index preserves ordered shard reads. |
| TTL | Backend-native where available | TiDB uses native table TTL and has no ExtendDB TTL deletion worker. Backends without native TTL need exactly-one worker coordination. |
| Auth/IAM | None | Auth data is in the catalog, read on every request (No Caching Rule). |
| Management console | None | Console reads from catalog like any other request. |
| Metrics | TiDB-native append-only samples | TiDB writes immutable `metrics_samples` rows with native `AUTO_RANDOM` clustered IDs and native TTL, so concurrent frontends do not contend on shared aggregate rows. Aggregation queries sum samples by bucket and metric labels. |
| Login rate limiting | TiDB-sharded append-only attempts | TiDB stores failed-login attempts with native TTL and `SHARD_ROW_ID_BITS`, so concurrent frontend inserts use sharded implicit row IDs instead of a single hot row-id range. |
| Import/Export | None | File I/O is local to the frontend that received the request. |
| Strongly consistent GSIs | API-compatible | DynamoDB-compatible GSI reads reject `ConsistentRead=true`; backend index writes remain atomic with base-row writes (§8.2.3). |
| Async GSIs (non-zero delay) | Backend-specific | TiDB does not use an async GSI worker; native online DDL performs backfill and native indexes are maintained with base-table writes. A backend with delayed application-level GSI propagation needs worker coordination. |

## 16. Success Criteria

1. All existing tests pass with `consistent_read` threaded through storage reads.
2. TiDB default reads use the follower-read pool and strong reads use the strong pool.
3. Two frontends sharing a TiDB catalog can serve concurrent table, item, TTL, stream, backup, and index operations without application-level DDL ownership.
4. TiDB secondary-index state is always transactional with the base row because TiDB owns native secondary indexes.
5. The TiDB backend passes the same workspace test suite gates as the default build.

## 17. Open Questions for Reviewer Deliberation

1. **Backend lag visibility:** Should extenddb expose backend lag to clients (e.g., via a response header)? DynamoDB doesn't, but it could be useful for debugging. **Proposed answer:** No — fidelity tenet applies. Expose only on authenticated management endpoints.

2. **Default-read path health:** Should a backend expose policy for when default reads fall back to its strong path? **Proposed answer:** Backend-specific. TiDB should follow TiDB's own follower-read behavior rather than duplicating health policy in ExtendDB.

3. ~~**Worker lock granularity:**~~ **Decided:** Global locks for Stage 3 (see §10). Per-table is a future optimization if profiling shows need.

4. **Instance registry cleanup:** How long before a stale heartbeat entry is considered dead? **Proposed answer:** 5 minutes, configurable via settings. Dead entries are informational only (advisory locks handle real coordination).

5. **Index atomicity on future backends:** TiDB's native secondary indexes are part of the base table's transactional state. PostgreSQL companion indexes are updated in the same transaction as the base row. Other databases may have different transaction semantics. **Proposed answer:** Each storage backend must guarantee that base-row and secondary-index state are atomic, or reject the index feature at `CreateTable`/`UpdateTable` time.

6. **GSI write amplification under HA:** A table with N GSIs increases backend write work. PostgreSQL writes companion index rows in the same transaction. TiDB maintains native secondary indexes from the base row, so amplification is handled by TiDB's index maintenance path rather than ExtendDB item replay. **Proposed answer:** This is an operational consideration, not a design change.

### Resolved Questions

5. **TransactGetItems consistency:** Resolved in §8.2. DynamoDB requires `ConsistentRead = true` for all items in `TransactGetItems`. TiDB uses one transaction snapshot with plain reads. Not an open question.

6. **Strongly consistent GSI read routing:** Resolved in §8.2.3. DynamoDB-compatible GSI reads reject `ConsistentRead=true` before storage routing.

7. **Async GSI worker:** Resolved in §8.2.3 and §10. TiDB has no async GSI worker; native online DDL handles backfill and native indexes are maintained with base-row writes.

## 18. References

- [DynamoDB Read Consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)
- [PostgreSQL Streaming Replication](https://www.postgresql.org/docs/current/warm-standby.html)
- [CockroachDB Architecture](https://www.cockroachlabs.com/docs/stable/architecture/overview.html) — inspiration for per-range leadership
- [Cassandra Consistency Levels](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html)
- [MongoDB Read Preference](https://www.mongodb.com/docs/manual/core/read-preference/)
- [FoundationDB Layer Concept](https://apple.github.io/foundationdb/layer-concept.html) — inspiration for storage-agnostic design
