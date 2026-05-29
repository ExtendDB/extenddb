# Auth / Authz Cache — Design

> Status: implemented (single-instance). Multi-instance fanout in
> Appendix B is designed but deferred — implementation is gated on a
> separate review.
> Author: assisted, reviewed by repository owner
> Companion document to: [05-component-auth.md](05-component-auth.md), [06-component-server.md](06-component-server.md)
>
> Note: this cache is the deliberate revision of the "No In-Process State"
> rule in [`11-high-availability.md`](11-high-availability.md) §D1. The
> revision is scoped to IAM data only (credentials, policies, tags, table
> key info). The data path remains uncached.

## 1. Motivation

Each DynamoDB request issues 6+ catalog queries before dispatch can begin:

| # | Data | Source |
|---|------|--------|
| 1 | Decrypted credential lookup | `DbCredentialStore::lookup_credential` |
| 2 | Identity policies (user or role) | `AuthorizationStore::fetch_user_policies` / `fetch_role_policies` |
| 3 | Group policies (users only) | `fetch_user_group_policies` |
| 4 | Permissions boundary | `fetch_user_boundary` / `fetch_role_boundary` |
| 5 | Principal tags | `fetch_user_tags` / `fetch_role_tags` (+ `fetch_session_data` for role sessions) |
| 6 | Resource tags | `fetch_resource_tags` |
| 7 | TableKeyInfo (item-level ops only) | `request_helpers.rs::authorize_request` (serial, outside the `try_join!`) |

Queries 2–6 fan out concurrently via `tokio::try_join!` (`crates/server/src/authorization.rs:82`), but they still each consume a connection from the catalog pool. With a 1024-connection catalog pool (post-PR #46), the pool itself is no longer the limit — but the **per-request DB roundtrips and JSON-parse cost** are now the dominant overhead before dispatch.

In addition, every call to `fetch_policies` reparses policy JSON via `PolicyDocument::from_json` (`crates/server/src/authorization.rs:189`). Caching parsed `PolicyDocument` values eliminates that cost, not just the DB roundtrip.

This document specifies an in-memory cache with stale-while-revalidate semantics that eliminates the steady-state cost of these queries while preserving correctness on credential rotation and policy change.

## 2. Goals

- Eliminate the per-request catalog roundtrips for credentials, policies, boundaries, principal tags, resource tags, and TableKeyInfo on the steady-state hot path.
- Eliminate the per-request `PolicyDocument::from_json` parse cost.
- Refresh stale entries **without** making the user request wait (refresh-ahead, a.k.a. stale-while-revalidate).
- Bound memory with an LRU upper limit per cache.
- Allow operators to disable the cache entirely as a kill switch.
- Propagate self-induced changes (admin API mutations) instantly within a single process via write-through invalidation hooks.

## 3. Non-goals

- **Cross-process cache coherence** for multi-instance deployments. Fanout invalidation across nodes is out of scope. Operators running multiple instances accept the configured TTL as the worst-case lag for off-instance changes. This is documented and made explicit.
- **Persistence across restarts.** Cold-start performance is identical to today; warm-up is fast (a few seconds at production traffic).
- **Caching mutable, transactional state** like throttle buckets or stream cursors. This document is strictly about IAM and table metadata.
- **Caching successful authorization decisions.** We cache the inputs (policies, tags), not the verdict. Re-evaluating the policies in CPU is essentially free once the inputs are cached, and verdict caching has subtle correctness issues with conditions that depend on per-request context.

## 4. Refresh strategy: stale-while-revalidate (SWR), not background scan

Each cache entry has two timestamps:

```
fetched_at ──── soft_ttl ──── hard_ttl ──── ∞
              fresh        stale-but-usable   miss
```

| State | Action |
|-------|--------|
| `now − fetched_at < soft_ttl` | Return cached value. No work. |
| `soft_ttl ≤ now − fetched_at < hard_ttl` | Return cached value. **Spawn** a tokio task to refetch and replace. |
| `now − fetched_at ≥ hard_ttl` | Cache miss. Await refetch. Single-flight: concurrent misses for the same key share one in-flight load. |

**Why not a background worker that periodically scans and refreshes the cache?**

- It refreshes entries that may never be touched again (waste).
- It must walk the cache, contending on the very lock we're trying to take off the hot path.
- Refresh-on-access naturally distributes refresh load over time.
- Under any non-trivial load, every entry is touched well before its hard TTL anyway.

A background scan only adds value under traffic so light that some hot entry might cross the soft-TTL boundary and stay there until eviction. That is not a real scenario for this system.

**Why not "if-modified-since" / version-column refresh?**

Considered: fetch a cheap `updated_at` first, only refetch full data if changed. Rejected. The full refetch query has the same RTT as the change-check query (network is the bottleneck, not row size), and the saved JSON parse is a small fraction of the total cost. The complexity of a two-query refetch isn't worth the marginal gain.

## 5. Negative caching

Negative results (`Ok(None)` — key not found) are cached for a **short** TTL (default: 5 seconds). Reasons:

- Stops attackers from hammering the catalog with random `AKIA...` access keys.
- 5s ensures newly-created keys become usable quickly without manual cache flush.

**Errors are never cached.** Transient DB failures must not poison the cache.

## 6. Invalidation: write-through hooks

When the management API **or the web console** mutates IAM data, it invalidates the corresponding cache entry **synchronously, in-process**. Both admin paths share `AuthCacheRegistry` and call the same hooks, so it doesn't matter whether an admin clicks through the console or hits the management API: self-induced changes propagate instantly on the local instance.

| Mutation | Invalidates |
|----------|-------------|
| `CreateAccessKey`, `DeleteAccessKey`, `ImportAccessKey` | credential cache for that `access_key_id` |
| `AssumeRole` | credential cache for the new ASIA*; session_data for `(account, role, session_name)` |
| `PutUserPolicy`, `DeleteUserPolicy` | user-policies cache for `(account_id, user_name)` |
| `AddUserToGroup`, `RemoveUserFromGroup` | user-group-policies cache for `(account_id, user_name)` |
| `PutGroupPolicy`, `DeleteGroupPolicy` | user-group-policies for **every member** of that group (cheap: lookup membership, invalidate each) |
| `PutUserPermissionsBoundary`, `DeleteUserPermissionsBoundary` | user-boundary cache |
| `TagUser`, `UntagUser` | user-tags cache |
| `PutRolePolicy`, `DeleteRolePolicy` | role-policies cache for `(account_id, role_name)` |
| `PutRolePermissionsBoundary`, `DeleteRolePermissionsBoundary` | role-boundary cache |
| `TagRole`, `UntagRole` | role-tags cache |
| `TagResource`, `UntagResource` | resource-tags cache for that ARN |
| `CreateTable`, `DeleteTable`, `UpdateTable` (key schema, indexes, streams, throughput) | TableKeyInfo cache for `(account_id, table_name)`; `CreateTable` and `DeleteTable` also invalidate `resource_tags` for the table ARN |
| `ImportTable`, `RestoreTableFromBackup` | TableKeyInfo cache for the new table |
| `DeleteUser` | credential (per-key + principal-fanout), user-policies, user-group-policies, user-boundary, user-tags |
| `DeleteRole` | credential (principal-fanout for ASIA*), role-policies, role-boundary, role-tags, all session_data for the role |
| `DeleteGroup` | user-group-policies for every member |
| `DeleteAccount` | every cached entry across every authz subcache + credentials, scoped to the account_id |

`UpdateTimeToLive` does NOT invalidate `TableKeyInfo` because TTL state is not part of `TableKeyInfo`.

A cross-process invalidation channel is **not** added in this iteration. Multi-node deployments document the configured TTL as the worst-case lag.

## 6.1. Manual invalidation: admin break-glass API

Write-through hooks (§6) cover every IAM mutation we own. They do **not** cover off-instance changes on multi-node deployments before TTL kicks in, bugs in the write-through plumbing, or test scenarios that need a deterministic flush. For those cases, an admin-authenticated **manual invalidation API** complements the automatic hooks.

Both the management API and the web console expose this surface; the CLI (`extenddb manage cache invalidate ...`) is a thin client over the management API. The console route and the management API route call the same handler logic, so behavior is identical regardless of how it's invoked. All three paths use the existing admin authentication mechanism (Basic auth for the API, session-based auth for the console — neither introduces a new auth path).

### HTTP endpoint

`POST /management/cache/invalidate` — admin-only, JSON body, returns 200 with an audit-friendly response or 400 on malformed scope.

```json
{ "scope": "<scope-tag>", "selectors": { ... } }
```

| `scope` | Required selectors | Maps to (in `AuthCacheRegistry`) |
|---------|--------------------|----------------------------------|
| `all` | (none; requires explicit confirmation) | every authz subcache + credentials + table_key_info |
| `account` | `account_id` | `invalidate_account` |
| `credential` | `access_key_id` | `invalidate_credential` |
| `user` | `account_id`, `user_name` | composite: user_policies + user_group_policies + user_boundary + user_tags + principal_credentials |
| `role` | `account_id`, `role_name` | composite: role_policies + role_boundary + role_tags + role_sessions + principal_credentials |
| `group_members` | `account_id`, `user_names[]` | `invalidate_users_group_policies` |
| `table_key_info` | `account_id`, `table_name` | `invalidate_table_key_info` |
| `resource_tags` | `arn` | `invalidate_resource_tags` |

Composite scopes (`user`, `role`) match what an operator most often actually wants when they reach for this tool ("forget everything about user X"). The narrow scopes are still exposed for surgical use. `all` is the broadest hammer and requires `--yes` from the CLI / a confirmation token from the console.

`account` sweeps the authz and credential caches but not `table_key_info` (the `TableKeyInfo` cache is keyed by `(account, table)` and the registry's `invalidate_account` does not currently fan out across it). To clear cached table-key-info for an account, either call `scope: table_key_info` per table or use `scope: all`. This is a deliberate scoping choice — the table-key-info cache rarely diverges per account in practice, so the narrower `invalidate_account` semantics keep the most common path cheap.

### Response shape

```json
{ "scope": "user", "invalidated": ["user_policies", "user_group_policies", "user_boundary", "user_tags", "principal_credentials"] }
```

The `invalidated` array tells operators which subcaches were touched — useful for confirming that composite scopes did what was expected, and for the console UI's success banner.

### Audit trail

Every invocation logs at INFO via `tracing`:

```
auth_cache: admin-triggered invalidation scope=user account_id=... user_name=... admin=alice
```

Cache invalidation counters on each subcache (exposed via `/management/auth-cache-metrics`) also increase. The structured log line gives forensic context ("who flushed, when, why") that the counter alone cannot.

### Behavior when caches are disabled

When `auth.cache.enabled = false`, the cache layer is in pass-through mode and there is nothing to invalidate. The endpoint still returns 200 — the `AuthCacheRegistry` invalidate methods are no-ops in this state, and the operator's contract ("cache-driven staleness for X is now cleared") is trivially satisfied.

### CLI

```
extenddb manage --user admin --password ... cache invalidate <scope> [...selectors]
```

Concrete subcommands match the `scope` taxonomy 1:1: `cache invalidate all --yes`, `cache invalidate account --account-id ...`, `cache invalidate user --account-id ... --user-name ...`, etc. The `all` form requires `--yes` mirroring `extenddb destroy` and `import-access-key`. Authentication is the standard admin Basic-auth flow shared with every other `manage` subcommand.

### Console

A new admin-only page at `/console/cache` renders a form whose scope `<select>` reveals the relevant selector inputs via JS. Submitting POSTs to `/console/cache/invalidate` (CSRF-protected, admin-gated). The page also shows a live read of `/management/auth-cache-metrics` so operators can see what's cached before flushing it. Submitting `scope=all` requires typing `INVALIDATE` into a confirmation field, mirroring the CLI's `--yes`.

### What this is *not*

- **Not cross-instance.** Manual invalidation only clears the local instance's caches. Operators of multi-frontend deployments must call this on each instance, or wait `ttl_seconds` for natural expiry. Cross-instance fanout is the same Appendix B work that's deferred from automatic invalidation; it would land for both paths together.
- **Not a cache inspector.** "What's currently cached for user X?" has different threat-model implications (admin-readable mirror of cached credential material) and is out of scope.
- **Not for routine use.** This is break-glass tooling; the write-through hooks (§6) remain the primary mechanism.

## 7. Configuration

Static configuration in `extenddb.toml`:

```toml
[auth.cache]
# All fields optional; defaults shown.
enabled            = true
ttl_seconds        = 60   # hard TTL — entries beyond this are full misses
soft_ttl_seconds   = 30   # entries beyond soft_ttl trigger background refresh on access
negative_ttl_seconds = 5  # how long to cache "not found" results
max_entries        = 10000 # per-cache LRU size limit
```

Default rationale:

- **60 s hard TTL** mirrors AWS IAM's "policy propagation" expectation (~30 s–2 min). Long enough to absorb the perf bottleneck (60 s of caching → 99.99%+ hit rate per hot key under any meaningful load), short enough to bound the blast radius of off-instance credential revocation.
- **30 s soft TTL** = TTL / 2 keeps refreshes from clustering at the boundary.
- **5 s negative TTL** is short enough that newly-issued keys work quickly, long enough to absorb a dictionary attack against `lookup_credential`.
- **10 000 entries per cache** is a generous default for typical deployments; under memory pressure the LRU evicts cold entries.
- **`enabled = false`** is a kill switch for incident response.

The cache is configured statically (`extenddb.toml`). Runtime tunability is not added — cache TTL changes are rare and a server restart is acceptable. Adding it to the existing `settings` poller table is an option for a follow-up if operationally needed.

## 8. Architecture: decorator pattern, one cache per data shape

Caching is layered as **wrapper types** over the existing trait implementations, not inlined into the storage layer. This preserves a clean test seam and keeps the storage-postgres crate focused on Postgres.

```
                                 ┌─────────────────────────┐
                                 │ AuthProvider            │
                                 │ (BuiltinAuthProvider)   │
                                 └────────────┬────────────┘
                                              │
                                              ▼
                              ┌────────────────────────────────────┐
                              │ CachedCredentialStore<Inner>       │  ← new
                              │   .lookup_credential() with SWR    │
                              └────────────┬───────────────────────┘
                                           │
                                           ▼
                              ┌────────────────────────────────────┐
                              │ DbCredentialStore                  │  ← existing
                              │   (PostgreSQL queries)             │
                              └────────────────────────────────────┘


                       ┌──────────────────────────────────────────┐
                       │ authorization::check_authorization       │
                       │ (now uses parsed PolicyDocuments)        │
                       └──────────────────┬───────────────────────┘
                                          │
                                          ▼
                       ┌─────────────────────────────────────────────┐
                       │ CachedAuthorizationStore                    │  ← new
                       │   wraps Arc<dyn AuthorizationStore>         │
                       │   caches:                                   │
                       │     user/role policies → Vec<Arc<PolicyDoc>>│
                       │     boundaries        → Option<Arc<PolicyDoc>>│
                       │     principal tags    → HashMap<String, String>│
                       │     resource tags     → HashMap<String, String>│
                       │     session data      → SessionData          │
                       └────────────────┬────────────────────────────┘
                                        │
                                        ▼
                       ┌─────────────────────────────────────────────┐
                       │ PostgresCatalogStore                         │
                       │ (impl AuthorizationStore via JSON columns)   │
                       └─────────────────────────────────────────────┘


                              ┌────────────────────────────────────┐
                              │ CachedTableKeyInfoStore (helper)   │  ← new
                              │   wraps Arc<dyn StorageEngine>     │
                              │   on .table_key_info()             │
                              └────────────┬───────────────────────┘
                                           │
                                           ▼
                              ┌────────────────────────────────────┐
                              │ PostgresEngine::fetch_table_key_info│
                              └────────────────────────────────────┘
```

The auth cache stores **parsed `PolicyDocument`s** wrapped in `Arc<PolicyDocument>` — multiple cache entries (e.g. a shared inline policy attached to several users) share the parsed document via `Arc::clone`. The `check_authorization` path is updated to call a new method on the cached wrapper that returns parsed documents, eliminating the `from_json` step from the request hot path.

## 9. Library choice: `moka`

The implementation uses the `moka` crate (`features = ["future"]`):

- Async-aware (`moka::future::Cache`) integrates cleanly with `tokio`.
- Built-in time-based expiration with O(1) eviction.
- Bounded with LRU/W-TinyLFU eviction policy under memory pressure.
- **Single-flight** via `entry().or_insert_with(future)` — concurrent misses for the same key share one load future. This is critical: prevents thundering herd at cache cold-start.
- Internal sharding — no global lock contention.

The SWR layer is a thin (~80 line) wrapper on top of `moka` that adds the soft-TTL / spawn-refresh logic. We do **not** hand-roll the cache itself; that would be 200+ lines of subtle concurrency code with the same external behavior.

The `moka` `future` feature uses `tokio` internals consistent with the rest of the codebase. License is MIT/Apache-2.0 — compatible with our Apache-2.0 license.

## 10. Cache key shapes and value types

```rust
// Credentials
key:   String                // access_key_id (e.g. "AKIA...")
value: Option<StoredCredential>

// Identity policies (user or role) — parsed
key:   (String, String)      // (account_id, principal_name)
value: Vec<Arc<PolicyDocument>>

// Group policies for a user — flattens membership server-side
key:   (String, String)      // (account_id, user_name)
value: Vec<Arc<PolicyDocument>>

// Boundaries — parsed
key:   (String, String)      // (account_id, principal_name)
value: Option<Arc<PolicyDocument>>

// Principal tags
key:   (String, String)      // (account_id, principal_name)
value: HashMap<String, String>

// Resource tags
key:   String                // ARN
value: HashMap<String, String>

// Role-session data
key:   (String, String, String) // (account_id, role_name, session_name)
value: Option<SessionData>      // already a struct, no parsing

// TableKeyInfo
key:   (String, String)      // (account_id, table_name)
value: Result<TableKeyInfo, NotFoundOrInactive>
```

`Arc<PolicyDocument>` is the value type for parsed policies so multiple cache entries can share the underlying document without cloning.

## 11. Metrics

Per-cache counters and entry counts are exposed as a JSON snapshot at the
admin-authenticated `GET /management/auth-cache-metrics` endpoint (Basic
auth, admin user). The response shape is:

```json
{
  "credential":     { "hits": …, "stale_hits": …, "misses": …,
                       "negative_hits": …, "refresh_success": …,
                       "refresh_failure": …, "refresh_skipped_inflight": …,
                       "refresh_dropped_epoch": …, "invalidations": …,
                       "entry_count": … },
  "table_key_info": { … same fields … },
  "authz": {
    "user_policies":       { … same fields, no entry_count … },
    "user_group_policies": { … },
    "user_boundary":       { … },
    "user_tags":           { … },
    "role_policies":       { … },
    "role_boundary":       { … },
    "role_tags":           { … },
    "session_data":        { … },
    "resource_tags":       { … }
  }
}
```

| Field | Description |
|-------|-------------|
| `hits` | Fresh cache hits (younger than `soft_ttl`). |
| `stale_hits` | Hits between `soft_ttl` and `ttl` — served immediately, refresh spawned. |
| `misses` | Hard misses (caller waited for the loader). |
| `negative_hits` | Cached `None` returned (negative cache). |
| `refresh_success` | Background refresh wrote a fresh entry. |
| `refresh_failure` | Background refresh failed (stale entry retained until hard TTL). |
| `refresh_skipped_inflight` | Refresh trigger skipped because one was already running for the entry. |
| `refresh_dropped_epoch` | Refresh result discarded (an explicit invalidation or a hard miss replaced the slot). |
| `invalidations` | Explicit `invalidate*` calls (admin mutations). |
| `entry_count` | Best-effort current size (top-level caches only). |

The endpoint is admin-only because cache hit-rate and entry-count signals
are workload-fingerprinting telemetry. A future revision may add Prometheus
gauges/counters under `/metrics`; until then, scrape the JSON endpoint.

## 12. Error semantics

- DB errors during a **request-blocking load** propagate to the caller as today (`InternalServerError`).
- DB errors during a **background refresh** are logged and counted in `auth_cache_refresh_failure`. The stale entry is **not** evicted — the next request continues serving it (until the hard TTL passes, at which point it transitions back to a request-blocking load).
- Decrypt failures during a credential load return `InternalServerError` and are not cached.

## 13. Test strategy

**Unit tests** (per-wrapper):

1. Hit path: second call to the wrapper does not invoke the inner store.
2. TTL: after `ttl_seconds`, the next call invokes the inner store.
3. SWR: between `soft_ttl` and `ttl`, the call returns immediately and the inner is invoked exactly once on a background task.
4. Single-flight: `N` concurrent misses for the same key invoke the inner store exactly once.
5. Negative cache: `Ok(None)` is cached, expires at `negative_ttl`.
6. Errors are not cached.
7. Invalidation: `invalidate(key)` causes the next call to miss.
8. LRU bound: `max_entries + 1` insertions evict an entry.

**Integration tests** (end-to-end against a running server):

- 1000 sequential `GetItem` calls with the same access key produce **1** credential lookup against the inner DB. Verified via a counter on the inner store wired in test mode.
- Mutating `Put-User-Policy` causes the next `PutItem` to see the new policy (write-through invalidation works end-to-end).
- After deletion of an access key, in-flight requests using the cached value succeed only until the next invalidation; new requests see `UnrecognizedClientException`.
- Deletion of an IAM user invalidates all its caches.
- `auth.cache.enabled = false` produces identical behavior to today (regression check).

**Property tests** (focused on the SWR cache primitive):

- Concurrent reads + occasional writes never observe a stale value beyond `ttl_seconds`.
- Concurrent invalidation + reads always observe the post-invalidation value on the next call.

## 14. Phased rollout

| Phase | Scope | Risk |
|-------|-------|------|
| 1 | `SwrCache<K, V>` primitive + `moka` dep | low — pure library code, no behavior change |
| 2 | `CachedCredentialStore` + invalidation hooks for access-key mutations | medium — touches auth path |
| 3 | `CachedAuthorizationStore` (with parsed `PolicyDocument` cache) + invalidation hooks for IAM mutations | medium — touches authorization path |
| 4 | `CachedTableKeyInfoStore` + invalidation hooks on `CreateTable`/`UpdateTable`/`DeleteTable` | low |
| 5 | Documentation, config sample, `init` template, admin guide, metrics console row | trivial |

Each phase is a separate PR. After each phase the test suite runs green and the system is shippable. Phases 2/3/4 each independently improve hot-path performance.

## 15. Operator guide (excerpt for admin manual)

> ExtendDB caches authentication and authorization data in memory to eliminate per-request catalog lookups. The cache uses **stale-while-revalidate**: stale entries are served immediately while a background refresh runs.
>
> **Propagation timing for invalidations**:
>
> - **Single-key invalidations** (e.g. `DeleteAccessKey`, `PutUserPolicy`) bump an internal epoch counter and drop the moka entry immediately. The next request blocks on a fresh load.
> - **Fanout invalidations** (e.g. `DeleteAccount`, `DeleteRole`'s session sweep, `DeleteGroup`'s member fanout) use moka's `invalidate_entries_if`, which is **asynchronous** — predicate evaluation runs on moka's internal worker after the management call returns. There is a small window (~ms) between the API responding 200/204 and the matching entries actually being evicted from the underlying store. The accompanying epoch bump ensures any in-flight refresh on those keys is dropped on completion, but a request that reads the slot during this window can still see the pre-invalidation value. Operators should treat fanout invalidation as "eventually consistent within a few ms"; for hard cutover (e.g. revoking a compromised key), prefer the single-key invalidations (`DeleteAccessKey`) over the cascade ones.
> - **Off-instance changes** (a separate process modifying the catalog directly, or a different instance in a multi-node deployment) take up to `auth.cache.ttl_seconds` to propagate. The cross-process invalidation channel described in Appendix B is not yet implemented.
>
> Self-induced single-key changes via the admin API or the web console propagate instantly on the local instance via write-through invalidation; both paths share the same `AuthCacheRegistry`.
>
> To force-flush the cache without restart: not currently supported. To disable: set `auth.cache.enabled = false` in `extenddb.toml` and restart.

---

## Appendix A — alternatives considered

- **No cache, just larger pool.** Insufficient. The catalog roundtrip itself is ~1 ms on a co-located DB; per-request 6× roundtrips × 1 ms = 6 ms of pre-dispatch latency that is impossible to remove without caching. No pool size fixes this.
- **Verdict cache (cache `Allow`/`Deny` for `(principal, action, resource)`).** Considered. Rejected: condition expressions can depend on per-request context (`aws:CurrentTime`, request IP, leading keys, attribute names) that varies per call. Caching the verdict requires either invalidating on every condition-relevant input change (impractical) or only caching for a subset of verdicts (complexity not worth it). Caching the inputs and re-evaluating in CPU per request gives the same throughput at lower complexity and zero correctness risk.
- **Hand-roll the cache primitive.** Rejected. ~200 lines of concurrent code we'd then have to maintain, vs. ~80 lines on top of `moka`. `moka` is well-tested and widely used.
- **Background scan worker for refresh.** Rejected, see §4.
- **Cross-process invalidation via Postgres `LISTEN/NOTIFY`.** Considered for the multi-node case. Out of scope for this iteration; see Appendix B for the full proposed design.
- **Long TTL (5 minutes or more).** Rejected. The marginal perf gain over 60 s is negligible (cache hit rates are already 99.9%+ at 60 s under load). Long TTLs trade safety for nothing.

---

## Appendix B — Multi-instance cache invalidation (deferred)

> **Status:** designed, **not implemented**. Single-instance correctness is the focus of the initial cache PR; multi-instance fanout requires additional engineering review (failure modes, schema, ordering guarantees) before implementation.
>
> When this lands, every IAM-mutation handler will replace its single in-process `invalidate_*` call with a transactional pair: enqueue a row + `pg_notify`. Listeners on every other instance receive the notification and apply the local invalidation.

### B.1 Why the current state is not enough for multi-instance deployments

In a multi-instance deployment (multiple extenddb processes sharing a catalog DB), each instance's in-memory cache is local. Self-induced changes propagate instantly **on the local instance** via the existing write-through hooks. Off-instance changes — an admin-API mutation on instance A, where instance B is serving the same principal — only become visible on instance B when its cache entry expires (`auth.cache.ttl_seconds`, default 60 s).

For an auth/authz cache, "60 seconds of stale policy" can mean a deleted access key still authenticates, or a Deny statement still being shadowed by a stale Allow. That window is acceptable for soft state but not for IAM revocation.

### B.2 Proposed architecture: Postgres LISTEN/NOTIFY + a durable backstop table

```text
   Instance A           Instance B           Instance C
       │                    │                    │
       │  pg_notify(...)    │                    │
       │   (in TX commit)   │                    │
       │  ──────────────►   │  (LISTEN task)     │
       │                    │  ──────────────►   │  (LISTEN task)
       │
       │  Same NOTIFY also delivered to A's listener — skipped via instance_id
       │
   cache_invalidations table (durable backstop for reconnect catch-up)
```

**Schema** (new migration, additive):

```sql
CREATE TABLE cache_invalidations (
    seq          BIGSERIAL PRIMARY KEY,
    cache_name   TEXT NOT NULL,           -- "credential" | "user_policies" | ...
    key_payload  JSONB NOT NULL,          -- shape depends on cache_name
    instance_id  UUID NOT NULL,           -- sender's identity, used to skip self
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX cache_inval_seq_idx ON cache_invalidations (seq);
CREATE INDEX cache_inval_created_at_idx ON cache_invalidations (created_at);
```

**Write path.** Every IAM-mutation handler that today calls `state.auth_cache.invalidate_*(...)` does, **in the same transaction as the catalog write**:

1. INSERT a row into `cache_invalidations`. `RETURNING seq` gives the assigned sequence number.
2. `SELECT pg_notify('extenddb_cache', payload)` where `payload` is `{"seq", "cache", "key", "from": <instance_uuid>}`.
3. Locally invalidate via the existing in-process call (so the sending instance's cache is correct without a network roundtrip).

Postgres delivers NOTIFY only on commit, so ordering between the catalog write and the remote invalidations is provably correct: a remote instance never sees an invalidation for a write that hasn't committed.

**Listener task.** One per instance, on a dedicated long-lived Postgres connection. Pseudocode:

```rust
loop {
    let notification = listener.recv().await?;
    let payload: Payload = serde_json::from_str(&notification.payload)?;
    if payload.from == self.instance_id {
        // We sent it; already invalidated locally.
        continue;
    }
    // Apply locally. The same `invalidate_*` methods used by self-induced
    // mutations.
    apply_invalidation(&self.auth_cache, payload).await;
    self.last_seen_seq.fetch_max(payload.seq, Ordering::AcqRel);
}
```

**Reconnect catch-up.** When the listener reconnects after a transient failure (network hiccup, DB restart, NOTIFY queue overflow), it must replay any invalidations it missed:

```rust
async fn catch_up(&self, last_seen: u64) -> Result<()> {
    let rows = sqlx::query("SELECT seq, cache_name, key_payload, instance_id
                            FROM cache_invalidations
                            WHERE seq > $1 ORDER BY seq")
        .bind(last_seen as i64).fetch_all(&self.pool).await?;
    for r in rows {
        if r.instance_id == self.instance_id { continue; }
        apply_invalidation(&self.auth_cache, r.payload).await;
        self.last_seen_seq.store(r.seq as u64, Ordering::Release);
    }
    Ok(())
}
```

If the catch-up scan exceeds the cleanup window (B.4), assume too much state has been missed and call `cache.invalidate_all()` on every cache as a safe fallback. This is no worse than today's TTL-only behavior.

**Cleanup worker.** A periodic background task DELETEs rows older than `4 × ttl_seconds` (default ≈ 4 minutes). Safe because any instance offline that long would have TTL-flushed every entry anyway.

### B.3 Failure modes

| Scenario | Behavior |
|----------|----------|
| Listener connection drops, reconnects within the cleanup window | Catch-up via table scan. No data loss. |
| Listener can't drain fast enough → NOTIFY queue overflow | Same — catch-up via table scan on next iteration. |
| Network partition between instance and DB | NOTIFY paused; catch-up on heal. |
| Instance offline > cleanup window | `invalidate_all` on every cache. Equivalent to a fresh start; correct but cold. |
| Total LISTEN channel failure | Falls back gracefully to TTL-bounded propagation. No worse than today. |
| Sender's pg_notify fails after row INSERT commits | Row is durable; receivers catch up on next NOTIFY or polling sweep. |
| Sender's row INSERT fails | Catalog write is rolled back (same TX); no inconsistency. |

### B.4 Latency expectations

- Within-DC: typical NOTIFY roundtrip < 50 ms.
- Across-region (rare; multi-region should usually use one extenddb cluster per region): bounded by the catalog DB's replication lag.
- Worst case (listener disconnected): catch-up window equals the cleanup window (default 4 min). Operators tune by adjusting the cleanup interval.

### B.5 What this does **not** solve

- **Direct DB writes outside the management API.** Anything that bypasses `extenddb` writes won't generate notifications. Operators who modify the catalog directly must accept the configured TTL as the propagation window.
- **Process crash between INSERT and NOTIFY.** The row is committed but no NOTIFY fires. The next LISTEN's reconnect-catch-up picks it up. Bounded latency = cleanup interval.
- **Cross-cluster replication.** Out of scope; one cluster per region remains the recommended deployment shape.

### B.6 Configuration sketch

```toml
[auth.cache.fanout]
# Multi-instance invalidation fanout via Postgres LISTEN/NOTIFY.
# Default: false (safe for single-instance; no extra connection overhead).
enabled = false
# DELETE rows older than this many seconds. Default: 4 × ttl_seconds.
# cleanup_interval_seconds = 240
```

### B.7 Implementation cost estimate

- ~400–500 LOC, self-contained, in `crates/storage-postgres/src/cache_fanout.rs`.
- One new migration.
- ~10 lines per IAM mutation handler to insert the row and trigger NOTIFY.
- Behind the `auth.cache.fanout.enabled` flag (default off) so single-instance deployments are unaffected.

### B.8 Open questions

These should be resolved before implementation:

1. **Schema location.** `cache_invalidations` lives in the catalog DB. Should it have its own `cache_*` schema for isolation, or sit alongside `tables`/`accounts`?
2. **Backpressure semantics.** When the cleanup worker can't keep up with insert rate, do we drop oldest, or pause writers? Latter conflicts with the "in-line with catalog write TX" model.
3. **Compaction.** Multiple invalidations for the same `(cache_name, key)` in the cleanup window are redundant. Worth deduping at write time, or at catch-up?
4. **Auth-token rotation.** When the postgres auth method changes (rare), the listener's long-lived connection becomes invalid. Is the existing reconnect loop's exponential backoff sufficient, or do we need a credential-refresh hook?
5. **Multi-tenant deployments.** Different accounts on the same instance share one channel. Is the per-channel filtering by `instance_id` sufficient, or do we need per-account channels for noise reduction?

These are flagged for review by other engineers (deployment shape, durability requirements, network topology) before this lands.
