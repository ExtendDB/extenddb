// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stale-while-revalidate cache implementation.
//!
//! See `lib.rs` for the API surface and `docs/design/12-auth-authz-cache.md`
//! for the design rationale.

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use moka::future::Cache as MokaCache;

use crate::entry::Entry;

/// Configuration for an `SwrCache`.
#[derive(Debug, Clone)]
pub struct SwrCacheConfig {
    /// Hard TTL — entries older than this are full misses.
    pub ttl: Duration,
    /// Soft TTL — entries older than this trigger background refresh on access.
    /// Must be `<= ttl`.
    pub soft_ttl: Duration,
    /// TTL applied to negative entries (`Ok(None)` from the loader).
    /// Must be `<= ttl`.
    pub negative_ttl: Duration,
    /// Maximum number of entries before LRU eviction kicks in. Must be `> 0`.
    pub max_entries: u64,
    /// Optional name used in logs and metrics. Defaults to "swr-cache".
    pub name: &'static str,
}

impl Default for SwrCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(60),
            soft_ttl: Duration::from_secs(30),
            negative_ttl: Duration::from_secs(5),
            max_entries: 10_000,
            name: "swr-cache",
        }
    }
}

/// Reasons a [`SwrCacheConfig`] may fail validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    ZeroTtl,
    ZeroMaxEntries,
    SoftTtlExceedsTtl,
    NegativeTtlExceedsTtl,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            Self::ZeroTtl => "ttl must be > 0",
            Self::ZeroMaxEntries => "max_entries must be > 0",
            Self::SoftTtlExceedsTtl => "soft_ttl must be <= ttl",
            Self::NegativeTtlExceedsTtl => "negative_ttl must be <= ttl",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for ConfigError {}

impl SwrCacheConfig {
    /// Validate config invariants. Operators that accept untrusted config
    /// (e.g. TOML) MUST call this at startup; bad values silently produce a
    /// thrash cache otherwise.
    ///
    /// # Errors
    /// Returns `ConfigError` describing the first invariant violation found.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.ttl.is_zero() {
            return Err(ConfigError::ZeroTtl);
        }
        if self.max_entries == 0 {
            return Err(ConfigError::ZeroMaxEntries);
        }
        if self.soft_ttl > self.ttl {
            return Err(ConfigError::SoftTtlExceedsTtl);
        }
        if self.negative_ttl > self.ttl {
            return Err(ConfigError::NegativeTtlExceedsTtl);
        }
        Ok(())
    }
}

/// Type alias for a boxed loader future.
///
/// Loaders are stored as `Arc<dyn Fn(K) -> BoxFuture<...>>` so the cache can
/// invoke them from both the request path and spawned refresh tasks.
///
/// # Concurrency contract
///
/// Loaders may be invoked concurrently across **different keys**. Within a
/// single key, the cache deduplicates concurrent invocations on hard miss
/// via moka's single-flight semantics; only one loader future runs and all
/// racing callers share its outcome. Background refreshes are deduped via
/// the per-entry `refresh_in_flight` flag.
///
/// Loaders MUST be effect-free with respect to the cache's externally-
/// observable state. Capturing shared `Arc<dyn ...Store>` handles is fine;
/// capturing a `Mutex` or `mpsc::Sender` is a mistake.
pub type Loader<K, V, E> = Arc<dyn Fn(K) -> BoxFuture<'static, Result<Option<V>, E>> + Send + Sync>;

/// Atomic counters exposed for observability. Cheap to read; cheap to
/// `clone` (it returns an `Arc`-shared handle).
#[derive(Debug, Default)]
pub struct SwrMetrics {
    pub hits: AtomicU64,
    pub stale_hits: AtomicU64,
    pub misses: AtomicU64,
    pub negative_hits: AtomicU64,
    pub refresh_success: AtomicU64,
    pub refresh_failure: AtomicU64,
    pub refresh_skipped_inflight: AtomicU64,
    /// Refreshes whose result was discarded because the cache's epoch
    /// advanced (i.e. an explicit invalidation happened during the refresh).
    /// High counts mean the cache is doing wasted refresh work; low counts
    /// are the typical case.
    pub refresh_dropped_epoch: AtomicU64,
    pub invalidations: AtomicU64,
}

impl SwrMetrics {
    fn incr(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot all counters for export (e.g. to Prometheus).
    #[must_use]
    pub fn snapshot(&self) -> SwrMetricsSnapshot {
        SwrMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            stale_hits: self.stale_hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            negative_hits: self.negative_hits.load(Ordering::Relaxed),
            refresh_success: self.refresh_success.load(Ordering::Relaxed),
            refresh_failure: self.refresh_failure.load(Ordering::Relaxed),
            refresh_skipped_inflight: self.refresh_skipped_inflight.load(Ordering::Relaxed),
            refresh_dropped_epoch: self.refresh_dropped_epoch.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
        }
    }
}

/// Plain-old-data view of `SwrMetrics`. Useful for tests and metric exports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SwrMetricsSnapshot {
    pub hits: u64,
    pub stale_hits: u64,
    pub misses: u64,
    pub negative_hits: u64,
    pub refresh_success: u64,
    pub refresh_failure: u64,
    pub refresh_skipped_inflight: u64,
    #[serde(default)]
    pub refresh_dropped_epoch: u64,
    pub invalidations: u64,
}

/// Stale-while-revalidate cache.
///
/// `K` is the key type. `V` is the value type. `E` is the loader's error type.
///
/// Cloning is cheap — clones share the underlying `moka` cache, loader,
/// configuration, and metrics. All operations are async-safe and may be
/// invoked concurrently from any number of tasks.
///
/// # Single-flight on hard miss
///
/// Concurrent callers requesting the same missing key share one loader
/// invocation via `moka::future::Cache::try_get_with`. moka requires the
/// error type be `Clone + Send + Sync + 'static` so it can hand each racing
/// caller their own copy of the same error.
pub struct SwrCache<K, V, E>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    /// The underlying moka cache. `None` in pass-through mode so the kill
    /// switch costs zero memory beyond a tag bit, and `get` provably
    /// cannot accidentally read or write moka state.
    inner: Option<MokaCache<K, Arc<Entry<V>>>>,
    loader: Loader<K, V, E>,
    config: Arc<SwrCacheConfig>,
    metrics: Arc<SwrMetrics>,
    /// Monotonic counter incremented on every explicit invalidation
    /// (`invalidate`, `invalidate_if`, `invalidate_all`). The hard-miss and
    /// background-refresh paths capture the epoch BEFORE running the loader
    /// and only `insert` if the epoch hasn't moved. Closes the
    /// refresh-races-invalidation race.
    epoch: Arc<AtomicU64>,
}

impl<K, V, E> Clone for SwrCache<K, V, E>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            loader: self.loader.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            epoch: self.epoch.clone(),
        }
    }
}

impl<K, V, E> SwrCache<K, V, E>
where
    K: Hash + Eq + Send + Sync + Clone + Debug + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + Debug + 'static,
{
    /// Construct a new cache with the given loader and configuration.
    ///
    /// The loader is invoked on a hard miss (request blocks) and from a
    /// spawned task on stale-hit refresh (request does not block).
    ///
    /// # Errors
    /// Returns `ConfigError` if `config` fails [`SwrCacheConfig::validate`].
    pub fn try_new(loader: Loader<K, V, E>, config: SwrCacheConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        let inner = MokaCache::builder()
            .max_capacity(config.max_entries)
            // moka's TTL caps how long an entry lives in the underlying map.
            // `validate()` guarantees `negative_ttl <= ttl`, so the hard
            // TTL is sufficient — we enforce the per-entry semantic TTL
            // (positive vs negative) at lookup time.
            .time_to_live(config.ttl)
            // Opt in to `invalidate_entries_if` (used for fanout
            // invalidations like `invalidate_role_sessions`).
            .support_invalidation_closures()
            .build();
        Ok(Self {
            inner: Some(inner),
            loader,
            config: Arc::new(config),
            metrics: Arc::new(SwrMetrics::default()),
            epoch: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Construct a pass-through cache: every `get` invokes the loader
    /// directly without consulting the underlying map; every `invalidate*`
    /// is a no-op.
    ///
    /// Used when the operator sets `auth.cache.enabled = false`. The cache
    /// wrappers stay in place so the call-graph is unchanged, but the cache
    /// itself adds zero overhead beyond a single `if` branch.
    ///
    /// `config` is still validated (the `name` is used for metric labels);
    /// TTL fields are ignored at runtime since nothing is cached.
    ///
    /// # Errors
    /// Returns `ConfigError` if `config` fails [`SwrCacheConfig::validate`].
    pub fn try_pass_through(
        loader: Loader<K, V, E>,
        config: SwrCacheConfig,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        // Pass-through carries no moka instance: every `get` short-circuits
        // before consulting `inner`, so the underlying allocator + LRU
        // bookkeeping would be pure overhead. The `Option::None` here also
        // makes it impossible for a future refactor to accidentally read
        // or write a stale entry while in pass-through mode.
        Ok(Self {
            inner: None,
            loader,
            config: Arc::new(config),
            metrics: Arc::new(SwrMetrics::default()),
            epoch: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Construct a pass-through cache, panicking on invalid configuration.
    ///
    /// # Panics
    /// Panics if `config` fails [`SwrCacheConfig::validate`].
    #[must_use]
    pub fn pass_through(loader: Loader<K, V, E>, config: SwrCacheConfig) -> Self {
        Self::try_pass_through(loader, config).expect("invalid SwrCacheConfig")
    }

    /// Construct a new cache, panicking on invalid configuration.
    ///
    /// Prefer [`Self::try_new`] in production code paths that accept user
    /// config; reserve `new` for tests and statically-valid configurations.
    ///
    /// # Panics
    /// Panics if `config` fails [`SwrCacheConfig::validate`].
    #[must_use]
    pub fn new(loader: Loader<K, V, E>, config: SwrCacheConfig) -> Self {
        Self::try_new(loader, config).expect("invalid SwrCacheConfig")
    }

    /// Returns a shared handle to the metrics. Cheap to clone.
    #[must_use]
    pub fn metrics(&self) -> Arc<SwrMetrics> {
        self.metrics.clone()
    }

    /// Returns the configured cache name (for logging / metric labelling).
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.config.name
    }

    /// Returns the current number of entries (best-effort, may lag eviction).
    /// Always 0 in pass-through mode (no underlying storage).
    #[must_use]
    pub fn entry_count(&self) -> u64 {
        match &self.inner {
            Some(c) => c.entry_count(),
            None => 0,
        }
    }

    /// Returns `true` when this cache was constructed via `pass_through` /
    /// `try_pass_through` and forwards every `get` directly to the loader.
    #[must_use]
    pub fn is_pass_through(&self) -> bool {
        self.inner.is_none()
    }

    /// Look up a key, fetching from the loader on miss and serving stale on
    /// soft expiry while refreshing in the background.
    ///
    /// Returns `Ok(None)` if the loader returned `Ok(None)` (negative hit).
    /// Returns `Err(_)` if the loader failed and no cached value was usable.
    ///
    /// # Errors
    ///
    /// Propagates any error from the loader on a hard miss. Concurrent hard-
    /// miss callers share one loader invocation (single-flight); all racing
    /// callers see the same error value (cloned). Errors during background
    /// refresh are logged and counted, but the cached (stale) value
    /// continues to be served until `ttl` elapses.
    pub async fn get(&self, key: K) -> Result<Option<V>, E> {
        let Some(inner) = self.inner.as_ref() else {
            // Kill-switch mode: bypass the cache entirely. Counted as a miss
            // for observability so operators can confirm bypass behavior.
            SwrMetrics::incr(&self.metrics.misses);
            return (self.loader)(key).await;
        };

        let now = Instant::now();

        // Fast path: check for an existing entry without touching the loader.
        if let Some(entry) = inner.get(&key).await {
            let age = now.saturating_duration_since(entry.fetched_at);
            let entry_ttl = if entry.value.is_some() {
                self.config.ttl
            } else {
                self.config.negative_ttl
            };

            if age < entry_ttl {
                if entry.value.is_none() {
                    SwrMetrics::incr(&self.metrics.negative_hits);
                } else if age < self.config.soft_ttl {
                    SwrMetrics::incr(&self.metrics.hits);
                } else {
                    SwrMetrics::incr(&self.metrics.stale_hits);
                    self.maybe_spawn_refresh(inner, &key, &entry);
                }
                return Ok(entry.value.clone());
            }
            // Otherwise: fall through to a hard miss / load.
        }

        // Hard miss — go through moka's single-flight path so concurrent
        // callers for the same key share one loader invocation.
        SwrMetrics::incr(&self.metrics.misses);
        self.load_single_flight(inner, key).await
    }

    /// Drive a hard-miss load through moka's `try_get_with` so concurrent
    /// callers share one loader future.
    ///
    /// We invalidate the existing entry first so `try_get_with`'s
    /// "I have an existing value" fast path doesn't return our just-expired
    /// entry; the invalidate is necessary because moka's per-entry TTL is
    /// one global hard expiry while our `ttl`/`negative_ttl` distinction is
    /// per-entry-state.
    async fn load_single_flight(
        &self,
        inner: &MokaCache<K, Arc<Entry<V>>>,
        key: K,
    ) -> Result<Option<V>, E> {
        // Drop the current entry (if any) so try_get_with's first invocation
        // executes the loader. The invalidate is a fast in-memory mark; even
        // if a concurrent caller's try_get_with collides with ours, moka
        // dedupes the actual loader invocation.
        inner.invalidate(&key).await;

        let loader = self.loader.clone();
        let epoch_before = self.epoch.load(Ordering::Acquire);
        let key_for_init = key.clone();

        let inserted: Result<Arc<Entry<V>>, Arc<E>> = inner
            .try_get_with(key.clone(), async move {
                (loader)(key_for_init).await.map(Entry::new)
            })
            .await;

        match inserted {
            Ok(entry) => {
                // Epoch guard — if invalidate fired while the loader was
                // running, the cached entry is now potentially stale relative
                // to the new state. Drop it so the next caller takes the
                // fresh path. We still return THIS load's value to the
                // current caller — it reflects the loader's view of the
                // world at fetch time, which is the contract we promise.
                if self.epoch.load(Ordering::Acquire) != epoch_before {
                    inner.invalidate(&key).await;
                    SwrMetrics::incr(&self.metrics.refresh_dropped_epoch);
                }
                Ok(entry.value.clone())
            }
            Err(shared_err) => {
                // moka shared one Arc<E> across every racing caller. Clone
                // the inner E so each caller gets an owned value. Errors are
                // not cached; the next call will re-attempt.
                Err((*shared_err).clone())
            }
        }
    }

    /// Spawn a background refresh task for `key` if one is not already in
    /// flight.
    ///
    /// Single-flight is enforced by an atomic CAS on the entry's
    /// `refresh_in_flight` flag. The flag is cleared on task completion via
    /// an RAII guard, so a panicking loader cannot leave the flag stuck
    /// (panic safety).
    ///
    /// The refresh task captures the cache's `epoch` BEFORE running the
    /// loader and only inserts on success if the epoch hasn't moved.
    /// Otherwise the result is discarded (closes the refresh-races-
    /// invalidate / refresh-races-fresh-write data hazards).
    fn maybe_spawn_refresh(
        &self,
        inner: &MokaCache<K, Arc<Entry<V>>>,
        key: &K,
        entry: &Arc<Entry<V>>,
    ) {
        // CAS: only one task wins the right to spawn a refresh.
        if entry
            .refresh_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            SwrMetrics::incr(&self.metrics.refresh_skipped_inflight);
            return;
        }

        let cache = inner.clone();
        let loader = self.loader.clone();
        let metrics = self.metrics.clone();
        let epoch = self.epoch.clone();
        let key = key.clone();
        let entry_for_clear = entry.clone();
        let name = self.config.name;
        let epoch_before = epoch.load(Ordering::Acquire);

        tokio::spawn(async move {
            // Panic-safe: clear `refresh_in_flight` on any exit, including
            // panic. Tokio swallows task panics into a JoinHandle which we
            // drop, but without this guard the flag leaks `true` and
            // suppresses all future refreshes for this entry until hard TTL
            // evicts it.
            let _guard = ClearRefreshFlag(entry_for_clear.clone());
            let result = (loader)(key.clone()).await;
            match result {
                Ok(value) => {
                    // Epoch guard — if invalidate fired during the load, the
                    // value we just fetched is potentially stale relative to
                    // the post-invalidate state. Drop it; next caller takes
                    // the fresh path. Same logic as `load_single_flight`.
                    if epoch.load(Ordering::Acquire) != epoch_before {
                        SwrMetrics::incr(&metrics.refresh_dropped_epoch);
                        return;
                    }
                    // Identity guard — even if no explicit invalidate fired,
                    // a concurrent hard miss (e.g. hard TTL elapsed during
                    // this slow refresh) may have replaced the cache slot
                    // with a freshly-loaded entry. Inserting our result
                    // would clobber that fresher value. Compare the current
                    // slot's Arc identity with the entry we started from;
                    // only write back if they still match.
                    let still_ours = cache
                        .get(&key)
                        .await
                        .is_some_and(|cur| Arc::ptr_eq(&cur, &entry_for_clear));
                    if !still_ours {
                        SwrMetrics::incr(&metrics.refresh_dropped_epoch);
                        return;
                    }
                    let new_entry = Entry::new(value);
                    cache.insert(key, new_entry).await;
                    SwrMetrics::incr(&metrics.refresh_success);
                }
                Err(e) => {
                    tracing::warn!(
                        cache = name,
                        error = ?e,
                        "background cache refresh failed; continuing to serve stale entry"
                    );
                    SwrMetrics::incr(&metrics.refresh_failure);
                }
            }
        });
    }

    /// Forget the cached value for `key`. The next call will re-fetch.
    ///
    /// Used for write-through invalidation when the underlying data changes.
    /// In pass-through mode this is a no-op since nothing is cached.
    pub async fn invalidate(&self, key: &K) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        // Bump epoch BEFORE invalidating so any in-flight refresh that
        // captured the prior epoch will discard its result on completion.
        self.epoch.fetch_add(1, Ordering::AcqRel);
        inner.invalidate(key).await;
        SwrMetrics::incr(&self.metrics.invalidations);
    }

    /// Forget every entry whose key matches `predicate`.
    ///
    /// `moka`'s `invalidate_entries_if` is asynchronous — entries become
    /// inaccessible immediately but are evicted from the underlying map by a
    /// background task.
    ///
    /// # Errors
    /// Returns the underlying moka error if predicate registration fails (in
    /// practice, only at shutdown).
    pub fn invalidate_if<F>(&self, key_predicate: F) -> Result<(), moka::PredicateError>
    where
        F: Fn(&K) -> bool + Send + Sync + 'static,
    {
        let Some(inner) = self.inner.as_ref() else {
            return Ok(());
        };
        self.epoch.fetch_add(1, Ordering::AcqRel);
        SwrMetrics::incr(&self.metrics.invalidations);
        inner
            .invalidate_entries_if(move |k, _v| key_predicate(k))
            .map(|_| ())
    }

    /// Forget every entry whose value matches `predicate`.
    ///
    /// Used by callers (e.g. credential cache fanout on role deletion) that
    /// need to invalidate by a value field rather than by key. The
    /// predicate receives the cached value if positive (`Some(&V)`), or
    /// `None` for negative entries; predicates that only care about the
    /// positive case can short-circuit on `None`.
    ///
    /// # Errors
    /// Returns the underlying moka error if predicate registration fails (in
    /// practice, only at shutdown).
    pub fn invalidate_if_value<F>(&self, value_predicate: F) -> Result<(), moka::PredicateError>
    where
        F: Fn(Option<&V>) -> bool + Send + Sync + 'static,
    {
        let Some(inner) = self.inner.as_ref() else {
            return Ok(());
        };
        self.epoch.fetch_add(1, Ordering::AcqRel);
        SwrMetrics::incr(&self.metrics.invalidations);
        inner
            .invalidate_entries_if(move |_k, v| value_predicate(v.value.as_ref()))
            .map(|_| ())
    }

    /// Forget every cached value. No-op in pass-through mode.
    pub fn invalidate_all(&self) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        self.epoch.fetch_add(1, Ordering::AcqRel);
        inner.invalidate_all();
        SwrMetrics::incr(&self.metrics.invalidations);
    }

    /// Returns the current epoch counter. Tests assert correct epoch
    /// behavior; not part of the public production API contract.
    #[doc(hidden)]
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Run any pending eviction / expiry housekeeping.
    ///
    /// Used by tests to make `entry_count()` deterministic without waiting
    /// on moka's internal timers. Production code does not need to call
    /// this; moka schedules its own background work. No-op in pass-through
    /// mode (no underlying cache to drain).
    ///
    /// Available only with the `test-util` feature, or to other tests
    /// inside this crate.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn run_pending_tasks(&self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.run_pending_tasks().await;
        }
    }
}

/// RAII guard that clears the `refresh_in_flight` flag on the original entry
/// when the spawned refresh task exits — including via panic. Without this
/// guard, a panicking loader leaks `true` on the flag and suppresses all
/// future refreshes for that entry until hard TTL eviction.
struct ClearRefreshFlag<V>(Arc<Entry<V>>);

impl<V> Drop for ClearRefreshFlag<V> {
    fn drop(&mut self) {
        self.0.refresh_in_flight.store(false, Ordering::Release);
    }
}
