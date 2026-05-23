// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the stale-while-revalidate cache primitive.
//!
//! These tests use a counting loader to assert that the cache invokes the
//! upstream the expected number of times. Where the test asserts behavior
//! around timing, we use `tokio::time::sleep` with a short real-world delay —
//! we do not depend on `tokio::time::pause` because `moka`'s internal expiry
//! uses `std::time::Instant` directly, which is unaffected by pause/advance.

#![allow(clippy::unwrap_used)] // tests

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::FutureExt;

use crate::swr::{Loader, SwrCache, SwrCacheConfig};

/// Wraps a `Fn` so we can build an `Arc<dyn Fn>` loader inline. The closure
/// signature `(K) -> impl Future` is wrapped in `boxed()` to satisfy the
/// `BoxFuture<'static, ...>` return type the cache expects.
fn make_loader<K, V, E, F, Fut>(f: F) -> Loader<K, V, E>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    E: Send + Sync + 'static,
    F: Fn(K) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<Option<V>, E>> + Send + 'static,
{
    Arc::new(move |k| f(k).boxed())
}

/// A loader that counts how many times it has been invoked, per-key.
/// Returns `Ok(Some(format!("v={key}, n={count}")))` so tests can also
/// distinguish *which* call returned the cached value.
struct CountingLoader {
    calls: Arc<AtomicUsize>,
    /// If set, the loader sleeps before returning. Useful for racing tests.
    delay: Duration,
}

impl CountingLoader {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Self {
                calls: calls.clone(),
                delay: Duration::ZERO,
            },
            calls,
        )
    }

    fn with_delay(delay: Duration) -> (Self, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Self {
                calls: calls.clone(),
                delay,
            },
            calls,
        )
    }

    fn into_loader(self) -> Loader<String, String, &'static str> {
        let calls = self.calls;
        let delay = self.delay;
        make_loader(move |k: String| {
            let calls = calls.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                Ok::<_, &'static str>(Some(format!("v={k}, n={n}")))
            }
        })
    }
}

fn fast_config() -> SwrCacheConfig {
    SwrCacheConfig {
        ttl: Duration::from_millis(200),
        soft_ttl: Duration::from_millis(50),
        negative_ttl: Duration::from_millis(50),
        max_entries: 100,
        name: "test",
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_call_returns_cached_value_without_loading() {
    let (loader, calls) = CountingLoader::new();
    let cache = SwrCache::new(loader.into_loader(), fast_config());

    let v1 = cache.get("k".into()).await.unwrap().unwrap();
    let v2 = cache.get("k".into()).await.unwrap().unwrap();

    assert_eq!(v1, v2, "cached value should be identical to first load");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "loader should be called once"
    );

    let m = cache.metrics().snapshot();
    assert_eq!(m.misses, 1);
    assert_eq!(m.hits, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entry_expires_at_hard_ttl() {
    let (loader, calls) = CountingLoader::new();
    let cache = SwrCache::new(loader.into_loader(), fast_config());

    cache.get("k".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    cache.get("k".into()).await.unwrap();

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "loader should be invoked twice (initial + post-TTL)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_hit_serves_cached_and_spawns_refresh() {
    let (loader, calls) = CountingLoader::new();
    let cache = SwrCache::new(loader.into_loader(), fast_config());

    // First load: miss + insert. n=1.
    let v1 = cache.get("k".into()).await.unwrap().unwrap();
    assert!(v1.contains("n=1"), "first value: {v1}");

    // Wait past soft_ttl (50ms) but well before hard_ttl (200ms).
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Stale hit: should return cached n=1 immediately and spawn a refresh.
    let v2 = cache.get("k".into()).await.unwrap().unwrap();
    assert!(
        v2.contains("n=1"),
        "stale hit must return cached n=1, got {v2}"
    );

    // The refresh task is spawned but hasn't yielded yet. Yield so it runs,
    // and give moka's `insert` time to apply. We use a short sleep that's
    // well below soft_ttl (50ms) so the next read is within the fresh
    // window of the refreshed entry.
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "refresh task should have invoked the loader once more"
    );

    // The next access should now see the refreshed value (n=2). The refresh
    // reset fetched_at, so this is a *fresh* hit (not another stale hit).
    let v3 = cache.get("k".into()).await.unwrap().unwrap();
    assert!(v3.contains("n=2"), "after refresh, expect n=2, got {v3}");

    let m = cache.metrics().snapshot();
    assert_eq!(m.misses, 1, "exactly one hard miss");
    assert_eq!(m.stale_hits, 1, "exactly one stale hit");
    assert_eq!(m.hits, 1, "v3 is a fresh hit on the refreshed entry");
    assert_eq!(m.refresh_success, 1);
    assert_eq!(m.refresh_failure, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_stale_hits_spawn_only_one_refresh() {
    let (loader, calls) = CountingLoader::with_delay(Duration::from_millis(50));
    let cache = SwrCache::new(loader.into_loader(), fast_config());

    // Prime the cache.
    cache.get("k".into()).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Wait past soft_ttl. The entry is now stale-but-usable.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Fire many concurrent stale-hits.
    let mut handles = Vec::new();
    for _ in 0..16 {
        let cache = cache.clone();
        handles.push(tokio::spawn(
            async move { cache.get("k".into()).await.unwrap() },
        ));
    }
    for h in handles {
        let _ = h.await;
    }

    // Give the refresh task time to complete.
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "16 concurrent stale-hits must trigger exactly one refresh"
    );

    let m = cache.metrics().snapshot();
    assert_eq!(m.refresh_success, 1, "exactly one refresh succeeded");
    assert!(
        m.refresh_skipped_inflight >= 14,
        "most concurrent triggers should be skipped (got {})",
        m.refresh_skipped_inflight
    );
}

// `concurrent_misses_collapse_to_one_load` was removed: its loose
// (`<= 32`) upper bound is fully subsumed by the strict
// `concurrent_hard_miss_invokes_loader_exactly_once` test added in PR1,
// and the post-warmup hit check is covered by
// `second_call_returns_cached_value_without_loading`.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negative_result_is_cached_for_negative_ttl_only() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_loader = calls.clone();
    let loader: Loader<String, String, &'static str> = make_loader(move |_k: String| {
        let calls = calls_for_loader.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, &'static str>(None)
        }
    });
    // Configure: hard TTL 1s, negative TTL 50ms.
    let config = SwrCacheConfig {
        ttl: Duration::from_secs(1),
        soft_ttl: Duration::from_millis(500),
        negative_ttl: Duration::from_millis(50),
        max_entries: 100,
        name: "test",
    };
    let cache = SwrCache::new(loader, config);

    // First call loads None.
    let v1 = cache.get("k".into()).await.unwrap();
    assert!(v1.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Within the negative TTL, second call hits cache.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let v2 = cache.get("k".into()).await.unwrap();
    assert!(v2.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1, "negative cache hit");

    // Past the negative TTL, third call re-fetches.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let v3 = cache.get("k".into()).await.unwrap();
    assert!(v3.is_none());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "negative cache should expire at negative_ttl, not at ttl"
    );

    let m = cache.metrics().snapshot();
    assert_eq!(m.negative_hits, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn errors_are_not_cached() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_loader = calls.clone();
    // Loader fails the first call, succeeds the second.
    let loader: Loader<String, String, &'static str> = make_loader(move |_k: String| {
        let calls = calls_for_loader.clone();
        async move {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                Err::<Option<String>, _>("boom")
            } else {
                Ok(Some("ok".to_owned()))
            }
        }
    });
    let cache = SwrCache::new(loader, fast_config());

    // First call: error propagates.
    let r1 = cache.get("k".into()).await;
    assert_eq!(r1, Err("boom"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Second call: loader is invoked again (error not cached).
    let r2 = cache.get("k".into()).await.unwrap().unwrap();
    assert_eq!(r2, "ok");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    // Third call: cached.
    let r3 = cache.get("k".into()).await.unwrap().unwrap();
    assert_eq!(r3, "ok");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalidate_forces_next_call_to_load() {
    let (loader, calls) = CountingLoader::new();
    let cache = SwrCache::new(loader.into_loader(), fast_config());

    cache.get("k".into()).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    cache.invalidate(&"k".to_owned()).await;

    cache.get("k".into()).await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "invalidation should force a re-fetch"
    );

    let m = cache.metrics().snapshot();
    assert_eq!(m.invalidations, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_failure_does_not_evict_stale_entry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_loader = calls.clone();
    let loader: Loader<String, String, &'static str> = make_loader(move |_k: String| {
        let calls = calls_for_loader.clone();
        async move {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // First call: succeeds and primes the cache.
                Ok::<_, &'static str>(Some("v1".to_owned()))
            } else {
                // Subsequent calls (refresh): fail.
                Err("refresh boom")
            }
        }
    });
    let cache = SwrCache::new(loader, fast_config());

    let v1 = cache.get("k".into()).await.unwrap().unwrap();
    assert_eq!(v1, "v1");

    // Cross the soft TTL.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Stale-hit triggers a refresh, which fails. We still see v1.
    let v2 = cache.get("k".into()).await.unwrap().unwrap();
    assert_eq!(v2, "v1", "stale-but-usable value still served");

    // Give the refresh task time to complete.
    tokio::time::sleep(Duration::from_millis(80)).await;

    let m = cache.metrics().snapshot();
    assert_eq!(m.refresh_failure, 1);
    assert_eq!(m.refresh_success, 0);

    // Another call before hard TTL still serves the stale value (the failed
    // refresh did not evict it). However, since the refresh_in_flight flag was
    // cleared, the next stale-hit will spawn another refresh attempt.
    let v3 = cache.get("k".into()).await.unwrap().unwrap();
    assert_eq!(v3, "v1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lru_eviction_caps_size_at_max_entries() {
    let (loader, _) = CountingLoader::new();
    let config = SwrCacheConfig {
        ttl: Duration::from_secs(60),
        soft_ttl: Duration::from_secs(30),
        negative_ttl: Duration::from_secs(5),
        max_entries: 4,
        name: "test",
    };
    let cache = SwrCache::new(loader.into_loader(), config);

    for i in 0..16 {
        cache.get(format!("k{i}")).await.unwrap();
    }
    // moka eviction is asynchronous; allow it to settle.
    cache.run_pending_tasks().await;

    let n = cache.entry_count();
    assert!(n <= 4, "entry count must be ≤ max_entries=4, got {n}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalidate_all_clears_every_entry() {
    let (loader, _) = CountingLoader::new();
    let cache = SwrCache::new(loader.into_loader(), fast_config());

    for i in 0..5 {
        cache.get(format!("k{i}")).await.unwrap();
    }
    cache.run_pending_tasks().await;
    assert!(cache.entry_count() > 0);

    cache.invalidate_all();
    cache.run_pending_tasks().await;
    assert_eq!(cache.entry_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_default_has_expected_values() {
    let cfg = SwrCacheConfig::default();
    assert_eq!(cfg.ttl, Duration::from_secs(60));
    assert_eq!(cfg.soft_ttl, Duration::from_secs(30));
    assert_eq!(cfg.negative_ttl, Duration::from_secs(5));
    assert_eq!(cfg.max_entries, 10_000);
    assert_eq!(cfg.name, "swr-cache");
}

// ─────────────────────────────────────────────────────────────────────
// PR1 tests — single-flight, race epochs, panic safety, config validation
// ─────────────────────────────────────────────────────────────────────

use crate::swr::ConfigError;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_validate_rejects_zero_ttl() {
    let cfg = SwrCacheConfig {
        ttl: Duration::ZERO,
        ..fast_config()
    };
    assert_eq!(cfg.validate(), Err(ConfigError::ZeroTtl));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_validate_rejects_zero_max_entries() {
    let cfg = SwrCacheConfig {
        max_entries: 0,
        ..fast_config()
    };
    assert_eq!(cfg.validate(), Err(ConfigError::ZeroMaxEntries));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_validate_rejects_soft_ttl_above_ttl() {
    let cfg = SwrCacheConfig {
        ttl: Duration::from_secs(10),
        soft_ttl: Duration::from_secs(20),
        negative_ttl: Duration::from_secs(5),
        max_entries: 10,
        name: "test",
    };
    assert_eq!(cfg.validate(), Err(ConfigError::SoftTtlExceedsTtl));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_validate_rejects_negative_ttl_above_ttl() {
    let cfg = SwrCacheConfig {
        ttl: Duration::from_secs(10),
        soft_ttl: Duration::from_secs(5),
        negative_ttl: Duration::from_secs(20),
        max_entries: 10,
        name: "test",
    };
    assert_eq!(cfg.validate(), Err(ConfigError::NegativeTtlExceedsTtl));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn try_new_returns_error_for_invalid_config() {
    let (loader, _) = CountingLoader::new();
    let bad = SwrCacheConfig {
        ttl: Duration::ZERO,
        ..fast_config()
    };
    let result = SwrCache::try_new(loader.into_loader(), bad);
    assert!(matches!(result, Err(ConfigError::ZeroTtl)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_validate_passes_default() {
    SwrCacheConfig::default().validate().unwrap();
}

/// PR1 C1: with proper single-flight, N concurrent hard-miss callers must
/// invoke the loader **exactly once**. Pre-PR1 this assertion was loose
/// (`<= 32`) because the implementation didn't actually dedup.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_hard_miss_invokes_loader_exactly_once() {
    let (loader, calls) = CountingLoader::with_delay(Duration::from_millis(50));
    let cache = SwrCache::new(loader.into_loader(), fast_config());

    let mut handles = Vec::new();
    for _ in 0..32 {
        let cache = cache.clone();
        handles.push(tokio::spawn(
            async move { cache.get("k".into()).await.unwrap() },
        ));
    }
    for h in handles {
        let _ = h.await.unwrap();
    }

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "32 concurrent hard misses must collapse to exactly one loader call"
    );
}

/// PR1 C2/C3: a refresh that completes after an explicit `invalidate` must
/// NOT clobber the (now-empty) cache slot with stale loader output.
///
/// Sequence:
/// 1. Prime cache. n=1.
/// 2. Sleep past `soft_ttl`. Stale-hit triggers refresh; loader sleeps inside.
/// 3. Issue invalidate concurrently with the refresh in flight.
/// 4. Refresh returns n=2; epoch guard discards the value.
/// 5. Next access hard-misses, gets n=3 (the actually-current value).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_dropped_when_invalidate_races() {
    let (loader, calls) = CountingLoader::with_delay(Duration::from_millis(80));
    let cache = SwrCache::new(loader.into_loader(), fast_config());

    // First load (n=1) — fast path returns "v=k, n=1".
    let v1 = cache.get("k".into()).await.unwrap().unwrap();
    assert!(v1.contains("n=1"));

    // Cross soft_ttl so next access spawns a refresh.
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Stale-hit: spawns a refresh whose loader will sleep 80ms.
    let v2 = cache.get("k".into()).await.unwrap().unwrap();
    assert!(v2.contains("n=1"), "stale-hit returns cached, got {v2}");

    // Race: invalidate while the refresh is still in-flight (~50ms in).
    tokio::time::sleep(Duration::from_millis(20)).await;
    cache.invalidate(&"k".to_owned()).await;

    // Wait for the refresh to complete. Its result must be discarded.
    tokio::time::sleep(Duration::from_millis(120)).await;
    let m = cache.metrics().snapshot();
    assert_eq!(
        m.refresh_dropped_epoch, 1,
        "refresh that raced invalidate must drop its result"
    );
    assert_eq!(
        m.refresh_success, 0,
        "the refresh did not write to the cache"
    );

    // Next call hard-misses since the entry was invalidated. The loader
    // counter records: 1 (initial) + 1 (refresh, dropped) + 1 (this miss) = 3.
    let v3 = cache.get("k".into()).await.unwrap().unwrap();
    let total = calls.load(Ordering::SeqCst);
    assert_eq!(total, 3, "loader called 3 times: initial, refresh, miss");
    assert!(
        v3.contains("n=3"),
        "post-invalidate read sees the most recent loader call: got {v3}"
    );
}

/// PR-review S1: a slow refresh whose loader started before a concurrent
/// hard miss must NOT clobber the freshly-loaded value.
///
/// Sequence:
/// 1. Prime cache (n=1).
/// 2. Sleep past `soft_ttl`. Stale-hit triggers refresh; loader sleeps inside.
/// 3. Sleep past `hard_ttl` (without firing invalidate). The cached entry is
///    now hard-expired. A fresh request hard-misses → loads n=3 (n=2 is the
///    refresh's loader call). Cache slot now holds n=3.
/// 4. The original refresh's loader returns its n=2 value.
/// 5. The identity guard sees the cache slot's Arc no longer matches the
///    Arc the refresh started from → drops the refresh result.
/// 6. Subsequent reads continue to see n=3, not n=2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_dropped_when_hard_miss_races() {
    // Timing budget chosen so the post-hard-miss entry stays alive in the
    // cache for the duration of the test:
    //   soft_ttl  = 50ms, hard_ttl = 200ms, slow refresh = 300ms.
    // Sequence (approximate elapsed):
    //   t=0    prime n=1 (Entry-A)
    //   t=80   stale-hit → refresh #2 starts (sleeps 300ms, finishes ~t=380)
    //   t=250  Entry-A past hard_ttl → hard miss loads n=3 (Entry-B)
    //   t=380  refresh #2 finishes; identity guard sees the slot now holds
    //          Entry-B (Arc::ptr_eq != entry_for_clear) → drop the result
    //   t=400  read v4: Entry-B still fresh, returns "n=3"
    let cfg = SwrCacheConfig {
        ttl: Duration::from_millis(200),
        soft_ttl: Duration::from_millis(50),
        negative_ttl: Duration::from_millis(50),
        max_entries: 100,
        name: "test",
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_loader = calls.clone();
    let loader: Loader<String, String, &'static str> = make_loader(move |_k: String| {
        let calls = calls_for_loader.clone();
        async move {
            let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
            // The refresh that runs AFTER the initial load is the one we
            // want slow — that's call #2. Initial and post-hard-miss loads
            // return promptly so the test stays fast.
            if n == 2 {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Ok::<_, &'static str>(Some(format!("n={n}")))
        }
    });
    let cache = SwrCache::new(loader, cfg);

    // 1. Prime cache. n=1.
    let v1 = cache.get("k".into()).await.unwrap().unwrap();
    assert_eq!(v1, "n=1");

    // 2. Cross soft_ttl, trigger refresh. The refresh loader sleeps 300ms.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let v2 = cache.get("k".into()).await.unwrap().unwrap();
    assert_eq!(v2, "n=1", "stale-hit returns cached value");

    // 3. Cross hard_ttl (no invalidate). Next read is a hard miss → n=3.
    // (Total elapsed ~250ms, past the 200ms hard TTL.)
    tokio::time::sleep(Duration::from_millis(170)).await;
    let v3 = cache.get("k".into()).await.unwrap().unwrap();
    assert_eq!(v3, "n=3", "hard-miss path loads fresh value");

    // 4–5. Wait for the slow refresh (n=2) to finish. Its result must be
    // discarded by the identity guard, NOT inserted on top of n=3.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 6. Subsequent reads still see n=3. The post-hard-miss entry is ~150ms
    // old now (well within the 200ms hard TTL). If the refresh had clobbered,
    // we'd see n=2 here.
    let v4 = cache.get("k".into()).await.unwrap().unwrap();
    assert_eq!(
        v4, "n=3",
        "stale refresh must not overwrite the post-hard-miss value"
    );

    let m = cache.metrics().snapshot();
    assert_eq!(
        m.refresh_success, 0,
        "the slow refresh must NOT have written back; got {m:?}"
    );
    assert!(
        m.refresh_dropped_epoch >= 1,
        "the slow refresh must be counted as dropped; got {m:?}"
    );
}

/// PR1 H4: a panicking loader must NOT leak `refresh_in_flight = true`.
/// Without the RAII guard, future refreshes would be permanently
/// suppressed for that entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_refresh_does_not_leak_inflight_flag() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_loader = calls.clone();
    let loader: Loader<String, String, &'static str> = make_loader(move |_k: String| {
        let calls = calls_for_loader.clone();
        async move {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // First call (initial load) — succeeds.
                Ok::<_, &'static str>(Some("v1".to_owned()))
            } else if n == 1 {
                // Second call (first refresh) — panics inside the spawned task.
                panic!("simulated refresh panic");
            } else {
                // Subsequent calls (subsequent refreshes) — succeed.
                Ok::<_, &'static str>(Some("v2".to_owned()))
            }
        }
    });
    let cache = SwrCache::new(loader, fast_config());

    // Prime cache.
    let v1 = cache.get("k".into()).await.unwrap().unwrap();
    assert_eq!(v1, "v1");

    // Cross soft_ttl. Stale-hit spawns refresh #1, which panics.
    tokio::time::sleep(Duration::from_millis(80)).await;
    cache.get("k".into()).await.unwrap();

    // Give the panicking refresh time to land. Tokio swallows the panic,
    // but the RAII guard must have cleared `refresh_in_flight`.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Without the guard, this stale-hit would increment
    // `refresh_skipped_inflight` because the flag would still be `true`. With
    // the guard, the next stale-hit successfully spawns refresh #2.
    cache.get("k".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let m = cache.metrics().snapshot();
    assert!(
        m.refresh_success >= 1,
        "second refresh must succeed after first one panicked; got {m:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "loader called: initial + panicking refresh + recovered refresh"
    );
}

/// PR1: epoch counter increments on every invalidation form.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn epoch_increments_on_each_invalidation() {
    let (loader, _) = CountingLoader::new();
    let cache = SwrCache::new(loader.into_loader(), fast_config());

    let e0 = cache.epoch();
    cache.invalidate(&"k".to_owned()).await;
    let e1 = cache.epoch();
    assert!(e1 > e0);
    cache.invalidate_all();
    let e2 = cache.epoch();
    assert!(e2 > e1);
    cache.invalidate_if(|_k: &String| true).unwrap();
    let e3 = cache.epoch();
    assert!(e3 > e2);
}

// ─────────────────────────────────────────────────────────────────────
// PR2 tests — pass-through (kill switch) mode
// ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pass_through_invokes_loader_on_every_call() {
    let (loader, calls) = CountingLoader::new();
    let cache = SwrCache::pass_through(loader.into_loader(), fast_config());

    assert!(cache.is_pass_through());
    cache.get("k".into()).await.unwrap();
    cache.get("k".into()).await.unwrap();
    cache.get("k".into()).await.unwrap();

    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "pass-through must call loader on every get; nothing is cached"
    );
    assert_eq!(cache.entry_count(), 0, "no entries are stored");

    // Misses are still counted for observability.
    let m = cache.metrics().snapshot();
    assert_eq!(m.misses, 3);
    assert_eq!(m.hits, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pass_through_invalidate_is_noop() {
    let (loader, calls) = CountingLoader::new();
    let cache = SwrCache::pass_through(loader.into_loader(), fast_config());

    cache.get("k".into()).await.unwrap();
    cache.invalidate(&"k".to_owned()).await;
    cache.invalidate_all();
    cache.invalidate_if(|_: &String| true).unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1, "1 get call");
    let m = cache.metrics().snapshot();
    // No invalidation counters bumped in pass-through mode (the calls were no-ops).
    assert_eq!(m.invalidations, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pass_through_propagates_loader_errors() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_loader = calls.clone();
    let loader: Loader<String, String, &'static str> = make_loader(move |_k: String| {
        let calls = calls_for_loader.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<Option<String>, _>("oops")
        }
    });
    let cache = SwrCache::pass_through(loader, fast_config());

    let r1 = cache.get("k".into()).await;
    let r2 = cache.get("k".into()).await;
    assert_eq!(r1, Err("oops"));
    assert_eq!(r2, Err("oops"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "pass-through doesn't cache errors (or anything); both calls hit the loader"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn try_pass_through_validates_config() {
    let (loader, _) = CountingLoader::new();
    let bad = SwrCacheConfig {
        ttl: Duration::ZERO,
        ..fast_config()
    };
    let result = SwrCache::try_pass_through(loader.into_loader(), bad);
    assert!(matches!(result, Err(crate::swr::ConfigError::ZeroTtl)));
}

/// PR1: hard-miss followers receive a *cloned* error value (via Clone bound),
/// not a panic. moka's `try_get_with` returns `Arc<E>` for racing callers; we
/// surface each follower's own owned `E`. With single-flight, all 8 racing
/// callers share one loader invocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_hard_miss_errors_are_returned_to_each_caller() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_loader = calls.clone();
    let loader: Loader<String, String, &'static str> = make_loader(move |_k: String| {
        let calls = calls_for_loader.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            // Sleep so concurrent callers race on the single-flight entry.
            tokio::time::sleep(Duration::from_millis(30)).await;
            Err::<Option<String>, _>("loader-fail")
        }
    });
    let cache = SwrCache::new(loader, fast_config());

    // 8 concurrent callers. With single-flight, the loader runs once and
    // every caller sees the same error value (cloned).
    let mut handles = Vec::new();
    for _ in 0..8 {
        let cache = cache.clone();
        handles.push(tokio::spawn(async move { cache.get("k".into()).await }));
    }
    let mut err_count = 0;
    for h in handles {
        let r = h.await.unwrap();
        assert_eq!(r, Err("loader-fail"));
        err_count += 1;
    }
    assert_eq!(err_count, 8, "every caller gets the error");
    // moka's try_get_with deduplicates the single-flight call: exactly one
    // loader invocation serves all 8 racing callers. Errors are NOT cached
    // afterward (confirmed by `errors_are_not_cached`), so a subsequent
    // get() would re-attempt.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "8 concurrent hard-miss callers must collapse to exactly one loader call"
    );
}
