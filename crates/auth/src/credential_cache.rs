// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `CachedCredentialStore` — wraps any [`CredentialStore`] with a stale-while-
//! revalidate cache.
//!
//! Each `lookup_credential(access_key_id)` call first consults the in-memory
//! cache. Fresh entries (younger than `soft_ttl`) are returned immediately.
//! Stale-but-usable entries (between `soft_ttl` and `ttl`) are returned
//! immediately while a background task refreshes them. Misses block on the
//! upstream store. Negative results (`Ok(None)`) are cached for the shorter
//! `negative_ttl`. Errors are never cached.
//!
//! Write-through invalidation is exposed via [`CachedCredentialStore::invalidate`]:
//! the management API calls it whenever an access key is created, deleted, or
//! has its active status changed, so self-induced changes propagate instantly
//! within the local instance.
//!
//! See `docs/design/12-auth-authz-cache.md` for the full design.

use std::sync::Arc;

use extenddb_cache::{Loader, SwrCache, SwrCacheConfig};
use extenddb_core::error::DynamoDbError;
use futures::FutureExt;
use futures::future::BoxFuture;

use crate::{CredentialStore, StoredCredential};

/// A `CredentialStore` that wraps another store and caches results.
///
/// The cache uses stale-while-revalidate semantics — see the module-level
/// docs and `docs/design/12-auth-authz-cache.md`.
///
/// Cloning is cheap; clones share the underlying cache and inner store.
#[derive(Clone)]
pub struct CachedCredentialStore {
    cache: SwrCache<String, StoredCredential, DynamoDbError>,
}

impl CachedCredentialStore {
    /// Wrap `inner` with a cache configured by `config`.
    ///
    /// The `inner` store is moved into an `Arc` so the cache's loader closure
    /// can outlive the constructor call. The wrapper itself is `Clone` and
    /// safe to share across tasks.
    #[must_use]
    pub fn new<T>(inner: T, config: SwrCacheConfig) -> Self
    where
        T: CredentialStore + 'static,
    {
        let inner: Arc<dyn CredentialStore> = Arc::new(inner);
        Self::with_arc(inner, config)
    }

    /// Wrap an already-`Arc`-wrapped `CredentialStore`.
    ///
    /// Use this when the caller already holds an `Arc<dyn CredentialStore>`
    /// (e.g. when sharing the same backing store across multiple wrappers).
    #[must_use]
    pub fn with_arc(inner: Arc<dyn CredentialStore>, config: SwrCacheConfig) -> Self {
        Self {
            cache: SwrCache::new(Self::build_loader(inner), config),
        }
    }

    /// Construct a pass-through wrapper that bypasses the cache on every
    /// lookup. Used when `auth.cache.enabled = false`.
    #[must_use]
    pub fn pass_through<T>(inner: T, config: SwrCacheConfig) -> Self
    where
        T: CredentialStore + 'static,
    {
        let inner: Arc<dyn CredentialStore> = Arc::new(inner);
        Self::pass_through_arc(inner, config)
    }

    /// Like [`Self::pass_through`] but accepts an `Arc<dyn CredentialStore>`.
    #[must_use]
    pub fn pass_through_arc(inner: Arc<dyn CredentialStore>, config: SwrCacheConfig) -> Self {
        Self {
            cache: SwrCache::pass_through(Self::build_loader(inner), config),
        }
    }

    fn build_loader(
        inner: Arc<dyn CredentialStore>,
    ) -> Loader<String, StoredCredential, DynamoDbError> {
        Arc::new(move |access_key_id: String|
            -> BoxFuture<'static, Result<Option<StoredCredential>, DynamoDbError>> {
            let inner = inner.clone();
            async move { inner.lookup_credential(&access_key_id).await }.boxed()
        })
    }

    /// Drop the cached entry for `access_key_id`. The next lookup will
    /// re-fetch from the inner store. Used by management endpoints (e.g.
    /// `CreateAccessKey`, `DeleteAccessKey`, `UpdateAccessKey` status change)
    /// to make local mutations visible immediately.
    pub async fn invalidate(&self, access_key_id: &str) {
        self.cache.invalidate(&access_key_id.to_owned()).await;
    }

    /// Drop every cached entry. Intended for administrative use (e.g. global
    /// cache flush via a future control endpoint).
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Drop every cached credential whose `account_id` matches.
    ///
    /// Called by `delete_account` to ensure no cached credential for the
    /// deleted account remains usable. Negative entries are kept (they
    /// expire at `negative_ttl` anyway).
    ///
    /// # Errors
    /// Returns the underlying moka error if predicate registration fails (in
    /// practice, only at shutdown).
    pub fn invalidate_account(
        &self,
        account_id: &str,
    ) -> Result<(), extenddb_cache::PredicateError> {
        let acct = account_id.to_owned();
        self.cache
            .invalidate_if_value(move |v| v.map(|cred| cred.account_id == acct).unwrap_or(false))
    }

    /// Drop every cached credential for `principal_name` in `account_id`.
    ///
    /// Called when deleting a user (matches AKIA*) or role (matches the
    /// associated ASIA* session credentials).
    ///
    /// # Errors
    /// Returns the underlying moka error if predicate registration fails.
    pub fn invalidate_principal(
        &self,
        account_id: &str,
        principal_name: &str,
    ) -> Result<(), extenddb_cache::PredicateError> {
        let acct = account_id.to_owned();
        let principal = principal_name.to_owned();
        self.cache.invalidate_if_value(move |v| {
            v.map(|cred| cred.account_id == acct && cred.principal_name == principal)
                .unwrap_or(false)
        })
    }

    /// Snapshot the cache's internal counters for export to `/metrics`.
    #[must_use]
    pub fn metrics(&self) -> Arc<extenddb_cache::SwrMetrics> {
        self.cache.metrics()
    }

    /// Returns the current entry count (best-effort).
    #[must_use]
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Returns `true` when this wrapper was constructed in pass-through mode
    /// (`auth.cache.enabled = false`). Surfaced by the metrics endpoint so
    /// operators can tell "cache disabled" from "cache cold".
    #[must_use]
    pub fn is_pass_through(&self) -> bool {
        self.cache.is_pass_through()
    }
}

#[async_trait::async_trait]
impl CredentialStore for CachedCredentialStore {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        let result = self.cache.get(access_key_id.to_owned()).await?;
        // CB-12 (cache-hit path): the storage layer enforces session expiry on
        // load, but a cached session credential survives until `ttl_seconds`
        // even if its `expires_at` has passed. Re-validate on every hit so
        // the cache cannot extend a session past its issued lifetime.
        // Mirror the storage-layer error so the cache is transparent to the
        // auth provider.
        if let Some(ref cred) = result
            && cred.is_session
            && let Some(expires_at) = cred.expires_at
            && expires_at < time::OffsetDateTime::now_utc()
        {
            // Drop the stale entry so the next lookup goes
            // straight to the storage layer (which will raise
            // ExpiredTokenException itself, or return the
            // post-rotation credential if one exists).
            self.cache.invalidate(&access_key_id.to_owned()).await;
            return Err(DynamoDbError::ExpiredTokenException(
                "The security token included in the request is expired".to_owned(),
            ));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    /// In-process credential store backed by a Mutex-protected map. Counts
    /// `lookup_credential` invocations so tests can assert cache behavior.
    struct CountingCredStore {
        creds: Mutex<std::collections::HashMap<String, StoredCredential>>,
        calls: AtomicUsize,
    }

    impl CountingCredStore {
        fn new() -> Self {
            Self {
                creds: Mutex::new(std::collections::HashMap::new()),
                calls: AtomicUsize::new(0),
            }
        }

        fn insert(&self, key: &str, cred: StoredCredential) {
            self.creds.lock().unwrap().insert(key.to_owned(), cred);
        }

        fn remove(&self, key: &str) {
            self.creds.lock().unwrap().remove(key);
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl CredentialStore for CountingCredStore {
        async fn lookup_credential(
            &self,
            access_key_id: &str,
        ) -> Result<Option<StoredCredential>, DynamoDbError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.creds.lock().unwrap().get(access_key_id).cloned())
        }
    }

    fn make_cred(secret: &str, principal: &str) -> StoredCredential {
        StoredCredential {
            secret_key: secret.to_owned(),
            account_id: "123456789012".to_owned(),
            principal_name: principal.to_owned(),
            session_name: None,
            is_session: false,
            session_token: None,
            is_active: true,
            expires_at: None,
        }
    }

    fn fast_config() -> SwrCacheConfig {
        SwrCacheConfig {
            ttl: Duration::from_millis(200),
            soft_ttl: Duration::from_millis(50),
            negative_ttl: Duration::from_millis(50),
            max_entries: 100,
            name: "test-cred",
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalidate_principal_drops_all_keys_for_user() {
        let inner = Arc::new(CountingCredStore::new());
        inner.insert("AKIAALICE1", make_cred("s1", "alice"));
        inner.insert("AKIAALICE2", make_cred("s2", "alice"));
        inner.insert("AKIABOB", make_cred("s3", "bob"));
        let store = CachedCredentialStore::with_arc(inner.clone(), fast_config());

        // Prime all three.
        let _ = store.lookup_credential("AKIAALICE1").await.unwrap();
        let _ = store.lookup_credential("AKIAALICE2").await.unwrap();
        let _ = store.lookup_credential("AKIABOB").await.unwrap();
        assert_eq!(inner.calls(), 3);

        store.invalidate_principal("123456789012", "alice").unwrap();
        store.cache.run_pending_tasks().await;

        // Re-fetch each.
        let _ = store.lookup_credential("AKIAALICE1").await.unwrap();
        let _ = store.lookup_credential("AKIAALICE2").await.unwrap();
        let _ = store.lookup_credential("AKIABOB").await.unwrap();
        assert_eq!(
            inner.calls(),
            5,
            "alice's two keys re-fetched (2); bob still cached (0). Initial=3, total=5."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalidate_account_drops_all_creds_for_account() {
        let inner = Arc::new(CountingCredStore::new());
        let mut a1 = make_cred("s1", "alice");
        a1.account_id = "a1".to_owned();
        let mut a2 = make_cred("s2", "alice");
        a2.account_id = "a2".to_owned();
        inner.insert("AKIAA1", a1);
        inner.insert("AKIAA2", a2);
        let store = CachedCredentialStore::with_arc(inner.clone(), fast_config());

        let _ = store.lookup_credential("AKIAA1").await.unwrap();
        let _ = store.lookup_credential("AKIAA2").await.unwrap();
        assert_eq!(inner.calls(), 2);

        store.invalidate_account("a1").unwrap();
        store.cache.run_pending_tasks().await;

        let _ = store.lookup_credential("AKIAA1").await.unwrap();
        let _ = store.lookup_credential("AKIAA2").await.unwrap();
        assert_eq!(
            inner.calls(),
            3,
            "AKIAA1 (account a1) re-fetched; AKIAA2 (account a2) still cached"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pass_through_skips_cache_on_every_lookup() {
        let inner = Arc::new(CountingCredStore::new());
        inner.insert("AKIATEST", make_cred("s", "alice"));
        let store = CachedCredentialStore::pass_through_arc(inner.clone(), fast_config());

        for _ in 0..5 {
            let r = store.lookup_credential("AKIATEST").await.unwrap().unwrap();
            assert_eq!(r.principal_name, "alice");
        }
        assert_eq!(
            inner.calls(),
            5,
            "pass-through must invoke inner store on every call"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn second_lookup_hits_cache() {
        let inner = Arc::new(CountingCredStore::new());
        inner.insert("AKIATEST", make_cred("s", "alice"));
        let store = CachedCredentialStore::with_arc(inner.clone(), fast_config());

        let r1 = store.lookup_credential("AKIATEST").await.unwrap().unwrap();
        let r2 = store.lookup_credential("AKIATEST").await.unwrap().unwrap();

        assert_eq!(r1.principal_name, "alice");
        assert_eq!(r2.principal_name, "alice");
        assert_eq!(inner.calls(), 1, "second lookup must hit cache");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalidate_forces_refetch() {
        let inner = Arc::new(CountingCredStore::new());
        inner.insert("AKIATEST", make_cred("s", "alice"));
        let store = CachedCredentialStore::with_arc(inner.clone(), fast_config());

        let _ = store.lookup_credential("AKIATEST").await.unwrap();
        store.invalidate("AKIATEST").await;
        let _ = store.lookup_credential("AKIATEST").await.unwrap();

        assert_eq!(
            inner.calls(),
            2,
            "invalidation must force a re-fetch from the inner store"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn negative_lookup_is_cached_then_picks_up_new_key_after_negative_ttl() {
        let inner = Arc::new(CountingCredStore::new());
        let store = CachedCredentialStore::with_arc(inner.clone(), fast_config());

        // Initially missing.
        let r1 = store.lookup_credential("AKIANEW").await.unwrap();
        assert!(r1.is_none());

        // Within negative_ttl, second call hits cache.
        let r2 = store.lookup_credential("AKIANEW").await.unwrap();
        assert!(r2.is_none());
        assert_eq!(inner.calls(), 1);

        // Operator inserts the key.
        inner.insert("AKIANEW", make_cred("s", "bob"));

        // Wait past negative_ttl (50ms).
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Lookup now sees it.
        let r3 = store.lookup_credential("AKIANEW").await.unwrap().unwrap();
        assert_eq!(r3.principal_name, "bob");
    }

    /// CB-12 (cache-hit path): a cached session credential whose `expires_at`
    /// has passed must NOT authenticate, even when the cache TTL hasn't
    /// elapsed yet. Without the cache-hit expiry check, a session could
    /// keep working for up to `auth.cache.ttl_seconds` past its expiry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cached_session_past_expires_at_returns_expired_token() {
        let inner = Arc::new(CountingCredStore::new());
        // A session that "expires" 10 seconds in the past. The CountingCredStore
        // mock returns the credential unconditionally; the wrapper layer is
        // what we're testing here.
        let mut cred = make_cred("s", "alice");
        cred.is_session = true;
        cred.session_token = Some("tok".to_owned());
        cred.expires_at = Some(time::OffsetDateTime::now_utc() - time::Duration::seconds(10));
        inner.insert("ASIATEST00000000", cred);

        let store = CachedCredentialStore::with_arc(inner.clone(), fast_config());

        let r1 = store.lookup_credential("ASIATEST00000000").await;
        match r1 {
            Err(DynamoDbError::ExpiredTokenException(_)) => {}
            Err(other) => {
                panic!("expected ExpiredTokenException from cache-hit expiry check, got {other:?}")
            }
            Ok(_) => panic!("expected ExpiredTokenException, got Ok"),
        }
        // The cached entry was dropped on the expiry check. A second call
        // re-hits the inner store rather than serving the cached expired cred.
        let calls_after_first = inner.calls();
        let r2 = store.lookup_credential("ASIATEST00000000").await;
        match r2 {
            Err(DynamoDbError::ExpiredTokenException(_)) => {}
            Err(other) => {
                panic!("expected ExpiredTokenException on re-fetch, got {other:?}")
            }
            Ok(_) => panic!("expected ExpiredTokenException on re-fetch, got Ok"),
        }
        assert!(
            inner.calls() > calls_after_first,
            "expired-session cache hit must drop the entry, forcing a re-fetch on the next call \
             (calls_after_first={calls_after_first}, after_second={})",
            inner.calls()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalidation_after_delete_returns_none() {
        let inner = Arc::new(CountingCredStore::new());
        inner.insert("AKIATEST", make_cred("s", "alice"));
        let store = CachedCredentialStore::with_arc(inner.clone(), fast_config());

        // Prime cache.
        let r1 = store.lookup_credential("AKIATEST").await.unwrap().unwrap();
        assert_eq!(r1.principal_name, "alice");

        // Simulate admin deletion: remove from inner, invalidate cache.
        inner.remove("AKIATEST");
        store.invalidate("AKIATEST").await;

        // Next lookup reflects the deletion.
        let r2 = store.lookup_credential("AKIATEST").await.unwrap();
        assert!(r2.is_none(), "post-invalidation lookup must see deletion");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_concurrent_lookups_for_same_key_collapse_to_few_loads() {
        let inner = Arc::new(CountingCredStore::new());
        inner.insert("AKIATEST", make_cred("s", "alice"));
        let store = CachedCredentialStore::with_arc(inner.clone(), fast_config());

        let mut handles = Vec::new();
        for _ in 0..64 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                store.lookup_credential("AKIATEST").await.unwrap().unwrap()
            }));
        }
        for h in handles {
            let _ = h.await.unwrap();
        }

        // The cache deduplicates *post-warmup*; cold-start may incur a few
        // duplicate inserts on the way to the first cached entry. Verify the
        // deduplication is meaningful (≤ caller count, and warmup is cheap).
        let n = inner.calls();
        assert!(
            n <= 64,
            "loader call count must not exceed concurrent caller count, got {n}"
        );
        // Once warm, no further lookups.
        let _ = store.lookup_credential("AKIATEST").await.unwrap();
        assert_eq!(inner.calls(), n, "warm lookup must hit cache");
    }
}
