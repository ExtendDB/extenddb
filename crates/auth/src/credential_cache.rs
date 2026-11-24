// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `CachedCredentialStore`: the signing-key cache over any [`CredentialStore`]
//! (RFC-0000, section 6.1), with stale-while-revalidate semantics.
//!
//! The cache entry of an access key holds the principal fields, the session
//! flag, `expires_at`, the SHA-256 of the session token, and the `SigV4`
//! signing key for each scope date in use, in a slot of the locked key store
//! ([`crate::secmem`]). It holds no plaintext secret and no plaintext token.
//!
//! `lookup(access_key_id, scope_date)` first consults the in-memory cache.
//! Fresh entries (younger than `soft_ttl`) are returned immediately.
//! Stale-but-usable entries (between `soft_ttl` and `ttl`) are returned
//! immediately while a background task refreshes them. Misses block on the
//! upstream store: one decrypt, and one derive per scope date valid now.
//! Negative results (`Ok(None)`) are cached for the shorter `negative_ttl`.
//! Errors are never cached.
//!
//! Write-through invalidation is exposed via [`CachedCredentialStore::invalidate`]:
//! the management API calls it whenever an access key is created, deleted, or
//! has its active status changed, so self-induced changes propagate instantly
//! within the local instance.
//!
//! See `docs/design/12-auth-authz-cache.md` for the full design.

mod entry;

use std::sync::Arc;

use extenddb_cache::{Loader, SwrCache, SwrCacheConfig};
use extenddb_core::error::DynamoDbError;
use futures::FutureExt;
use futures::future::BoxFuture;
use time::OffsetDateTime;

use entry::{BufferPool, Destination, Key, ScopeDate, valid_dates};
pub use entry::{CachedCredential, ResolvedCredential};

use crate::secmem::{KeySlot, StoreFull};
use crate::sigv4::signing_key::derive_signing_key_into;
use crate::sigv4::verify::SCOPE_DATE_COMPLAINT;
use crate::{CredentialStore, StoredCredential};

/// The one service a signing key is derived for (`verify.rs` refuses any
/// other scope service).
const SERVICE: &str = "dynamodb";

/// The message of the `InternalServerError` the raw stores answer with.
const AUTH_INTERNAL_ERROR: &str = "Internal error during authentication";

/// The signing-key cache over a raw [`CredentialStore`].
///
/// The cache uses stale-while-revalidate semantics: see the module-level
/// docs and `docs/design/12-auth-authz-cache.md`.
///
/// Cloning is cheap; clones share the underlying cache and inner store.
#[derive(Clone)]
pub struct CachedCredentialStore {
    cache: SwrCache<String, Arc<CachedCredential>, DynamoDbError>,
    inner: Arc<dyn CredentialStore>,
    region: Arc<str>,
    buffers: Arc<BufferPool>,
}

impl CachedCredentialStore {
    /// Wrap `inner` with a cache configured by `config`, deriving signing
    /// keys for `region` (the server's region), with `workers` temporary
    /// buffers (one per runtime worker thread).
    ///
    /// The `inner` store is moved into an `Arc` so the cache's loader closure
    /// can outlive the constructor call. The wrapper itself is `Clone` and
    /// safe to share across tasks.
    #[must_use]
    pub fn new<T>(inner: T, config: SwrCacheConfig, region: &str, workers: usize) -> Self
    where
        T: CredentialStore + 'static,
    {
        let inner: Arc<dyn CredentialStore> = Arc::new(inner);
        Self::with_arc(inner, config, region, workers)
    }

    /// Wrap an already-`Arc`-wrapped `CredentialStore`.
    ///
    /// Use this when the caller already holds an `Arc<dyn CredentialStore>`
    /// (e.g. when sharing the same backing store across multiple wrappers).
    #[must_use]
    pub fn with_arc(
        inner: Arc<dyn CredentialStore>,
        config: SwrCacheConfig,
        region: &str,
        workers: usize,
    ) -> Self {
        let region: Arc<str> = Arc::from(region);
        Self {
            cache: SwrCache::new(Self::build_loader(&inner, &region), config),
            inner,
            region,
            buffers: BufferPool::new(workers),
        }
    }

    /// Construct a pass-through wrapper that bypasses the cache on every
    /// lookup. Used when `auth.cache.enabled = false`.
    #[must_use]
    pub fn pass_through<T>(inner: T, config: SwrCacheConfig, region: &str, workers: usize) -> Self
    where
        T: CredentialStore + 'static,
    {
        let inner: Arc<dyn CredentialStore> = Arc::new(inner);
        Self::pass_through_arc(inner, config, region, workers)
    }

    /// Like [`Self::pass_through`] but accepts an `Arc<dyn CredentialStore>`.
    #[must_use]
    pub fn pass_through_arc(
        inner: Arc<dyn CredentialStore>,
        config: SwrCacheConfig,
        region: &str,
        workers: usize,
    ) -> Self {
        let region: Arc<str> = Arc::from(region);
        Self {
            cache: SwrCache::pass_through(Self::build_loader(&inner, &region), config),
            inner,
            region,
            buffers: BufferPool::new(workers),
        }
    }

    /// The region signing keys are derived for.
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }

    fn build_loader(
        inner: &Arc<dyn CredentialStore>,
        region: &Arc<str>,
    ) -> Loader<String, Arc<CachedCredential>, DynamoDbError> {
        let inner = Arc::clone(inner);
        let region = Arc::clone(region);
        Arc::new(move |access_key_id: String|
            -> BoxFuture<'static, Result<Option<Arc<CachedCredential>>, DynamoDbError>> {
            let inner = inner.clone();
            let region = region.clone();
            async move { load(&*inner, &region, &access_key_id).await }.boxed()
        })
    }

    /// The entry of `access_key_id` with its signing key for `scope_date`
    /// (`YYYYMMDD`, checked against `X-Amz-Date` by `check_scope`).
    ///
    /// A hit derives nothing. A miss is one decrypt through the inner store
    /// and one derive per scope date valid now. A hit whose entry has no key
    /// for `scope_date` (a new date) is one decrypt and one derive, into the
    /// slot of a stale date when both are taken. When that read names another
    /// identity than the entry, the entry is dropped and the read answers.
    /// When the key store has no free slot the key is derived into a
    /// temporary buffer and not cached, with a warning at most once a
    /// minute. The slot or buffer is taken after the decrypt, so none is
    /// held across the inner store's await.
    ///
    /// # Errors
    ///
    /// `ExpiredTokenException` for a session past `expires_at`,
    /// `InvalidSignatureException` for a `scope_date` that is not eight
    /// bytes, `InternalServerError` when the key store has no free slot and
    /// no temporary buffer is free, and the inner store's errors.
    pub async fn lookup(
        &self,
        access_key_id: &str,
        scope_date: &str,
    ) -> Result<Option<ResolvedCredential>, DynamoDbError> {
        let Some(date) = ScopeDate::parse(scope_date) else {
            return Err(DynamoDbError::InvalidSignatureException(
                SCOPE_DATE_COMPLAINT.to_owned(),
            ));
        };
        let Some(entry) = self.cache.get(access_key_id.to_owned()).await? else {
            return Ok(None);
        };
        // CB-12 (cache-hit path): the storage layer enforces session expiry on
        // load, but a cached session credential survives until `ttl_seconds`
        // even if its `expires_at` has passed. Re-validate on every hit so
        // the cache cannot extend a session past its issued lifetime.
        // Mirror the storage-layer error so the cache is transparent to the
        // auth provider.
        let now = OffsetDateTime::now_utc();
        if entry.is_session
            && let Some(expires_at) = entry.expires_at
            && expires_at < now
        {
            // Drop the stale entry so the next lookup goes straight to the
            // storage layer (which will raise ExpiredTokenException itself,
            // or return the post-rotation credential if one exists).
            self.cache.invalidate(&access_key_id.to_owned()).await;
            return Err(DynamoDbError::ExpiredTokenException(
                "The security token included in the request is expired".to_owned(),
            ));
        }
        if let Some(slot) = entry.key(date) {
            return Ok(Some(ResolvedCredential {
                entry,
                key: Key::Slot(slot),
            }));
        }
        // A new scope date, or a key the loader could not place: one decrypt
        // and one derive. The secret is read first and the slot or buffer is
        // taken after the read, so none is held across the inner store's
        // await: other requests on this worker thread run during that await.
        let Some(secret) = Secret::read(&*self.inner, access_key_id).await? else {
            self.cache.invalidate(&access_key_id.to_owned()).await;
            return Ok(None);
        };
        // The key comes from this read, so the identity does too. When the
        // read names another identity than the entry (the access key ID was
        // deleted and imported for another principal, or changed, through
        // another server), the entry is dropped and this read answers.
        let read = secret.fields();
        let entry = if read.same_identity(&entry) {
            entry
        } else {
            self.cache.invalidate(&access_key_id.to_owned()).await;
            Arc::new(read)
        };
        let mut destination = self.destination()?;
        secret.derive_into(
            date,
            &self.region,
            match &mut destination {
                Destination::Slot(slot) => slot.expose_mut(),
                Destination::Buffer(lease) => lease.expose_mut(),
            },
        );
        drop(secret);
        let key = match destination {
            Destination::Slot(slot) => {
                let slot = Arc::new(slot);
                entry.insert_key(date, Arc::clone(&slot), now);
                Key::Slot(slot)
            }
            Destination::Buffer(lease) => Key::Buffer(lease),
        };
        Ok(Some(ResolvedCredential { entry, key }))
    }

    /// Where the next derived key goes: a slot of the key store, or, when
    /// the store has no free slot, a temporary buffer that serves this
    /// request only.
    ///
    /// # Errors
    ///
    /// `InternalServerError` when no buffer is free either: the key is never
    /// derived into unlocked memory.
    fn destination(&self) -> Result<Destination, DynamoDbError> {
        match KeySlot::new() {
            Ok(slot) => Ok(Destination::Slot(slot)),
            Err(StoreFull) => {
                let Some(lease) = self.buffers.lease() else {
                    if self.buffers.exhausted_warning.due() {
                        tracing::warn!(
                            cache = self.cache.name(),
                            "the locked key store has no free slot and every temporary buffer \
                             is in use; the request fails rather than derive a signing key into \
                             unlocked memory"
                        );
                    }
                    return Err(DynamoDbError::InternalServerError(
                        AUTH_INTERNAL_ERROR.to_owned(),
                    ));
                };
                if self.buffers.full_warning.due() {
                    tracing::warn!(
                        cache = self.cache.name(),
                        live_entries = self.cache.entry_count(),
                        "the locked key store has no free slot; signing keys are derived into \
                         a temporary buffer and not cached until eviction frees slots"
                    );
                }
                Ok(Destination::Buffer(lease))
            }
        }
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
            .invalidate_if_value(move |v| v.is_some_and(|cred| cred.account_id == acct))
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
            v.is_some_and(|cred| cred.account_id == acct && cred.principal_name == principal)
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

/// The loader of a miss: one read, then a slot for each scope date valid
/// now, each filled by [`Secret::derive_into`]. The slots are taken after
/// the read, so a miss holds none across the inner store's await.
async fn load(
    inner: &dyn CredentialStore,
    region: &str,
    access_key_id: &str,
) -> Result<Option<Arc<CachedCredential>>, DynamoDbError> {
    let Some(secret) = Secret::read(inner, access_key_id).await? else {
        return Ok(None);
    };
    let now = OffsetDateTime::now_utc();
    let (today, adjacent) = valid_dates(now);
    let entry = secret.fields();
    for date in std::iter::once(today).chain(adjacent) {
        // A full store leaves the entry without keys; `lookup` then derives
        // into a temporary buffer for each request until eviction frees slots.
        let Ok(mut slot) = KeySlot::new() else {
            break;
        };
        secret.derive_into(date, region, slot.expose_mut());
        entry.insert_key(date, Arc::new(slot), now);
    }
    Ok(Some(Arc::new(entry)))
}

/// The stored credential of one access key, read through the inner store.
/// The one place that sees the secret: it derives signing keys and gives the
/// credential's fields, and dropping it wipes the secret and the token
/// (`StoredCredential` is `ZeroizeOnDrop`). A later key process (RFC-0000,
/// section 6.3) replaces it with a call that returns the signing keys.
struct Secret(StoredCredential);

impl Secret {
    /// Decrypt the stored secret of `access_key_id` through `inner`. `None`
    /// when the key does not exist. Callers take their slot or temporary
    /// buffer after this returns, never before.
    async fn read(
        inner: &dyn CredentialStore,
        access_key_id: &str,
    ) -> Result<Option<Self>, DynamoDbError> {
        Ok(inner.lookup_credential(access_key_id).await?.map(Self))
    }

    /// Derive the signing key of `date` into `out`.
    fn derive_into(&self, date: ScopeDate, region: &str, out: &mut [u8; 32]) {
        derive_signing_key_into(
            self.0.secret_key.as_bytes(),
            date.as_str(),
            region,
            SERVICE,
            out,
        );
    }

    /// The credential's fields, with no keys.
    fn fields(&self) -> CachedCredential {
        CachedCredential::from_stored(&self.0)
    }
}

#[cfg(test)]
mod tests;
