// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `CachedTableKeyInfoStore` — stale-while-revalidate cache for
//! `TableKeyInfo` lookups.
//!
//! `TableKeyInfo` is fetched per request by the auth path
//! (`request_helpers.rs::authorize_request` for `LeadingKeys` extraction)
//! and by engine handlers that operate on multiple tables (batch / transact
//! operations). Caching it eliminates a catalog roundtrip per request on the
//! steady-state hot path.
//!
//! Negative results — `Err(TableNotFound)` or table missing — are stored as
//! `None` and capped at the configured `negative_ttl` (defaults to 5s) so
//! newly-created tables become available quickly without restart.
//!
//! Write-through invalidation is exposed via [`CachedTableKeyInfoStore::invalidate`].
//! Engine handlers for `CreateTable`, `UpdateTable`, and `DeleteTable` (and
//! anything that mutates the catalog row, like `UpdateTimeToLive` or stream
//! enable/disable) call it after the catalog mutation succeeds.
//!
//! See `docs/design/12-auth-authz-cache.md` for the full design.

#![allow(clippy::module_name_repetitions)]

use std::sync::Arc;

use extenddb_cache::{Loader, SwrCache, SwrCacheConfig};
use extenddb_core::types::TableKeyInfo;
use extenddb_storage::StorageEngine;
use extenddb_storage::error::StorageError;
use futures::FutureExt;
use futures::future::BoxFuture;

/// Cache for `TableKeyInfo` lookups.
///
/// Cloning is cheap; clones share the underlying cache and the inner storage
/// reference.
#[derive(Clone)]
pub struct CachedTableKeyInfoStore {
    cache: SwrCache<(String, String), TableKeyInfo, StorageError>,
}

impl CachedTableKeyInfoStore {
    /// Wrap `inner` (typically the same `Arc<dyn StorageEngine>` held in
    /// `AppState.storage`) with a TTL'd cache.
    #[must_use]
    pub fn new(inner: Arc<dyn StorageEngine>, config: SwrCacheConfig) -> Self {
        Self {
            cache: SwrCache::new(Self::build_loader(inner), config),
        }
    }

    /// Construct a pass-through wrapper that bypasses the cache on every
    /// lookup. Used when `auth.cache.enabled = false`.
    #[must_use]
    pub fn pass_through(inner: Arc<dyn StorageEngine>, config: SwrCacheConfig) -> Self {
        Self {
            cache: SwrCache::pass_through(Self::build_loader(inner), config),
        }
    }

    fn build_loader(
        inner: Arc<dyn StorageEngine>,
    ) -> Loader<(String, String), TableKeyInfo, StorageError> {
        Arc::new(
            move |(account_id, table_name): (String, String)|
                  -> BoxFuture<'static, Result<Option<TableKeyInfo>, StorageError>> {
                let inner = inner.clone();
                async move {
                    match inner.table_key_info(&account_id, &table_name).await {
                        Ok(info) => Ok(Some(info)),
                        // Map "not found" to negative cache.
                        Err(StorageError::TableNotFound(_)) => Ok(None),
                        // Other errors (TableNotActive, Connection, Internal, etc.)
                        // are not cached.
                        Err(e) => Err(e),
                    }
                }
                .boxed()
            },
        )
    }

    /// Look up `TableKeyInfo` for the (account, table) pair.
    ///
    /// Returns the cached value if fresh, schedules a background refresh if
    /// stale-but-usable, or blocks on a fresh load on hard miss. Negative
    /// hits are reported back to the caller as `Err(TableNotFound)`.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::TableNotFound` for missing tables (cached
    /// negatively for `negative_ttl`). Returns the underlying storage error
    /// for any other failure (these errors are not cached).
    pub async fn get(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableKeyInfo, StorageError> {
        match self
            .cache
            .get((account_id.to_owned(), table_name.to_owned()))
            .await
        {
            Ok(Some(info)) => Ok(info),
            Ok(None) => Err(StorageError::TableNotFound(table_name.to_owned())),
            Err(e) => Err(e),
        }
    }

    /// Same as `get`, but returns `Ok(None)` for missing tables — matches the
    /// `.ok()` semantic used by the auth path's `LeadingKeys` extraction.
    pub async fn get_optional(&self, account_id: &str, table_name: &str) -> Option<TableKeyInfo> {
        self.cache
            .get((account_id.to_owned(), table_name.to_owned()))
            .await
            .ok()
            .flatten()
    }

    /// Drop the cached entry for `(account_id, table_name)`.
    ///
    /// Called by engine handlers after `CreateTable`, `UpdateTable`,
    /// `DeleteTable`, and any other operation that changes the table's
    /// key schema, indexes, stream specification, or active state.
    pub async fn invalidate(&self, account_id: &str, table_name: &str) {
        self.cache
            .invalidate(&(account_id.to_owned(), table_name.to_owned()))
            .await;
    }

    /// Drop every cached entry. Used by the manual `cache invalidate all`
    /// admin endpoint; not called from any normal write-through path.
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Snapshot the cache's internal counters.
    #[must_use]
    pub fn metrics(&self) -> Arc<extenddb_cache::SwrMetrics> {
        self.cache.metrics()
    }

    #[must_use]
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Returns `true` when the cache was constructed in pass-through mode
    /// (`auth.cache.enabled = false`). Used by the metrics endpoint so
    /// operators can distinguish "cache disabled" from "cache cold".
    #[must_use]
    pub fn is_pass_through(&self) -> bool {
        self.cache.is_pass_through()
    }
}

impl extenddb_storage::TableKeyInfoLookup for CachedTableKeyInfoStore {
    fn lookup<'a>(
        &'a self,
        account_id: &'a str,
        table_name: &'a str,
    ) -> BoxFuture<'a, Result<TableKeyInfo, StorageError>> {
        Box::pin(self.get(account_id, table_name))
    }
}

impl extenddb_auth::TableKeyInfoCacheInvalidator for CachedTableKeyInfoStore {
    fn invalidate<'a>(&'a self, account_id: &'a str, table_name: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate(account_id, table_name))
    }

    fn invalidate_all(&self) {
        self.invalidate_all();
    }
}

// No unit tests in this module: the SWR mechanics are covered by the
// extenddb-cache crate's tests (same `Loader<K, V, E>` shape), and the
// table_key_info round-trip is exercised end-to-end by tests/test_cache_coherence.py
// against a live Postgres backend.
