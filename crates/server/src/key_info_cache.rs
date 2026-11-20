// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `CachedTableKeyInfoStore`, a stale-while-revalidate cache for
//! `TableKeyInfo` lookups.
//!
//! `TableKeyInfo` is fetched through this cache by the auth path for the
//! single-table operations (`request_helpers.rs::authorize_request`, for
//! `LeadingKeys` extraction) and by single-table engine handlers for a table
//! the auth path did not read. The batch and transaction operations do not use
//! it: authorization reads their tables from storage and the engine resolves
//! them from what authorization read. Caching it eliminates a catalog
//! roundtrip per request on the steady-state hot path.
//!
//! Negative results (`Err(TableNotFound)` or table missing) are stored as
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
                    let key_info = inner.table_key_info(&account_id, &table_name).await;
                    load_key_info(key_info, || async {
                        inner
                            .describe_table(
                                &account_id,
                                extenddb_core::types::DescribeTableInput {
                                    table_name: table_name.clone(),
                                },
                            )
                            .await
                            .map(|desc| desc.table_status)
                    })
                    .await
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

/// What the loader caches for one `table_key_info` answer.
///
/// `Ok(Some)` is the key info. `Ok(None)` is a negative entry, cached for
/// `negative_ttl`: the table is not in the catalog at all. `Err` is not
/// cached and is what the caller sees.
///
/// Every backend's `table_key_info` answers `TableNotFound` both for a name
/// the catalog does not have and for one it has in a state other than
/// `ACTIVE` (a `CreateTable` or `RestoreTableFromBackup` target that is still
/// `CREATING`; the data plane reports both as `ResourceNotFoundException`, as
/// the service does). Caching the second as a negative entry made reads on a
/// restored table fail for up to `negative_ttl` after `DescribeTable` reported
/// it `ACTIVE`: a probe during the copy stored the entry, and nothing
/// invalidated it at the transition (the service's first
/// read after `ACTIVE` succeeds). So on `TableNotFound` the catalog is asked
/// once more, with `describe_table`, which answers for a table in any state:
/// a table it finds is reported not found to the caller but not cached, so
/// the next read asks the catalog again; a table it does not find is cached
/// negatively as before. An error from the probe is treated as "found" (not
/// cached): a miss is rare, and a negative entry stored on a database error
/// would be the wrong answer for 5 s. The cost: a request against a name
/// that is not `ACTIVE` loads once, uncached, which is one `table_key_info`
/// and one `describe_table` on the auth path; the engine does not load
/// again, because authorization records the table as missing in the
/// snapshot it hands the engine (`OperationContext::authorized_key_info`). On
/// PostgreSQL
/// `describe_table` is itself two queries. Measured on SQLite with a
/// `GetItem` loop on a `CREATING` restore target: see
/// `docs/differences-from-dynamodb.md`, "Reads on a restore target".
async fn load_key_info<P, F>(
    key_info: Result<TableKeyInfo, StorageError>,
    probe: P,
) -> Result<Option<TableKeyInfo>, StorageError>
where
    P: FnOnce() -> F,
    F: std::future::Future<Output = Result<extenddb_core::types::TableStatus, StorageError>>,
{
    match key_info {
        Ok(info) => Ok(Some(info)),
        Err(StorageError::TableNotFound(name)) => match probe().await {
            Err(StorageError::TableNotFound(_)) => Ok(None),
            // In the catalog but not ACTIVE (or the probe failed): not found
            // to this caller, and not remembered.
            Ok(_) | Err(_) => Err(StorageError::TableNotFound(name)),
        },
        // Other errors (TableNotActive, Connection, Internal, etc.)
        // are not cached.
        Err(e) => Err(e),
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

// The SWR mechanics are covered by the extenddb-cache crate's tests (same
// `Loader<K, V, E>` shape), and the table_key_info round-trip is exercised
// end-to-end by tests/test_cache_coherence.py against a live Postgres backend
// and by tests/test_backup_restore_fidelity.py (a read on a restore target
// while CREATING, then the first read after ACTIVE).
#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::TableStatus;
    use std::cell::Cell;

    fn info() -> TableKeyInfo {
        TableKeyInfo::default()
    }

    /// A probe that records whether it ran and answers `status`.
    fn probe(
        ran: &Cell<bool>,
        status: Result<TableStatus, StorageError>,
    ) -> impl FnOnce() -> std::future::Ready<Result<TableStatus, StorageError>> + '_ {
        move || {
            ran.set(true);
            std::future::ready(status)
        }
    }

    #[tokio::test]
    async fn an_active_table_is_cached_without_a_probe() {
        let ran = Cell::new(false);
        let got = load_key_info(Ok(info()), probe(&ran, Ok(TableStatus::Active))).await;
        assert!(matches!(got, Ok(Some(_))));
        assert!(!ran.get(), "no probe on a hit");
    }

    #[tokio::test]
    async fn a_name_the_catalog_does_not_have_is_cached_negatively() {
        let ran = Cell::new(false);
        let got = load_key_info(
            Err(StorageError::TableNotFound("t".to_owned())),
            probe(&ran, Err(StorageError::TableNotFound("t".to_owned()))),
        )
        .await;
        assert!(matches!(got, Ok(None)), "{got:?}");
        assert!(ran.get());
    }

    /// Measured on DynamoDB: a name the catalog has in a non-ACTIVE state is not
    /// found to the caller, and not cached, so the first read after the
    /// table becomes ACTIVE asks the catalog again.
    #[tokio::test]
    async fn a_creating_table_is_not_found_but_not_cached() {
        for status in [
            TableStatus::Creating,
            TableStatus::Updating,
            TableStatus::Deleting,
            TableStatus::Active,
        ] {
            let ran = Cell::new(false);
            let got = load_key_info(
                Err(StorageError::TableNotFound("t".to_owned())),
                probe(&ran, Ok(status)),
            )
            .await;
            assert!(
                matches!(&got, Err(StorageError::TableNotFound(n)) if n == "t"),
                "{status:?}: {got:?}"
            );
            assert!(ran.get());
        }
    }

    #[tokio::test]
    async fn a_failed_probe_is_not_cached_either() {
        let ran = Cell::new(false);
        let got = load_key_info(
            Err(StorageError::TableNotFound("t".to_owned())),
            probe(&ran, Err(StorageError::Connection("down".to_owned()))),
        )
        .await;
        assert!(
            matches!(got, Err(StorageError::TableNotFound(_))),
            "{got:?}"
        );
    }

    #[tokio::test]
    async fn other_errors_pass_through_without_a_probe() {
        let ran = Cell::new(false);
        let got = load_key_info(
            Err(StorageError::TableNotActive("t".to_owned())),
            probe(&ran, Ok(TableStatus::Active)),
        )
        .await;
        assert!(
            matches!(got, Err(StorageError::TableNotActive(_))),
            "{got:?}"
        );
        assert!(!ran.get());
    }
}
