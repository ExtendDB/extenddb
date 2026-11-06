// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bulk store resolution at startup: the `[bulk]` section, the dev-mode
//! default, the startup pass over the accounts, and the count of the
//! operations recorded on a store other than the configured one.

use std::collections::BTreeMap;
use std::sync::Arc;

use extenddb_auth::CredentialStore;
use extenddb_config as config;
use extenddb_engine::bulk_put::store::{
    BulkStore, S3_STORE_LABEL, S3StoreSettings, recorded_store_kind,
};
use extenddb_storage::{CatalogStore, StorageEngine};

/// Operations read per page while the startup pass counts them.
const COUNT_PAGE: u32 = 100;

/// The bulk store the server serves bulk operations from, or `None` when
/// bulk operations are disabled. When the S3 store's credentials are an
/// access key this server also accepts from its clients, in
/// `credential_store` or as the dev-mode pair, a warning says so.
///
/// # Errors
///
/// A `[bulk]` section the server cannot serve: a `[bulk.s3]` section that
/// fails its checks, a missing `[bulk.filesystem] path`, or a root that
/// cannot be resolved.
pub(crate) async fn resolve_bulk_store(
    app_config: &config::AppConfig,
    dev_mode: bool,
    catalog_store: &Arc<dyn CatalogStore>,
    storage: &Arc<dyn StorageEngine>,
    credential_store: &Arc<dyn CredentialStore>,
) -> anyhow::Result<Option<Arc<BulkStore>>> {
    resolve_checked(
        app_config.bulk.as_ref(),
        &app_config.server.region,
        dev_mode,
        catalog_store,
        storage,
        credential_store,
    )
    .await
}

/// [`resolve`], then the check of the S3 store's key against the keys the
/// server accepts from its clients.
async fn resolve_checked(
    bulk: Option<&config::BulkConfig>,
    server_region: &str,
    dev_mode: bool,
    catalog_store: &Arc<dyn CatalogStore>,
    storage: &Arc<dyn StorageEngine>,
    credential_store: &Arc<dyn CredentialStore>,
) -> anyhow::Result<Option<Arc<BulkStore>>> {
    let store = resolve(bulk, server_region, dev_mode, catalog_store, storage).await?;
    if let Some(access_key_id) = store.as_deref().and_then(BulkStore::s3_access_key_id) {
        crate::s3_client_key::warn_of_a_client_key(
            S3_STORE_LABEL,
            access_key_id,
            credential_store.as_ref(),
            dev_mode,
        )
        .await;
    }
    Ok(store)
}

/// [`resolve_bulk_store`] over the `[bulk]` section and `[server] region`.
async fn resolve(
    bulk: Option<&config::BulkConfig>,
    server_region: &str,
    dev_mode: bool,
    catalog_store: &Arc<dyn CatalogStore>,
    storage: &Arc<dyn StorageEngine>,
) -> anyhow::Result<Option<Arc<BulkStore>>> {
    // Resolve the bulk store ([bulk] section). Bulk put reads its input
    // files and writes its error reports through this one store: the
    // filesystem store beneath one root, namespaced per account, or the S3
    // store; it is independent of the [import] and [export] roots. Absent
    // means the feature is disabled, except in dev mode, which substitutes
    // a local directory so zero-config serve has a working store.
    let dev_default_bulk;
    let effective_bulk = match (bulk, dev_mode) {
        (Some(cfg), _) => Some((cfg, false)),
        (None, true) => {
            dev_default_bulk = config::BulkConfig {
                store: config::BulkStoreKind::Filesystem,
                filesystem: Some(config::BulkFilesystemConfig {
                    path: config::DEV_DEFAULT_BULK_PATH.to_owned(),
                }),
                s3: None,
            };
            Some((&dev_default_bulk, true))
        }
        (None, false) => None,
    };
    let bulk_store: Option<Arc<BulkStore>> = match effective_bulk {
        None => {
            tracing::info!("Bulk operations disabled: no [bulk] section");
            // With no store the worker cannot read input, so an IN_PROGRESS
            // operation cannot progress: it stays non-terminal, counts
            // against the one-in-flight-per-table guard and the per-account
            // limit, and never ages into retention. Count what the catalog
            // still holds non-terminal and warn once, so an operator who
            // removed the section sees the operations are frozen.
            // Cancellations are the exception: the bulk operation worker
            // still finishes CANCELLING rows (see bulk_operation_worker).
            match catalog_store.list_all_accounts().await {
                Ok(accounts) => {
                    let mut non_terminal: u64 = 0;
                    for (account_id, _) in accounts {
                        match storage.count_non_terminal(&account_id).await {
                            Ok(count) => non_terminal = non_terminal.saturating_add(count),
                            // A backend with no bulk support answers
                            // Validation; there is nothing to count.
                            Err(extenddb_storage::error::StorageError::Validation(_)) => break,
                            Err(e) => tracing::warn!(
                                "Cannot count non-terminal bulk operations for account \
                                 {account_id}: {e}"
                            ),
                        }
                    }
                    if non_terminal > 0 {
                        tracing::warn!(
                            non_terminal_bulk_operations = non_terminal,
                            "Bulk operations are disabled but {non_terminal} operation(s) are \
                             non-terminal in the catalog; they will not progress until a [bulk] \
                             store is configured, except cancellations, which the bulk operation \
                             worker still finishes"
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    "Cannot list accounts to count non-terminal bulk operations: {e:?}"
                ),
            }
            None
        }
        Some((bulk_cfg, is_dev_default)) => {
            match bulk_cfg.store {
                config::BulkStoreKind::S3 => {
                    // No account directories exist on this store: every
                    // account names S3 buckets directly.
                    let settings = s3_settings(bulk_cfg, server_region)?;
                    let store = BulkStore::s3(&settings).await;
                    let described = bulk_cfg
                        .s3
                        .clone()
                        .unwrap_or_default()
                        .describe(server_region);
                    tracing::info!("Bulk store: {described}");
                    warn_of_other_store_operations(catalog_store, storage, &store).await;
                    return Ok(Some(Arc::new(store)));
                }
                config::BulkStoreKind::Filesystem => {
                    // The section is checked whatever the store, so a
                    // credential or a URL with user information in it is
                    // refused before a later `store = "s3"` reads it.
                    if let Some(s3) = &bulk_cfg.s3 {
                        s3.validate().map_err(anyhow::Error::msg)?;
                    }
                }
            }
            let Some(fs_cfg) = &bulk_cfg.filesystem else {
                if bulk_cfg.s3.is_some() {
                    anyhow::bail!(
                        "[bulk.filesystem] path is required when store = \"filesystem\", the \
                         default; to use the [bulk.s3] section, set store = \"s3\" in [bulk], \
                         or add [bulk.filesystem]"
                    );
                }
                anyhow::bail!("[bulk.filesystem] path is required when store = \"filesystem\"");
            };
            if bulk_cfg.s3.is_some() {
                tracing::info!("[bulk.s3] is ignored: store = \"filesystem\"");
            }
            let expanded = config::expand_tilde(&fs_cfg.path);
            let canonical = extenddb_engine::bulk_put::store_root::resolve_store_root(
                std::path::Path::new(&expanded),
            )
            .map_err(|e| anyhow::anyhow!("Cannot resolve bulk store path {expanded}: {e}"))?;
            if is_dev_default {
                // The dev-mode banner names the configured value; this line
                // carries the canonical path, at warn so it sits with the
                // banner's visibility.
                tracing::warn!("Bulk store: {}", canonical.display());
            } else {
                tracing::info!("Bulk store: filesystem at {}", canonical.display());
            }
            // Create every account's directory so a caller can place files
            // without a first Start. A failure is per account and not fatal:
            // Start creates the directory again when it is missing.
            match catalog_store.list_all_accounts().await {
                Ok(accounts) => {
                    for (account_id, _) in accounts {
                        if let Err(e) = extenddb_engine::bulk_put::store_root::ensure_account_dir(
                            &canonical,
                            &account_id,
                        ) {
                            tracing::warn!(
                                "Cannot create bulk store directory for account {account_id}: {e}"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Cannot list accounts to create bulk store directories: {e:?}");
                }
            }
            let store = BulkStore::filesystem(Arc::new(canonical));
            warn_of_other_store_operations(catalog_store, storage, &store).await;
            Some(Arc::new(store))
        }
    };
    Ok(bulk_store)
}

/// The S3 store's settings from `[bulk.s3]`, after its startup checks. An
/// absent section takes every default, and the region defaults to
/// `[server] region`. The checks' notices are logged at warn, and a
/// `[bulk.filesystem]` section beside `store = "s3"` is ignored with an info
/// line.
fn s3_settings(
    bulk_cfg: &config::BulkConfig,
    server_region: &str,
) -> anyhow::Result<S3StoreSettings> {
    let s3 = bulk_cfg.s3.clone().unwrap_or_default();
    for notice in s3.validate().map_err(anyhow::Error::msg)? {
        tracing::warn!("{notice}");
    }
    if bulk_cfg.filesystem.is_some() {
        tracing::info!("[bulk.filesystem] is ignored: store = \"s3\"");
    }
    Ok(S3StoreSettings {
        region: s3.region.unwrap_or_else(|| server_region.to_owned()),
        endpoint_url: s3.endpoint_url,
        force_path_style: s3.force_path_style,
    })
}

/// Warn once per store kind of the non-terminal operations that were
/// started on a store of another kind than `store`, as an operator who
/// switched `[bulk] store` with operations in flight needs to know: the bulk
/// operation worker fails each of them with `InternalServerError` without
/// reading their input, and finishes a cancellation as `CANCELLED`.
async fn warn_of_other_store_operations(
    catalog_store: &Arc<dyn CatalogStore>,
    storage: &Arc<dyn StorageEngine>,
    store: &BulkStore,
) {
    let configured = store.kind_name();
    let counts = other_store_operations(catalog_store, storage, configured, COUNT_PAGE).await;
    for (recorded, count) in counts {
        tracing::warn!(
            other_store_bulk_operations = count,
            recorded_store = %recorded,
            configured_store = configured,
            "{count} non-terminal bulk operation(s) were started on the {recorded} store, but \
             the {configured} store is configured; the bulk operation worker fails them with \
             InternalServerError without reading their input, except cancellations, which it \
             finishes"
        );
    }
}

/// The non-terminal (`IN_PROGRESS` or `CANCELLING`) operations the catalog
/// holds whose recorded store is not `configured`, counted by the store
/// they recorded. An operation with no recorded run context was started on
/// the filesystem store; one whose run context does not parse is not
/// counted (the worker fails it and logs the detail). Only an account with
/// non-terminal operations is listed, `page` operations at a time, and only
/// until all of them were seen.
async fn other_store_operations(
    catalog_store: &Arc<dyn CatalogStore>,
    storage: &Arc<dyn StorageEngine>,
    configured: &str,
    page: u32,
) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    let accounts = match catalog_store.list_all_accounts().await {
        Ok(accounts) => accounts,
        Err(e) => {
            tracing::warn!(
                "Cannot list accounts to count bulk operations started on another store: {e:?}"
            );
            return counts;
        }
    };
    'accounts: for (account_id, _) in accounts {
        let mut unseen = match storage.count_non_terminal(&account_id).await {
            Ok(0) => continue,
            Ok(count) => count,
            // A backend with no bulk support answers Validation; there is
            // nothing to count.
            Err(extenddb_storage::error::StorageError::Validation(_)) => break,
            Err(e) => {
                tracing::warn!(
                    "Cannot count non-terminal bulk operations for account {account_id}: {e}"
                );
                continue;
            }
        };
        let mut after = None;
        while unseen > 0 {
            let (records, next) = match storage.list_bulk_operations(&account_id, page, after).await
            {
                Ok(listed) => listed,
                Err(e) => {
                    tracing::warn!("Cannot list bulk operations for account {account_id}: {e}");
                    continue 'accounts;
                }
            };
            for record in &records {
                if !matches!(record.status.as_str(), "IN_PROGRESS" | "CANCELLING") {
                    continue;
                }
                unseen = unseen.saturating_sub(1);
                if let Ok(recorded) = recorded_store_kind(record)
                    && recorded != configured
                {
                    *counts.entry(recorded).or_insert(0) += 1;
                }
            }
            match next {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
    }
    counts
}

#[cfg(test)]
mod tests;
