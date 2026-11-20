// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! GSI Backfill Recovery Worker (`CREATING -> ACTIVE`).
//!
//! Steady-state writes and `sweeper` roll-forwards maintain `ACTIVE` GSI shadow
//! tables synchronously, so this background worker ONLY drives interrupted
//! `CREATING` GSIs whose per-index distributed build lease
//! (`lock:gsi_build:<table_id>:<index_name>`) is no longer held by the
//! originating `UpdateTable` call.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::types::{GsiDescription, IndexInfo, IndexType, Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::{CancellationToken, sleep_or_shutdown};

use crate::BigtableEngine;
use crate::catalog::{Catalog, CatalogJsonAction};
use crate::data::admin::AdminClient;
use crate::data::encoding::row_key;
use crate::data::item_ops::{ItemOps, next_occ_version};
use crate::data::query_scan::QueryScan;
use crate::runtime_hooks::run_leased_worker;

const SCAN_BATCH: i64 = 200;
const GSI_BUILD_LEASE: Duration = Duration::from_secs(60);

/// Distributed catalog lock name guarding a single GSI's `CREATING -> ACTIVE`
/// build across nodes (`lock:gsi_build:<table_id>:<index_name>`).
#[must_use]
pub fn gsi_build_lock_name(table_id: &str, index_name: &str) -> String {
    format!("gsi_build:{table_id}:{index_name}")
}

pub async fn run(engine: Arc<BigtableEngine>, cadence: Duration, shutdown: CancellationToken) {
    tracing::info!("bigtable GSI recovery worker started; cadence={cadence:?}");
    let catalog_client = engine.catalog_client_arc();
    let owner = engine.node_id().to_owned();
    let lease_duration = (cadence * 3).max(Duration::from_secs(30));

    run_leased_worker(
        catalog_client,
        "gsi_reconciler",
        owner,
        cadence,
        lease_duration,
        shutdown,
        |pass_token| {
            let engine = engine.clone();
            async move {
                if let Err(e) = sweep_once_with_token(&engine, &pass_token).await {
                    tracing::warn!("GSI recovery worker pass error: {e}");
                }
            }
        },
    )
    .await;
    tracing::info!("bigtable GSI recovery worker stopped");
}

pub async fn sweep_once(engine: &BigtableEngine) -> Result<(), StorageError> {
    let token = CancellationToken::new();
    sweep_once_with_token(engine, &token).await
}

async fn sweep_once_with_token(
    engine: &BigtableEngine,
    shutdown: &CancellationToken,
) -> Result<(), StorageError> {
    let cat = engine.cat();
    let tables_raw = cat
        .scan_prefix(crate::catalog::keys::TABLE_META_SCAN_PREFIX)
        .await?;

    for (k, val) in tables_raw {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let mut parts = k.splitn(3, ':');
        let _ = parts.next();
        let (Some(acct_id), Some(table_name)) = (parts.next(), parts.next()) else {
            continue;
        };

        let Some(data_table) = val.get("data_table").and_then(serde_json::Value::as_str) else {
            tracing::warn!("missing data_table in catalog row {k}");
            continue;
        };
        let desc = match engine.deserialize_table_description(&val) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("deserialize TableDescription for {k} failed: {e}");
                continue;
            }
        };

        let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
        if !gsis.iter().any(|g| g.index_status == "CREATING") {
            continue;
        }

        let base_key_info = build_reconciler_base_key_info(acct_id, table_name, &desc);

        for gsi in gsis {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            // Only drive `CREATING` indexes; `ACTIVE` indexes are maintained
            // synchronously by writes and post-commit `sweeper` roll-forwards.
            if gsi.index_status != "CREATING"
                || engine.is_gsi_building(&desc.table_id, &gsi.index_name)
            {
                continue;
            }

            let build_lock = gsi_build_lock_name(&desc.table_id, &gsi.index_name);
            let owner = format!("gsi-worker-{}", engine.node_id());
            match cat.try_lock(&build_lock, &owner, GSI_BUILD_LEASE).await {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    tracing::warn!(
                        index = %gsi.index_name,
                        table = %table_name,
                        error = %e,
                        "failed to check GSI build lease"
                    );
                    continue;
                }
            }

            let renew_token = shutdown.child_token();
            let renew_tok_bg = renew_token.clone();
            let renew_client = engine.catalog_client_arc();
            let renew_lock_name = build_lock.clone();
            let renew_owner = owner.clone();
            let renewal = tokio::spawn(async move {
                let every = GSI_BUILD_LEASE / 3;
                while sleep_or_shutdown(&renew_tok_bg, every).await {
                    let c = Catalog::new(&renew_client);
                    if let Ok(false) = c
                        .renew_lock(&renew_lock_name, &renew_owner, GSI_BUILD_LEASE)
                        .await
                    {
                        renew_tok_bg.cancel();
                        return;
                    }
                }
            });

            let outcome = recover_creating_gsi(
                engine,
                acct_id,
                table_name,
                &base_key_info,
                data_table,
                &gsi,
                &renew_token,
            )
            .await;

            renew_token.cancel();
            renewal.abort();
            let _ = renewal.await;
            let _ = cat.release_lock(&build_lock, &owner).await;

            if let Err(e) = outcome {
                tracing::warn!(
                    "recover CREATING GSI {} for table {table_name} failed: {e}",
                    gsi.index_name
                );
            }
        }
    }
    Ok(())
}

async fn recover_creating_gsi(
    engine: &BigtableEngine,
    acct_id: &str,
    table_name: &str,
    base_key_info: &TableKeyInfo,
    data_table: &str,
    gsi: &GsiDescription,
    token: &CancellationToken,
) -> Result<(), StorageError> {
    // Re-verify under the build lock that the GSI is still `CREATING`.
    let meta_key = crate::catalog::keys::table_meta(acct_id, table_name);
    let Some(cur_row) = engine.cat().get(&meta_key).await? else {
        return Ok(());
    };
    let cur_desc = engine.deserialize_table_description(&cur_row)?;
    let still_creating = cur_desc
        .global_secondary_indexes
        .as_ref()
        .is_some_and(|gsis| {
            gsis.iter()
                .any(|g| g.index_name == gsi.index_name && g.index_status == "CREATING")
        });
    if !still_creating {
        return Ok(());
    }

    ensure_gsi_shadow_table(engine, data_table, &gsi.index_name).await?;
    // Wait out peer metadata-cache + fast-write grace before scanning base rows
    // so no peer can commit a base write without seeing the `CREATING` GSI.
    engine
        .await_schema_visibility_fence(data_table, token)
        .await?;
    if token.is_cancelled() {
        return Err(StorageError::Transient(
            "GSI build lease cancelled during visibility fence".into(),
        ));
    }
    reconcile_table_gsi(engine, base_key_info, data_table, gsi).await?;
    if token.is_cancelled() {
        return Err(StorageError::Transient(
            "GSI build lease lost before ACTIVE promotion".into(),
        ));
    }
    promote_creating_gsi_to_active(engine, acct_id, table_name, gsi).await
}

async fn ensure_gsi_shadow_table(
    engine: &BigtableEngine,
    data_table: &str,
    index_name: &str,
) -> Result<(), StorageError> {
    let shadow_table = crate::gsi::shadow_table_id(data_table, index_name);
    let mut admin = AdminClient::connect(engine.client_ref())
        .await
        .map_err(StorageError::Internal)?;
    let families = vec![
        ("d", Some(crate::data::admin::gc_max_versions(1))),
        ("m", Some(crate::data::admin::gc_max_versions(1))),
        ("t", None),
    ];
    admin
        .create_table(&shadow_table, &families)
        .await
        .map_err(StorageError::Internal)
}

pub(crate) fn build_reconciler_base_key_info(
    acct_id: &str,
    table_name: &str,
    desc: &extenddb_core::types::TableDescription,
) -> TableKeyInfo {
    TableKeyInfo {
        table_name: table_name.to_string(),
        account_id: acct_id.to_string(),
        table_id: desc.table_id.clone(),
        key_schema: desc.key_schema.clone(),
        base_key_schema: desc.key_schema.clone(),
        attribute_definitions: desc.attribute_definitions.clone(),
        has_lsi: desc.local_secondary_indexes.is_some(),
        global_secondary_indexes: desc
            .global_secondary_indexes
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|g| IndexInfo {
                index_name: g.index_name.clone(),
                index_id: g.index_name,
                index_type: IndexType::Gsi,
                key_schema: g.key_schema,
                projection: g.projection,
            })
            .collect(),
        local_secondary_indexes: desc
            .local_secondary_indexes
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|l| IndexInfo {
                index_name: l.index_name.clone(),
                index_id: l.index_name,
                index_type: IndexType::Lsi,
                key_schema: l.key_schema,
                projection: l.projection,
            })
            .collect(),
        stream_specification: None,
        vector_indexes: Vec::new(),
    }
}

async fn promote_creating_gsi_to_active(
    engine: &BigtableEngine,
    acct_id: &str,
    table_name: &str,
    gsi: &GsiDescription,
) -> Result<(), StorageError> {
    let meta_key = crate::catalog::keys::table_meta(acct_id, table_name);
    let idx_name = gsi.index_name.clone();
    let changed = engine
        .cat()
        .mutate_json_cas(&meta_key, std::convert::identity, |cur| {
            let Some(mut row) = cur else {
                return Ok((CatalogJsonAction::NoOp, false));
            };
            let mut desc = engine.deserialize_table_description(&row)?;
            let mut promoted = false;
            if let Some(ref mut gsis) = desc.global_secondary_indexes {
                for g in gsis.iter_mut() {
                    if g.index_name == idx_name && g.index_status == "CREATING" {
                        g.index_status = "ACTIVE".to_string();
                        promoted = true;
                    }
                }
            }
            if !promoted {
                return Ok((CatalogJsonAction::NoOp, false));
            }
            engine.write_table_description_into_row(&mut row, &desc)?;
            Ok((CatalogJsonAction::Put(row), true))
        })
        .await?;
    if changed {
        engine.invalidate_table_cache(acct_id, table_name).await;
    }
    Ok(())
}

/// Reconciles both shadow-to-base and base-to-shadow directions for a single GSI,
/// propagating errors so `"CREATING"` GSIs are never marked `"ACTIVE"` while
/// backfill is incomplete.
pub(crate) async fn reconcile_table_gsi(
    engine: &BigtableEngine,
    base_key_info: &TableKeyInfo,
    data_table: &str,
    gsi: &GsiDescription,
) -> Result<(), StorageError> {
    engine
        .roll_forward_committed_txns_for_table(data_table)
        .await?;
    reconcile_gsi_shadow_to_base(engine, base_key_info, data_table, gsi).await?;
    reconcile_gsi_base_to_shadow(engine, base_key_info, data_table, gsi).await?;
    Ok(())
}

async fn reconcile_gsi_shadow_to_base(
    engine: &BigtableEngine,
    base_key_info: &TableKeyInfo,
    data_table: &str,
    gsi: &GsiDescription,
) -> Result<(), StorageError> {
    let shadow_table = crate::gsi::shadow_table_id(data_table, &gsi.index_name);
    let shadow_qs = QueryScan::new(engine.client_ref(), &shadow_table);

    let gsi_key_info = TableKeyInfo {
        table_name: format!("{}__gsi_{}", base_key_info.table_name, gsi.index_name),
        account_id: base_key_info.account_id.clone(),
        table_id: format!("{}::{}", base_key_info.table_id, gsi.index_name),
        key_schema: gsi.key_schema.clone(),
        base_key_schema: base_key_info.key_schema.clone(),
        attribute_definitions: base_key_info.attribute_definitions.clone(),
        ..TableKeyInfo::default()
    };

    let shadow_ops = ItemOps::new(engine.client_ref(), &shadow_table, 0);
    let base_ops = ItemOps::new(engine.client_ref(), data_table, 0);

    let mut start_key = None;
    loop {
        let (items, last) = shadow_qs
            .scan(
                &gsi_key_info,
                true,
                Some(SCAN_BATCH),
                start_key.as_ref(),
                None,
                None,
            )
            .await?;

        if items.is_empty() {
            break;
        }

        let base_keys: Vec<Item> = items
            .iter()
            .map(|shadow_item| {
                extenddb_core::types::extract_key(shadow_item, &base_key_info.key_schema)
            })
            .collect();

        let base_rows = base_ops.batch_get(base_key_info, &base_keys).await?;

        for ((shadow_item, base_key), base_row) in items.into_iter().zip(base_keys).zip(base_rows) {
            reconcile_shadow_item_with_base(
                engine,
                data_table,
                &shadow_ops,
                base_key_info,
                gsi,
                &shadow_item,
                &base_key,
                base_row.as_ref(),
            )
            .await?;
        }

        match last {
            Some(k) => start_key = Some(k),
            None => break,
        }
    }

    Ok(())
}

async fn reconcile_gsi_base_to_shadow(
    engine: &BigtableEngine,
    base_key_info: &TableKeyInfo,
    data_table: &str,
    gsi: &GsiDescription,
) -> Result<(), StorageError> {
    let base_qs = QueryScan::new(engine.client_ref(), data_table);
    let shadow_table = crate::gsi::shadow_table_id(data_table, &gsi.index_name);
    let shadow_ops = ItemOps::new(engine.client_ref(), &shadow_table, 0);

    let mut start_key = None;
    loop {
        let (items, last) = base_qs
            .scan(
                base_key_info,
                false,
                Some(SCAN_BATCH),
                start_key.as_ref(),
                None,
                None,
            )
            .await?;

        if items.is_empty() {
            break;
        }

        let mut targets: Vec<(Item, Item, Vec<u8>)> = Vec::new();
        for base_item in &items {
            if let Some(key) = crate::gsi::shadow_row_key_for_item(
                base_item,
                &gsi.key_schema,
                &base_key_info.key_schema,
            )? {
                let expected_projected = crate::gsi::project_for_shadow(
                    base_item,
                    &gsi.projection,
                    &base_key_info.key_schema,
                    &gsi.key_schema,
                );
                targets.push((base_item.clone(), expected_projected, key));
            }
        }

        if !targets.is_empty() {
            let raw_keys: Vec<Vec<u8>> = targets.iter().map(|(_, _, key)| key.clone()).collect();
            let existing_shadows = shadow_ops.batch_get_by_raw_keys(&raw_keys).await?;

            for ((base_item, expected_projected, _key), existing_shadow) in
                targets.into_iter().zip(existing_shadows)
            {
                let needs_write = match existing_shadow {
                    None => true,
                    Some(shadow_item) => shadow_item != expected_projected,
                };
                if needs_write {
                    let base_key =
                        extenddb_core::types::extract_key(&base_item, &base_key_info.key_schema);
                    let base_key_bytes = row_key::encode_key(&base_key, &base_key_info.key_schema)?;
                    let (fresh_base_opt, fresh_ver) = engine
                        .read_base_row_for_worker(data_table, &base_key_bytes)
                        .await?;
                    let Some(fresh_base) = fresh_base_opt else {
                        continue;
                    };
                    let Some(fresh_key) = crate::gsi::shadow_row_key_for_item(
                        &fresh_base,
                        &gsi.key_schema,
                        &base_key_info.key_schema,
                    )?
                    else {
                        continue;
                    };
                    let fresh_projected = crate::gsi::project_for_shadow(
                        &fresh_base,
                        &gsi.projection,
                        &base_key_info.key_schema,
                        &gsi.key_schema,
                    );
                    let version = fresh_ver.unwrap_or_else(|| next_occ_version(None));
                    shadow_ops
                        .put_shadow_versioned(&fresh_key, &fresh_projected, version)
                        .await?;
                }
            }
        }

        match last {
            Some(k) => start_key = Some(k),
            None => break,
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_shadow_item_with_base(
    engine: &BigtableEngine,
    data_table: &str,
    shadow_ops: &ItemOps<'_>,
    base_key_info: &TableKeyInfo,
    gsi: &GsiDescription,
    shadow_item: &Item,
    base_key: &Item,
    base_row: Option<&Item>,
) -> Result<(), StorageError> {
    let shadow_key_bytes = crate::gsi::shadow_row_key_for_item(
        shadow_item,
        &gsi.key_schema,
        &base_key_info.key_schema,
    )?
    .ok_or_else(|| StorageError::Internal("shadow item missing GSI keys".to_string()))?;

    if let Some(base_item) = base_row {
        let expected_projected = crate::gsi::project_for_shadow(
            base_item,
            &gsi.projection,
            &base_key_info.key_schema,
            &gsi.key_schema,
        );
        let new_shadow_key = crate::gsi::shadow_row_key_for_item(
            base_item,
            &gsi.key_schema,
            &base_key_info.key_schema,
        )?;
        if Some(&shadow_key_bytes) == new_shadow_key.as_ref() && expected_projected == *shadow_item
        {
            return Ok(());
        }
    }

    let base_key_bytes = row_key::encode_key(base_key, &base_key_info.key_schema)?;
    let (fresh_base_row, fresh_ver) = engine
        .read_base_row_for_worker(data_table, &base_key_bytes)
        .await?;

    match (fresh_base_row, fresh_ver) {
        (None, Some(tombstone_ver)) => {
            shadow_ops
                .delete_shadow_versioned(&shadow_key_bytes, tombstone_ver)
                .await?;
            engine
                .enqueue_tombstone_gc_best_effort(
                    &shadow_ops_table_name(data_table, gsi),
                    &shadow_key_bytes,
                    tombstone_ver,
                )
                .await;
        }
        (None, None) => {
            shadow_ops
                .delete_shadow_orphan_if_unchanged(&shadow_key_bytes)
                .await?;
        }
        (Some(fresh_base), ver_opt) => {
            let expected_projected = crate::gsi::project_for_shadow(
                &fresh_base,
                &gsi.projection,
                &base_key_info.key_schema,
                &gsi.key_schema,
            );
            let new_shadow_key = crate::gsi::shadow_row_key_for_item(
                &fresh_base,
                &gsi.key_schema,
                &base_key_info.key_schema,
            )?;
            let version = ver_opt.unwrap_or_else(|| next_occ_version(None));

            if Some(&shadow_key_bytes) == new_shadow_key.as_ref() {
                if expected_projected != *shadow_item {
                    shadow_ops
                        .put_shadow_versioned(&shadow_key_bytes, &expected_projected, version)
                        .await?;
                }
            } else {
                shadow_ops
                    .delete_shadow_versioned(&shadow_key_bytes, version)
                    .await?;
                engine
                    .enqueue_tombstone_gc_best_effort(
                        &shadow_ops_table_name(data_table, gsi),
                        &shadow_key_bytes,
                        version,
                    )
                    .await;

                if let Some(key) = new_shadow_key {
                    shadow_ops
                        .put_shadow_versioned(&key, &expected_projected, version)
                        .await?;
                }
            }
        }
    }

    Ok(())
}

fn shadow_ops_table_name(data_table: &str, gsi: &GsiDescription) -> String {
    crate::gsi::shadow_table_id(data_table, &gsi.index_name)
}
