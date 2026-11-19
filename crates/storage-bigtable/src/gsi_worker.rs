// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! GSI Background Reconciler Worker & Backfill Engine.
//!
//! Scans GSI shadow tables, verifies them against base table rows, and repairs
//! mismatches using freshly re-read `(base_item, occ_version)` pairs guarded by
//! monotonic shadow `m:v` checks (`put_shadow_versioned`,
//! `delete_shadow_versioned`, `delete_shadow_orphan_if_unchanged`) without
//! unbounded `DeleteFromFamily("d")` or `DeleteFromRow`, so delayed writers or
//! reconcilers (`v1 < v2`) never overwrite newer shadow cells.
//! Also completes interrupted `"CREATING" -> "ACTIVE"` GSI backfills via CAS on
//! `table_meta` when no in-process `UpdateTable` build owns the index.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::types::{GsiDescription, IndexInfo, IndexType, Item, TableKeyInfo};
use extenddb_storage::{CancellationToken, sleep_or_shutdown};

use crate::BigtableEngine;
use crate::catalog::CatalogJsonAction;
use crate::data::admin::AdminClient;
use crate::data::encoding::row_key;
use crate::data::item_ops::{ItemOps, next_occ_version};
use crate::data::query_scan::QueryScan;

const SCAN_BATCH: i64 = 200;

pub async fn run(engine: Arc<BigtableEngine>, cadence: Duration, shutdown: CancellationToken) {
    tracing::info!("bigtable GSI reconciler worker started; cadence={cadence:?}");
    let lock_name = "gsi_reconciler";
    let owner = engine.node_id();
    let lease_duration = cadence * 2;

    while sleep_or_shutdown(&shutdown, cadence).await {
        let cat = engine.cat();
        match cat.try_lock(lock_name, owner, lease_duration).await {
            Ok(true) => {
                tracing::debug!("acquired lock '{lock_name}' for node '{owner}'");
                if let Err(e) = sweep_once(&engine).await {
                    tracing::warn!("GSI reconciler sweep error: {e}");
                }
                if let Err(e) = cat.release_lock(lock_name, owner).await {
                    tracing::warn!("failed to release lock '{lock_name}': {e}");
                }
            }
            Ok(false) => {
                tracing::debug!("lock '{lock_name}' is busy, skipping GSI reconciliation");
            }
            Err(e) => {
                tracing::warn!("error trying to acquire lock '{lock_name}': {e}");
            }
        }
    }
    tracing::info!("bigtable GSI reconciler worker stopped");
}

pub async fn sweep_once(engine: &BigtableEngine) -> Result<(), String> {
    let cat = engine.cat();

    let tables_raw = cat
        .scan_prefix(crate::catalog::keys::TABLE_META_SCAN_PREFIX)
        .await
        .map_err(|e| format!("scan tables: {e}"))?;

    for (k, val) in tables_raw {
        let mut parts = k.splitn(3, ':');
        let _ = parts.next();
        let (Some(acct_id), Some(table_name)) = (parts.next(), parts.next()) else {
            continue;
        };

        let Some(data_table) = val.get("data_table").and_then(|v| v.as_str()) else {
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
        if gsis.is_empty() {
            continue;
        }

        let base_key_info = build_reconciler_base_key_info(acct_id, table_name, &desc);

        for gsi in gsis {
            if gsi.index_status == "DELETING"
                || engine.is_gsi_building(&desc.table_id, &gsi.index_name)
            {
                continue;
            }
            if gsi.index_status == "CREATING"
                && let Err(e) = ensure_gsi_shadow_table(engine, data_table, &gsi.index_name).await
            {
                tracing::warn!(
                    "ensure shadow table for CREATING GSI {} on {table_name} failed: {e}",
                    gsi.index_name
                );
                continue;
            }
            match reconcile_table_gsi(engine, &base_key_info, data_table, &gsi).await {
                Ok(()) => {
                    if gsi.index_status == "CREATING"
                        && let Err(e) =
                            promote_creating_gsi_to_active(engine, acct_id, table_name, &gsi).await
                    {
                        tracing::warn!(
                            "promote CREATING GSI {} for table {table_name} failed: {e}",
                            gsi.index_name
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "reconcile GSI {} for table {table_name} failed: {e}",
                        gsi.index_name
                    );
                }
            }
        }
    }
    Ok(())
}

async fn ensure_gsi_shadow_table(
    engine: &BigtableEngine,
    data_table: &str,
    index_name: &str,
) -> Result<(), String> {
    let shadow_table = crate::gsi::shadow_table_id(data_table, index_name);
    let mut admin = AdminClient::connect(engine.client_ref()).await?;
    let families = vec![
        ("d", Some(crate::data::admin::gc_max_versions(1))),
        ("m", Some(crate::data::admin::gc_max_versions(1))),
        ("t", None),
    ];
    admin.create_table(&shadow_table, &families).await
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
) -> Result<(), String> {
    let meta_key = crate::catalog::keys::table_meta(acct_id, table_name);
    let idx_name = gsi.index_name.clone();
    let changed = engine
        .cat()
        .mutate_json_cas(&meta_key, std::convert::identity, |cur| {
            let Some(mut row) = cur else {
                return Ok((CatalogJsonAction::NoOp, false));
            };
            let mut desc = engine
                .deserialize_table_description(&row)
                .map_err(|e| e.to_string())?;
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
            engine
                .write_table_description_into_row(&mut row, &desc)
                .map_err(|e| e.to_string())?;
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
) -> Result<(), String> {
    // Materialize any already-COMMITTED coordinator mutations targeting `data_table`
    // so newly committed rows are visible in `d:*` before base-to-shadow scan.
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
) -> Result<(), String> {
    let shadow_table = crate::gsi::shadow_table_id(data_table, &gsi.index_name);
    let shadow_qs = QueryScan::new(engine.client_ref(), &shadow_table);

    let gsi_key_info = TableKeyInfo {
        table_name: format!("{}__gsi_{}", base_key_info.table_name, gsi.index_name),
        account_id: base_key_info.account_id.clone(),
        table_id: format!("{}::{}", base_key_info.table_id, gsi.index_name),
        key_schema: gsi.key_schema.clone(),
        base_key_schema: base_key_info.key_schema.clone(),
        ..TableKeyInfo::default()
    };

    let shadow_ops = ItemOps::new(engine.client_ref(), &shadow_table, 0);
    let base_ops = ItemOps::new(engine.client_ref(), data_table, 0);

    let mut start_key = None;
    loop {
        let (items, last) = shadow_qs
            .scan(
                &gsi_key_info,
                Some(SCAN_BATCH),
                start_key.as_ref(),
                None,
                None,
            )
            .await
            .map_err(|e| format!("scan shadow table {shadow_table}: {e}"))?;

        if items.is_empty() {
            break;
        }

        let base_keys: Vec<Item> = items
            .iter()
            .map(|shadow_item| {
                extenddb_core::types::extract_key(shadow_item, &base_key_info.key_schema)
            })
            .collect();

        let base_rows = base_ops
            .batch_get(base_key_info, &base_keys)
            .await
            .map_err(|e| format!("batch get base rows: {e}"))?;

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
) -> Result<(), String> {
    let base_qs = QueryScan::new(engine.client_ref(), data_table);
    let shadow_table = crate::gsi::shadow_table_id(data_table, &gsi.index_name);
    let shadow_ops = ItemOps::new(engine.client_ref(), &shadow_table, 0);

    let mut start_key = None;
    loop {
        let (items, last) = base_qs
            .scan(
                base_key_info,
                Some(SCAN_BATCH),
                start_key.as_ref(),
                None,
                None,
            )
            .await
            .map_err(|e| format!("scan base table {data_table}: {e}"))?;

        if items.is_empty() {
            break;
        }

        let targets: Vec<(Item, Item, Vec<u8>)> = items
            .iter()
            .filter_map(|base_item| {
                let shadow_key = crate::gsi::shadow_row_key_for_item(
                    base_item,
                    &gsi.key_schema,
                    &base_key_info.key_schema,
                )
                .ok()
                .flatten();

                shadow_key.map(|key| {
                    let expected_projected = crate::gsi::project_for_shadow(
                        base_item,
                        &gsi.projection,
                        &base_key_info.key_schema,
                        &gsi.key_schema,
                    );
                    (base_item.clone(), expected_projected, key)
                })
            })
            .collect();

        if !targets.is_empty() {
            let raw_keys: Vec<Vec<u8>> = targets.iter().map(|(_, _, key)| key.clone()).collect();
            let existing_shadows = shadow_ops
                .batch_get_by_raw_keys(&raw_keys)
                .await
                .map_err(|e| format!("batch get shadow rows: {e}"))?;

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
                    let base_key_bytes = row_key::encode_key(&base_key, &base_key_info.key_schema)
                        .map_err(|e| format!("encode base key: {e}"))?;
                    let (fresh_base_opt, fresh_ver) = engine
                        .read_base_row_for_worker(data_table, &base_key_bytes)
                        .await
                        .map_err(|e| format!("re-read base row version: {e}"))?;
                    let Some(fresh_base) = fresh_base_opt else {
                        continue;
                    };
                    let Some(fresh_key) = crate::gsi::shadow_row_key_for_item(
                        &fresh_base,
                        &gsi.key_schema,
                        &base_key_info.key_schema,
                    )
                    .map_err(|e| format!("derive fresh shadow key: {e}"))?
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
                        .await
                        .map_err(|e| format!("repair GSI entry: {e}"))?;
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
) -> Result<(), String> {
    let shadow_key_bytes = crate::gsi::shadow_row_key_for_item(
        shadow_item,
        &gsi.key_schema,
        &base_key_info.key_schema,
    )
    .map_err(|e| format!("derive shadow key: {e}"))?
    .ok_or_else(|| "shadow item missing GSI keys".to_string())?;

    // Fast path when batch_get already shows exact agreement.
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
        )
        .map_err(|e| format!("derive new shadow key: {e}"))?;
        if Some(&shadow_key_bytes) == new_shadow_key.as_ref() && expected_projected == *shadow_item
        {
            return Ok(());
        }
    }

    let base_key_bytes = row_key::encode_key(base_key, &base_key_info.key_schema)
        .map_err(|e| format!("encode base key: {e}"))?;
    let (fresh_base_row, fresh_ver) = engine
        .read_base_row_for_worker(data_table, &base_key_bytes)
        .await
        .map_err(|e| format!("re-read base row with version: {e}"))?;

    match (fresh_base_row, fresh_ver) {
        (None, Some(tombstone_ver)) => {
            shadow_ops
                .delete_shadow_versioned(&shadow_key_bytes, tombstone_ver)
                .await
                .map_err(|e| format!("delete stale shadow row at tombstone version: {e}"))?;
        }
        (None, None) => {
            shadow_ops
                .delete_shadow_orphan_if_unchanged(&shadow_key_bytes)
                .await
                .map_err(|e| format!("delete orphaned shadow row if unchanged: {e}"))?;
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
            )
            .map_err(|e| format!("derive new shadow key: {e}"))?;
            let version = ver_opt.unwrap_or_else(|| next_occ_version(None));

            if Some(&shadow_key_bytes) == new_shadow_key.as_ref() {
                if expected_projected != *shadow_item {
                    shadow_ops
                        .put_shadow_versioned(&shadow_key_bytes, &expected_projected, version)
                        .await
                        .map_err(|e| format!("repair GSI entry: {e}"))?;
                }
            } else {
                shadow_ops
                    .delete_shadow_versioned(&shadow_key_bytes, version)
                    .await
                    .map_err(|e| format!("delete stale GSI entry: {e}"))?;

                if let Some(key) = new_shadow_key {
                    shadow_ops
                        .put_shadow_versioned(&key, &expected_projected, version)
                        .await
                        .map_err(|e| format!("write new GSI entry: {e}"))?;
                }
            }
        }
    }

    Ok(())
}
