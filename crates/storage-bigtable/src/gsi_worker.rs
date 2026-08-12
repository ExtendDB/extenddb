//! GSI Background Reconciler Worker.
//!
//! Scans GSI shadow tables, verifies them against base table rows,
//! and repairs mismatches (deletes stale shadow entries, updates incorrect ones).

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::types::{GsiDescription, Item, TableDescription, TableKeyInfo};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    Mutation, mutation,
};

use crate::BigtableEngine;
use crate::catalog::Catalog;
use crate::data::item_ops::ItemOps;
use crate::data::query_scan::QueryScan;

const SCAN_BATCH: i64 = 200;

pub async fn run(engine: Arc<BigtableEngine>, cadence: Duration) {
    tracing::info!("bigtable GSI reconciler worker started; cadence={:?}", cadence);
    let mut tick = tokio::time::interval(cadence);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let lock_name = "gsi_reconciler";
    let owner = engine.node_id();
    let lease_duration = cadence * 2;

    loop {
        tick.tick().await;
        let cat = Catalog::new(engine.client_ref());
        match cat.try_lock(lock_name, owner, lease_duration).await {
            Ok(true) => {
                tracing::debug!("acquired lock '{}' for node '{}'", lock_name, owner);
                if let Err(e) = sweep_once(&engine).await {
                    tracing::warn!("GSI reconciler sweep error: {e}");
                }
                if let Err(e) = cat.release_lock(lock_name, owner).await {
                    tracing::warn!("failed to release lock '{}': {e}", lock_name);
                }
            }
            Ok(false) => {
                tracing::debug!("lock '{}' is busy, skipping GSI reconciliation", lock_name);
            }
            Err(e) => {
                tracing::warn!("error trying to acquire lock '{}': {e}", lock_name);
            }
        }
    }
}

async fn sweep_once(engine: &BigtableEngine) -> Result<(), String> {
    let cat = Catalog::new(engine.client_ref());
    
    // 1. Get all accounts
    let accounts_raw = cat.scan_prefix(crate::catalog::keys::ACCOUNT_SCAN_PREFIX)
        .await
        .map_err(|e| format!("scan accounts: {e}"))?;
    
    let mut accounts = Vec::new();
    for (k, _) in accounts_raw {
        if let Some(acct_id) = k.strip_prefix(crate::catalog::keys::ACCOUNT_SCAN_PREFIX) {
            accounts.push(acct_id.to_string());
        }
    }

    // 2. For each account, get all tables
    for acct_id in accounts {
        let prefix = crate::catalog::keys::table_meta_scan_prefix(&acct_id);
        let tables_raw = cat.scan_prefix(&prefix)
            .await
            .map_err(|e| format!("scan tables for {acct_id}: {e}"))?;

        for (k, val) in tables_raw {
            // k is "table_meta:<account_id>:<table_name>"
            let parts: Vec<&str> = k.split(':').collect();
            if parts.len() != 3 {
                continue;
            }
            let table_name = parts[2].to_string();

            let data_table = match val.get("data_table").and_then(|v| v.as_str()) {
                Some(dt) => dt,
                None => {
                    tracing::warn!("missing data_table in catalog row {k}");
                    continue;
                }
            };
            let desc_val = match val.get("description") {
                Some(d) => d,
                None => {
                    tracing::warn!("missing description in catalog row {k}");
                    continue;
                }
            };
            let desc: TableDescription = match serde_json::from_value(desc_val.clone()) {
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
            
            let base_key_info = TableKeyInfo {
                table_name: table_name.clone(),
                account_id: acct_id.clone(),
                table_id: desc.table_id.clone(),
                key_schema: desc.key_schema.clone(),
                base_key_schema: desc.key_schema.clone(),
                attribute_definitions: desc.attribute_definitions.clone(),
                has_lsi: desc.local_secondary_indexes.is_some(),
                global_secondary_indexes: desc.global_secondary_indexes.clone().unwrap_or_default().into_iter().map(|g| extenddb_core::types::IndexInfo {
                    index_name: g.index_name.clone(),
                    index_id: g.index_name,
                    index_type: extenddb_core::types::IndexType::Gsi,
                    key_schema: g.key_schema,
                    projection: g.projection,
                }).collect(),
                local_secondary_indexes: desc.local_secondary_indexes.clone().unwrap_or_default().into_iter().map(|l| extenddb_core::types::IndexInfo {
                    index_name: l.index_name.clone(),
                    index_id: l.index_name,
                    index_type: extenddb_core::types::IndexType::Lsi,
                    key_schema: l.key_schema,
                    projection: l.projection,
                }).collect(),
                stream_specification: None,
            };

            for gsi in gsis {
                if let Err(e) = reconcile_gsi(engine, &base_key_info, data_table, &gsi).await {
                    tracing::warn!("reconcile GSI {} for table {} failed: {e}", gsi.index_name, table_name);
                }
            }
        }
    }
    Ok(())
}

async fn reconcile_gsi(
    engine: &BigtableEngine,
    base_key_info: &TableKeyInfo,
    data_table: &str,
    gsi: &GsiDescription,
) -> Result<(), String> {
    if let Err(e) = reconcile_gsi_shadow_to_base(engine, base_key_info, data_table, gsi).await {
        tracing::warn!("GSI shadow-to-base reconciliation failed for {}: {}", gsi.index_name, e);
    }
    if let Err(e) = reconcile_gsi_base_to_shadow(engine, base_key_info, data_table, gsi).await {
        tracing::warn!("GSI base-to-shadow reconciliation failed for {}: {}", gsi.index_name, e);
    }
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
        attribute_definitions: Vec::new(),
        has_lsi: false,
        global_secondary_indexes: Vec::new(),
        local_secondary_indexes: Vec::new(),
        stream_specification: None,
    };

    let shadow_ops = ItemOps::new(engine.client_ref(), &shadow_table, 0);
    let base_ops = ItemOps::new(engine.client_ref(), data_table, 0);

    let mut start_key = None;
    loop {
        let (items, last) = shadow_qs.scan(
            &gsi_key_info,
            Some(SCAN_BATCH),
            start_key.as_ref(),
            None,
            None,
        ).await.map_err(|e| format!("scan shadow table {shadow_table}: {e}"))?;
        
        if items.is_empty() {
            break;
        }

        // Batch get base rows
        let base_keys: Vec<Item> = items.iter().map(|shadow_item| {
            extenddb_core::types::extract_key(shadow_item, &base_key_info.key_schema)
        }).collect();

        let base_rows = base_ops.batch_get(base_key_info, &base_keys).await
            .map_err(|e| format!("batch get base rows: {e}"))?;

        for (shadow_item, base_row) in items.into_iter().zip(base_rows.into_iter()) {
            if let Err(e) = reconcile_shadow_item_with_base(
                &shadow_ops,
                base_key_info,
                gsi,
                &shadow_item,
                base_row.as_ref(),
            ).await {
                tracing::warn!("reconcile shadow item failed: {e}");
            }
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

    let shadow_key_info = TableKeyInfo {
        table_name: format!("{}__gsi_{}", base_key_info.table_name, gsi.index_name),
        account_id: base_key_info.account_id.clone(),
        table_id: format!("{}::{}", base_key_info.table_id, gsi.index_name),
        key_schema: gsi.key_schema.clone(),
        base_key_schema: base_key_info.key_schema.clone(),
        attribute_definitions: Vec::new(),
        has_lsi: false,
        global_secondary_indexes: Vec::new(),
        local_secondary_indexes: Vec::new(),
        stream_specification: None,
    };

    let mut start_key = None;
    loop {
        let (items, last) = base_qs.scan(
            base_key_info,
            Some(SCAN_BATCH),
            start_key.as_ref(),
            None,
            None,
        ).await.map_err(|e| format!("scan base table {data_table}: {e}"))?;
        
        if items.is_empty() {
            break;
        }

        // Filter items that have GSI keys and keep their expected projected images
        let targets: Vec<(Item, Item, Vec<u8>)> = items.iter().filter_map(|base_item| {
            let shadow_key = crate::gsi::shadow_row_key_for_item(
                base_item,
                &gsi.key_schema,
                &base_key_info.key_schema,
            ).ok().flatten();
            
            shadow_key.map(|key| {
                let expected_projected = crate::gsi::project_for_shadow(
                    base_item,
                    &gsi.projection,
                    &base_key_info.key_schema,
                    &gsi.key_schema,
                );
                (base_item.clone(), expected_projected, key)
            })
        }).collect();

        if !targets.is_empty() {
            let keys_to_get: Vec<Item> = targets.iter().map(|(_, proj, _)| proj.clone()).collect();
            let existing_shadows = shadow_ops.batch_get(&shadow_key_info, &keys_to_get).await
                .map_err(|e| format!("batch get shadow rows: {e}"))?;

            for ((_base_item, expected_projected, key), existing_shadow) in targets.into_iter().zip(existing_shadows.into_iter()) {
                match existing_shadow {
                    None => {
                        let mutations = shadow_ops.item_to_mutations(&expected_projected, true)
                            .map_err(|e| format!("item_to_mutations: {e}"))?;
                        shadow_ops.mutate_cells(key, mutations)
                            .await
                            .map_err(|e| format!("write missing GSI entry: {e}"))?;
                        tracing::info!("GSI Reconciler: wrote missing GSI entry");
                    }
                    Some(shadow_item) => {
                        if shadow_item != expected_projected {
                            let mutations = shadow_ops.item_to_mutations(&expected_projected, true)
                                .map_err(|e| format!("item_to_mutations: {e}"))?;
                            shadow_ops.mutate_cells(key, mutations)
                                .await
                                .map_err(|e| format!("repair GSI entry: {e}"))?;
                            tracing::info!("GSI Reconciler: repaired mismatched GSI entry");
                        }
                    }
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

async fn reconcile_shadow_item_with_base(
    shadow_ops: &ItemOps<'_>,
    base_key_info: &TableKeyInfo,
    gsi: &GsiDescription,
    shadow_item: &Item,
    base_row: Option<&Item>,
) -> Result<(), String> {
    // Compute shadow key for the item we just read (to delete it if stale)
    let shadow_key_bytes = crate::gsi::shadow_row_key_for_item(
        shadow_item,
        &gsi.key_schema,
        &base_key_info.key_schema,
    ).map_err(|e| format!("derive shadow key: {e}"))?
    .ok_or_else(|| "shadow item missing GSI keys".to_string())?;

    match base_row {
        None => {
            // Stale shadow entry: base row is gone. Delete it.
            shadow_ops.mutate_cells(
                shadow_key_bytes,
                vec![Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromRow(mutation::DeleteFromRow {})),
                }],
            ).await.map_err(|e| format!("delete stale shadow row: {e}"))?;
            tracing::info!("GSI Reconciler: deleted stale GSI entry for non-existent base row");
        }
        Some(base_item) => {
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
            ).map_err(|e| format!("derive new shadow key: {e}"))?;

            if Some(&shadow_key_bytes) == new_shadow_key.as_ref() {
                // Same GSI key. Check if projected attributes match.
                if expected_projected != *shadow_item {
                    // Mismatch in projected attributes. Overwrite with correct ones.
                    let mutations = shadow_ops.item_to_mutations(&expected_projected, true)
                        .map_err(|e| format!("item_to_mutations: {e}"))?;
                    shadow_ops.mutate_cells(shadow_key_bytes, mutations)
                        .await
                        .map_err(|e| format!("repair GSI entry: {e}"))?;
                    tracing::info!("GSI Reconciler: repaired projected attributes in GSI entry");
                }
            } else {
                // GSI key changed or item is now sparse (new key is None).
                // Delete the old shadow entry we scanned.
                shadow_ops.mutate_cells(
                    shadow_key_bytes,
                    vec![Mutation {
                        mutation: Some(mutation::Mutation::DeleteFromRow(mutation::DeleteFromRow {})),
                    }],
                ).await.map_err(|e| format!("delete stale GSI entry: {e}"))?;
                tracing::info!("GSI Reconciler: deleted stale GSI entry (GSI key changed or sparse)");

                // Write new shadow entry if it is not sparse.
                if let Some(key) = new_shadow_key {
                    let mutations = shadow_ops.item_to_mutations(&expected_projected, true)
                        .map_err(|e| format!("item_to_mutations: {e}"))?;
                    shadow_ops.mutate_cells(key, mutations)
                        .await
                        .map_err(|e| format!("write new GSI entry: {e}"))?;
                    tracing::info!("GSI Reconciler: wrote new GSI entry");
                }
            }
        }
    }

    Ok(())
}
