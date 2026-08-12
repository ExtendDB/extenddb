//! TTL cleanup worker.
//!
//! Sweeps the `__extenddb_ttl_index__` table in shards, finding expired keys,
//! and deletes them from the base table.

use std::sync::Arc;
use std::time::Duration;

use extenddb_storage::{DataEngine, TableEngine};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet,
    mutation,
    mutation::DeleteFromRow,
    row_filter::Filter,
    row_range::{EndKey, StartKey},
};

use crate::BigtableEngine;
use crate::data::client::BigtableClient;
use crate::catalog::Catalog;

pub async fn run(engine: Arc<BigtableEngine>, cadence: Duration) {
    tracing::info!("bigtable TTL worker started; cadence={:?}", cadence);
    
    if let Err(e) = ensure_ttl_index_table(engine.client_ref()).await {
        tracing::warn!("could not ensure TTL index table: {e}");
    }

    let mut tick = tokio::time::interval(cadence);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let lock_name = "ttl_worker";
    let owner = engine.node_id();
    let lease_duration = cadence * 2;

    loop {
        tick.tick().await;
        let cat = Catalog::new(engine.client_ref());
        match cat.try_lock(lock_name, owner, lease_duration).await {
            Ok(true) => {
                tracing::debug!("acquired lock '{}' for node '{}'", lock_name, owner);
                if let Err(e) = sweep_once(&engine).await {
                    tracing::warn!("TTL worker sweep error: {e}");
                }
                if let Err(e) = cat.release_lock(lock_name, owner).await {
                    tracing::warn!("failed to release lock '{}': {e}", lock_name);
                }
            }
            Ok(false) => {
                tracing::debug!("lock '{}' is busy, skipping TTL sweep", lock_name);
            }
            Err(e) => {
                tracing::warn!("error trying to acquire lock '{}': {e}", lock_name);
            }
        }
    }
}

async fn sweep_once(engine: &BigtableEngine) -> Result<(), String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs() as i64;

    for shard in 0..crate::data::encoding::ttl_key::NUM_SHARDS {
        if let Err(e) = sweep_shard(engine, shard, now).await {
            tracing::warn!("TTL worker sweep failed for shard {shard}: {e}");
        }
    }
    Ok(())
}

async fn sweep_shard(
    engine: &BigtableEngine,
    shard_id: u8,
    now_epoch_s: i64,
) -> Result<(), String> {
    let client = engine.client_ref();
    let mut data = client.data();
    let full_index_table = client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE);

    let end_key = {
        let mut k = Vec::with_capacity(9);
        k.push(shard_id);
        k.extend_from_slice(&(now_epoch_s + 1).to_be_bytes());
        k
    };

    let limit = 100; // Paginate by 100 rows
    let mut start_key = {
        let mut k = Vec::with_capacity(9);
        k.push(shard_id);
        k.extend_from_slice(&0i64.to_be_bytes());
        k
    };
    let mut exclude_start = false;

    loop {
        let range = RowRange {
            start_key: Some(if exclude_start {
                StartKey::StartKeyOpen(start_key.clone())
            } else {
                StartKey::StartKeyClosed(start_key.clone())
            }),
            end_key: Some(EndKey::EndKeyOpen(end_key.clone())),
        };

        let req = ReadRowsRequest {
            table_name: full_index_table.clone(),
            rows_limit: limit,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![range],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            }),
            ..ReadRowsRequest::default()
        };

        let resp = data.read_rows(req)
            .await
            .map_err(|e| format!("ReadRows from TTL index: {e}"))?;

        if resp.is_empty() {
            break;
        }

        let mut last_key = None;

        for (raw_key, _) in resp {
            last_key = Some(raw_key.clone());
            let (_, expiry, account_id, table_name, base_row_key) = 
                match crate::data::encoding::ttl_key::decode_ttl_key(&raw_key) {
                    Ok(decoded) => decoded,
                    Err(e) => {
                        tracing::warn!("failed to decode TTL index key: {e}");
                        continue;
                    }
                };
            
            if expiry > now_epoch_s {
                continue;
            }

            let key_info = match TableEngine::table_key_info(engine, &account_id, &table_name).await {
                Ok(ki) => ki,
                Err(e) => {
                    tracing::warn!("could not fetch key info for table {account_id}/{table_name}: {e}");
                    let _ = delete_ttl_index_entry_raw(engine, &raw_key).await;
                    continue;
                }
            };

            let base_key = match crate::data::encoding::row_key::decode_key(&base_row_key, &key_info.key_schema) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!("failed to decode base row key for table {account_id}/{table_name}: {e}");
                    let _ = delete_ttl_index_entry_raw(engine, &raw_key).await;
                    continue;
                }
            };

            let maps = extenddb_core::expression::ExpressionMaps::default();
            match DataEngine::delete_item(
                engine,
                &key_info,
                &base_key,
                false,
                None,
                &maps,
                None,
            )
            .await
            {
                Ok(_) => {
                    let _ = delete_ttl_index_entry_raw(engine, &raw_key).await;
                }
                Err(e) => {
                    tracing::warn!(
                        "TTL delete of expired item in {account_id}/{table_name} failed: {e}"
                    );
                }
            }
        }

        if let Some(key) = last_key {
            start_key = key;
            exclude_start = true;
        } else {
            break;
        }
    }
    Ok(())
}

async fn delete_ttl_index_entry_raw(
    engine: &BigtableEngine,
    raw_key: &[u8],
) -> Result<(), String> {
    let client = engine.client_ref();
    let mut data = client.data();
    let full_index_table = client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE);
    
    let req = MutateRowRequest {
        table_name: full_index_table,
        row_key: raw_key.to_vec(),
        mutations: vec![Mutation {
            mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
        }],
        ..MutateRowRequest::default()
    };
    data.mutate_row(req)
        .await
        .map_err(|e| format!("delete raw TTL index: {e}"))?;
    Ok(())
}

pub async fn ensure_ttl_index_table(client: &BigtableClient) -> Result<(), String> {
    let mut admin = crate::data::admin::AdminClient::connect(client).await?;
    admin.create_table(crate::data::encoding::ttl_key::TTL_INDEX_TABLE, &[("d", None)])
        .await
        .map_err(|e| format!("create TTL index table: {e}"))?;
    Ok(())
}
