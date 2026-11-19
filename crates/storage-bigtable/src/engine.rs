// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BigtableEngine`: implements the `StorageEngine` sub-traits (`TableEngine`,
//! `DataEngine`, `MetadataEngine`, `StreamEngine`, `WorkerStore`, plus backup
//! refusal stubs in `crate::backup_stubs`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction, removes_only};
use extenddb_core::types::{
    AttributeValue, BillingMode, BillingModeSummary, CreateTableInput, DeleteTableInput,
    DescribeStreamInput, DescribeTableInput, GsiDescription, IndexInfo, IndexType, Item,
    ListTablesInput, ListTablesOutput, LsiDescription, ONLINE_INDEX_LIMIT,
    ProvisionedThroughputDescription, ReturnValuesOnConditionCheckFailure, SseDescription,
    StreamDescription, StreamRecord, TableDescription, TableKeyInfo, TableStatus, Tag,
    TimeToLiveDescription, TimeToLiveStatus, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    building_refuses, effective_attribute_definitions, parse_warm_throughput_record,
    warm_throughput_record_for_create, warm_throughput_record_for_update,
    warm_throughput_record_json,
};
use extenddb_storage::{
    DataEngine, ItemPairResult, MetadataEngine, QueryResult, StreamCapture, StreamEngine,
    StreamListResult, StreamRecordsResult, TableEngine, TransactGetOp, TransactWriteOp,
    TtlTableInfo, WorkerStore,
};
use futures::future::BoxFuture;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    MutateRowRequest, Mutation, ReadRowsRequest, RowRange, RowSet, mutation,
    mutation::{DeleteFromColumn, SetCell},
};
use moka::future::Cache;
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::catalog::{Catalog, CatalogJsonAction, keys};
use crate::data::admin::AdminClient;
use crate::data::client::{BigtableClient, map_bigtable_err, map_catalog_err};
use crate::data::item_ops::{
    CasOutcome, ItemOps, RowSnapshot, apply_update_actions_validated, check_condition,
    next_occ_version, now_millis_micros, older_than_ts_range, parse_ttl_epoch_secs,
    version_to_timestamp_micros,
};
use crate::data::query_scan::{QueryScan, key_belongs_to_segment};

const DEFAULT_REGION: &str = "us-east-1";
const MAX_OCC_ATTEMPTS: usize = 128;
const IDEMPOTENCY_WINDOW_SECS: i64 = 600;
const MAX_TRANSACT_GET_ATTEMPTS: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IntentBlockState {
    Cleared,
    SingleWriteInFlight,
    MultiTxnInFlight,
}

enum PendingStreamOp {
    None,
    RefuseAfterClosingDisabled {
        table_id: String,
        label_to_close: String,
        refusal_message: String,
    },
    Disable {
        table_id: String,
        label_to_close: Option<String>,
        label_to_close_prior: Option<String>,
    },
    Enable {
        table_id: String,
        label: String,
        view: extenddb_core::types::StreamViewType,
        label_to_close_prior: Option<String>,
    },
}

struct BuildingGsiGuard<'a> {
    set: &'a std::sync::Mutex<HashSet<(String, String)>>,
    key: (String, String),
}

impl Drop for BuildingGsiGuard<'_> {
    fn drop(&mut self) {
        let mut guard = self.set.lock().unwrap_or_else(|e| e.into_inner());
        guard.remove(&self.key);
    }
}

type SingleWriteLockKey = (String, Vec<u8>);
type SingleWriteLockMap = HashMap<SingleWriteLockKey, Weak<tokio::sync::Mutex<()>>>;

/// Maps a logical `(account_id, table_name)` pair's `table_id` UUID to its
/// BigTable short table name (`t` + 16 hex chars).
fn data_table_id(table_id: &str) -> String {
    format!("t{}", &table_id.replace('-', "")[..16])
}

fn now_unix_f64() -> f64 {
    time::OffsetDateTime::now_utc().unix_timestamp() as f64
}

fn deserialize_table_description_row(
    row: &serde_json::Value,
) -> Result<TableDescription, StorageError> {
    let mut desc: TableDescription = serde_json::from_value(
        row.get("description")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )
    .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;

    desc.warm_throughput_record =
        parse_warm_throughput_record(row.get("warm_throughput_record").and_then(|v| v.as_str()));

    if let (Some(gsis), Some(gsi_wt_map)) = (
        desc.global_secondary_indexes.as_mut(),
        row.get("gsi_warm_throughput_records")
            .and_then(|v| v.as_object()),
    ) {
        for gsi in gsis.iter_mut() {
            gsi.warm_throughput_record = parse_warm_throughput_record(
                gsi_wt_map.get(&gsi.index_name).and_then(|v| v.as_str()),
            );
        }
    }

    Ok(desc)
}

fn insert_table_description_into_row(
    row: &mut serde_json::Value,
    desc: &TableDescription,
) -> Result<(), StorageError> {
    let table_wt_json = desc
        .warm_throughput_record
        .as_ref()
        .map(warm_throughput_record_json)
        .transpose()?;

    let mut gsi_wt_json_map = serde_json::Map::new();
    if let Some(gsis) = &desc.global_secondary_indexes {
        for gsi in gsis {
            if let Some(rec) = &gsi.warm_throughput_record {
                gsi_wt_json_map.insert(
                    gsi.index_name.clone(),
                    serde_json::Value::String(warm_throughput_record_json(rec)?),
                );
            }
        }
    }

    let desc_val = serde_json::to_value(desc)
        .map_err(|e| StorageError::Internal(format!("serialize description: {e}")))?;
    if let Some(obj) = row.as_object_mut() {
        obj.insert("description".to_string(), desc_val);
        match table_wt_json {
            Some(s) => {
                obj.insert(
                    "warm_throughput_record".to_string(),
                    serde_json::Value::String(s),
                );
            }
            None => {
                obj.remove("warm_throughput_record");
            }
        }
        if gsi_wt_json_map.is_empty() {
            obj.remove("gsi_warm_throughput_records");
        } else {
            obj.insert(
                "gsi_warm_throughput_records".to_string(),
                serde_json::Value::Object(gsi_wt_json_map),
            );
        }
    }
    Ok(())
}

enum SingleWriteOp<'a> {
    Put {
        item: Item,
    },
    Delete {
        key: &'a Item,
    },
    Update {
        key: &'a Item,
        actions: &'a [UpdateAction],
    },
}

impl<'a> SingleWriteOp<'a> {
    fn lookup_key(&self) -> &Item {
        match self {
            SingleWriteOp::Put { item } => item,
            SingleWriteOp::Delete { key } | SingleWriteOp::Update { key, .. } => key,
        }
    }
}

enum OwnedTxnKind {
    Put {
        item: Item,
    },
    Delete {
        key: Item,
    },
    Update {
        key: Item,
        actions: Vec<UpdateAction>,
    },
    ConditionCheck {
        key: Item,
    },
}

struct OwnedTxnOp {
    key_info: TableKeyInfo,
    kind: OwnedTxnKind,
    condition: Option<Expr>,
    maps: ExpressionMaps,
    return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
    stream: Option<StreamCapture>,
}

impl OwnedTxnOp {
    fn lookup_key(&self) -> &Item {
        match &self.kind {
            OwnedTxnKind::Put { item } => item,
            OwnedTxnKind::Delete { key }
            | OwnedTxnKind::Update { key, .. }
            | OwnedTxnKind::ConditionCheck { key } => key,
        }
    }
}

impl From<&TransactWriteOp<'_>> for OwnedTxnOp {
    fn from(op: &TransactWriteOp<'_>) -> Self {
        match op {
            TransactWriteOp::Put {
                key_info,
                item,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::Put {
                    item: (*item).clone(),
                },
                condition: condition.cloned(),
                maps: (*maps).clone(),
                return_values_on_ccf: *return_values_on_ccf,
                stream: stream.clone(),
            },
            TransactWriteOp::Delete {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::Delete {
                    key: (*key).clone(),
                },
                condition: condition.cloned(),
                maps: (*maps).clone(),
                return_values_on_ccf: *return_values_on_ccf,
                stream: stream.clone(),
            },
            TransactWriteOp::Update {
                key_info,
                key,
                actions,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::Update {
                    key: (*key).clone(),
                    actions: actions.to_vec(),
                },
                condition: condition.cloned(),
                maps: (*maps).clone(),
                return_values_on_ccf: *return_values_on_ccf,
                stream: stream.clone(),
            },
            TransactWriteOp::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf,
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::ConditionCheck {
                    key: (*key).clone(),
                },
                condition: Some((*condition).clone()),
                maps: (*maps).clone(),
                return_values_on_ccf: *return_values_on_ccf,
                stream: None,
            },
        }
    }
}

fn derive_txn_id(account_id: &str, token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(account_id.as_bytes());
    hasher.update(b":");
    hasher.update(token.as_bytes());
    let hash = hasher.finalize();
    format!("{hash:x}")
}

async fn rollback_pending_txn(
    coord: &crate::transact::TxnCoordinator<'_>,
    txn_id: &str,
    started_at_micros: i64,
    placed_indices: &[usize],
    participants: &[crate::transact::ParticipantRow],
) {
    let _ = coord
        .try_abort_pending(txn_id, started_at_micros, started_at_micros)
        .await;
    let mut all_cleared = true;
    for &idx in placed_indices.iter().rev() {
        if let Some(p) = participants.get(idx)
            && coord.clear_intent(txn_id, p).await.is_err()
        {
            all_cleared = false;
        }
    }
    if all_cleared {
        let _ = coord.drop_matching_start(txn_id, started_at_micros).await;
    }
}

pub struct BigtableEngine {
    catalog_client: Arc<BigtableClient>,
    client: Arc<BigtableClient>,
    intent_timeout_secs: u64,
    metadata_cache: Cache<(String, String), (String, TableDescription, Option<String>)>,
    single_write_locks: std::sync::Mutex<SingleWriteLockMap>,
    building_gsis: std::sync::Mutex<HashSet<(String, String)>>,
    node_id: String,
    region: String,
}

impl BigtableEngine {
    pub fn new(
        catalog_client: Arc<BigtableClient>,
        data_client: Arc<BigtableClient>,
        intent_timeout_secs: u64,
    ) -> Self {
        Self::with_region(
            catalog_client,
            data_client,
            intent_timeout_secs,
            DEFAULT_REGION.to_owned(),
        )
    }

    pub fn with_region(
        catalog_client: Arc<BigtableClient>,
        data_client: Arc<BigtableClient>,
        intent_timeout_secs: u64,
        region: String,
    ) -> Self {
        let metadata_cache = Cache::builder()
            .time_to_live(Duration::from_secs(5))
            .build();
        let node_id = Uuid::new_v4().to_string();
        Self {
            catalog_client,
            client: data_client,
            intent_timeout_secs,
            metadata_cache,
            single_write_locks: std::sync::Mutex::new(HashMap::new()),
            building_gsis: std::sync::Mutex::new(HashSet::new()),
            node_id,
            region,
        }
    }

    async fn acquire_single_write_lock(
        &self,
        data_table: &str,
        row_key: &[u8],
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let mtx = {
            let mut map = self
                .single_write_locks
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let key = (data_table.to_owned(), row_key.to_vec());
            if let Some(existing) = map.get(&key).and_then(Weak::upgrade) {
                existing
            } else {
                if map.len() >= 1_024 {
                    map.retain(|_, v| v.strong_count() > 0);
                }
                let arc = Arc::new(tokio::sync::Mutex::new(()));
                map.insert(key, Arc::downgrade(&arc));
                arc
            }
        };
        mtx.lock_owned().await
    }

    fn register_building_gsi(&self, table_id: &str, index_name: &str) -> BuildingGsiGuard<'_> {
        let key = (table_id.to_owned(), index_name.to_owned());
        {
            let mut set = self.building_gsis.lock().unwrap_or_else(|e| e.into_inner());
            set.insert(key.clone());
        }
        BuildingGsiGuard {
            set: &self.building_gsis,
            key,
        }
    }

    pub(crate) fn is_gsi_building(&self, table_id: &str, index_name: &str) -> bool {
        let set = self.building_gsis.lock().unwrap_or_else(|e| e.into_inner());
        set.contains(&(table_id.to_owned(), index_name.to_owned()))
    }

    pub(crate) fn deserialize_table_description(
        &self,
        row: &serde_json::Value,
    ) -> Result<TableDescription, StorageError> {
        deserialize_table_description_row(row)
    }

    pub(crate) fn write_table_description_into_row(
        &self,
        row: &mut serde_json::Value,
        desc: &TableDescription,
    ) -> Result<(), StorageError> {
        insert_table_description_into_row(row, desc)
    }

    pub(crate) async fn invalidate_table_cache(&self, account_id: &str, table_name: &str) {
        self.metadata_cache
            .invalidate(&(account_id.to_owned(), table_name.to_owned()))
            .await;
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn client_ref(&self) -> &BigtableClient {
        &self.client
    }

    pub fn catalog_client_ref(&self) -> &BigtableClient {
        &self.catalog_client
    }

    pub fn intent_timeout_secs(&self) -> u64 {
        self.intent_timeout_secs
    }

    pub(crate) fn cat(&self) -> Catalog<'_> {
        Catalog::new(&self.catalog_client)
    }

    fn table_stream_row(
        desc: &TableDescription,
    ) -> extenddb_storage::stream_lineage::TableStreamRow {
        let status = match desc.table_status {
            TableStatus::Creating => "CREATING",
            TableStatus::Updating => "UPDATING",
            TableStatus::Deleting => "DELETING",
            TableStatus::Active => "ACTIVE",
        };
        extenddb_storage::stream_lineage::TableStreamRow {
            table_name: desc.table_name.clone(),
            table_id: desc.table_id.clone(),
            table_status: status.to_owned(),
            stream_label: desc.latest_stream_label.clone(),
            stream_specification: desc.stream_specification.clone(),
        }
    }

    fn shard_index_for_item(desc: &TableDescription, key_or_item: &Item) -> u32 {
        desc.key_schema
            .iter()
            .find(|k| k.key_type == extenddb_core::types::KeyType::Hash)
            .and_then(|k| key_or_item.get(&k.attribute_name))
            .and_then(|pk_val| extenddb_storage::util::pk_to_text(pk_val).ok())
            .map(|pk_text| {
                crc32fast::hash(pk_text.as_bytes())
                    % extenddb_storage::stream_lineage::SHARDS_PER_STREAM
            })
            .unwrap_or(0)
    }

    fn active_stream_target_from_desc(
        desc: &TableDescription,
        view_override: Option<extenddb_core::types::StreamViewType>,
        key_or_item: &Item,
    ) -> Result<Option<(String, String)>, StorageError> {
        let enabled = desc
            .stream_specification
            .as_ref()
            .is_some_and(|s| s.stream_enabled);
        if !enabled {
            return Ok(None);
        }
        let (Some(arn), Some(label)) = (
            desc.latest_stream_arn.clone(),
            desc.latest_stream_label.as_deref(),
        ) else {
            return Ok(None);
        };
        let view = desc
            .stream_specification
            .as_ref()
            .and_then(|s| s.stream_view_type)
            .or(view_override)
            .unwrap_or(extenddb_core::types::StreamViewType::NewAndOldImages);
        let shard_idx = Self::shard_index_for_item(desc, key_or_item);
        let sid =
            extenddb_storage::stream_lineage::shard_id(&desc.table_id, label, view, shard_idx)?;
        Ok(Some((arn, sid)))
    }

    /// Resolve the active stream `(arn, shard_id)` for a `DataEngine` write
    /// carrying `capture`. When `capture` is `None` (for example during
    /// `ImportTable` bulk imports or when streams are disabled at the caller),
    /// returns `Ok(None)` so no stream event is emitted.
    fn active_stream_target_for_capture(
        desc: &TableDescription,
        capture: Option<&StreamCapture>,
        key_or_item: &Item,
    ) -> Result<Option<(String, String)>, StorageError> {
        let Some(cap) = capture else {
            return Ok(None);
        };
        Self::active_stream_target_from_desc(desc, Some(cap.view_type), key_or_item)
    }

    fn index_key_refs(desc: &TableDescription) -> Vec<extenddb_core::validation::IndexKeyRef<'_>> {
        let mut refs = Vec::new();
        if let Some(gsis) = &desc.global_secondary_indexes {
            for g in gsis {
                refs.push(extenddb_core::validation::IndexKeyRef {
                    index_name: &g.index_name,
                    key_schema: &g.key_schema,
                });
            }
        }
        if let Some(lsis) = &desc.local_secondary_indexes {
            for l in lsis {
                refs.push(extenddb_core::validation::IndexKeyRef {
                    index_name: &l.index_name,
                    key_schema: &l.key_schema,
                });
            }
        }
        refs
    }

    fn validate_put_index_keys(desc: &TableDescription, item: &Item) -> Result<(), StorageError> {
        let refs = Self::index_key_refs(desc);
        if !refs.is_empty() {
            extenddb_core::validation::validate_index_keys(
                item,
                &refs,
                &desc.attribute_definitions,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        }
        Ok(())
    }

    fn validate_update_index_literals(
        desc: &TableDescription,
        actions: &[UpdateAction],
        maps: &ExpressionMaps,
    ) -> Result<(), StorageError> {
        let refs = Self::index_key_refs(desc);
        if !refs.is_empty() {
            extenddb_core::validation::validate_index_key_literal_operands(
                actions,
                &refs,
                &desc.attribute_definitions,
                maps,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        }
        Ok(())
    }

    fn validate_updated_item_index_keys(
        desc: &TableDescription,
        item: &Item,
    ) -> Result<(), StorageError> {
        let refs = Self::index_key_refs(desc);
        if !refs.is_empty() {
            extenddb_core::validation::validate_index_key_types_after_update(
                item,
                &refs,
                &desc.attribute_definitions,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
            extenddb_core::validation::validate_index_key_not_empty(
                item,
                &refs,
                extenddb_core::validation::SecondaryIndexEmptyContext::UpdateExpression,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        }
        Ok(())
    }

    /// Look up the BigTable data-table short name for `(account, table_name)`.
    pub async fn data_table_for(&self, key_info: &TableKeyInfo) -> Result<String, StorageError> {
        let (dt, _, _) = self.table_full_meta_for(key_info).await?;
        Ok(dt)
    }

    async fn table_full_meta_for_raw(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<(String, TableDescription, Option<String>), StorageError> {
        let cache_key = (account_id.to_string(), table_name.to_string());
        if let Some(cached) = self.metadata_cache.get(&cache_key).await {
            return Ok(cached);
        }

        let row = self
            .cat()
            .get(&keys::table_meta(account_id, table_name))
            .await
            .map_err(map_catalog_err)?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        let data_table = row
            .get("data_table")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .ok_or_else(|| StorageError::Internal("missing data_table in catalog".into()))?;
        let desc = deserialize_table_description_row(&row)?;

        let ttl = row.get("ttl").cloned().unwrap_or(serde_json::Value::Null);
        let ttl_enabled = ttl
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let ttl_attr = if ttl_enabled {
            ttl.get("attribute")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        } else {
            None
        };

        let result = (data_table, desc, ttl_attr);
        self.metadata_cache.insert(cache_key, result.clone()).await;
        Ok(result)
    }

    async fn table_full_meta_for(
        &self,
        key_info: &TableKeyInfo,
    ) -> Result<(String, TableDescription, Option<String>), StorageError> {
        self.table_full_meta_for_raw(&key_info.account_id, &key_info.table_name)
            .await
    }

    pub(crate) async fn read_base_row_for_worker(
        &self,
        data_table: &str,
        row_key: &[u8],
    ) -> Result<(Option<Item>, Option<u64>), StorageError> {
        let ops = ItemOps::new(&self.client, data_table, self.intent_timeout_secs);
        let (_, item, ver) = self
            .read_row_resolving_committed_intents(&ops, data_table, row_key)
            .await?;
        Ok((item, ver))
    }

    pub(crate) async fn get_item_with_version(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<(Option<Item>, Option<u64>), StorageError> {
        let data_table = self.data_table_for(key_info).await?;
        let row_key = crate::data::encoding::row_key::encode_key(key, &key_info.key_schema)?;
        self.read_base_row_for_worker(&data_table, &row_key).await
    }

    pub(crate) async fn ensure_ttl_index_entry(
        &self,
        key_info: &TableKeyInfo,
        base_row_key: &[u8],
        expiry: i64,
        version: u64,
    ) -> Result<(), StorageError> {
        insert_ttl_index_entry_at(
            &self.client,
            &key_info.account_id,
            &key_info.table_name,
            base_row_key,
            expiry,
            version_to_timestamp_micros(version),
        )
        .await
    }

    /// Roll forward any already-`COMMITTED` coordinator entries in `__extenddb_txn_log__`
    /// that touch `data_table` (without touching `PENDING` transactions) so GSI
    /// backfill scans see committed rows in `d:*`.
    pub(crate) async fn roll_forward_committed_txns_for_table(
        &self,
        data_table: &str,
    ) -> Result<(), String> {
        let mut data = self.client.data();
        let txn_log_table = self.client.full_table_name(crate::transact::TXN_LOG_TABLE);
        let start_prefix = b"txn:".to_vec();
        let mut end_key = start_prefix.clone();
        end_key.push(0xFF);

        let mut start_key = start_prefix;
        let mut exclude_start = false;
        let coord = crate::transact::TxnCoordinator::new(
            &self.client,
            Duration::from_secs(self.intent_timeout_secs.max(1)),
        );

        loop {
            let range = RowRange {
                start_key: Some(if exclude_start {
                    StartKey::StartKeyOpen(start_key.clone())
                } else {
                    StartKey::StartKeyClosed(start_key.clone())
                }),
                end_key: Some(EndKey::EndKeyClosed(end_key.clone())),
            };
            let req = ReadRowsRequest {
                table_name: txn_log_table.clone(),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                rows_limit: 100,
                rows: Some(RowSet {
                    row_keys: vec![],
                    row_ranges: vec![range],
                }),
                filter: Some(crate::transact::build_txn_log_latest_filter()),
                ..ReadRowsRequest::default()
            };
            let resp = data
                .read_rows(req)
                .await
                .map_err(|e| format!("ReadRows committed txns for backfill: {e}"))?;
            if resp.is_empty() {
                break;
            }
            let mut last_key = None;
            for (rk, _) in resp {
                last_key = Some(rk.clone());
                let Ok(s) = String::from_utf8(rk) else {
                    continue;
                };
                let Some(tid) = s.strip_prefix("txn:") else {
                    continue;
                };
                if let Ok(Some(st)) = coord.get_state(tid).await
                    && st.state == "COMMITTED"
                    && st
                        .participants
                        .as_ref()
                        .is_some_and(|ps| ps.iter().any(|p| p.data_table == data_table))
                {
                    self.roll_forward(tid, &st)
                        .await
                        .map_err(|e| format!("roll_forward {tid}: {e}"))?;
                }
            }
            if let Some(k) = last_key {
                start_key = k;
                exclude_start = true;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Inspect all physical `m:intent:*` markers on `(data_table, row_key)`:
    /// - `COMMITTED` -> cooperatively roll forward immediately.
    /// - `CLEANED` / `ABORTED` / absent -> clear stale intent cell bounded by
    ///   observed timestamp (`clear_intent_up_to`) so newer reused-token
    ///   incarnations are never stripped.
    /// - `PENDING` older than `intent_timeout_secs` -> CAS `PENDING -> ABORTED`
    ///   (`try_abort_pending`) before clearing participant intents, or
    ///   roll forward if it concurrently committed.
    /// - Unexpired `PENDING` -> classify as `SingleWriteInFlight` vs `MultiTxnInFlight`.
    async fn resolve_blocking_intents(
        &self,
        data_table: &str,
        row_key: &[u8],
        exclude_txn_id: Option<&str>,
    ) -> Result<IntentBlockState, StorageError> {
        let ops = ItemOps::new(&self.client, data_table, self.intent_timeout_secs);
        let snap = ops.get_row_snapshot_by_raw_key(row_key).await?;
        if snap.intents.is_empty() {
            return Ok(IntentBlockState::Cleared);
        }

        let coord = crate::transact::TxnCoordinator::new(
            &self.client,
            Duration::from_secs(self.intent_timeout_secs.max(1)),
        );
        let timeout_micros = (self.intent_timeout_secs.saturating_mul(1_000_000)) as i64;
        let cutoff = now_millis_micros().saturating_sub(timeout_micros);
        let this_part = crate::transact::ParticipantRow {
            data_table: data_table.to_owned(),
            row_key: row_key.to_vec(),
        };

        let mut saw_single = false;
        let mut saw_multi = false;

        for (tid, intent_ts) in snap.intents {
            if exclude_txn_id.is_some_and(|ex| ex == tid) {
                continue;
            }
            match coord.get_state(&tid).await? {
                None => {
                    let _ = coord.clear_intent_up_to(&tid, &this_part, intent_ts).await;
                }
                Some(ts) => match ts.state.as_str() {
                    "COMMITTED" => {
                        self.roll_forward(&tid, &ts).await?;
                    }
                    "CLEANED" | "ABORTED" => {
                        let bound = intent_ts.max(ts.state_ts_micros);
                        let _ = coord.clear_intent_up_to(&tid, &this_part, bound).await;
                        let mut all_cleared = true;
                        if let Some(parts) = &ts.participants {
                            for p in parts {
                                if coord.clear_intent_up_to(&tid, p, bound).await.is_err() {
                                    all_cleared = false;
                                }
                            }
                        }
                        if all_cleared && ts.state == "ABORTED" {
                            if let Some(s) = ts.started_at {
                                let _ = coord.drop_matching_start(&tid, s).await;
                            } else {
                                let _ = coord.drop(&tid).await;
                            }
                        }
                    }
                    "PENDING" => {
                        if let Some(start) = ts.started_at
                            && start < cutoff
                        {
                            if coord
                                .try_abort_pending(&tid, start, start.max(ts.state_ts_micros))
                                .await?
                            {
                                let _ = coord.clear_intent(&tid, &this_part).await;
                                let mut all_cleared = true;
                                if let Some(parts) = &ts.participants {
                                    for p in parts {
                                        if coord.clear_intent(&tid, p).await.is_err() {
                                            all_cleared = false;
                                        }
                                    }
                                }
                                if all_cleared {
                                    let _ = coord.drop_matching_start(&tid, start).await;
                                }
                            } else if let Some(fresh) = coord.get_state(&tid).await? {
                                if fresh.state == "COMMITTED" && fresh.started_at == Some(start) {
                                    self.roll_forward(&tid, &fresh).await?;
                                } else if fresh.state == "ABORTED" || fresh.state == "CLEANED" {
                                    let bound = intent_ts.max(fresh.state_ts_micros);
                                    let _ = coord.clear_intent_up_to(&tid, &this_part, bound).await;
                                }
                            }
                        } else if crate::transact::TxnCoordinator::is_single_item_txn(&tid) {
                            saw_single = true;
                        } else {
                            saw_multi = true;
                        }
                    }
                    _ => {
                        saw_multi = true;
                    }
                },
            }
        }

        if saw_multi {
            Ok(IntentBlockState::MultiTxnInFlight)
        } else if saw_single {
            Ok(IntentBlockState::SingleWriteInFlight)
        } else {
            Ok(IntentBlockState::Cleared)
        }
    }

    /// Write/refresh shadow entries for every GSI using version-guarded
    /// `put_shadow_versioned` / `delete_shadow_versioned`.
    async fn write_shadows_only(
        &self,
        key_info: &TableKeyInfo,
        data_table: &str,
        gsis: &[GsiDescription],
        new_item: &Item,
        old_item: Option<&Item>,
        new_version: u64,
    ) -> Result<(), StorageError> {
        if gsis.is_empty() {
            return Ok(());
        }
        let mut futures = Vec::with_capacity(gsis.len());

        for g in gsis {
            let new_key =
                crate::gsi::shadow_row_key_for_item(new_item, &g.key_schema, &key_info.key_schema)?;
            let old_key = match old_item {
                Some(o) => {
                    crate::gsi::shadow_row_key_for_item(o, &g.key_schema, &key_info.key_schema)?
                }
                None => None,
            };
            let shadow = crate::gsi::shadow_table_id(data_table, &g.index_name);
            let client = self.client.clone();
            let intent_timeout_secs = self.intent_timeout_secs;
            let index_name = g.index_name.clone();
            let table_name = key_info.table_name.clone();
            let g_key_schema = g.key_schema.clone();
            let g_projection = g.projection.clone();
            let base_key_schema = key_info.key_schema.clone();

            futures.push(async move {
                let ops = ItemOps::new(&client, &shadow, intent_timeout_secs);

                if let Some(old) = old_key
                    && new_key.as_ref() != Some(&old)
                    && let Err(e) = ops.delete_shadow_versioned(&old, new_version).await
                {
                    tracing::warn!(
                        "shadow stale-delete to gsi {index_name} for table {table_name} failed: {e}"
                    );
                }

                if let Some(row_key) = new_key {
                    let projected = crate::gsi::project_for_shadow(
                        new_item,
                        &g_projection,
                        &base_key_schema,
                        &g_key_schema,
                    );
                    if let Err(e) = ops
                        .put_shadow_versioned(&row_key, &projected, new_version)
                        .await
                    {
                        tracing::warn!(
                            "shadow write to gsi {index_name} for table {table_name} failed: {e}"
                        );
                    }
                }
                Ok::<(), StorageError>(())
            });
        }

        for res in futures::future::join_all(futures).await {
            res?;
        }
        Ok(())
    }

    async fn delete_shadows(
        &self,
        key_info: &TableKeyInfo,
        data_table: &str,
        gsis: &[GsiDescription],
        item: &Item,
        new_version: u64,
    ) -> Result<(), StorageError> {
        if gsis.is_empty() {
            return Ok(());
        }
        let mut futures = Vec::new();
        for g in gsis {
            let Some(row_key) =
                crate::gsi::shadow_row_key_for_item(item, &g.key_schema, &key_info.key_schema)?
            else {
                continue;
            };
            let shadow = crate::gsi::shadow_table_id(data_table, &g.index_name);
            let client = self.client.clone();
            let intent_timeout_secs = self.intent_timeout_secs;
            let index_name = g.index_name.clone();
            let table_name = key_info.table_name.clone();

            futures.push(async move {
                let ops = ItemOps::new(&client, &shadow, intent_timeout_secs);
                if let Err(e) = ops.delete_shadow_versioned(&row_key, new_version).await {
                    tracing::warn!(
                        "shadow delete on gsi {index_name} for table {table_name} failed: {e}"
                    );
                }
                Ok::<(), StorageError>(())
            });
        }

        for res in futures::future::join_all(futures).await {
            res?;
        }
        Ok(())
    }

    /// Maintain `__extenddb_ttl_index__` using OCC-derived millisecond-aligned
    /// cell timestamps. Always restamps `new_exp` even when
    /// `old_exp == new_exp` (e.g. `"1000" -> "1000.5"`) so a racing `ttl_worker`
    /// `ConditionFailed` bounded cleanup cannot orphan the updated item.
    async fn maintain_ttl_index(
        &self,
        key_info: &TableKeyInfo,
        ttl_attr: Option<&str>,
        existing: Option<&Item>,
        new_item: Option<&Item>,
        new_version: u64,
    ) -> Result<(), StorageError> {
        let Some(attr_name) = ttl_attr else {
            return Ok(());
        };
        let old_exp = existing.and_then(|prior| get_ttl_expiry(prior, attr_name));
        let new_exp = new_item.and_then(|next| get_ttl_expiry(next, attr_name));
        if old_exp.is_none() && new_exp.is_none() {
            return Ok(());
        }
        let key_source = new_item
            .or(existing)
            .ok_or_else(|| StorageError::Internal("maintain_ttl_index missing item key".into()))?;
        let base_row_key =
            crate::data::encoding::row_key::encode_key(key_source, &key_info.key_schema)?;
        apply_ttl_mutation_versioned(
            &self.client,
            &key_info.account_id,
            &key_info.table_name,
            &base_row_key,
            old_exp,
            new_exp,
            new_version,
        )
        .await
    }

    fn build_gsi_participant_mutations(
        key_info: &TableKeyInfo,
        data_table: &str,
        gsis: &[GsiDescription],
        existing: Option<&Item>,
        new_item: Option<&Item>,
        new_version: u64,
        out: &mut Vec<crate::transact::ParticipantMutation>,
    ) -> Result<(), StorageError> {
        if let Some(item) = new_item {
            for g in gsis {
                let new_key =
                    crate::gsi::shadow_row_key_for_item(item, &g.key_schema, &key_info.key_schema)?;
                let old_key = match existing {
                    Some(o) => {
                        crate::gsi::shadow_row_key_for_item(o, &g.key_schema, &key_info.key_schema)?
                    }
                    None => None,
                };
                let shadow_table = crate::gsi::shadow_table_id(data_table, &g.index_name);
                if let Some(old) = old_key
                    && new_key.as_ref() != Some(&old)
                {
                    out.push(crate::transact::ParticipantMutation {
                        participant: crate::transact::ParticipantRow {
                            data_table: shadow_table.clone(),
                            row_key: old,
                        },
                        payload: crate::transact::TxnOpPayload::Delete,
                        expected_version: None,
                        row_existed: false,
                        new_version: Some(new_version),
                    });
                }
                if let Some(row_key) = new_key {
                    let projected = crate::gsi::project_for_shadow(
                        item,
                        &g.projection,
                        &key_info.key_schema,
                        &g.key_schema,
                    );
                    out.push(crate::transact::ParticipantMutation {
                        participant: crate::transact::ParticipantRow {
                            data_table: shadow_table,
                            row_key,
                        },
                        payload: crate::transact::TxnOpPayload::Put { item: projected },
                        expected_version: None,
                        row_existed: false,
                        new_version: Some(new_version),
                    });
                }
            }
        } else if let Some(prior_item) = existing {
            for g in gsis {
                if let Some(row_key) = crate::gsi::shadow_row_key_for_item(
                    prior_item,
                    &g.key_schema,
                    &key_info.key_schema,
                )? {
                    let shadow_table = crate::gsi::shadow_table_id(data_table, &g.index_name);
                    out.push(crate::transact::ParticipantMutation {
                        participant: crate::transact::ParticipantRow {
                            data_table: shadow_table,
                            row_key,
                        },
                        payload: crate::transact::TxnOpPayload::Delete,
                        expected_version: None,
                        row_existed: false,
                        new_version: Some(new_version),
                    });
                }
            }
        }
        Ok(())
    }

    async fn check_idempotency_record(
        &self,
        account_id: &str,
        token: &str,
        fingerprint: &str,
    ) -> Result<(), StorageError> {
        let key = keys::idempotency(account_id, token);
        let Some(prior) = self.cat().get(&key).await.map_err(map_catalog_err)? else {
            return Ok(());
        };
        let now_secs = time::OffsetDateTime::now_utc().unix_timestamp();
        let applied_at = prior
            .get("applied_at")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        if now_secs.saturating_sub(applied_at) >= IDEMPOTENCY_WINDOW_SECS {
            let _ = self
                .cat()
                .mutate_json_cas(&key, map_catalog_err, move |cur| {
                    if let Some(c) = cur {
                        let ts = c.get("applied_at").and_then(|v| v.as_i64()).unwrap_or(0);
                        if now_secs.saturating_sub(ts) >= IDEMPOTENCY_WINDOW_SECS {
                            return Ok((CatalogJsonAction::Delete, ()));
                        }
                    }
                    Ok((CatalogJsonAction::NoOp, ()))
                })
                .await;
            return Ok(());
        }
        let prior_fp = prior
            .get("fingerprint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if prior_fp == fingerprint {
            Err(StorageError::IdempotentReplay)
        } else {
            Err(StorageError::IdempotentMismatch)
        }
    }

    pub(crate) async fn persist_idempotency_if_unexpired(
        &self,
        account_id: &str,
        token: &str,
        fingerprint: &str,
        txn_id: &str,
        commit_ts_micros: i64,
    ) -> Result<(), StorageError> {
        let key = keys::idempotency(account_id, token);
        let fp_owned = fingerprint.to_owned();
        let tid_owned = txn_id.to_owned();
        let now_secs = time::OffsetDateTime::now_utc().unix_timestamp();
        let applied_secs = if commit_ts_micros > 0 {
            (commit_ts_micros / 1_000_000).min(now_secs)
        } else {
            now_secs
        };
        if now_secs.saturating_sub(applied_secs) >= IDEMPOTENCY_WINDOW_SECS {
            return Ok(());
        }
        self.cat()
            .mutate_json_cas(&key, map_catalog_err, move |existing| {
                if let Some(cur) = existing {
                    let cur_ts = cur.get("applied_at").and_then(|v| v.as_i64()).unwrap_or(0);
                    if now_secs.saturating_sub(cur_ts) < IDEMPOTENCY_WINDOW_SECS {
                        return Ok((CatalogJsonAction::NoOp, ()));
                    }
                }
                let rec = json!({
                    "fingerprint": fp_owned,
                    "txn_id": tid_owned,
                    "applied_at": applied_secs,
                });
                Ok((CatalogJsonAction::Put(rec), ()))
            })
            .await
    }

    /// Execute Phase 1 (`open`, `place_intent_once`, `get_with_version`,
    /// condition/update evaluation, and `try_commit_full`) of a single-item write
    /// (`PutItem` / `UpdateItem` / `DeleteItem`) through the `__extenddb_txn_log__`
    /// coordinator fence whenever streams are enabled on the table.
    #[allow(clippy::too_many_arguments)]
    async fn commit_fenced_single_item_write(
        &self,
        key_info: &TableKeyInfo,
        data_table: &str,
        desc: &TableDescription,
        ttl_attr: Option<&str>,
        op: SingleWriteOp<'_>,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<
        (
            Option<Item>,
            Option<Item>,
            String,
            Option<crate::transact::TxnState>,
        ),
        StorageError,
    > {
        let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
        let row_key =
            crate::data::encoding::row_key::encode_key(op.lookup_key(), &key_info.key_schema)?;
        let _row_guard = self.acquire_single_write_lock(data_table, &row_key).await;

        let participant = crate::transact::ParticipantRow {
            data_table: data_table.to_owned(),
            row_key,
        };
        let coord = crate::transact::TxnCoordinator::new(
            &self.client,
            Duration::from_secs(self.intent_timeout_secs.max(1)),
        );

        for incarnation in 0..8usize {
            let txn_id = crate::transact::TxnCoordinator::new_single_txn_id();
            let (_, started_at_micros) = coord
                .open(&txn_id, std::slice::from_ref(&participant))
                .await?;

            let single_wait_deadline = std::time::Instant::now()
                + Duration::from_secs(self.intent_timeout_secs.max(2) + 2);
            let mut multi_txn_conflicts = 0usize;
            let mut backoff = Duration::from_millis(8);

            loop {
                match coord.place_intent_once(&txn_id, &participant).await {
                    Ok(true) => break,
                    Ok(false) => {
                        match self
                            .resolve_blocking_intents(
                                data_table,
                                &participant.row_key,
                                Some(&txn_id),
                            )
                            .await?
                        {
                            IntentBlockState::Cleared => continue,
                            IntentBlockState::SingleWriteInFlight => {
                                if std::time::Instant::now() >= single_wait_deadline {
                                    let _ = coord
                                        .try_abort_pending(
                                            &txn_id,
                                            started_at_micros,
                                            started_at_micros,
                                        )
                                        .await;
                                    let _ =
                                        coord.drop_matching_start(&txn_id, started_at_micros).await;
                                    return Err(StorageError::TransactionConflict(
                                        "single-item stream write contention timeout".to_string(),
                                    ));
                                }
                                let jitter = rand::random_range(0..8u64);
                                tokio::time::sleep(Duration::from_millis(6 + jitter)).await;
                            }
                            IntentBlockState::MultiTxnInFlight => {
                                multi_txn_conflicts += 1;
                                if multi_txn_conflicts >= 8 {
                                    let _ = coord
                                        .try_abort_pending(
                                            &txn_id,
                                            started_at_micros,
                                            started_at_micros,
                                        )
                                        .await;
                                    let _ =
                                        coord.drop_matching_start(&txn_id, started_at_micros).await;
                                    return Err(StorageError::TransactionConflict(
                                        "concurrent transaction holds an intent on this row"
                                            .to_string(),
                                    ));
                                }
                                let jitter = rand::random_range(0..backoff.as_millis() as u64 + 1);
                                tokio::time::sleep(backoff + Duration::from_millis(jitter)).await;
                                backoff = (backoff * 2).min(Duration::from_millis(64));
                            }
                        }
                    }
                    Err(e) => {
                        let _ = coord
                            .try_abort_pending(&txn_id, started_at_micros, started_at_micros)
                            .await;
                        let _ = coord.clear_intent(&txn_id, &participant).await;
                        let _ = coord.drop_matching_start(&txn_id, started_at_micros).await;
                        return Err(e);
                    }
                }
            }

            let abort_cleanly = |txn_id: String, participant: crate::transact::ParticipantRow| async move {
                let _ = coord
                    .try_abort_pending(&txn_id, started_at_micros, started_at_micros)
                    .await;
                if coord.clear_intent(&txn_id, &participant).await.is_ok() {
                    let _ = coord.drop_matching_start(&txn_id, started_at_micros).await;
                }
            };

            let ops = ItemOps::new(&self.client, data_table, self.intent_timeout_secs);
            let (existing, read_version) =
                match ops.get_with_version(key_info, op.lookup_key()).await {
                    Ok(res) => res,
                    Err(e) => {
                        abort_cleanly(txn_id, participant).await;
                        return Err(e);
                    }
                };

            if let Err(e) = check_condition(existing.as_ref(), condition, maps) {
                abort_cleanly(txn_id, participant).await;
                return Err(e);
            }

            let new_item: Option<Item> = match &op {
                SingleWriteOp::Put { item } => Some(item.clone()),
                SingleWriteOp::Delete { .. } => {
                    if existing.is_none() {
                        abort_cleanly(txn_id.clone(), participant).await;
                        return Ok((None, None, txn_id, None));
                    }
                    None
                }
                SingleWriteOp::Update { key, actions } => {
                    let base = existing.clone().unwrap_or_else(|| (*key).clone());
                    let updated = match apply_update_actions_validated(
                        &base,
                        actions,
                        maps,
                        key_info,
                        &desc.attribute_definitions,
                    )
                    .and_then(|u| {
                        Self::validate_updated_item_index_keys(desc, &u)?;
                        Ok(u)
                    }) {
                        Ok(u) => u,
                        Err(e) => {
                            abort_cleanly(txn_id, participant).await;
                            return Err(e);
                        }
                    };
                    if existing.is_none() && removes_only(actions) {
                        abort_cleanly(txn_id.clone(), participant).await;
                        return Ok((None, None, txn_id, None));
                    }
                    Some(updated)
                }
            };

            let row_existed = existing.is_some();
            let next_version = next_occ_version(read_version);
            let base_payload = match &new_item {
                Some(item) => crate::transact::TxnOpPayload::Put { item: item.clone() },
                None => crate::transact::TxnOpPayload::Delete,
            };

            let mut txn_mutations = vec![crate::transact::ParticipantMutation {
                participant: participant.clone(),
                payload: base_payload,
                expected_version: read_version,
                row_existed,
                new_version: Some(next_version),
            }];
            if let Err(e) = Self::build_gsi_participant_mutations(
                key_info,
                data_table,
                &gsis,
                existing.as_ref(),
                new_item.as_ref(),
                next_version,
                &mut txn_mutations,
            ) {
                abort_cleanly(txn_id, participant).await;
                return Err(e);
            }

            let mut ttl_mutations = Vec::new();
            if let Some(attr_name) = ttl_attr {
                let old_exp = existing.as_ref().and_then(|p| get_ttl_expiry(p, attr_name));
                let new_exp = new_item.as_ref().and_then(|n| get_ttl_expiry(n, attr_name));
                if old_exp.is_some() || new_exp.is_some() {
                    ttl_mutations.push(crate::transact::TxnTtlMutation {
                        participant: participant.clone(),
                        account_id: key_info.account_id.clone(),
                        table_name: key_info.table_name.clone(),
                        old_expiry: old_exp,
                        new_expiry: new_exp,
                        new_version: next_version,
                    });
                }
            }

            let mut log_records: Vec<crate::transact::TxnStreamRecord> = Vec::new();
            let representative = new_item
                .as_ref()
                .or(existing.as_ref())
                .unwrap_or_else(|| op.lookup_key());
            match Self::active_stream_target_for_capture(desc, stream, representative) {
                Ok(Some((arn, shard_id))) => {
                    if let Some(record) = crate::streams::build_record(
                        desc.stream_specification.as_ref(),
                        stream,
                        &key_info.key_schema,
                        existing.as_ref(),
                        new_item.as_ref(),
                        crate::streams::UNASSIGNED_SEQUENCE_NUMBER,
                        &self.region,
                    ) {
                        log_records.push(crate::transact::TxnStreamRecord {
                            stream_arn: arn,
                            shard_id,
                            record,
                            participant: Some(participant.clone()),
                            dedup_key: Some(format!("{txn_id}:0")),
                        });
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    abort_cleanly(txn_id, participant).await;
                    return Err(e);
                }
            }

            let commit_ts_micros = match coord
                .try_commit_full(
                    &txn_id,
                    started_at_micros,
                    &txn_mutations,
                    Some(&log_records),
                    Some(&ttl_mutations),
                )
                .await
            {
                Ok(Some(ts)) => ts,
                Ok(None) => {
                    abort_cleanly(txn_id, participant.clone()).await;
                    if condition.is_none() && incarnation + 1 < 8 {
                        continue;
                    }
                    return Err(StorageError::TransactionConflict(
                        "fenced single-item write aborted due to intent timeout".to_string(),
                    ));
                }
                Err(e) => {
                    abort_cleanly(txn_id, participant).await;
                    return Err(e);
                }
            };

            let committed_state = crate::transact::TxnState {
                state: "COMMITTED".to_owned(),
                state_ts_micros: commit_ts_micros,
                started_at: Some(started_at_micros),
                participants: Some(vec![participant]),
                mutations: Some(txn_mutations),
                stream_records: Some(log_records),
                ttl_mutations: Some(ttl_mutations),
                token_meta: None,
            };

            return Ok((existing, new_item, txn_id, Some(committed_state)));
        }

        Err(StorageError::TransactionConflict(
            "fenced single-item write retry limit exceeded".to_string(),
        ))
    }

    /// Execute a single-item write (`PutItem` / `UpdateItem` / `DeleteItem`)
    /// through the `__extenddb_txn_log__` coordinator fence whenever streams
    /// are enabled on the table.
    #[allow(clippy::too_many_arguments)]
    async fn execute_fenced_single_item_write(
        &self,
        key_info: &TableKeyInfo,
        data_table: &str,
        desc: &TableDescription,
        ttl_attr: Option<&str>,
        op: SingleWriteOp<'_>,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<(Option<Item>, Option<Item>, String), StorageError> {
        let (existing, new_item, txn_id, committed_state) = self
            .commit_fenced_single_item_write(
                key_info, data_table, desc, ttl_attr, op, condition, maps, stream,
            )
            .await?;
        if let Some(state) = committed_state {
            self.roll_forward(&txn_id, &state).await?;
        }
        Ok((existing, new_item, txn_id))
    }

    /// Test-only entrypoint simulating a process crash immediately after the
    /// coordinator `COMMITTED` point of a stream-enabled `PutItem` (before base
    /// row or stream record apply). Returns the `txn_id` in `__extenddb_txn_log__`.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn debug_fenced_put_commit_only(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        stream: Option<&StreamCapture>,
    ) -> Result<String, StorageError> {
        let (data_table, desc, ttl_attr) = self.table_full_meta_for(key_info).await?;
        let empty_maps = ExpressionMaps::default();
        let (_, _, txn_id, _) = self
            .commit_fenced_single_item_write(
                key_info,
                &data_table,
                &desc,
                ttl_attr.as_deref(),
                SingleWriteOp::Put { item },
                None,
                &empty_maps,
                stream,
            )
            .await?;
        Ok(txn_id)
    }

    /// Test-only entrypoint simulating a process crash *before* the coordinator
    /// `COMMITTED` point of a stream-enabled `PutItem` (`PENDING` coordinator
    /// row + `m:intent:<txn_id>` placed, no commit). Returns `txn_id`.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn debug_fenced_put_open_intent_only(
        &self,
        key_info: &TableKeyInfo,
        item: &Item,
    ) -> Result<String, StorageError> {
        let (data_table, _, _) = self.table_full_meta_for(key_info).await?;
        let row_key = crate::data::encoding::row_key::encode_key(item, &key_info.key_schema)?;
        let participant = crate::transact::ParticipantRow {
            data_table,
            row_key,
        };
        let coord = crate::transact::TxnCoordinator::new(
            &self.client,
            Duration::from_secs(self.intent_timeout_secs.max(1)),
        );
        let txn_id = crate::transact::TxnCoordinator::new_single_txn_id();
        coord
            .open(&txn_id, std::slice::from_ref(&participant))
            .await?;
        let placed = coord.place_intent(&txn_id, &participant).await?;
        if !placed {
            return Err(StorageError::TransactionConflict(
                "failed to place intent in debug_fenced_put_open_intent_only".to_string(),
            ));
        }
        Ok(txn_id)
    }

    /// Idempotently roll forward a `COMMITTED` coordinator entry across base
    /// participants, version-guarded GSI shadow rows, OCC-bounded TTL
    /// index mutations, active-participant contiguous stream records, and `ClientRequestToken` `idem:` persistence.
    pub async fn roll_forward(
        &self,
        txn_id: &str,
        state: &crate::transact::TxnState,
    ) -> Result<(), StorageError> {
        let client = &self.client;
        let coord = crate::transact::TxnCoordinator::new(
            client,
            Duration::from_secs(self.intent_timeout_secs.max(1)),
        );

        let mut materialized_participants: HashSet<crate::transact::ParticipantRow> =
            HashSet::new();

        if let Some(muts) = &state.mutations {
            let mut last_base_still_current = true;
            for m in muts {
                let is_base_participant = if let Some(parts) = &state.participants {
                    parts.contains(&m.participant)
                } else {
                    !crate::gsi::is_shadow_table_id(&m.participant.data_table)
                };

                let new_version = m.new_version.unwrap_or_else(|| next_occ_version(None));
                if is_base_participant {
                    let applied = coord
                        .apply_fenced_mutation(
                            txn_id,
                            &m.participant,
                            &m.payload,
                            m.expected_version,
                            m.row_existed,
                            new_version,
                        )
                        .await?;
                    if applied {
                        last_base_still_current = true;
                        materialized_participants.insert(m.participant.clone());
                    } else {
                        let ops = ItemOps::new(
                            client,
                            &m.participant.data_table,
                            self.intent_timeout_secs,
                        );
                        let (cur_item, cur_ver) = ops
                            .get_by_raw_key_with_version(&m.participant.row_key)
                            .await?;
                        let matches_our_write = cur_ver == Some(new_version)
                            && match &m.payload {
                                crate::transact::TxnOpPayload::Put { item } => {
                                    cur_item.as_ref() == Some(item)
                                }
                                crate::transact::TxnOpPayload::Delete => cur_item.is_none(),
                            };
                        last_base_still_current = matches_our_write;
                        if matches_our_write {
                            materialized_participants.insert(m.participant.clone());
                        }
                    }
                } else {
                    if !last_base_still_current {
                        continue;
                    }
                    let ops =
                        ItemOps::new(client, &m.participant.data_table, self.intent_timeout_secs);
                    match &m.payload {
                        crate::transact::TxnOpPayload::Put { item } => {
                            ops.put_shadow_versioned(&m.participant.row_key, item, new_version)
                                .await?;
                        }
                        crate::transact::TxnOpPayload::Delete => {
                            ops.delete_shadow_versioned(&m.participant.row_key, new_version)
                                .await?;
                        }
                    }
                }
            }
        }

        if let Some(ttl_muts) = &state.ttl_mutations {
            for tm in ttl_muts {
                if state.mutations.is_none() || materialized_participants.contains(&tm.participant)
                {
                    apply_ttl_mutation_versioned(
                        client,
                        &tm.account_id,
                        &tm.table_name,
                        &tm.participant.row_key,
                        tm.old_expiry,
                        tm.new_expiry,
                        tm.new_version,
                    )
                    .await?;
                }
            }
        }

        if let Some(records) = &state.stream_records {
            let cat = self.cat();
            for r in records {
                if r.shard_id.is_empty() {
                    continue;
                }
                let should_emit = match &r.participant {
                    Some(part) => materialized_participants.contains(part),
                    None => state.mutations.is_none() || !materialized_participants.is_empty(),
                };
                if should_emit {
                    crate::streams::write_record_ordered(
                        &cat,
                        &r.shard_id,
                        &r.record,
                        r.dedup_key.as_deref(),
                    )
                    .await
                    .map_err(map_catalog_err)?;
                }
            }
        }

        let after_ts = state.state_ts_micros.max(state.started_at.unwrap_or(0));
        if let Some(tm) = &state.token_meta {
            self.persist_idempotency_if_unexpired(
                &tm.account_id,
                &tm.token,
                &tm.fingerprint,
                txn_id,
                after_ts,
            )
            .await?;
        }

        let mut all_intents_cleared = true;
        if let Some(parts) = &state.participants {
            for p in parts {
                if coord.clear_intent(txn_id, p).await.is_err() {
                    all_intents_cleared = false;
                }
            }
        }

        coord.cleaned(txn_id, after_ts).await?;
        if all_intents_cleared {
            if let Some(start) = state.started_at {
                coord.drop_matching_start(txn_id, start).await?;
            } else {
                coord.drop(txn_id).await?;
            }
        }

        Ok(())
    }

    fn transaction_conflict_canceled(
        total_ops: usize,
        conflict_idx: usize,
        msg: &str,
    ) -> StorageError {
        let mut reasons: Vec<extenddb_core::types::CancellationReason> = (0..total_ops)
            .map(|_| crate::transact::cancellation_reason("None", None, None))
            .collect();
        if conflict_idx < reasons.len() {
            reasons[conflict_idx] =
                crate::transact::cancellation_reason("TransactionConflict", Some(msg), None);
        }
        StorageError::TransactionCanceled(reasons)
    }

    async fn read_row_resolving_committed_intents(
        &self,
        ops: &ItemOps<'_>,
        data_table: &str,
        row_key: &[u8],
    ) -> Result<(bool, Option<Item>, Option<u64>), StorageError> {
        let snap = ops.get_row_snapshot_by_raw_key(row_key).await?;
        if snap.intents.is_empty() {
            return Ok((false, snap.item, snap.version));
        }
        if self
            .resolve_blocking_intents(data_table, row_key, None)
            .await?
            == IntentBlockState::Cleared
        {
            let refreshed = ops.get_row_snapshot_by_raw_key(row_key).await?;
            return Ok((
                !refreshed.intents.is_empty(),
                refreshed.item,
                refreshed.version,
            ));
        }
        Ok((true, snap.item, snap.version))
    }

    async fn wait_or_fail_occ_intent(
        &self,
        data_table: &str,
        row_key: &[u8],
        single_deadline: std::time::Instant,
        multi_conflicts: &mut usize,
        attempt: usize,
    ) -> Result<(), StorageError> {
        match self
            .resolve_blocking_intents(data_table, row_key, None)
            .await?
        {
            IntentBlockState::Cleared => Ok(()),
            IntentBlockState::SingleWriteInFlight => {
                if std::time::Instant::now() >= single_deadline {
                    return Err(StorageError::TransactionConflict(
                        "single-item write contention timeout".to_string(),
                    ));
                }
                let jitter = rand::random_range(0..6u64);
                tokio::time::sleep(Duration::from_millis(4 + jitter)).await;
                Ok(())
            }
            IntentBlockState::MultiTxnInFlight => {
                *multi_conflicts += 1;
                if *multi_conflicts >= 8 {
                    return Err(StorageError::TransactionConflict(
                        "concurrent transaction holds an intent on this row".to_string(),
                    ));
                }
                let backoff_ms = (1u64 << attempt.min(4)) + rand::random_range(0..4);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                Ok(())
            }
        }
    }
}

// =========== TableEngine ===========

impl TableEngine for BigtableEngine {
    fn create_table(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let input = extenddb_storage::stream_lineage::without_disabled_specification(input);
        Box::pin(async move {
            let meta_key = keys::table_meta(&account_id, &input.table_name);
            if self
                .cat()
                .get(&meta_key)
                .await
                .map_err(map_catalog_err)?
                .is_some()
            {
                return Err(StorageError::TableAlreadyExists(input.table_name));
            }

            let table_id = Uuid::new_v4().to_string();
            let data_table = data_table_id(&table_id);

            let mut admin = AdminClient::connect(&self.client)
                .await
                .map_err(map_catalog_err)?;
            let families = vec![
                ("d", Some(crate::data::admin::gc_max_versions(1))),
                ("m", Some(crate::data::admin::gc_max_versions(1))),
                ("t", None),
            ];
            admin
                .create_table(&data_table, &families)
                .await
                .map_err(map_catalog_err)?;

            let mut created_shadows = Vec::new();
            if let Some(gsis) = &input.global_secondary_indexes {
                for g in gsis {
                    let shadow = crate::gsi::shadow_table_id(&data_table, &g.index_name);
                    admin
                        .create_table(&shadow, &families)
                        .await
                        .map_err(map_catalog_err)?;
                    created_shadows.push(shadow);
                }
            }

            let billing = input.billing_mode.unwrap_or(BillingMode::PayPerRequest);
            let throughput = match input.provisioned_throughput {
                Some(p) => ProvisionedThroughputDescription {
                    read_capacity_units: p.read_capacity_units,
                    write_capacity_units: p.write_capacity_units,
                    number_of_decreases_today: 0,
                    last_increase_date_time: None,
                    last_decrease_date_time: None,
                },
                None => ProvisionedThroughputDescription::default(),
            };
            let table_arn =
                extenddb_storage::util::table_arn(&self.region, &account_id, &input.table_name);
            let gsis = input.global_secondary_indexes.as_ref().map(|inputs| {
                inputs
                    .iter()
                    .map(|g| GsiDescription {
                        index_name: g.index_name.clone(),
                        key_schema: g.key_schema.clone(),
                        projection: g.projection.clone(),
                        index_status: "ACTIVE".to_string(),
                        provisioned_throughput: g.provisioned_throughput.as_ref().map(|p| {
                            ProvisionedThroughputDescription {
                                read_capacity_units: p.read_capacity_units,
                                write_capacity_units: p.write_capacity_units,
                                number_of_decreases_today: 0,
                                last_increase_date_time: None,
                                last_decrease_date_time: None,
                            }
                        }),
                        index_size_bytes: 0,
                        item_count: 0,
                        index_arn: format!("{table_arn}/index/{}", g.index_name),
                        warm_throughput: None,
                        warm_throughput_record: warm_throughput_record_for_create(
                            g.warm_throughput.as_ref(),
                            0.0,
                        ),
                    })
                    .collect()
            });
            let lsis = input.local_secondary_indexes.as_ref().map(|inputs| {
                inputs
                    .iter()
                    .map(|l| LsiDescription {
                        index_name: l.index_name.clone(),
                        key_schema: l.key_schema.clone(),
                        projection: l.projection.clone(),
                        index_size_bytes: 0,
                        item_count: 0,
                        index_arn: format!("{table_arn}/index/{}", l.index_name),
                    })
                    .collect()
            });

            let stream_spec = input.stream_specification.clone();
            let (latest_stream_arn, latest_stream_label, initial_shards) = if let Some(spec) =
                stream_spec.as_ref().filter(|s| s.stream_enabled)
            {
                let view = spec
                    .stream_view_type
                    .unwrap_or(extenddb_core::types::StreamViewType::NewAndOldImages);
                let label = extenddb_storage::stream_lineage::next_stream_label(None);
                let arn = crate::streams::build_stream_arn(
                    &self.region,
                    &account_id,
                    &input.table_name,
                    &label,
                );
                let mut shards = Vec::with_capacity(
                    extenddb_storage::stream_lineage::SHARDS_PER_STREAM as usize,
                );
                for idx in 0..extenddb_storage::stream_lineage::SHARDS_PER_STREAM {
                    let sid =
                        extenddb_storage::stream_lineage::shard_id(&table_id, &label, view, idx)?;
                    shards.push(crate::streams::ShardMeta::new_open(
                        table_id.clone(),
                        sid,
                        None,
                    ));
                }
                (Some(arn), Some(label), shards)
            } else {
                (None, None, Vec::new())
            };

            let sse_description = SseDescription::from_specification(
                input.sse_specification.as_ref(),
                &self.region,
                &account_id,
            );
            let table_class_summary = input
                .table_class
                .as_ref()
                .map(|tc| json!({ "TableClass": tc }));
            let table_wt_record =
                warm_throughput_record_for_create(input.warm_throughput.as_ref(), 0.0);

            let description = TableDescription {
                table_name: input.table_name.clone(),
                key_schema: input.key_schema,
                attribute_definitions: input.attribute_definitions,
                table_status: TableStatus::Active,
                creation_date_time: now_unix_f64(),
                table_size_bytes: 0,
                item_count: 0,
                table_arn,
                table_id: table_id.clone(),
                provisioned_throughput: throughput,
                billing_mode_summary: Some(BillingModeSummary {
                    billing_mode: billing,
                    last_update_to_pay_per_request_date_time: None,
                }),
                global_secondary_indexes: gsis,
                local_secondary_indexes: lsis,
                stream_specification: input.stream_specification,
                latest_stream_arn,
                latest_stream_label,
                deletion_protection_enabled: input.deletion_protection_enabled.unwrap_or(false),
                sse_description,
                table_class_summary,
                on_demand_throughput: input.on_demand_throughput,
                warm_throughput_record: table_wt_record,
                ..TableDescription::default()
            };

            for shard_meta in &initial_shards {
                crate::streams::put_shard(&self.cat(), shard_meta)
                    .await
                    .map_err(map_catalog_err)?;
            }

            let mut record = json!({
                "data_table": data_table,
                "ttl": {"enabled": false, "attribute": null},
            });
            insert_table_description_into_row(&mut record, &description)?;

            let inserted = self
                .cat()
                .put_if_absent(&meta_key, &record)
                .await
                .map_err(map_catalog_err)?;
            if !inserted {
                for shadow in &created_shadows {
                    let _ = admin.delete_table(shadow).await;
                }
                let _ = admin.delete_table(&data_table).await;
                for shard_meta in &initial_shards {
                    let _ = self
                        .cat()
                        .delete(&keys::stream_shard(&shard_meta.shard_id))
                        .await;
                }
                return Err(StorageError::TableAlreadyExists(input.table_name));
            }

            if let Some(tags) = &input.tags
                && !tags.is_empty()
            {
                self.tag_resource(&description.table_arn, tags).await?;
            }

            self.metadata_cache
                .invalidate(&(account_id, input.table_name))
                .await;
            Ok(description)
        })
    }

    fn delete_table(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::table_meta(&account_id, &input.table_name))
                .await
                .map_err(map_catalog_err)?
                .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

            let description = deserialize_table_description_row(&row)?;

            if description.deletion_protection_enabled {
                return Err(StorageError::DeletionProtected(input.table_name));
            }

            let data_table = row
                .get("data_table")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    StorageError::Internal("missing data_table in catalog row".into())
                })?;

            let mut admin = AdminClient::connect(&self.client)
                .await
                .map_err(map_catalog_err)?;
            if let Some(gsis) = &description.global_secondary_indexes {
                for g in gsis {
                    let shadow = crate::gsi::shadow_table_id(data_table, &g.index_name);
                    let _ = admin.delete_table(&shadow).await;
                }
            }
            admin
                .delete_table(data_table)
                .await
                .map_err(map_catalog_err)?;

            let shards = crate::streams::load_table_shards(&self.cat(), &description.table_id)
                .await
                .map_err(map_catalog_err)?;
            for shard in &shards {
                self.cat()
                    .delete_prefix(&keys::stream_record_shard_prefix(&shard.shard_id))
                    .await
                    .map_err(map_catalog_err)?;
                self.cat()
                    .delete_prefix(&keys::stream_dedup_shard_prefix(&shard.shard_id))
                    .await
                    .map_err(map_catalog_err)?;
                self.cat()
                    .delete(&keys::stream_shard(&shard.shard_id))
                    .await
                    .map_err(map_catalog_err)?;
            }
            self.cat()
                .delete(&keys::tags(&description.table_arn))
                .await
                .map_err(map_catalog_err)?;

            self.cat()
                .delete(&keys::table_meta(&account_id, &input.table_name))
                .await
                .map_err(map_catalog_err)?;

            let mut returned = description;
            returned.table_status = TableStatus::Deleting;
            self.metadata_cache
                .invalidate(&(account_id, input.table_name))
                .await;
            Ok(returned)
        })
    }

    fn describe_table(
        &self,
        account_id: &str,
        input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::table_meta(&account_id, &input.table_name))
                .await
                .map_err(map_catalog_err)?
                .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
            deserialize_table_description_row(&row)
        })
    }

    fn list_tables(
        &self,
        account_id: &str,
        input: ListTablesInput,
    ) -> BoxFuture<'_, Result<ListTablesOutput, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(&keys::table_meta_scan_prefix(&account_id))
                .await
                .map_err(map_catalog_err)?;
            let mut names: Vec<String> = rows
                .into_iter()
                .filter_map(|(key, _)| key.rsplit_once(':').map(|(_, last)| last.to_owned()))
                .collect();
            names.sort();

            if let Some(start) = &input.exclusive_start_table_name {
                names.retain(|n| n.as_str() > start.as_str());
            }
            let last_evaluated_table_name = match input.limit {
                Some(l) if l > 0 && (l as usize) < names.len() => {
                    names.truncate(l as usize);
                    names.last().cloned()
                }
                _ => None,
            };
            Ok(ListTablesOutput {
                table_names: names,
                last_evaluated_table_name,
            })
        })
    }

    fn update_table(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            if input
                .vector_index_updates
                .as_ref()
                .is_some_and(|v| !v.is_empty())
            {
                return Err(StorageError::Unsupported(
                    "VectorIndexes are not supported by the Cloud Bigtable backend".to_owned(),
                ));
            }

            let mut input = input;
            input.resolve_table_throughput_mode();
            let meta_key = keys::table_meta(&account_id, &input.table_name);

            // Pre-create Bigtable physical shadow tables for any GSI Create
            // actions so live writers can project to the shadow table the
            // instant `CREATING` commits in `table_meta`.
            let mut precreated_shadows: Vec<String> = Vec::new();
            if let Some(updates) = &input.global_secondary_index_updates
                && updates.iter().any(|u| u.create.is_some())
            {
                let initial_row = self
                    .cat()
                    .get(&meta_key)
                    .await
                    .map_err(map_catalog_err)?
                    .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
                let initial_dt = initial_row
                    .get("data_table")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| StorageError::Internal("missing data_table in catalog".into()))?
                    .to_owned();
                let initial_desc = deserialize_table_description_row(&initial_row)?;
                if initial_desc.table_status != TableStatus::Active {
                    return Err(StorageError::TableNotActive(input.table_name.clone()));
                }
                let building: Vec<String> = initial_desc
                    .global_secondary_indexes
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .filter(|g| g.index_status == "CREATING")
                    .map(|g| g.index_name.clone())
                    .collect();
                if building_refuses(&building, updates) {
                    return Err(StorageError::LimitExceeded(ONLINE_INDEX_LIMIT.to_owned()));
                }
                let mut admin = AdminClient::connect(&self.client)
                    .await
                    .map_err(map_catalog_err)?;
                let families = vec![
                    ("d", Some(crate::data::admin::gc_max_versions(1))),
                    ("m", Some(crate::data::admin::gc_max_versions(1))),
                    ("t", None),
                ];
                for u in updates {
                    if let Some(create) = &u.create {
                        let name_taken = initial_desc
                            .global_secondary_indexes
                            .as_deref()
                            .unwrap_or_default()
                            .iter()
                            .any(|g| {
                                g.index_name == create.index_name
                                    && !updates.iter().any(|du| {
                                        du.delete
                                            .as_ref()
                                            .is_some_and(|d| d.index_name == create.index_name)
                                    })
                            })
                            || initial_desc
                                .local_secondary_indexes
                                .as_deref()
                                .unwrap_or_default()
                                .iter()
                                .any(|l| l.index_name == create.index_name);
                        if name_taken {
                            for s in &precreated_shadows {
                                let _ = admin.delete_table(s).await;
                            }
                            return Err(StorageError::IndexAlreadyExists(
                                create.index_name.clone(),
                            ));
                        }
                        let shadow = crate::gsi::shadow_table_id(&initial_dt, &create.index_name);
                        admin
                            .create_table(&shadow, &families)
                            .await
                            .map_err(map_catalog_err)?;
                        precreated_shadows.push(shadow);
                    }
                }
            }

            let region = self.region.clone();
            let acct_for_cas = account_id.clone();
            let input_for_cas = input.clone();

            let cas_result = self
                .cat()
                .mutate_json_cas(&meta_key, map_catalog_err, move |existing| {
                    let mut row = existing.ok_or_else(|| {
                        StorageError::TableNotFound(input_for_cas.table_name.clone())
                    })?;
                    let data_table = row
                        .get("data_table")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            StorageError::Internal("missing data_table in catalog".into())
                        })?
                        .to_owned();
                    let mut description = deserialize_table_description_row(&row)?;
                    if description.table_status != TableStatus::Active {
                        return Err(StorageError::TableNotActive(
                            input_for_cas.table_name.clone(),
                        ));
                    }

                    let stored_billing = description
                        .billing_mode_summary
                        .as_ref()
                        .map_or(BillingMode::Provisioned, |s| s.billing_mode);
                    let effective_billing = input_for_cas.billing_mode.unwrap_or(stored_billing);

                    if input_for_cas.provisioned_throughput.is_some()
                        && effective_billing == BillingMode::PayPerRequest
                    {
                        return Err(StorageError::Validation(
                            "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST"
                                .to_owned(),
                        ));
                    }

                    if matches!(input_for_cas.billing_mode, Some(BillingMode::Provisioned))
                        && let Some(pt) = &input_for_cas.provisioned_throughput
                        && stored_billing == BillingMode::Provisioned
                        && description.provisioned_throughput.read_capacity_units
                            == pt.read_capacity_units
                        && description.provisioned_throughput.write_capacity_units
                            == pt.write_capacity_units
                    {
                        return Err(StorageError::NoOpUpdate(format!(
                            "The provisioned throughput for the table will not change. The requested value equals the current value. Current ReadCapacityUnits provisioned for the table: {}. Requested ReadCapacityUnits: {}. Current WriteCapacityUnits provisioned for the table: {}. Requested WriteCapacityUnits: {}.",
                            description.provisioned_throughput.read_capacity_units,
                            pt.read_capacity_units,
                            description.provisioned_throughput.write_capacity_units,
                            pt.write_capacity_units
                        )));
                    }

                    if let Some(b) = input_for_cas.billing_mode {
                        match b {
                            BillingMode::PayPerRequest => {
                                description.billing_mode_summary = Some(BillingModeSummary {
                                    billing_mode: BillingMode::PayPerRequest,
                                    last_update_to_pay_per_request_date_time: Some(now_unix_f64()),
                                });
                                description.provisioned_throughput =
                                    ProvisionedThroughputDescription::default();
                                if let Some(gsis) = description.global_secondary_indexes.as_mut() {
                                    for g in gsis.iter_mut() {
                                        g.provisioned_throughput = None;
                                    }
                                }
                            }
                            BillingMode::Provisioned => {
                                let prev_ts = description
                                    .billing_mode_summary
                                    .as_ref()
                                    .and_then(|s| s.last_update_to_pay_per_request_date_time);
                                description.billing_mode_summary = Some(BillingModeSummary {
                                    billing_mode: BillingMode::Provisioned,
                                    last_update_to_pay_per_request_date_time: prev_ts,
                                });
                            }
                        }
                    }
                    if let Some(pt) = &input_for_cas.provisioned_throughput {
                        description.provisioned_throughput.read_capacity_units =
                            pt.read_capacity_units;
                        description.provisioned_throughput.write_capacity_units =
                            pt.write_capacity_units;
                    }
                    if let Some(odt) = &input_for_cas.on_demand_throughput {
                        description.on_demand_throughput = Some(odt.clone());
                    }
                    if let Some(d) = input_for_cas.deletion_protection_enabled {
                        description.deletion_protection_enabled = d;
                    }
                    if let Some(tc) = &input_for_cas.table_class {
                        description.table_class_summary = Some(json!({ "TableClass": tc }));
                    }
                    if let Some(wt) = &input_for_cas.warm_throughput {
                        let prev_json = description
                            .warm_throughput_record
                            .as_ref()
                            .map(warm_throughput_record_json)
                            .transpose()?;
                        description.warm_throughput_record =
                            Some(warm_throughput_record_for_update(prev_json.as_deref(), wt, 0.0));
                    }

                    let mut created_gsis: Vec<GsiDescription> = Vec::new();
                    let mut deleted_gsi_names: Vec<String> = Vec::new();

                    if let Some(updates) = &input_for_cas.global_secondary_index_updates {
                        let building: Vec<String> = description
                            .global_secondary_indexes
                            .as_deref()
                            .unwrap_or_default()
                            .iter()
                            .filter(|g| g.index_status == "CREATING")
                            .map(|g| g.index_name.clone())
                            .collect();
                        if building_refuses(&building, updates) {
                            return Err(StorageError::LimitExceeded(
                                ONLINE_INDEX_LIMIT.to_owned(),
                            ));
                        }

                        // Check attribute definition type consistency against existing defs.
                        let req_attrs = input_for_cas
                            .attribute_definitions
                            .as_deref()
                            .unwrap_or(&[]);
                        for req_ad in req_attrs {
                            if let Some(existing_ad) = description
                                .attribute_definitions
                                .iter()
                                .find(|a| a.attribute_name == req_ad.attribute_name)
                                && existing_ad.attribute_type != req_ad.attribute_type
                            {
                                return Err(StorageError::Validation(format!(
                                    "AttributeDefinition type mismatch for '{}'",
                                    req_ad.attribute_name
                                )));
                            }
                        }

                        for upd_action in updates {
                            let action_count = usize::from(upd_action.create.is_some())
                                + usize::from(upd_action.update.is_some())
                                + usize::from(upd_action.delete.is_some());
                            if action_count != 1 {
                                return Err(StorageError::Validation(
                                    "GlobalSecondaryIndexUpdate must contain exactly one of Create, Update, or Delete"
                                        .to_owned(),
                                ));
                            }

                            if let Some(upd) = &upd_action.update {
                                if upd.carries_throughput() || upd.warm_throughput.is_none() {
                                    return Err(StorageError::Validation(
                                        "Updating ProvisionedThroughput or OnDemandThroughput on a global secondary index is not yet supported"
                                            .to_owned(),
                                    ));
                                }
                                let gsi_mut = description
                                    .global_secondary_indexes
                                    .as_mut()
                                    .and_then(|gsis| {
                                        gsis.iter_mut()
                                            .find(|g| g.index_name == upd.index_name)
                                    })
                                    .ok_or_else(|| {
                                        StorageError::IndexNotFound(upd.index_name.clone())
                                    })?;
                                if let Some(wt) = &upd.warm_throughput {
                                    let prev_json = gsi_mut
                                        .warm_throughput_record
                                        .as_ref()
                                        .map(warm_throughput_record_json)
                                        .transpose()?;
                                    gsi_mut.warm_throughput_record = Some(
                                        warm_throughput_record_for_update(
                                            prev_json.as_deref(),
                                            wt,
                                            0.0,
                                        ),
                                    );
                                }
                            }
                            if let Some(del) = &upd_action.delete {
                                let gsis = description
                                    .global_secondary_indexes
                                    .as_mut()
                                    .ok_or_else(|| {
                                        StorageError::IndexNotFound(del.index_name.clone())
                                    })?;
                                let pos = gsis
                                    .iter()
                                    .position(|g| g.index_name == del.index_name)
                                    .ok_or_else(|| {
                                        StorageError::IndexNotFound(del.index_name.clone())
                                    })?;
                                gsis.remove(pos);
                                if gsis.is_empty() {
                                    description.global_secondary_indexes = None;
                                }
                                deleted_gsi_names.push(del.index_name.clone());
                            }
                            if let Some(create) = &upd_action.create {
                                if create.key_schema.is_empty() {
                                    return Err(StorageError::Validation(
                                        "KeySchema must not be empty for GSI creation".to_owned(),
                                    ));
                                }
                                if effective_billing == BillingMode::PayPerRequest
                                    && create.provisioned_throughput.is_some()
                                {
                                    return Err(StorageError::Validation(
                                        "ProvisionedThroughput cannot be specified for GSI when BillingMode is PAY_PER_REQUEST"
                                            .to_owned(),
                                    ));
                                }
                                if effective_billing == BillingMode::Provisioned
                                    && create.provisioned_throughput.is_none()
                                {
                                    return Err(StorageError::Validation(
                                        "ProvisionedThroughput must be specified for GSI when BillingMode is PROVISIONED"
                                            .to_owned(),
                                    ));
                                }
                                for ks in &create.key_schema {
                                    let defined = description
                                        .attribute_definitions
                                        .iter()
                                        .chain(req_attrs.iter())
                                        .any(|ad| ad.attribute_name == ks.attribute_name);
                                    if !defined {
                                        return Err(StorageError::Validation(format!(
                                            "One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [{}]",
                                            ks.attribute_name
                                        )));
                                    }
                                }

                                let taken_in_gsi = description
                                    .global_secondary_indexes
                                    .as_deref()
                                    .unwrap_or_default()
                                    .iter()
                                    .any(|g| g.index_name == create.index_name);
                                let taken_in_lsi = description
                                    .local_secondary_indexes
                                    .as_deref()
                                    .unwrap_or_default()
                                    .iter()
                                    .any(|l| l.index_name == create.index_name);
                                if taken_in_gsi || taken_in_lsi {
                                    return Err(StorageError::IndexAlreadyExists(
                                        create.index_name.clone(),
                                    ));
                                }
                                let gsi_desc = GsiDescription {
                                    index_name: create.index_name.clone(),
                                    key_schema: create.key_schema.clone(),
                                    projection: create.projection.clone(),
                                    index_status: "CREATING".to_string(),
                                    provisioned_throughput: create
                                        .provisioned_throughput
                                        .as_ref()
                                        .map(|p| ProvisionedThroughputDescription {
                                            read_capacity_units: p.read_capacity_units,
                                            write_capacity_units: p.write_capacity_units,
                                            number_of_decreases_today: 0,
                                            last_increase_date_time: None,
                                            last_decrease_date_time: None,
                                        }),
                                    index_size_bytes: 0,
                                    item_count: 0,
                                    index_arn: format!(
                                        "{}/index/{}",
                                        description.table_arn, create.index_name
                                    ),
                                    warm_throughput: None,
                                    warm_throughput_record: warm_throughput_record_for_create(
                                        create.warm_throughput.as_ref(),
                                        0.0,
                                    ),
                                };
                                description
                                    .global_secondary_indexes
                                    .get_or_insert_with(Vec::new)
                                    .push(gsi_desc.clone());
                                created_gsis.push(gsi_desc);
                            }
                        }

                        let mut surviving_schemas: Vec<Vec<extenddb_core::types::KeySchemaElement>> =
                            Vec::new();
                        if let Some(gsis) = &description.global_secondary_indexes {
                            for g in gsis {
                                surviving_schemas.push(g.key_schema.clone());
                            }
                        }
                        if let Some(lsis) = &description.local_secondary_indexes {
                            for l in lsis {
                                surviving_schemas.push(l.key_schema.clone());
                            }
                        }
                        description.attribute_definitions = effective_attribute_definitions(
                            &description.attribute_definitions,
                            req_attrs,
                            &description.key_schema,
                            &surviving_schemas,
                        );
                    }

                    let mut stream_op = PendingStreamOp::None;
                    if let Some(spec) = &input_for_cas.stream_specification {
                        let label_to_close_prior =
                            extenddb_storage::stream_lineage::disabled_stream_to_close(
                                description.stream_specification.as_ref(),
                                description.latest_stream_label.as_deref(),
                                spec,
                            )
                            .map(str::to_owned);

                        match extenddb_storage::stream_lineage::stream_change(
                            &input_for_cas.table_name,
                            description.stream_specification.as_ref(),
                            spec,
                        ) {
                            Err(StorageError::Validation(msg)) => {
                                if let Some(lbl) = label_to_close_prior {
                                    stream_op = PendingStreamOp::RefuseAfterClosingDisabled {
                                        table_id: description.table_id.clone(),
                                        label_to_close: lbl,
                                        refusal_message: msg,
                                    };
                                    return Ok((
                                        CatalogJsonAction::NoOp,
                                        (
                                            data_table,
                                            description,
                                            created_gsis,
                                            deleted_gsi_names,
                                            stream_op,
                                        ),
                                    ));
                                }
                                return Err(StorageError::Validation(msg));
                            }
                            Err(other) => return Err(other),
                            Ok(change) => {
                                let next_spec =
                                    extenddb_storage::stream_lineage::stored_specification(
                                        change,
                                        description.stream_specification.as_ref(),
                                        spec,
                                    );
                                match change {
                                    extenddb_storage::stream_lineage::StreamChange::Enable(
                                        view,
                                    ) => {
                                        let label =
                                            extenddb_storage::stream_lineage::next_stream_label(
                                                description.latest_stream_label.as_deref(),
                                            );
                                        let arn = crate::streams::build_stream_arn(
                                            &region,
                                            &acct_for_cas,
                                            &input_for_cas.table_name,
                                            &label,
                                        );
                                        description.latest_stream_arn = Some(arn);
                                        description.latest_stream_label = Some(label.clone());
                                        description.stream_specification = Some(next_spec);
                                        stream_op = PendingStreamOp::Enable {
                                            table_id: description.table_id.clone(),
                                            label,
                                            view,
                                            label_to_close_prior,
                                        };
                                    }
                                    extenddb_storage::stream_lineage::StreamChange::Disable => {
                                        let label_to_close =
                                            description.latest_stream_label.clone();
                                        description.stream_specification = Some(next_spec);
                                        stream_op = PendingStreamOp::Disable {
                                            table_id: description.table_id.clone(),
                                            label_to_close,
                                            label_to_close_prior,
                                        };
                                    }
                                }
                            }
                        }
                    }

                    insert_table_description_into_row(&mut row, &description)?;
                    Ok((
                        CatalogJsonAction::Put(row),
                        (
                            data_table,
                            description,
                            created_gsis,
                            deleted_gsi_names,
                            stream_op,
                        ),
                    ))
                })
                .await;

            let (data_table, staged_desc, created_gsis, deleted_gsi_names, stream_op) =
                match cas_result {
                    Ok(v) => v,
                    Err(e) => {
                        if !precreated_shadows.is_empty()
                            && let Ok(mut admin) = AdminClient::connect(&self.client).await
                        {
                            for s in &precreated_shadows {
                                let _ = admin.delete_table(s).await;
                            }
                        }
                        return Err(e);
                    }
                };

            self.metadata_cache
                .invalidate(&(account_id.clone(), input.table_name.clone()))
                .await;

            match stream_op {
                PendingStreamOp::None => {}
                PendingStreamOp::RefuseAfterClosingDisabled {
                    table_id,
                    label_to_close,
                    refusal_message,
                } => {
                    let existing_shards = crate::streams::load_table_shards(&self.cat(), &table_id)
                        .await
                        .map_err(map_catalog_err)?;
                    let lineage_rows: Vec<_> = existing_shards
                        .iter()
                        .map(crate::streams::ShardMeta::to_lineage_row)
                        .collect();
                    let to_close = extenddb_storage::stream_lineage::open_shard_ids_of(
                        &table_id,
                        &label_to_close,
                        &lineage_rows,
                    );
                    crate::streams::close_shards_by_id(&self.cat(), &to_close)
                        .await
                        .map_err(map_catalog_err)?;
                    return Err(StorageError::Validation(refusal_message));
                }
                PendingStreamOp::Disable {
                    table_id,
                    label_to_close,
                    label_to_close_prior,
                } => {
                    // `stream_enabled = false` is ALREADY durable in `table_meta`.
                    let existing_shards = crate::streams::load_table_shards(&self.cat(), &table_id)
                        .await
                        .map_err(map_catalog_err)?;
                    let lineage_rows: Vec<_> = existing_shards
                        .iter()
                        .map(crate::streams::ShardMeta::to_lineage_row)
                        .collect();
                    let mut to_close = Vec::new();
                    if let Some(prior_lbl) = &label_to_close_prior {
                        to_close.extend(extenddb_storage::stream_lineage::open_shard_ids_of(
                            &table_id,
                            prior_lbl,
                            &lineage_rows,
                        ));
                    }
                    if let Some(lbl) = &label_to_close {
                        for sid in extenddb_storage::stream_lineage::open_shard_ids_of(
                            &table_id,
                            lbl,
                            &lineage_rows,
                        ) {
                            if !to_close.contains(&sid) {
                                to_close.push(sid);
                            }
                        }
                    }
                    crate::streams::close_shards_by_id(&self.cat(), &to_close)
                        .await
                        .map_err(map_catalog_err)?;
                }
                PendingStreamOp::Enable {
                    table_id,
                    label,
                    view,
                    label_to_close_prior,
                } => {
                    let existing_shards = crate::streams::load_table_shards(&self.cat(), &table_id)
                        .await
                        .map_err(map_catalog_err)?;
                    let lineage_rows: Vec<_> = existing_shards
                        .iter()
                        .map(crate::streams::ShardMeta::to_lineage_row)
                        .collect();
                    let mut older = extenddb_storage::stream_lineage::open_shard_ids_before(
                        &table_id,
                        &label,
                        &lineage_rows,
                    );
                    if let Some(prior_lbl) = &label_to_close_prior {
                        for sid in extenddb_storage::stream_lineage::open_shard_ids_of(
                            &table_id,
                            prior_lbl,
                            &lineage_rows,
                        ) {
                            if !older.contains(&sid) {
                                older.push(sid);
                            }
                        }
                    }
                    crate::streams::close_shards_by_id(&self.cat(), &older)
                        .await
                        .map_err(map_catalog_err)?;

                    let mut inserted_sids = Vec::with_capacity(
                        extenddb_storage::stream_lineage::SHARDS_PER_STREAM as usize,
                    );
                    for idx in 0..extenddb_storage::stream_lineage::SHARDS_PER_STREAM {
                        let sid = extenddb_storage::stream_lineage::shard_id(
                            &table_id, &label, view, idx,
                        )?;
                        let shard_meta = crate::streams::ShardMeta::new_open(
                            table_id.clone(),
                            sid.clone(),
                            None,
                        );
                        crate::streams::put_shard_if_absent(&self.cat(), &shard_meta)
                            .await
                            .map_err(map_catalog_err)?;
                        inserted_sids.push(sid);
                    }

                    // Re-verify `enable_still_current` against `table_meta`.
                    let fresh_row = self.cat().get(&meta_key).await.map_err(map_catalog_err)?;
                    let fresh_stream_row = fresh_row
                        .as_ref()
                        .and_then(|r| deserialize_table_description_row(r).ok())
                        .map(|d| Self::table_stream_row(&d));
                    if !extenddb_storage::stream_lineage::enable_still_current(
                        fresh_stream_row.as_ref(),
                        &label,
                    ) {
                        crate::streams::close_shards_by_id(&self.cat(), &inserted_sids)
                            .await
                            .map_err(map_catalog_err)?;
                    }
                }
            }

            if !created_gsis.is_empty() {
                let base_key_info = crate::gsi_worker::build_reconciler_base_key_info(
                    &account_id,
                    &input.table_name,
                    &staged_desc,
                );
                for created_gsi in &created_gsis {
                    let _building_guard =
                        self.register_building_gsi(&staged_desc.table_id, &created_gsi.index_name);
                    let backfill_res = crate::gsi_worker::reconcile_table_gsi(
                        self,
                        &base_key_info,
                        &data_table,
                        created_gsi,
                    )
                    .await
                    .map_err(map_catalog_err);
                    if let Err(e) = backfill_res {
                        let failed_name = created_gsi.index_name.clone();
                        let _ = self
                            .cat()
                            .mutate_json_cas(&meta_key, map_catalog_err, move |existing| {
                                let Some(mut row) = existing else {
                                    return Ok((CatalogJsonAction::NoOp, ()));
                                };
                                let mut desc = deserialize_table_description_row(&row)?;
                                if let Some(gsis) = desc.global_secondary_indexes.as_mut() {
                                    gsis.retain(|g| g.index_name != failed_name);
                                    if gsis.is_empty() {
                                        desc.global_secondary_indexes = None;
                                    }
                                }
                                insert_table_description_into_row(&mut row, &desc)?;
                                Ok((CatalogJsonAction::Put(row), ()))
                            })
                            .await;
                        if let Ok(mut admin) = AdminClient::connect(&self.client).await {
                            let shadow =
                                crate::gsi::shadow_table_id(&data_table, &created_gsi.index_name);
                            let _ = admin.delete_table(&shadow).await;
                        }
                        self.metadata_cache
                            .invalidate(&(account_id, input.table_name))
                            .await;
                        return Err(e);
                    }

                    let promoted_name = created_gsi.index_name.clone();
                    self.cat()
                        .mutate_json_cas(&meta_key, map_catalog_err, move |existing| {
                            let mut row = existing.ok_or_else(|| {
                                StorageError::TableNotFound(promoted_name.clone())
                            })?;
                            let mut desc = deserialize_table_description_row(&row)?;
                            if let Some(gsis) = desc.global_secondary_indexes.as_mut() {
                                for g in gsis.iter_mut() {
                                    if g.index_name == promoted_name && g.index_status == "CREATING"
                                    {
                                        g.index_status = "ACTIVE".to_string();
                                    }
                                }
                            }
                            insert_table_description_into_row(&mut row, &desc)?;
                            Ok((CatalogJsonAction::Put(row), ()))
                        })
                        .await?;
                }
            }

            if !deleted_gsi_names.is_empty()
                && let Ok(mut admin) = AdminClient::connect(&self.client).await
            {
                let current_desc = self
                    .cat()
                    .get(&meta_key)
                    .await
                    .map_err(map_catalog_err)?
                    .and_then(|r| deserialize_table_description_row(&r).ok());
                for del_name in &deleted_gsi_names {
                    let recreated = current_desc
                        .as_ref()
                        .and_then(|d| d.global_secondary_indexes.as_ref())
                        .is_some_and(|gsis| gsis.iter().any(|g| &g.index_name == del_name));
                    if !recreated {
                        let shadow = crate::gsi::shadow_table_id(&data_table, del_name);
                        let _ = admin.delete_table(&shadow).await;
                    }
                }
            }

            self.metadata_cache
                .invalidate(&(account_id.clone(), input.table_name.clone()))
                .await;

            if created_gsis.is_empty() && deleted_gsi_names.is_empty() {
                Ok(staged_desc)
            } else {
                self.describe_table(
                    &account_id,
                    DescribeTableInput {
                        table_name: input.table_name,
                    },
                )
                .await
            }
        })
    }

    fn table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableKeyInfo, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let (_, description, _) = self
                .table_full_meta_for_raw(&account_id, &table_name)
                .await?;
            let key_schema = description.key_schema;
            let global_secondary_indexes = description
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
                .collect();
            let local_secondary_indexes = description
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
                .collect();
            Ok(TableKeyInfo {
                table_name,
                account_id,
                table_id: description.table_id,
                base_key_schema: key_schema.clone(),
                key_schema,
                attribute_definitions: description.attribute_definitions,
                has_lsi: description.local_secondary_indexes.is_some(),
                global_secondary_indexes,
                local_secondary_indexes,
                stream_specification: description.stream_specification,
                vector_indexes: Vec::new(),
            })
        })
    }

    fn index_info(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let index_name = index_name.to_owned();
        Box::pin(async move {
            let (_, description, _) = self
                .table_full_meta_for_raw(&account_id, &table_name)
                .await?;
            if let Some(gsis) = &description.global_secondary_indexes
                && let Some(g) = gsis.iter().find(|g| g.index_name == index_name)
            {
                if g.index_status != "ACTIVE" {
                    return Err(StorageError::IndexNotFound(index_name));
                }
                return Ok(IndexInfo {
                    index_name: g.index_name.clone(),
                    index_id: format!("{}::{}", description.table_id, g.index_name),
                    index_type: IndexType::Gsi,
                    key_schema: g.key_schema.clone(),
                    projection: g.projection.clone(),
                });
            }
            if let Some(lsis) = &description.local_secondary_indexes
                && let Some(l) = lsis.iter().find(|l| l.index_name == index_name)
            {
                return Ok(IndexInfo {
                    index_name: l.index_name.clone(),
                    index_id: format!("{}::{}", description.table_id, l.index_name),
                    index_type: IndexType::Lsi,
                    key_schema: l.key_schema.clone(),
                    projection: l.projection.clone(),
                });
            }
            Err(StorageError::IndexNotFound(index_name))
        })
    }

    fn index_info_by_table_id(
        &self,
        table_id: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        let table_id = table_id.to_owned();
        let index_name = index_name.to_owned();
        Box::pin(async move {
            let metas = self
                .cat()
                .scan_prefix(keys::TABLE_META_SCAN_PREFIX)
                .await
                .map_err(map_catalog_err)?;
            for (meta_key, value) in metas {
                let Ok(desc) = deserialize_table_description_row(&value) else {
                    continue;
                };
                if desc.table_id != table_id {
                    continue;
                }
                let mut parts = meta_key.splitn(3, ':');
                let _ = parts.next();
                if let (Some(acct_id), Some(table_name)) = (parts.next(), parts.next()) {
                    return self.index_info(acct_id, table_name, &index_name).await;
                }
            }
            Err(StorageError::TableNotFound(format!("table_id={table_id}")))
        })
    }
}

// =========== DataEngine ===========

impl DataEngine for BigtableEngine {
    fn put_item(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            let (data_table, desc, ttl_attr) = self.table_full_meta_for(&key_info).await?;
            Self::validate_put_index_keys(&desc, &item)?;
            let should_capture_stream =
                Self::active_stream_target_for_capture(&desc, stream.as_ref(), &item)?.is_some();

            if should_capture_stream {
                let (existing, _, _) = self
                    .execute_fenced_single_item_write(
                        &key_info,
                        &data_table,
                        &desc,
                        ttl_attr.as_deref(),
                        SingleWriteOp::Put { item },
                        condition.as_ref(),
                        &maps,
                        stream.as_ref(),
                    )
                    .await?;
                return Ok(if return_old { existing } else { None });
            }

            let row_key = crate::data::encoding::row_key::encode_key(&item, &key_info.key_schema)?;
            let _row_guard = self.acquire_single_write_lock(&data_table, &row_key).await;

            let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
            let ops = ItemOps::new(&self.client, &data_table, self.intent_timeout_secs);
            let single_deadline = std::time::Instant::now()
                + Duration::from_secs(self.intent_timeout_secs.max(2) + 2);
            let mut multi_conflicts = 0usize;

            for attempt in 0..MAX_OCC_ATTEMPTS {
                let (_, existing, read_version) = self
                    .read_row_resolving_committed_intents(&ops, &data_table, &row_key)
                    .await?;
                check_condition(existing.as_ref(), condition.as_ref(), &maps)?;

                match ops
                    .put_cas(&key_info, &item, read_version, existing.is_some())
                    .await?
                {
                    CasOutcome::Applied(new_version) => {
                        self.write_shadows_only(
                            &key_info,
                            &data_table,
                            &gsis,
                            &item,
                            existing.as_ref(),
                            new_version,
                        )
                        .await?;
                        self.maintain_ttl_index(
                            &key_info,
                            ttl_attr.as_deref(),
                            existing.as_ref(),
                            Some(&item),
                            new_version,
                        )
                        .await?;
                        return Ok(if return_old { existing } else { None });
                    }
                    CasOutcome::IntentPresent => {
                        self.wait_or_fail_occ_intent(
                            &data_table,
                            &row_key,
                            single_deadline,
                            &mut multi_conflicts,
                            attempt,
                        )
                        .await?;
                    }
                    CasOutcome::OccMismatch => {
                        let backoff_ms = (1u64 << attempt.min(4)) + rand::random_range(0..4);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
            }

            Err(StorageError::TransactionConflict(
                "OCC contention limit exceeded on PutItem".to_string(),
            ))
        })
    }

    fn get_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        Box::pin(async move {
            let data_table = self.data_table_for(&key_info).await?;
            ItemOps::new(&self.client, &data_table, self.intent_timeout_secs)
                .get(&key_info, &key)
                .await
        })
    }

    fn delete_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            let (data_table, desc, ttl_attr) = self.table_full_meta_for(&key_info).await?;
            let should_capture_stream =
                Self::active_stream_target_for_capture(&desc, stream.as_ref(), &key)?.is_some();

            if should_capture_stream {
                let (existing, _, _) = self
                    .execute_fenced_single_item_write(
                        &key_info,
                        &data_table,
                        &desc,
                        ttl_attr.as_deref(),
                        SingleWriteOp::Delete { key: &key },
                        condition.as_ref(),
                        &maps,
                        stream.as_ref(),
                    )
                    .await?;
                return Ok(if return_old { existing } else { None });
            }

            let row_key = crate::data::encoding::row_key::encode_key(&key, &key_info.key_schema)?;
            let _row_guard = self.acquire_single_write_lock(&data_table, &row_key).await;

            let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
            let ops = ItemOps::new(&self.client, &data_table, self.intent_timeout_secs);
            let single_deadline = std::time::Instant::now()
                + Duration::from_secs(self.intent_timeout_secs.max(2) + 2);
            let mut multi_conflicts = 0usize;

            for attempt in 0..MAX_OCC_ATTEMPTS {
                let (has_live_intent, existing, read_version) = self
                    .read_row_resolving_committed_intents(&ops, &data_table, &row_key)
                    .await?;
                check_condition(existing.as_ref(), condition.as_ref(), &maps)?;
                if existing.is_none() && condition.is_none() && !has_live_intent {
                    return Ok(None);
                }

                match ops
                    .delete_cas(&key_info, &key, read_version, existing.is_some())
                    .await?
                {
                    CasOutcome::Applied(new_version) => {
                        if let Some(prior) = existing.as_ref() {
                            self.delete_shadows(&key_info, &data_table, &gsis, prior, new_version)
                                .await?;
                            self.maintain_ttl_index(
                                &key_info,
                                ttl_attr.as_deref(),
                                Some(prior),
                                None,
                                new_version,
                            )
                            .await?;
                        }
                        return Ok(if return_old { existing } else { None });
                    }
                    CasOutcome::IntentPresent => {
                        self.wait_or_fail_occ_intent(
                            &data_table,
                            &row_key,
                            single_deadline,
                            &mut multi_conflicts,
                            attempt,
                        )
                        .await?;
                    }
                    CasOutcome::OccMismatch => {
                        let backoff_ms = (1u64 << attempt.min(4)) + rand::random_range(0..4);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
            }

            Err(StorageError::TransactionConflict(
                "OCC contention limit exceeded on DeleteItem".to_string(),
            ))
        })
    }

    fn update_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, ItemPairResult> {
        let key_info = key_info.clone();
        let key = key.clone();
        let actions = actions.to_vec();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            let (data_table, desc, ttl_attr) = self.table_full_meta_for(&key_info).await?;
            Self::validate_update_index_literals(&desc, &actions, &maps)?;
            let should_capture_stream =
                Self::active_stream_target_for_capture(&desc, stream.as_ref(), &key)?.is_some();

            if should_capture_stream {
                let (existing, new_item, _) = self
                    .execute_fenced_single_item_write(
                        &key_info,
                        &data_table,
                        &desc,
                        ttl_attr.as_deref(),
                        SingleWriteOp::Update {
                            key: &key,
                            actions: &actions,
                        },
                        condition.as_ref(),
                        &maps,
                        stream.as_ref(),
                    )
                    .await?;
                if new_item.is_none() {
                    return Ok((None, None));
                }
                return Ok((
                    if return_old { existing } else { None },
                    if return_new { new_item } else { None },
                ));
            }

            let row_key = crate::data::encoding::row_key::encode_key(&key, &key_info.key_schema)?;
            let _row_guard = self.acquire_single_write_lock(&data_table, &row_key).await;

            let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
            let ops = ItemOps::new(&self.client, &data_table, self.intent_timeout_secs);
            let single_deadline = std::time::Instant::now()
                + Duration::from_secs(self.intent_timeout_secs.max(2) + 2);
            let mut multi_conflicts = 0usize;

            for attempt in 0..MAX_OCC_ATTEMPTS {
                let (_, existing, read_version) = self
                    .read_row_resolving_committed_intents(&ops, &data_table, &row_key)
                    .await?;
                check_condition(existing.as_ref(), condition.as_ref(), &maps)?;

                let base = match existing.as_ref() {
                    Some(e) => e.clone(),
                    None => key.clone(),
                };
                let new_item = apply_update_actions_validated(
                    &base,
                    &actions,
                    &maps,
                    &key_info,
                    &desc.attribute_definitions,
                )?;
                Self::validate_updated_item_index_keys(&desc, &new_item)?;

                if existing.is_none() && removes_only(&actions) {
                    return Ok((None, None));
                }

                match ops
                    .put_cas(&key_info, &new_item, read_version, existing.is_some())
                    .await?
                {
                    CasOutcome::Applied(new_version) => {
                        self.write_shadows_only(
                            &key_info,
                            &data_table,
                            &gsis,
                            &new_item,
                            existing.as_ref(),
                            new_version,
                        )
                        .await?;
                        self.maintain_ttl_index(
                            &key_info,
                            ttl_attr.as_deref(),
                            existing.as_ref(),
                            Some(&new_item),
                            new_version,
                        )
                        .await?;
                        return Ok((
                            if return_old { existing } else { None },
                            if return_new { Some(new_item) } else { None },
                        ));
                    }
                    CasOutcome::IntentPresent => {
                        self.wait_or_fail_occ_intent(
                            &data_table,
                            &row_key,
                            single_deadline,
                            &mut multi_conflicts,
                            attempt,
                        )
                        .await?;
                    }
                    CasOutcome::OccMismatch => {
                        let backoff_ms = (1u64 << attempt.min(4)) + rand::random_range(0..4);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
            }

            Err(StorageError::TransactionConflict(
                "OCC contention limit exceeded on UpdateItem".to_string(),
            ))
        })
    }

    fn query(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        let key_info = key_info.clone();
        let kc = key_condition.clone();
        let maps = maps.clone();
        let esk = exclusive_start_key.cloned();
        let index_name = index_name.map(str::to_owned);
        Box::pin(async move {
            let (data_table, desc, _) = self.table_full_meta_for(&key_info).await?;
            let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
            let lsis = desc.local_secondary_indexes.clone().unwrap_or_default();
            let base_ki = TableKeyInfo {
                key_schema: desc.key_schema.clone(),
                base_key_schema: desc.key_schema.clone(),
                attribute_definitions: desc.attribute_definitions.clone(),
                ..key_info.clone()
            };
            match index_name {
                Some(idx_name) => {
                    if let Some(gsi) = gsis.iter().find(|g| g.index_name == idx_name) {
                        if gsi.index_status != "ACTIVE" {
                            return Err(StorageError::IndexNotFound(idx_name));
                        }
                        let shadow_table =
                            crate::gsi::shadow_table_id(&data_table, &gsi.index_name);
                        let gsi_key_info = TableKeyInfo {
                            table_name: format!("{}__gsi_{}", key_info.table_name, gsi.index_name),
                            account_id: key_info.account_id.clone(),
                            table_id: format!("{}::{}", key_info.table_id, gsi.index_name),
                            key_schema: gsi.key_schema.clone(),
                            base_key_schema: desc.key_schema.clone(),
                            attribute_definitions: desc.attribute_definitions.clone(),
                            ..TableKeyInfo::default()
                        };
                        QueryScan::new(&self.client, &shadow_table)
                            .query(&gsi_key_info, &kc, &maps, forward, limit, esk.as_ref())
                            .await
                    } else if let Some(lsi) = lsis.iter().find(|l| l.index_name == idx_name) {
                        QueryScan::new(&self.client, &data_table)
                            .query_lsi(
                                &base_ki,
                                &lsi.key_schema,
                                &kc,
                                &maps,
                                forward,
                                limit,
                                esk.as_ref(),
                            )
                            .await
                    } else {
                        Err(StorageError::IndexNotFound(idx_name))
                    }
                }
                None => {
                    QueryScan::new(&self.client, &data_table)
                        .query(&base_ki, &kc, &maps, forward, limit, esk.as_ref())
                        .await
                }
            }
        })
    }

    fn scan(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        let key_info = key_info.clone();
        let esk = exclusive_start_key.cloned();
        let index_name = index_name.map(str::to_owned);
        Box::pin(async move {
            let (data_table, desc, _) = self.table_full_meta_for(&key_info).await?;
            let base_ki = TableKeyInfo {
                key_schema: desc.key_schema.clone(),
                base_key_schema: desc.key_schema.clone(),
                attribute_definitions: desc.attribute_definitions.clone(),
                ..key_info.clone()
            };
            match index_name {
                Some(idx_name) => {
                    let gsis = desc.global_secondary_indexes.unwrap_or_default();
                    let lsis = desc.local_secondary_indexes.unwrap_or_default();
                    if let Some(gsi) = gsis.iter().find(|g| g.index_name == idx_name) {
                        if gsi.index_status != "ACTIVE" {
                            return Err(StorageError::IndexNotFound(idx_name));
                        }
                        let shadow_table =
                            crate::gsi::shadow_table_id(&data_table, &gsi.index_name);
                        let gsi_key_info = TableKeyInfo {
                            table_name: format!("{}__gsi_{}", key_info.table_name, gsi.index_name),
                            account_id: key_info.account_id.clone(),
                            table_id: format!("{}::{}", key_info.table_id, gsi.index_name),
                            key_schema: gsi.key_schema.clone(),
                            base_key_schema: desc.key_schema.clone(),
                            attribute_definitions: desc.attribute_definitions.clone(),
                            ..TableKeyInfo::default()
                        };
                        QueryScan::new(&self.client, &shadow_table)
                            .scan(&gsi_key_info, limit, esk.as_ref(), segment, total_segments)
                            .await
                    } else if let Some(lsi) = lsis.iter().find(|l| l.index_name == idx_name) {
                        QueryScan::new(&self.client, &data_table)
                            .scan_lsi(
                                &base_ki,
                                &lsi.key_schema,
                                limit,
                                esk.as_ref(),
                                segment,
                                total_segments,
                            )
                            .await
                    } else {
                        Err(StorageError::IndexNotFound(idx_name))
                    }
                }
                None => {
                    QueryScan::new(&self.client, &data_table)
                        .scan(&base_ki, limit, esk.as_ref(), segment, total_segments)
                        .await
                }
            }
        })
    }

    fn scan_key_in_segment(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        segment: i64,
        total_segments: i64,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        let index_name = index_name.map(str::to_owned);
        Box::pin(async move {
            let schema = match index_name {
                None => {
                    if key_info.base_key_schema.is_empty() {
                        key_info.key_schema.clone()
                    } else {
                        key_info.base_key_schema.clone()
                    }
                }
                Some(idx_name) => {
                    if let Some(gsi) = key_info
                        .global_secondary_indexes
                        .iter()
                        .find(|g| g.index_name == idx_name)
                    {
                        gsi.key_schema.clone()
                    } else if let Some(lsi) = key_info
                        .local_secondary_indexes
                        .iter()
                        .find(|l| l.index_name == idx_name)
                    {
                        lsi.key_schema.clone()
                    } else {
                        let info = self
                            .index_info(&key_info.account_id, &key_info.table_name, &idx_name)
                            .await?;
                        info.key_schema
                    }
                }
            };
            key_belongs_to_segment(&key, &schema, segment, total_segments)
        })
    }

    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        let owned: Vec<(TableKeyInfo, Item)> = ops
            .iter()
            .map(|op| (op.key_info.clone(), op.key.clone()))
            .collect();
        Box::pin(async move {
            let total_ops = owned.len();
            if total_ops == 0 {
                return Ok(Vec::new());
            }

            let mut validation_reasons = Vec::with_capacity(total_ops);
            let mut any_validation_failed = false;
            let mut resolved_ops = Vec::with_capacity(total_ops);
            for (idx, (key_info, key)) in owned.iter().enumerate() {
                let (data_table, desc, _) = self.table_full_meta_for(key_info).await?;
                match extenddb_core::validation::validate_key_only(
                    key,
                    &desc.key_schema,
                    &desc.attribute_definitions,
                ) {
                    Ok(()) => {
                        let enc_key =
                            crate::data::encoding::row_key::encode_key(key, &desc.key_schema)?;
                        validation_reasons
                            .push(crate::transact::cancellation_reason("None", None, None));
                        resolved_ops.push((idx, data_table, enc_key));
                    }
                    Err(e) => {
                        any_validation_failed = true;
                        validation_reasons.push(crate::transact::cancellation_reason(
                            "ValidationError",
                            Some(&e.to_string()),
                            None,
                        ));
                    }
                }
            }
            if any_validation_failed {
                return Err(StorageError::TransactionCanceled(validation_reasons));
            }

            // Deduplicate unique `(data_table, enc_key)` pairs so duplicate keys in
            // one TransactGetItems request share a single atomic snapshot slot.
            let mut table_raw_keys: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
            for (_, data_table, enc_key) in &resolved_ops {
                let entry = table_raw_keys.entry(data_table.clone()).or_default();
                if !entry.contains(enc_key) {
                    entry.push(enc_key.clone());
                }
            }
            let unique_key_count: usize = table_raw_keys.values().map(Vec::len).sum();

            let coord = crate::transact::TxnCoordinator::new(
                &self.client,
                Duration::from_secs(self.intent_timeout_secs.max(1)),
            );

            for attempt in 0..MAX_TRANSACT_GET_ATTEMPTS {
                // Pass 1a: read `(d_item, m:v, all m:intent:*)` across all tables.
                let mut futs_1 = Vec::with_capacity(table_raw_keys.len());
                for (dt, raw_keys) in &table_raw_keys {
                    let client = self.client.clone();
                    let timeout = self.intent_timeout_secs;
                    let dt_clone = dt.clone();
                    let rk_clone = raw_keys.clone();
                    futs_1.push(async move {
                        let snaps = ItemOps::new(&client, &dt_clone, timeout)
                            .batch_get_row_snapshots_by_raw_keys(&rk_clone)
                            .await?;
                        Ok::<_, StorageError>((dt_clone, rk_clone, snaps))
                    });
                }
                let groups_1 = futures::future::try_join_all(futs_1).await?;

                let mut map_1: HashMap<(String, Vec<u8>), RowSnapshot> =
                    HashMap::with_capacity(unique_key_count);
                let mut all_tids: BTreeMap<String, ()> = BTreeMap::new();
                for (dt, raw_keys, snaps) in groups_1 {
                    for (rk, snap) in raw_keys.into_iter().zip(snaps) {
                        for (tid, _) in &snap.intents {
                            all_tids.insert(tid.clone(), ());
                        }
                        map_1.insert((dt.clone(), rk), snap);
                    }
                }

                // Single-key read with zero intents needs no second snapshot pass.
                if unique_key_count == 1 && all_tids.is_empty() {
                    let mut out = Vec::with_capacity(total_ops);
                    for (_, dt, rk) in &resolved_ops {
                        let item = map_1
                            .get(&(dt.clone(), rk.clone()))
                            .and_then(|s| s.item.clone());
                        out.push(item);
                    }
                    return Ok(out);
                }

                // Pass 1b: read coordinator state for every observed intent.
                let mut txns_1: HashMap<String, Option<crate::transact::TxnState>> =
                    HashMap::with_capacity(all_tids.len());
                for tid in all_tids.keys() {
                    let st = coord.get_state(tid).await?;
                    txns_1.insert(tid.clone(), st);
                }

                // Fast skew check: if a COMMITTED transaction observed on one key
                // also mutates another requested key whose Pass 1a read had
                // `occ_ver < new_version` without that intent, retry immediately.
                let mut cross_skew = false;
                for (tid, st_opt) in &txns_1 {
                    if let Some(st) = st_opt
                        && st.state == "COMMITTED"
                        && let Some(mutations) = &st.mutations
                    {
                        for m in mutations {
                            let target = (
                                m.participant.data_table.clone(),
                                m.participant.row_key.clone(),
                            );
                            if let Some(target_snap) = map_1.get(&target)
                                && !target_snap.intents.iter().any(|(t, _)| t == tid)
                                && m.new_version
                                    .is_some_and(|nv| target_snap.version.unwrap_or(0) < nv)
                            {
                                cross_skew = true;
                                break;
                            }
                        }
                    }
                    if cross_skew {
                        break;
                    }
                }
                if cross_skew {
                    let jitter_ms = 1 + rand::random_range(0..4u64) + (attempt as u64).min(8);
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                    continue;
                }

                // Pass 2a: re-read rows to verify `[t1_end..t3_start]` quiescence.
                let mut futs_2 = Vec::with_capacity(table_raw_keys.len());
                for (dt, raw_keys) in &table_raw_keys {
                    let client = self.client.clone();
                    let timeout = self.intent_timeout_secs;
                    let dt_clone = dt.clone();
                    let rk_clone = raw_keys.clone();
                    futs_2.push(async move {
                        let snaps = ItemOps::new(&client, &dt_clone, timeout)
                            .batch_get_row_snapshots_by_raw_keys(&rk_clone)
                            .await?;
                        Ok::<_, StorageError>((dt_clone, rk_clone, snaps))
                    });
                }
                let groups_2 = futures::future::try_join_all(futs_2).await?;

                let mut rows_match = true;
                for (dt, raw_keys, snaps) in groups_2 {
                    for (rk, snap_2) in raw_keys.into_iter().zip(snaps) {
                        if map_1.get(&(dt.clone(), rk)) != Some(&snap_2) {
                            rows_match = false;
                            break;
                        }
                    }
                    if !rows_match {
                        break;
                    }
                }
                if !rows_match {
                    let jitter_ms = 1 + rand::random_range(0..4u64) + (attempt as u64).min(8);
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                    continue;
                }

                // Pass 2b: re-read coordinator states to verify none transitioned
                // across `[t2_end..t3_start]`.
                let mut txns_match = true;
                for tid in all_tids.keys() {
                    let st_2 = coord.get_state(tid).await?;
                    let sig_1 = txns_1
                        .get(tid)
                        .and_then(Option::as_ref)
                        .map(|s| (s.state.as_str(), s.started_at));
                    let sig_2 = st_2.as_ref().map(|s| (s.state.as_str(), s.started_at));
                    if sig_1 != sig_2 {
                        txns_match = false;
                        break;
                    }
                }
                if !txns_match {
                    let jitter_ms = 1 + rand::random_range(0..4u64) + (attempt as u64).min(8);
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                    continue;
                }

                // Double-read snapshot validated across Pass 1 and Pass 2. Resolve each
                // requested item from `(map_1, txns_1)`:
                // - Intent present + txn COMMITTED (and not superseded by a
                //   strictly newer `m:v`) -> serve logged post-image from `t:mutations`.
                // - Otherwise -> serve current `d:*` value.
                let mut out: Vec<Option<Item>> = vec![None; total_ops];
                for (idx, data_table, enc_key) in &resolved_ops {
                    let Some(row_snap) = map_1.get(&(data_table.clone(), enc_key.clone())) else {
                        continue;
                    };

                    let mut resolved_item: Option<Option<Item>> = None;
                    for (tid, _) in &row_snap.intents {
                        let Some(Some(txn_state)) = txns_1.get(tid) else {
                            continue;
                        };
                        if txn_state.state != "COMMITTED" {
                            continue;
                        }
                        if let Some(mutations) = &txn_state.mutations {
                            for m in mutations {
                                if m.participant.data_table == *data_table
                                    && m.participant.row_key == *enc_key
                                {
                                    resolved_item = Some(match &m.payload {
                                        crate::transact::TxnOpPayload::Put { item } => {
                                            Some(item.clone())
                                        }
                                        crate::transact::TxnOpPayload::Delete => None,
                                    });
                                    break;
                                }
                            }
                        }
                        if resolved_item.is_some() {
                            break;
                        }
                    }

                    out[*idx] = resolved_item.unwrap_or_else(|| row_snap.item.clone());
                }

                return Ok(out);
            }

            let reasons = (0..total_ops)
                .map(|_| {
                    crate::transact::cancellation_reason(
                        "TransactionConflict",
                        Some("Transaction is ongoing for the item"),
                        None,
                    )
                })
                .collect();
            Err(StorageError::TransactionCanceled(reasons))
        })
    }

    fn transact_write_items(
        &self,
        ops: &[TransactWriteOp<'_>],
        token: Option<extenddb_storage::IdempotencyKey<'_>>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let token = token.map(|k| {
            (
                k.account_id.to_owned(),
                k.token.to_owned(),
                k.fingerprint.to_owned(),
            )
        });
        let owned: Vec<OwnedTxnOp> = ops.iter().map(OwnedTxnOp::from).collect();
        Box::pin(async move {
            if let Some((acct, tok, fp)) = &token {
                self.check_idempotency_record(acct, tok, fp).await?;
            }

            let mut resolved: Vec<(
                OwnedTxnOp,
                String,
                TableDescription,
                Option<String>,
                crate::transact::ParticipantRow,
            )> = Vec::with_capacity(owned.len());
            for op in owned {
                let (data_table, desc, ttl_attr) = self.table_full_meta_for(&op.key_info).await?;
                match &op.kind {
                    OwnedTxnKind::Put { item } => {
                        Self::validate_put_index_keys(&desc, item)?;
                    }
                    OwnedTxnKind::Update { actions, .. } => {
                        Self::validate_update_index_literals(&desc, actions, &op.maps)?;
                    }
                    OwnedTxnKind::Delete { .. } | OwnedTxnKind::ConditionCheck { .. } => {}
                }
                let row_key = crate::data::encoding::row_key::encode_key(
                    op.lookup_key(),
                    &op.key_info.key_schema,
                )?;
                let participant = crate::transact::ParticipantRow {
                    data_table: data_table.clone(),
                    row_key,
                };
                resolved.push((op, data_table, desc, ttl_attr, participant));
            }

            let mut seen_items = HashSet::new();
            for (_, _, _, _, p) in &resolved {
                if !seen_items.insert((p.data_table.clone(), p.row_key.clone())) {
                    return Err(StorageError::Validation(
                        "Transaction request cannot include multiple operations on one item"
                            .to_string(),
                    ));
                }
            }

            let intent_max_age = Duration::from_secs(self.intent_timeout_secs.max(1));
            let txn_id = if let Some((acct, tok, _)) = &token {
                derive_txn_id(acct, tok)
            } else {
                crate::transact::TxnCoordinator::new_txn_id()
            };
            let token_meta = token
                .as_ref()
                .map(|(acct, tok, fp)| crate::transact::TxnTokenMeta {
                    account_id: acct.clone(),
                    token: tok.clone(),
                    fingerprint: fp.clone(),
                });
            let coord = crate::transact::TxnCoordinator::new(&self.client, intent_max_age);
            let participants: Vec<crate::transact::ParticipantRow> =
                resolved.iter().map(|(_, _, _, _, p)| p.clone()).collect();

            // Atomically open coordinator row.
            let mut started_at_micros: i64 = 0;
            let mut leader_opened = false;
            for _open_round in 0..4usize {
                if let Some((acct, tok, fp)) = &token {
                    self.check_idempotency_record(acct, tok, fp).await?;
                }
                let (created, opened_ts) = coord
                    .open_with_token(&txn_id, &participants, token_meta.as_ref())
                    .await?;
                if created {
                    // Re-check `idem:` AFTER opening `txn:<id>`: if a predecessor
                    // finished `persist_idempotency_if_unexpired` + `drop_matching_start`
                    // right between our pre-open check and `open_with_token`, roll back
                    // this empty coordinator row and return `IdempotentReplay`/`IdempotentMismatch`.
                    if let Some((acct, tok, fp)) = &token
                        && let Err(idem_err) = self.check_idempotency_record(acct, tok, fp).await
                    {
                        let _ = coord.try_abort_pending(&txn_id, opened_ts, opened_ts).await;
                        let _ = coord.drop_matching_start(&txn_id, opened_ts).await;
                        return Err(idem_err);
                    }
                    started_at_micros = opened_ts;
                    leader_opened = true;
                    break;
                }

                let Some((acct, tok, fp)) = &token else {
                    return Err(Self::transaction_conflict_canceled(
                        resolved.len(),
                        0,
                        "coordinator row collision",
                    ));
                };

                let wait_deadline = std::time::Instant::now()
                    + Duration::from_secs(self.intent_timeout_secs.max(2) + 2);
                let mut delay = Duration::from_millis(15);

                loop {
                    self.check_idempotency_record(acct, tok, fp).await?;

                    let Some(ts) = coord.get_state(&txn_id).await? else {
                        self.check_idempotency_record(acct, tok, fp).await?;
                        break;
                    };

                    if let Some(tm) = &ts.token_meta
                        && tm.fingerprint != *fp
                    {
                        return Err(StorageError::IdempotentMismatch);
                    }

                    match ts.state.as_str() {
                        "COMMITTED" => {
                            self.roll_forward(&txn_id, &ts).await?;
                            if ts.token_meta.is_none() {
                                let after_ts = ts.state_ts_micros.max(ts.started_at.unwrap_or(0));
                                self.persist_idempotency_if_unexpired(
                                    acct, tok, fp, &txn_id, after_ts,
                                )
                                .await?;
                            }
                            return Err(StorageError::IdempotentReplay);
                        }
                        "CLEANED" => {
                            let after_ts = ts.state_ts_micros.max(ts.started_at.unwrap_or(0));
                            if let Some(tm) = &ts.token_meta {
                                self.persist_idempotency_if_unexpired(
                                    &tm.account_id,
                                    &tm.token,
                                    &tm.fingerprint,
                                    &txn_id,
                                    after_ts,
                                )
                                .await?;
                            } else {
                                self.persist_idempotency_if_unexpired(
                                    acct, tok, fp, &txn_id, after_ts,
                                )
                                .await?;
                            }
                            if let Some(s) = ts.started_at {
                                let _ = coord.drop_matching_start(&txn_id, s).await;
                            } else {
                                let _ = coord.drop(&txn_id).await;
                            }
                            return Err(StorageError::IdempotentReplay);
                        }
                        "ABORTED" => {
                            let mut all_cleared = true;
                            if let Some(parts) = &ts.participants {
                                for p in parts {
                                    if coord.clear_intent(&txn_id, p).await.is_err() {
                                        all_cleared = false;
                                    }
                                }
                            }
                            if all_cleared {
                                if let Some(s) = ts.started_at {
                                    let _ = coord.drop_matching_start(&txn_id, s).await;
                                } else {
                                    let _ = coord.drop(&txn_id).await;
                                }
                            }
                            break;
                        }
                        "PENDING" => {
                            let timeout_micros =
                                (self.intent_timeout_secs.saturating_mul(1_000_000)) as i64;
                            let cutoff = now_millis_micros().saturating_sub(timeout_micros);
                            if let Some(start) = ts.started_at
                                && start < cutoff
                            {
                                if coord
                                    .try_abort_pending(
                                        &txn_id,
                                        start,
                                        start.max(ts.state_ts_micros),
                                    )
                                    .await?
                                {
                                    let mut all_cleared = true;
                                    if let Some(parts) = &ts.participants {
                                        for p in parts {
                                            if coord.clear_intent(&txn_id, p).await.is_err() {
                                                all_cleared = false;
                                            }
                                        }
                                    }
                                    if all_cleared {
                                        let _ = coord.drop_matching_start(&txn_id, start).await;
                                    }
                                    break;
                                }
                                continue;
                            }

                            if std::time::Instant::now() >= wait_deadline {
                                return Err(Self::transaction_conflict_canceled(
                                    resolved.len(),
                                    0,
                                    "concurrent transaction with same token is pending",
                                ));
                            }
                            let jitter = rand::random_range(0..delay.as_millis() as u64 + 1);
                            tokio::time::sleep(delay + Duration::from_millis(jitter)).await;
                            delay = (delay * 2).min(Duration::from_millis(80));
                        }
                        _ => {
                            return Err(Self::transaction_conflict_canceled(
                                resolved.len(),
                                0,
                                "concurrent transaction conflict",
                            ));
                        }
                    }
                }
            }

            if !leader_opened {
                return Err(Self::transaction_conflict_canceled(
                    resolved.len(),
                    0,
                    "concurrent transaction with same token is pending",
                ));
            }

            // Acquire participant intents in canonical sorted `(data_table, row_key)`
            // order so `[k1, k2]` vs `[k2, k1]` never deadlocks.
            let mut lock_order: Vec<usize> = (0..resolved.len()).collect();
            lock_order.sort_by(|&a, &b| resolved[a].4.cmp(&resolved[b].4));

            let mut intents_placed: Vec<usize> = Vec::with_capacity(resolved.len());
            let mut txn_attempts = 0usize;
            let max_txn_attempts = 8usize;
            let mut txn_delay = Duration::from_millis(15);

            'outer: loop {
                intents_placed.clear();
                let mut conflict_orig_idx: Option<usize> = None;

                for &orig_idx in &lock_order {
                    let participant = &resolved[orig_idx].4;
                    let mut placed_this = false;
                    for _resolve_try in 0..2u8 {
                        match coord.place_intent_once(&txn_id, participant).await {
                            Ok(true) => {
                                placed_this = true;
                                break;
                            }
                            Ok(false) => {
                                if self
                                    .resolve_blocking_intents(
                                        &participant.data_table,
                                        &participant.row_key,
                                        Some(&txn_id),
                                    )
                                    .await?
                                    != IntentBlockState::Cleared
                                {
                                    break;
                                }
                            }
                            Err(e) => {
                                rollback_pending_txn(
                                    &coord,
                                    &txn_id,
                                    started_at_micros,
                                    &intents_placed,
                                    &participants,
                                )
                                .await;
                                return Err(e);
                            }
                        }
                    }

                    if placed_this {
                        intents_placed.push(orig_idx);
                    } else {
                        conflict_orig_idx = Some(orig_idx);
                        break;
                    }
                }

                let Some(failed_idx) = conflict_orig_idx else {
                    break 'outer;
                };

                for &idx in intents_placed.iter().rev() {
                    let _ = coord.clear_intent(&txn_id, &resolved[idx].4).await;
                }

                txn_attempts += 1;
                if txn_attempts >= max_txn_attempts {
                    rollback_pending_txn(&coord, &txn_id, started_at_micros, &[], &participants)
                        .await;
                    return Err(Self::transaction_conflict_canceled(
                        resolved.len(),
                        failed_idx,
                        "concurrent transaction holds an intent on this row",
                    ));
                }

                let jitter = rand::random_range(0..txn_delay.as_millis() as u64 + 1);
                tokio::time::sleep(txn_delay + Duration::from_millis(jitter)).await;
                txn_delay = (txn_delay * 2).min(Duration::from_millis(80));
            }

            let mut items_read: Vec<Option<Item>> = Vec::with_capacity(resolved.len());
            let mut read_versions: Vec<Option<u64>> = Vec::with_capacity(resolved.len());
            for (op, data_table, _, _, _) in &resolved {
                let (existing, version) =
                    match ItemOps::new(&self.client, data_table, self.intent_timeout_secs)
                        .get_with_version(&op.key_info, op.lookup_key())
                        .await
                    {
                        Ok(res) => res,
                        Err(e) => {
                            rollback_pending_txn(
                                &coord,
                                &txn_id,
                                started_at_micros,
                                &intents_placed,
                                &participants,
                            )
                            .await;
                            return Err(e);
                        }
                    };
                items_read.push(existing);
                read_versions.push(version);
            }

            let mut reasons: Vec<extenddb_core::types::CancellationReason> =
                Vec::with_capacity(resolved.len());
            let mut any_failed = false;
            for (idx, (op, _, _, _, _)) in resolved.iter().enumerate() {
                let existing = &items_read[idx];
                match check_condition(existing.as_ref(), op.condition.as_ref(), &op.maps) {
                    Ok(()) => {
                        reasons.push(crate::transact::cancellation_reason("None", None, None))
                    }
                    Err(StorageError::ConditionFailed(prior)) => {
                        any_failed = true;
                        let item_for_reason = match op.return_values_on_ccf {
                            ReturnValuesOnConditionCheckFailure::AllOld => prior,
                            ReturnValuesOnConditionCheckFailure::None => None,
                        };
                        reasons.push(crate::transact::cancellation_reason(
                            "ConditionalCheckFailed",
                            Some("The conditional request failed"),
                            item_for_reason,
                        ));
                    }
                    Err(e) => {
                        rollback_pending_txn(
                            &coord,
                            &txn_id,
                            started_at_micros,
                            &intents_placed,
                            &participants,
                        )
                        .await;
                        return Err(e);
                    }
                }
            }
            if any_failed {
                rollback_pending_txn(
                    &coord,
                    &txn_id,
                    started_at_micros,
                    &intents_placed,
                    &participants,
                )
                .await;
                return Err(StorageError::TransactionCanceled(reasons));
            }

            let mut log_records: Vec<crate::transact::TxnStreamRecord> = Vec::new();
            let mut ttl_mutations: Vec<crate::transact::TxnTtlMutation> = Vec::new();
            let mut txn_mutations = Vec::with_capacity(resolved.len());

            for (idx, (op, data_table, desc, ttl_attr, participant)) in resolved.iter().enumerate()
            {
                let existing = &items_read[idx];
                let new_item: Option<Item> = match &op.kind {
                    OwnedTxnKind::Put { item } => Some(item.clone()),
                    OwnedTxnKind::Delete { .. } => None,
                    OwnedTxnKind::Update { key, actions } => {
                        let base = existing.clone().unwrap_or_else(|| key.clone());
                        let updated = match apply_update_actions_validated(
                            &base,
                            actions,
                            &op.maps,
                            &op.key_info,
                            &desc.attribute_definitions,
                        )
                        .and_then(|u| {
                            Self::validate_updated_item_index_keys(desc, &u)?;
                            Ok(u)
                        }) {
                            Ok(u) => u,
                            Err(e) => {
                                rollback_pending_txn(
                                    &coord,
                                    &txn_id,
                                    started_at_micros,
                                    &intents_placed,
                                    &participants,
                                )
                                .await;
                                return Err(e);
                            }
                        };
                        if existing.is_none() && removes_only(actions) {
                            continue;
                        }
                        Some(updated)
                    }
                    OwnedTxnKind::ConditionCheck { .. } => {
                        continue;
                    }
                };

                let next_version = next_occ_version(read_versions[idx]);
                let payload = match &new_item {
                    Some(item) => crate::transact::TxnOpPayload::Put { item: item.clone() },
                    None => crate::transact::TxnOpPayload::Delete,
                };
                txn_mutations.push(crate::transact::ParticipantMutation {
                    participant: participant.clone(),
                    payload,
                    expected_version: read_versions[idx],
                    row_existed: existing.is_some(),
                    new_version: Some(next_version),
                });

                let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
                if let Err(e) = Self::build_gsi_participant_mutations(
                    &op.key_info,
                    data_table,
                    &gsis,
                    existing.as_ref(),
                    new_item.as_ref(),
                    next_version,
                    &mut txn_mutations,
                ) {
                    rollback_pending_txn(
                        &coord,
                        &txn_id,
                        started_at_micros,
                        &intents_placed,
                        &participants,
                    )
                    .await;
                    return Err(e);
                }

                if let Some(attr_name) = ttl_attr.as_deref() {
                    let old_exp = existing.as_ref().and_then(|p| get_ttl_expiry(p, attr_name));
                    let new_exp = new_item.as_ref().and_then(|n| get_ttl_expiry(n, attr_name));
                    if old_exp.is_some() || new_exp.is_some() {
                        ttl_mutations.push(crate::transact::TxnTtlMutation {
                            participant: participant.clone(),
                            account_id: op.key_info.account_id.clone(),
                            table_name: op.key_info.table_name.clone(),
                            old_expiry: old_exp,
                            new_expiry: new_exp,
                            new_version: next_version,
                        });
                    }
                }

                let representative = new_item
                    .as_ref()
                    .or(existing.as_ref())
                    .unwrap_or_else(|| op.lookup_key());
                match Self::active_stream_target_for_capture(
                    desc,
                    op.stream.as_ref(),
                    representative,
                ) {
                    Ok(Some((arn, shard_id))) => {
                        if let Some(record) = crate::streams::build_record(
                            desc.stream_specification.as_ref(),
                            op.stream.as_ref(),
                            &op.key_info.key_schema,
                            existing.as_ref(),
                            new_item.as_ref(),
                            crate::streams::UNASSIGNED_SEQUENCE_NUMBER,
                            &self.region,
                        ) {
                            log_records.push(crate::transact::TxnStreamRecord {
                                stream_arn: arn,
                                shard_id,
                                record,
                                participant: Some(participant.clone()),
                                dedup_key: Some(format!("{txn_id}:{idx}")),
                            });
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        rollback_pending_txn(
                            &coord,
                            &txn_id,
                            started_at_micros,
                            &intents_placed,
                            &participants,
                        )
                        .await;
                        return Err(e);
                    }
                }
            }

            let commit_ts_micros = match coord
                .try_commit_full(
                    &txn_id,
                    started_at_micros,
                    &txn_mutations,
                    Some(&log_records),
                    Some(&ttl_mutations),
                )
                .await
            {
                Ok(Some(ts)) => ts,
                Ok(None) => {
                    rollback_pending_txn(
                        &coord,
                        &txn_id,
                        started_at_micros,
                        &intents_placed,
                        &participants,
                    )
                    .await;
                    return Err(Self::transaction_conflict_canceled(
                        resolved.len(),
                        0,
                        "transaction exceeded intent timeout and was rolled back",
                    ));
                }
                Err(e) => {
                    rollback_pending_txn(
                        &coord,
                        &txn_id,
                        started_at_micros,
                        &intents_placed,
                        &participants,
                    )
                    .await;
                    return Err(e);
                }
            };

            let committed_state = crate::transact::TxnState {
                state: "COMMITTED".to_owned(),
                state_ts_micros: commit_ts_micros,
                started_at: Some(started_at_micros),
                participants: Some(participants),
                mutations: Some(txn_mutations),
                stream_records: Some(log_records),
                ttl_mutations: Some(ttl_mutations),
                token_meta,
            };
            self.roll_forward(&txn_id, &committed_state).await?;

            Ok(())
        })
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            let cutoff = time::OffsetDateTime::now_utc().unix_timestamp() - max_age_seconds;
            let rows = self
                .cat()
                .scan_prefix(keys::IDEMPOTENCY_SCAN_PREFIX)
                .await
                .map_err(map_catalog_err)?;
            let mut deleted = 0u64;
            for (k, val) in rows {
                if val
                    .get("applied_at")
                    .and_then(|v| v.as_i64())
                    .is_some_and(|ts| ts < cutoff)
                {
                    let removed = self
                        .cat()
                        .mutate_json_cas(&k, map_catalog_err, move |cur| {
                            if cur
                                .as_ref()
                                .and_then(|c| c.get("applied_at"))
                                .and_then(|v| v.as_i64())
                                .is_some_and(|ts| ts < cutoff)
                            {
                                Ok((CatalogJsonAction::Delete, true))
                            } else {
                                Ok((CatalogJsonAction::NoOp, false))
                            }
                        })
                        .await
                        .unwrap_or(false);
                    if removed {
                        deleted += 1;
                    }
                }
            }
            Ok(deleted)
        })
    }
}

// =========== MetadataEngine ===========

impl MetadataEngine for BigtableEngine {
    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::table_meta(&account_id, &table_name))
                .await
                .map_err(map_catalog_err)?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            let ttl = row.get("ttl").cloned().unwrap_or(serde_json::Value::Null);
            let enabled = ttl
                .get("enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let attribute = ttl
                .get("attribute")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            Ok(TimeToLiveDescription {
                time_to_live_status: if enabled {
                    TimeToLiveStatus::Enabled
                } else {
                    TimeToLiveStatus::Disabled
                },
                attribute_name: attribute,
            })
        })
    }

    fn update_ttl(
        &self,
        account_id: &str,
        table_name: &str,
        attribute_name: &str,
        enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let attribute_name = attribute_name.to_owned();
        Box::pin(async move {
            let meta_key = keys::table_meta(&account_id, &table_name);
            let tbl_for_err = table_name.clone();
            let attr_for_cas = attribute_name.clone();
            self.cat()
                .mutate_json_cas(&meta_key, map_catalog_err, move |existing| {
                    let mut row =
                        existing.ok_or_else(|| StorageError::TableNotFound(tbl_for_err.clone()))?;
                    if let Some(obj) = row.as_object_mut() {
                        obj.insert(
                            "ttl".to_string(),
                            json!({"enabled": enabled, "attribute": attr_for_cas}),
                        );
                    }
                    Ok((CatalogJsonAction::Put(row), ()))
                })
                .await?;
            self.metadata_cache
                .invalidate(&(account_id, table_name))
                .await;
            Ok(())
        })
    }

    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_owned();
        let tags = tags.to_vec();
        Box::pin(async move {
            let key = keys::tags(&arn);
            self.cat()
                .mutate_json_cas(&key, map_catalog_err, move |existing| {
                    let cur = existing.unwrap_or_else(|| json!({}));
                    let mut map: serde_json::Map<String, serde_json::Value> =
                        serde_json::from_value(cur).unwrap_or_default();
                    for t in &tags {
                        map.insert(t.key.clone(), serde_json::Value::String(t.value.clone()));
                    }
                    Ok((CatalogJsonAction::Put(serde_json::Value::Object(map)), ()))
                })
                .await
        })
    }

    fn untag_resource(
        &self,
        arn: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_owned();
        let tag_keys = tag_keys.to_vec();
        Box::pin(async move {
            let key = keys::tags(&arn);
            self.cat()
                .mutate_json_cas(&key, map_catalog_err, move |existing| {
                    let Some(cur) = existing else {
                        return Ok((CatalogJsonAction::NoOp, ()));
                    };
                    let mut map: serde_json::Map<String, serde_json::Value> =
                        serde_json::from_value(cur).unwrap_or_default();
                    for k in &tag_keys {
                        map.remove(k);
                    }
                    if map.is_empty() {
                        Ok((CatalogJsonAction::Delete, ()))
                    } else {
                        Ok((CatalogJsonAction::Put(serde_json::Value::Object(map)), ()))
                    }
                })
                .await
        })
    }

    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        let arn = arn.to_owned();
        Box::pin(async move {
            let key = keys::tags(&arn);
            let Some(cur) = self.cat().get(&key).await.map_err(map_catalog_err)? else {
                return Ok(Vec::new());
            };
            let map: serde_json::Map<String, serde_json::Value> =
                serde_json::from_value(cur).unwrap_or_default();
            Ok(map
                .into_iter()
                .filter_map(|(k, v)| match v {
                    serde_json::Value::String(s) => Some(Tag { key: k, value: s }),
                    _ => None,
                })
                .collect())
        })
    }

    fn tables_with_ttl(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(&keys::table_meta_scan_prefix(&account_id))
                .await
                .map_err(map_catalog_err)?;
            let mut out = Vec::new();
            for (key, value) in rows {
                let ttl = value.get("ttl").cloned().unwrap_or(serde_json::Value::Null);
                let enabled = ttl
                    .get("enabled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let attr = ttl.get("attribute").and_then(|v| v.as_str());
                if !enabled {
                    continue;
                }
                let Some(attr) = attr else { continue };
                let Some(table_name) = key.rsplit_once(':').map(|(_, n)| n.to_owned()) else {
                    continue;
                };
                out.push((table_name, attr.to_string()));
            }
            Ok(out)
        })
    }

    fn all_tables_with_ttl(&self) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(keys::TABLE_META_SCAN_PREFIX)
                .await
                .map_err(map_catalog_err)?;
            let mut out = Vec::new();
            for (key, value) in rows {
                let mut parts = key.splitn(3, ':');
                let _ = parts.next();
                let (Some(acct_id), Some(table_name)) = (parts.next(), parts.next()) else {
                    continue;
                };
                let ttl = value.get("ttl").cloned().unwrap_or(serde_json::Value::Null);
                let enabled = ttl
                    .get("enabled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let Some(attr) = ttl.get("attribute").and_then(|v| v.as_str()) else {
                    continue;
                };
                if enabled {
                    out.push((
                        acct_id.to_string(),
                        table_name.to_string(),
                        attr.to_string(),
                    ));
                }
            }
            Ok(out)
        })
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        self.all_tables_with_ttl()
    }

    fn create_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async { Ok(()) })
    }

    fn drop_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async { Ok(()) })
    }

    fn find_expired_items_indexed(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn refresh_table_size(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async { Ok(()) })
    }

    fn list_active_table_names(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(&keys::table_meta_scan_prefix(&account_id))
                .await
                .map_err(map_catalog_err)?;
            Ok(rows
                .into_iter()
                .filter_map(|(k, _)| k.rsplit_once(':').map(|(_, n)| n.to_owned()))
                .collect())
        })
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async move {
            let rows = self
                .cat()
                .scan_prefix(keys::TABLE_META_SCAN_PREFIX)
                .await
                .map_err(map_catalog_err)?;
            let mut out = Vec::new();
            for (key, _) in rows {
                let mut parts = key.splitn(3, ':');
                let _ = parts.next();
                if let (Some(acct_id), Some(table_name)) = (parts.next(), parts.next()) {
                    out.push((acct_id.to_string(), table_name.to_string()));
                }
            }
            Ok(out)
        })
    }
}

// =========== StreamEngine ===========

impl StreamEngine for BigtableEngine {
    fn write_stream_record(
        &self,
        account_id: &str,
        record: &StreamRecord,
        shard_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let record = record.clone();
        let shard_id = shard_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let target_shard = if shard_id.starts_with("shardId-") {
                shard_id
            } else {
                let (_, desc, _) = self
                    .table_full_meta_for_raw(&account_id, &table_name)
                    .await?;
                let Some((_, sid)) =
                    Self::active_stream_target_from_desc(&desc, None, &record.dynamodb.keys)?
                else {
                    return Ok(());
                };
                sid
            };
            crate::streams::write_record(&self.cat(), &target_shard, &record)
                .await
                .map_err(map_catalog_err)
        })
    }

    fn get_stream_records(
        &self,
        account_id: &str,
        stream_arn: &str,
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        let account_id = account_id.to_owned();
        let stream_arn = stream_arn.to_owned();
        let shard_id = shard_id.to_owned();
        let after = after_sequence.map(str::to_owned);
        Box::pin(async move {
            let (table_name, arn_label) = extenddb_storage::util::parse_stream_arn(&stream_arn)?;
            if extenddb_core::arn::parse(&stream_arn).is_none_or(|r| r.account != account_id) {
                return Err(extenddb_storage::util::stream_does_not_exist());
            }
            let Some(table_row) = self
                .cat()
                .get(&keys::table_meta(&account_id, &table_name))
                .await
                .map_err(map_catalog_err)?
            else {
                return Err(extenddb_storage::util::stream_does_not_exist());
            };
            let desc = deserialize_table_description_row(&table_row)?;

            let Some(shard_val) = self
                .cat()
                .get(&keys::stream_shard(&shard_id))
                .await
                .map_err(map_catalog_err)?
            else {
                return Err(extenddb_storage::util::stream_does_not_exist());
            };
            let shard_meta: crate::streams::ShardMeta = serde_json::from_value(shard_val)
                .map_err(|e| StorageError::Internal(format!("decode shard meta: {e}")))?;
            if shard_meta.table_id != desc.table_id {
                return Err(extenddb_storage::util::stream_does_not_exist());
            }

            let belongs = extenddb_storage::util::shard_in_stream(
                &desc.table_id,
                &shard_id,
                desc.latest_stream_label.as_deref(),
                &arn_label,
                || async {
                    let all = crate::streams::load_table_shards(&self.cat(), &desc.table_id)
                        .await
                        .map_err(map_catalog_err)?;
                    let lineage: Vec<_> = all
                        .iter()
                        .map(crate::streams::ShardMeta::to_lineage_row)
                        .collect();
                    Ok::<_, StorageError>(extenddb_storage::stream_lineage::has_labelled_shard(
                        &desc.table_id,
                        &lineage,
                    ))
                },
            )
            .await?;
            if !belongs {
                return Err(extenddb_storage::util::stream_does_not_exist());
            }

            let rows = self
                .cat()
                .scan_prefix(&keys::stream_record_shard_prefix(&shard_id))
                .await
                .map_err(map_catalog_err)?;
            let mut records: Vec<StreamRecord> = Vec::with_capacity(rows.len());
            for (key, value) in rows {
                let seq = key.rsplit_once(':').map(|(_, s)| s).unwrap_or("");
                if let Some(after) = &after
                    && seq <= after.as_str()
                {
                    continue;
                }
                if let Ok(record) = serde_json::from_value::<StreamRecord>(value) {
                    records.push(record);
                }
            }
            records.sort_by(|a, b| a.dynamodb.sequence_number.cmp(&b.dynamodb.sequence_number));
            if limit > 0 && records.len() > limit as usize {
                records.truncate(limit as usize);
            }
            let next_after = records.last().map(|r| r.dynamodb.sequence_number.clone());
            Ok((records, next_after))
        })
    }

    fn describe_stream(
        &self,
        account_id: &str,
        input: &DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let arn = input.stream_arn.clone();
        let limit = input.limit;
        let exclusive_start = input.exclusive_start_shard_id.clone();
        Box::pin(async move {
            let (table_name, label) = extenddb_storage::util::parse_stream_arn(&arn)?;
            if extenddb_core::arn::parse(&arn).is_none_or(|r| r.account != account_id) {
                return Err(extenddb_storage::util::stream_not_found(&arn));
            }
            let row = self
                .cat()
                .get(&keys::table_meta(&account_id, &table_name))
                .await
                .map_err(map_catalog_err)?
                .ok_or_else(|| extenddb_storage::util::stream_not_found(&arn))?;
            let desc = deserialize_table_description_row(&row)?;
            let shards = crate::streams::load_table_shards(&self.cat(), &desc.table_id)
                .await
                .map_err(map_catalog_err)?;
            let shard_rows: Vec<_> = shards
                .iter()
                .map(crate::streams::ShardMeta::to_lineage_row)
                .collect();
            extenddb_storage::stream_lineage::describe_stream(
                arn,
                &Self::table_stream_row(&desc),
                desc.key_schema.clone(),
                label,
                &shard_rows,
                limit,
                exclusive_start.as_deref(),
            )
        })
    }

    fn list_streams(
        &self,
        account_id: &str,
        table_name: Option<&str>,
        limit: i64,
        exclusive_start_stream_arn: Option<&str>,
    ) -> BoxFuture<'_, StreamListResult> {
        let account_id = account_id.to_owned();
        let table_name = table_name.map(str::to_owned);
        let esk = exclusive_start_stream_arn.map(str::to_owned);
        Box::pin(async move {
            let exclusive_start = esk
                .as_deref()
                .map(extenddb_storage::util::parse_stream_arn)
                .transpose()?;
            let rows = self
                .cat()
                .scan_prefix(&keys::table_meta_scan_prefix(&account_id))
                .await
                .map_err(map_catalog_err)?;
            let mut tables = Vec::with_capacity(rows.len());
            let mut all_shards = Vec::new();
            for (_key, value) in rows {
                let Ok(desc) = deserialize_table_description_row(&value) else {
                    continue;
                };
                if let Some(want) = &table_name
                    && desc.table_name != *want
                {
                    continue;
                }
                let shards = crate::streams::load_table_shards(&self.cat(), &desc.table_id)
                    .await
                    .map_err(map_catalog_err)?;
                all_shards.extend(shards.iter().map(crate::streams::ShardMeta::to_lineage_row));
                tables.push(Self::table_stream_row(&desc));
            }
            extenddb_storage::stream_lineage::list_streams(
                &tables,
                &all_shards,
                table_name.as_deref(),
                limit,
                exclusive_start,
                |t, l| crate::streams::build_stream_arn(&self.region, &account_id, t, l),
            )
        })
    }

    fn cleanup_expired_stream_records(
        &self,
        retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            let cutoff = time::OffsetDateTime::now_utc().unix_timestamp() - retention_hours * 3_600;
            let rows = self
                .cat()
                .scan_prefix("stream_record:")
                .await
                .map_err(map_catalog_err)?;
            let mut deleted = 0u64;
            for (k, value) in rows {
                if let Ok(rec) = serde_json::from_value::<StreamRecord>(value)
                    && rec.dynamodb.approximate_creation_date_time < cutoff
                {
                    let _ = crate::streams::ensure_shard_past_record(&self.cat(), &k).await;
                    if self.cat().delete(&k).await.is_ok() {
                        deleted += 1;
                    }
                }
            }
            if let Ok(dedups) = self.cat().scan_prefix(keys::STREAM_DEDUP_SCAN_PREFIX).await {
                for (dk, dval) in dedups {
                    if dval
                        .get("created_at")
                        .and_then(|v| v.as_i64())
                        .is_some_and(|ts| ts < cutoff)
                    {
                        let _ = self
                            .cat()
                            .mutate_json_cas(&dk, map_catalog_err, move |cur| {
                                if cur
                                    .as_ref()
                                    .and_then(|c| c.get("created_at"))
                                    .and_then(|v| v.as_i64())
                                    .is_some_and(|ts| ts < cutoff)
                                {
                                    Ok((CatalogJsonAction::Delete, ()))
                                } else {
                                    Ok((CatalogJsonAction::NoOp, ()))
                                }
                            })
                            .await;
                    }
                }
            }
            Ok(deleted)
        })
    }

    fn assign_shard(
        &self,
        account_id: &str,
        table_name: &str,
        partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let partition_key = partition_key.to_owned();
        Box::pin(async move {
            let (_, desc, _) = self
                .table_full_meta_for_raw(&account_id, &table_name)
                .await
                .map_err(|e| match e {
                    StorageError::TableNotFound(_) => {
                        extenddb_storage::util::stream_does_not_exist()
                    }
                    other => other,
                })?;
            let shards = crate::streams::load_table_shards(&self.cat(), &desc.table_id)
                .await
                .map_err(map_catalog_err)?;
            let shard_rows: Vec<_> = shards
                .iter()
                .map(crate::streams::ShardMeta::to_lineage_row)
                .collect();
            let open = extenddb_storage::stream_lineage::open_shard_ids(&shard_rows);
            if open.is_empty() {
                return Err(extenddb_storage::util::stream_does_not_exist());
            }
            let idx = (crc32fast::hash(partition_key.as_bytes()) as usize) % open.len();
            Ok(open[idx].clone())
        })
    }

    fn next_sequence_number(&self, _shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        Box::pin(async move { Ok(crate::streams::next_sequence_number()) })
    }

    fn sequence_number_width(&self) -> usize {
        crate::streams::SEQUENCE_NUMBER_WIDTH
    }

    fn validate_shard(
        &self,
        account_id: &str,
        stream_arn: &str,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let arn = stream_arn.to_owned();
        let shard = shard_id.to_owned();
        Box::pin(async move {
            let (table_name, label) = extenddb_storage::util::parse_stream_arn(&arn)?;
            if extenddb_core::arn::parse(&arn).is_none_or(|r| r.account != account_id) {
                return Err(extenddb_storage::util::stream_does_not_exist());
            }
            let (table_row, shard_rows) = match self
                .cat()
                .get(&keys::table_meta(&account_id, &table_name))
                .await
                .map_err(map_catalog_err)?
            {
                Some(row) => {
                    let desc = deserialize_table_description_row(&row)?;
                    let shards = crate::streams::load_table_shards(&self.cat(), &desc.table_id)
                        .await
                        .map_err(map_catalog_err)?;
                    let rows: Vec<_> = shards
                        .iter()
                        .map(crate::streams::ShardMeta::to_lineage_row)
                        .collect();
                    (Some(Self::table_stream_row(&desc)), rows)
                }
                None => (None, Vec::new()),
            };
            extenddb_storage::stream_lineage::validate_shard(
                table_row.as_ref(),
                &label,
                &shard_rows,
                &shard,
            )
        })
    }

    fn latest_sequence_number(
        &self,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        let shard = shard_id.to_owned();
        Box::pin(async move {
            crate::streams::latest_sequence_for_shard(&self.cat(), &shard)
                .await
                .map_err(map_catalog_err)
        })
    }
}

// =========== WorkerStore ===========

impl WorkerStore for BigtableEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

// =========== TTL Helpers ===========

async fn apply_ttl_mutation_versioned(
    client: &BigtableClient,
    account_id: &str,
    table_name: &str,
    base_row_key: &[u8],
    old_expiry: Option<i64>,
    new_expiry: Option<i64>,
    version: u64,
) -> Result<(), StorageError> {
    let ts_micros = version_to_timestamp_micros(version);
    if old_expiry != new_expiry
        && let Some(old_exp) = old_expiry
    {
        delete_ttl_index_entry_before(
            client,
            account_id,
            table_name,
            base_row_key,
            old_exp,
            ts_micros,
        )
        .await?;
    }
    if let Some(new_exp) = new_expiry {
        insert_ttl_index_entry_at(
            client,
            account_id,
            table_name,
            base_row_key,
            new_exp,
            ts_micros,
        )
        .await?;
    }
    Ok(())
}

async fn insert_ttl_index_entry_at(
    client: &BigtableClient,
    account_id: &str,
    table_name: &str,
    base_row_key: &[u8],
    expiry: i64,
    ts_micros: i64,
) -> Result<(), StorageError> {
    debug_assert!(
        ts_micros > 0 && ts_micros % 1_000 == 0,
        "TTL index timestamp must be positive and millisecond-aligned, got {ts_micros}"
    );
    let mut data = client.data();
    let ttl_key = crate::data::encoding::ttl_key::encode_ttl_key(
        account_id,
        table_name,
        base_row_key,
        expiry,
    );
    let mut mutations = Vec::with_capacity(2);
    // Place SetCell FIRST and bounded DeleteFromColumn([0, ts_micros)) SECOND so
    // sequential per-row mutation application on `cbtemulator` never exposes an
    // empty TTL index cell to a concurrent `ttl_worker` scan.
    mutations.push(Mutation {
        mutation: Some(mutation::Mutation::SetCell(SetCell {
            family_name: "d".to_string(),
            column_qualifier: vec![],
            timestamp_micros: ts_micros,
            value: vec![],
        })),
    });
    if let Some(range) = older_than_ts_range(ts_micros) {
        mutations.push(Mutation {
            mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                family_name: "d".to_string(),
                column_qualifier: vec![],
                time_range: Some(range),
            })),
        });
    }

    let req = MutateRowRequest {
        table_name: client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE),
        app_profile_id: client.app_profile_id.clone().unwrap_or_default(),
        row_key: ttl_key,
        mutations,
        ..Default::default()
    };
    data.mutate_row(req)
        .await
        .map_err(|e| map_bigtable_err("insert TTL index", e))?;
    Ok(())
}

async fn delete_ttl_index_entry_before(
    client: &BigtableClient,
    account_id: &str,
    table_name: &str,
    base_row_key: &[u8],
    expiry: i64,
    exclusive_end_ts_micros: i64,
) -> Result<(), StorageError> {
    debug_assert!(
        exclusive_end_ts_micros > 0 && exclusive_end_ts_micros % 1_000 == 0,
        "TTL index delete bound must be positive and millisecond-aligned, got {exclusive_end_ts_micros}"
    );
    let mut data = client.data();
    let ttl_key = crate::data::encoding::ttl_key::encode_ttl_key(
        account_id,
        table_name,
        base_row_key,
        expiry,
    );
    let req = MutateRowRequest {
        table_name: client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE),
        app_profile_id: client.app_profile_id.clone().unwrap_or_default(),
        row_key: ttl_key,
        mutations: vec![Mutation {
            mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                family_name: "d".to_string(),
                column_qualifier: vec![],
                time_range: older_than_ts_range(exclusive_end_ts_micros),
            })),
        }],
        ..Default::default()
    };
    data.mutate_row(req)
        .await
        .map_err(|e| map_bigtable_err("delete TTL index", e))?;
    Ok(())
}

fn get_ttl_expiry(item: &Item, attr_name: &str) -> Option<i64> {
    match item.get(attr_name) {
        Some(AttributeValue::N(s)) => parse_ttl_epoch_secs(s),
        _ => None,
    }
}
