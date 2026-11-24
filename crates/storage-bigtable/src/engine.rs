// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BigtableEngine`: implements the `StorageEngine` sub-traits (`TableEngine`,
//! `DataEngine`, `MetadataEngine`, `StreamEngine`, `WorkerStore`, plus backup
//! refusal stubs in `crate::backup_stubs`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, PoisonError, Weak};
use std::time::{Duration, Instant};

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction, removes_only};
use extenddb_core::types::{
    AttributeValue, BillingMode, BillingModeSummary, CancellationReason, CreateTableInput,
    DeleteTableInput, DescribeStreamInput, DescribeTableInput, GsiDescription, IndexInfo,
    IndexType, Item, KeySchemaElement, ListTablesInput, ListTablesOutput, LsiDescription,
    ONLINE_INDEX_LIMIT, Projection, ProvisionedThroughputDescription,
    ReturnValuesOnConditionCheckFailure, SseDescription, StreamDescription, StreamRecord,
    StreamViewType, TableDescription, TableKeyInfo, TableStatus, Tag, TimeToLiveDescription,
    TimeToLiveStatus, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::hooks::{CancellationToken, sleep_or_shutdown};
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
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Filter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet, mutation,
    mutation::{DeleteFromColumn, SetCell},
};
use moka::future::Cache;
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::catalog::{Catalog, CatalogJsonAction, keys};
use crate::data::admin::AdminClient;
use crate::data::client::{BigtableClient, map_bigtable_err};
use crate::data::encoding::ttl_key::{
    NUM_SHARDS, TOMBSTONE_GC_QUALIFIER, TTL_INDEX_TABLE, decode_ttl_key, encode_tombstone_gc_key,
    encode_tombstone_gc_value, encode_ttl_key,
};
use crate::data::item_ops::{
    CasOutcome, DIRECT_PUT_STAGE_PREFIX, ItemOps, RowSnapshot, apply_update_actions_validated,
    check_condition, max_mutations_per_rpc, next_occ_version, now_millis, now_millis_micros,
    older_than_ts_range, parse_ttl_epoch_secs, version_to_timestamp_micros,
};
use crate::data::query_scan::{QueryScan, key_belongs_to_segment};
use crate::gsi_worker::gsi_build_lock_name;

const DEFAULT_REGION: &str = "us-east-1";
const MAX_OCC_ATTEMPTS: usize = 128;
const IDEMPOTENCY_WINDOW_SECS: i64 = 600;
const MAX_TRANSACT_GET_ATTEMPTS: usize = 32;
const CATALOG_GC_PAGE_SIZE: i64 = 500;
const TTL_BACKFILL_SCAN_BATCH: i64 = 200;
const TTL_BACKFILL_LEASE: Duration = Duration::from_secs(60);
const GSI_BUILD_LEASE: Duration = Duration::from_secs(60);

/// Metadata cache TTL kept tight (`1 s`) so online GSI builds and TTL `ENABLING`
/// backfills only need a `1.35 s` visibility fence across nodes.
const METADATA_CACHE_TTL: Duration = Duration::from_secs(1);

/// Writers whose elapsed duration since reading `table_meta` exceeds
/// `FAST_WRITE_BUDGET` re-check uncached `table_meta` after committing and
/// self-repair any newly `CREATING`/`ACTIVE` GSI or `ENABLING`/`ENABLED` TTL.
const FAST_WRITE_BUDGET: Duration = Duration::from_millis(250);

/// Fence duration (`METADATA_CACHE_TTL + FAST_WRITE_BUDGET + 100 ms`) waited
/// after publishing `CREATING` (GSI) or `ENABLING` (TTL) before scanning base rows.
const SCHEMA_VISIBILITY_FENCE: Duration = Duration::from_millis(1_350);

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
        view: StreamViewType,
        label_to_close_prior: Option<String>,
        used_precreated_shards: bool,
    },
}

struct BuildingGsiGuard<'a> {
    set: &'a std::sync::Mutex<HashSet<(String, String)>>,
    key: (String, String),
}

impl Drop for BuildingGsiGuard<'_> {
    fn drop(&mut self) {
        let mut guard = self.set.lock().unwrap_or_else(PoisonError::into_inner);
        guard.remove(&self.key);
    }
}

/// Pre-validated shadow index mutation computed BEFORE base-row commit so a
/// shadow key validation error can never occur after the commit point.
#[derive(Clone)]
struct ShadowTargetPlan {
    shadow_table: String,
    old_key: Option<Vec<u8>>,
    new_entry: Option<(Vec<u8>, Item)>,
}

type SingleWriteLockKey = (String, Vec<u8>);
type SingleWriteLockMap = HashMap<SingleWriteLockKey, Weak<tokio::sync::Mutex<()>>>;

fn data_table_id(table_id: &str) -> String {
    format!("t{}", &table_id.replace('-', "")[..16])
}

#[allow(clippy::cast_precision_loss)]
fn now_unix_f64() -> f64 {
    time::OffsetDateTime::now_utc().unix_timestamp() as f64
}

fn ttl_backfill_lock_name(table_id: &str) -> String {
    format!("ttl_backfill:{table_id}")
}

fn parse_ttl_status_and_attr(
    row: &serde_json::Value,
) -> (TimeToLiveStatus, Option<String>, String) {
    let ttl = row.get("ttl").cloned().unwrap_or(serde_json::Value::Null);
    let attr = ttl
        .get("attribute")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let generation = ttl
        .get("generation")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_owned();
    let status = match ttl.get("status").and_then(serde_json::Value::as_str) {
        Some("ENABLING") => TimeToLiveStatus::Enabling,
        Some("ENABLED") => TimeToLiveStatus::Enabled,
        Some("DISABLING") => TimeToLiveStatus::Disabling,
        Some("DISABLED") => TimeToLiveStatus::Disabled,
        _ => {
            if ttl
                .get("enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                TimeToLiveStatus::Enabled
            } else {
                TimeToLiveStatus::Disabled
            }
        }
    };
    (status, attr, generation)
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

    desc.warm_throughput_record = parse_warm_throughput_record(
        row.get("warm_throughput_record")
            .and_then(serde_json::Value::as_str),
    );

    if let (Some(gsis), Some(gsi_wt_map)) = (
        desc.global_secondary_indexes.as_mut(),
        row.get("gsi_warm_throughput_records")
            .and_then(serde_json::Value::as_object),
    ) {
        for gsi in gsis.iter_mut() {
            gsi.warm_throughput_record = parse_warm_throughput_record(
                gsi_wt_map
                    .get(&gsi.index_name)
                    .and_then(serde_json::Value::as_str),
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

impl SingleWriteOp<'_> {
    fn lookup_key(&self) -> &Item {
        match self {
            SingleWriteOp::Put { item } => item,
            SingleWriteOp::Delete { key } | SingleWriteOp::Update { key, .. } => key,
        }
    }

    fn op_label(&self) -> &'static str {
        match self {
            SingleWriteOp::Put { .. } => "PutItem",
            SingleWriteOp::Delete { .. } => "DeleteItem",
            SingleWriteOp::Update { .. } => "UpdateItem",
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
    max_item_size_bytes: usize,
    metadata_cache: Cache<(String, String), (String, TableDescription, Option<String>)>,
    single_write_locks: std::sync::Mutex<SingleWriteLockMap>,
    building_gsis: std::sync::Mutex<HashSet<(String, String)>>,
    node_id: String,
    region: String,
}

impl BigtableEngine {
    #[must_use]
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

    #[must_use]
    pub fn with_region(
        catalog_client: Arc<BigtableClient>,
        data_client: Arc<BigtableClient>,
        intent_timeout_secs: u64,
        region: String,
    ) -> Self {
        Self::with_options(
            catalog_client,
            data_client,
            intent_timeout_secs,
            region,
            extenddb_core::limits::LimitsConfig::default().max_item_size_bytes,
        )
    }

    #[must_use]
    pub fn with_options(
        catalog_client: Arc<BigtableClient>,
        data_client: Arc<BigtableClient>,
        intent_timeout_secs: u64,
        region: String,
        max_item_size_bytes: usize,
    ) -> Self {
        let metadata_cache = Cache::builder().time_to_live(METADATA_CACHE_TTL).build();
        let node_id = Uuid::new_v4().to_string();
        Self {
            catalog_client,
            client: data_client,
            intent_timeout_secs,
            max_item_size_bytes,
            metadata_cache,
            single_write_locks: std::sync::Mutex::new(HashMap::new()),
            building_gsis: std::sync::Mutex::new(HashSet::new()),
            node_id,
            region,
        }
    }

    async fn control_plane_delay_secs(&self) -> f64 {
        self.cat()
            .get(&keys::setting("control_plane_delay_seconds"))
            .await
            .ok()
            .flatten()
            .and_then(|v| {
                v.get("value")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|s| s.parse::<f64>().ok())
            })
            .unwrap_or(crate::DEFAULT_CONTROL_PLANE_DELAY_SECONDS)
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
                .unwrap_or_else(PoisonError::into_inner);
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
            let mut set = self
                .building_gsis
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            set.insert(key.clone());
        }
        BuildingGsiGuard {
            set: &self.building_gsis,
            key,
        }
    }

    pub(crate) fn is_gsi_building(&self, table_id: &str, index_name: &str) -> bool {
        let set = self
            .building_gsis
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
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

    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }

    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    #[must_use]
    pub fn client_ref(&self) -> &BigtableClient {
        &self.client
    }

    #[must_use]
    pub fn catalog_client_ref(&self) -> &BigtableClient {
        &self.catalog_client
    }

    #[must_use]
    pub fn catalog_client_arc(&self) -> Arc<BigtableClient> {
        self.catalog_client.clone()
    }

    #[must_use]
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
        view_override: Option<StreamViewType>,
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
            .unwrap_or(StreamViewType::NewAndOldImages);
        let shard_idx = Self::shard_index_for_item(desc, key_or_item);
        let sid =
            extenddb_storage::stream_lineage::shard_id(&desc.table_id, label, view, shard_idx)?;
        Ok(Some((arn, sid)))
    }

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

    fn transact_op_key_checks(
        op: &OwnedTxnOp,
        desc: &TableDescription,
    ) -> Result<(), CancellationReason> {
        use extenddb_core::validation;

        let idx_refs = Self::index_key_refs(desc);
        match &op.kind {
            OwnedTxnKind::Put { item } => {
                validation::validate_item_keys(item, &desc.key_schema, &desc.attribute_definitions)
                    .map_err(|e| CancellationReason::key_type_mismatch(e.to_string()))?;
                if !idx_refs.is_empty() {
                    validation::validate_index_key_types(
                        item,
                        &idx_refs,
                        &desc.attribute_definitions,
                    )
                    .map_err(|e| CancellationReason::key_type_mismatch(e.to_string()))?;
                    validation::validate_index_key_not_empty(
                        item,
                        &idx_refs,
                        validation::SecondaryIndexEmptyContext::Item,
                    )
                    .map_err(|e| CancellationReason::index_key_empty_abort(e.to_string()))?;
                }
                Ok(())
            }
            OwnedTxnKind::Update { key, actions } => {
                validation::validate_batch_key_only(
                    key,
                    &desc.key_schema,
                    &desc.attribute_definitions,
                )
                .map_err(|e| CancellationReason::key_type_mismatch(e.to_string()))?;
                if !idx_refs.is_empty() {
                    validation::validate_index_key_literal_operands(
                        actions,
                        &idx_refs,
                        &desc.attribute_definitions,
                        &op.maps,
                    )
                    .map_err(|e| CancellationReason::key_type_mismatch(e.to_string()))?;
                }
                Ok(())
            }
            OwnedTxnKind::Delete { key } | OwnedTxnKind::ConditionCheck { key } => {
                validation::validate_batch_key_only(
                    key,
                    &desc.key_schema,
                    &desc.attribute_definitions,
                )
                .map_err(|e| CancellationReason::key_type_mismatch(e.to_string()))
            }
        }
    }

    /// Look up the Bigtable data-table short name for `(account, table_name)`.
    pub async fn data_table_for(&self, key_info: &TableKeyInfo) -> Result<String, StorageError> {
        let (dt, _, _) = self.table_full_meta_for(key_info).await?;
        Ok(dt)
    }

    async fn table_full_meta_uncached(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<(String, TableDescription, Option<String>), StorageError> {
        let row = self
            .cat()
            .get(&keys::table_meta(account_id, table_name))
            .await?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        let data_table = row
            .get("data_table")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| StorageError::Internal("missing data_table in catalog".into()))?;
        let desc = deserialize_table_description_row(&row)?;

        // Index TTL entries on writes during BOTH `ENABLING` and `ENABLED` so no
        // item written while backfill runs can be missed.
        let (ttl_status, ttl_attr_raw, _) = parse_ttl_status_and_attr(&row);
        let ttl_attr = matches!(
            ttl_status,
            TimeToLiveStatus::Enabling | TimeToLiveStatus::Enabled
        )
        .then_some(ttl_attr_raw)
        .flatten();

        Ok((data_table, desc, ttl_attr))
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

        let result = self
            .table_full_meta_uncached(account_id, table_name)
            .await?;
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

    /// Time budget for inline post-commit side effects (`< intent_timeout_secs`
    /// grace window so a stalled inline shadow/TTL write persists a `post-*`
    /// recovery record in `__extenddb_txn_log__` before tombstone GC can run).
    #[inline]
    fn inline_side_effect_budget(&self) -> Duration {
        let grace_ms = self.intent_timeout_secs.max(2).saturating_mul(1_000);
        Duration::from_millis((grace_ms.saturating_mul(2) / 5).clamp(500, 10_000))
    }

    /// Enqueue a best-effort delayed reclamation entry (`b"tomb:"`) in
    /// `__extenddb_ttl_index__` so `sweeper` deletes pure version-only tombstone
    /// rows once `intent_timeout_secs` has elapsed past `max(expected_version + 1, now_millis())`.
    pub(crate) async fn enqueue_tombstone_gc_best_effort(
        &self,
        table_short: &str,
        raw_row_key: &[u8],
        expected_version: u64,
    ) {
        let grace_ms = self.intent_timeout_secs.max(2).saturating_mul(1_000);
        let ready_ms = expected_version
            .saturating_add(1)
            .max(now_millis())
            .saturating_add(grace_ms);
        let queue_key = encode_tombstone_gc_key(ready_ms, table_short, raw_row_key);
        let queue_val = encode_tombstone_gc_value(expected_version, table_short, raw_row_key);
        let ts_micros = version_to_timestamp_micros(expected_version.max(1));

        let mut data = self.client.data();
        let req = MutateRowRequest {
            table_name: self.client.full_table_name(TTL_INDEX_TABLE),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            row_key: queue_key,
            mutations: vec![
                Mutation {
                    mutation: Some(mutation::Mutation::SetCell(SetCell {
                        family_name: "d".to_string(),
                        column_qualifier: TOMBSTONE_GC_QUALIFIER.to_vec(),
                        timestamp_micros: ts_micros,
                        value: queue_val,
                    })),
                },
                Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: "d".to_string(),
                        column_qualifier: TOMBSTONE_GC_QUALIFIER.to_vec(),
                        time_range: older_than_ts_range(ts_micros),
                    })),
                },
            ],
            ..MutateRowRequest::default()
        };
        let _ = data.mutate_row(req).await;
    }

    /// Wait out peer metadata-cache + fast-write grace, roll forward any
    /// `COMMITTED` transactions targeting `data_table`, and wait briefly for
    /// pre-fence `PENDING` transactions on `data_table` to drain before scanning
    /// base rows for online GSI build or TTL `ENABLING` backfill.
    pub(crate) async fn await_schema_visibility_fence(
        &self,
        data_table: &str,
        token: &CancellationToken,
    ) -> Result<(), StorageError> {
        if !sleep_or_shutdown(token, SCHEMA_VISIBILITY_FENCE).await {
            return Ok(());
        }
        let fence_micros = now_millis_micros();
        self.roll_forward_committed_txns_for_table(data_table)
            .await?;

        let drain_deadline =
            Instant::now() + Duration::from_secs(self.intent_timeout_secs.max(2) + 2);
        while Instant::now() < drain_deadline && !token.is_cancelled() {
            if !self
                .has_pending_txns_before(data_table, fence_micros)
                .await?
            {
                break;
            }
            if !sleep_or_shutdown(token, Duration::from_millis(50)).await {
                break;
            }
            self.roll_forward_committed_txns_for_table(data_table)
                .await?;
        }
        Ok(())
    }

    async fn has_pending_txns_before(
        &self,
        data_table: &str,
        fence_micros: i64,
    ) -> Result<bool, StorageError> {
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
        #[allow(clippy::cast_possible_wrap)]
        let timeout_micros = (self.intent_timeout_secs.saturating_mul(1_000_000)) as i64;
        let cutoff = now_millis_micros().saturating_sub(timeout_micros);

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
                .map_err(|e| map_bigtable_err("ReadRows pending txns fence", e))?;
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
                if let Some(st) = coord.get_state(tid).await?
                    && st.state == "PENDING"
                    && st.started_at.is_some_and(|sa| sa <= fence_micros)
                    && st
                        .participants
                        .as_ref()
                        .is_some_and(|ps| ps.iter().any(|p| p.data_table == data_table))
                {
                    if let Some(sa) = st.started_at
                        && sa < cutoff
                        && coord
                            .try_abort_pending(tid, sa, sa.max(st.state_ts_micros))
                            .await?
                    {
                        if let Some(parts) = &st.participants {
                            for p in parts {
                                let _ = coord.clear_intent(tid, p).await;
                            }
                        }
                        let _ = coord.drop_matching_start(tid, sa).await;
                        continue;
                    }
                    return Ok(true);
                }
            }
            if let Some(k) = last_key {
                start_key = k;
                exclude_start = true;
            } else {
                break;
            }
        }
        Ok(false)
    }

    /// Roll forward any already-`COMMITTED` coordinator entries in `__extenddb_txn_log__`
    /// touching `data_table` (or its secondary-index shadow tables).
    pub(crate) async fn roll_forward_committed_txns_for_table(
        &self,
        data_table: &str,
    ) -> Result<(), StorageError> {
        let mut data = self.client.data();
        let txn_log_table = self.client.full_table_name(crate::transact::TXN_LOG_TABLE);
        let start_prefix = b"txn:".to_vec();
        let mut end_key = start_prefix.clone();
        end_key.push(0xFF);
        let shadow_prefix = format!("{data_table}_");

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
                .map_err(|e| map_bigtable_err("ReadRows committed txns for table", e))?;
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
                if let Some(st) = coord.get_state(tid).await?
                    && st.state == "COMMITTED"
                {
                    let touches_part = st
                        .participants
                        .as_ref()
                        .is_some_and(|ps| ps.iter().any(|p| p.data_table == data_table));
                    let touches_mut = st.mutations.as_ref().is_some_and(|ms| {
                        ms.iter().any(|m| {
                            m.participant.data_table == data_table
                                || m.participant.data_table.starts_with(&shadow_prefix)
                        })
                    });
                    let touches_guard = st
                        .base_guard
                        .as_ref()
                        .is_some_and(|g| g.participant.data_table == data_table);
                    let touches_ttl = st.ttl_mutations.as_ref().is_some_and(|ts| {
                        ts.iter().any(|t| t.participant.data_table == data_table)
                    });
                    if touches_part || touches_mut || touches_guard || touches_ttl {
                        self.roll_forward(tid, &st).await?;
                    }
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
        #[allow(clippy::cast_possible_wrap)]
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
                    if tid.starts_with(DIRECT_PUT_STAGE_PREFIX) && intent_ts >= cutoff {
                        saw_single = true;
                    } else {
                        let _ = coord.clear_intent_up_to(&tid, &this_part, intent_ts).await;
                    }
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

    fn secondary_index_specs<'a>(
        data_table: &str,
        desc: &'a TableDescription,
    ) -> Vec<(String, &'a [KeySchemaElement], &'a Projection)> {
        let mut specs = Vec::new();
        if let Some(gsis) = &desc.global_secondary_indexes {
            for g in gsis {
                if g.index_status != "DELETING" {
                    specs.push((
                        crate::gsi::shadow_table_id(data_table, &g.index_name),
                        g.key_schema.as_slice(),
                        &g.projection,
                    ));
                }
            }
        }
        if let Some(lsis) = &desc.local_secondary_indexes {
            for l in lsis {
                specs.push((
                    crate::gsi::lsi_shadow_table_id(data_table, &l.index_name),
                    l.key_schema.as_slice(),
                    &l.projection,
                ));
            }
        }
        specs
    }

    /// Pre-compute and validate all GSI and LSI shadow row keys BEFORE `put_cas`
    /// / `delete_cas` / `try_commit_full` so a key validation error can never
    /// follow a durable base commit.
    fn prepare_shadow_plan(
        base_key_schema: &[KeySchemaElement],
        data_table: &str,
        desc: &TableDescription,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
    ) -> Result<Vec<ShadowTargetPlan>, StorageError> {
        let specs = Self::secondary_index_specs(data_table, desc);
        if specs.is_empty() {
            return Ok(Vec::new());
        }
        let mut plans = Vec::with_capacity(specs.len());
        for (shadow_table, idx_key_schema, projection) in specs {
            let old_key = match old_item {
                Some(o) => crate::gsi::shadow_row_key_for_item(o, idx_key_schema, base_key_schema)?,
                None => None,
            };
            let new_entry = match new_item {
                Some(n) => crate::gsi::shadow_row_key_for_item(n, idx_key_schema, base_key_schema)?
                    .map(|rk| {
                        let projected = crate::gsi::project_for_shadow(
                            n,
                            projection,
                            base_key_schema,
                            idx_key_schema,
                        );
                        (rk, projected)
                    }),
                None => None,
            };
            if old_key.is_some() || new_entry.is_some() {
                plans.push(ShadowTargetPlan {
                    shadow_table,
                    old_key,
                    new_entry,
                });
            }
        }
        Ok(plans)
    }

    fn shadow_plans_to_participant_mutations(
        plans: &[ShadowTargetPlan],
        new_version: u64,
        out: &mut Vec<crate::transact::ParticipantMutation>,
    ) {
        for plan in plans {
            let new_key_ref = plan.new_entry.as_ref().map(|(rk, _)| rk);
            if let Some(old_rk) = &plan.old_key
                && new_key_ref != Some(old_rk)
            {
                out.push(crate::transact::ParticipantMutation {
                    participant: crate::transact::ParticipantRow {
                        data_table: plan.shadow_table.clone(),
                        row_key: old_rk.clone(),
                    },
                    payload: crate::transact::TxnOpPayload::Delete,
                    expected_version: None,
                    row_existed: false,
                    new_version: Some(new_version),
                });
            }
            if let Some((new_rk, projected)) = &plan.new_entry {
                out.push(crate::transact::ParticipantMutation {
                    participant: crate::transact::ParticipantRow {
                        data_table: plan.shadow_table.clone(),
                        row_key: new_rk.clone(),
                    },
                    payload: crate::transact::TxnOpPayload::Put {
                        item: projected.clone(),
                    },
                    expected_version: None,
                    row_existed: false,
                    new_version: Some(new_version),
                });
            }
        }
    }

    /// Apply pre-computed GSI + LSI shadow updates, propagating transient/RPC
    /// errors (while treating `TableNotFound` on a concurrently dropped GSI
    /// shadow table as completed).
    async fn apply_shadow_plans(
        &self,
        plans: &[ShadowTargetPlan],
        new_version: u64,
    ) -> Result<(), StorageError> {
        if plans.is_empty() {
            return Ok(());
        }
        let mut futures = Vec::with_capacity(plans.len());
        for plan in plans {
            let client = self.client.clone();
            let timeout = self.intent_timeout_secs;
            let plan = plan.clone();
            futures.push(async move {
                let ops = ItemOps::new(&client, &plan.shadow_table, timeout);
                let new_key_ref = plan.new_entry.as_ref().map(|(rk, _)| rk);
                if let Some(old_rk) = &plan.old_key
                    && new_key_ref != Some(old_rk)
                {
                    match ops.delete_shadow_versioned(old_rk, new_version).await {
                        Ok(_) => {
                            self.enqueue_tombstone_gc_best_effort(
                                &plan.shadow_table,
                                old_rk,
                                new_version,
                            )
                            .await;
                        }
                        Err(StorageError::TableNotFound(_)) => return Ok(()),
                        Err(e) => return Err(e),
                    }
                }
                if let Some((new_rk, projected)) = &plan.new_entry {
                    match ops
                        .put_shadow_versioned(new_rk, projected, new_version)
                        .await
                    {
                        Ok(_) | Err(StorageError::TableNotFound(_)) => {}
                        Err(e) => return Err(e),
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

    /// Persist a `COMMITTED` side-effect recovery record into `__extenddb_txn_log__`
    /// if post-`put_cas`/`delete_cas` shadow or TTL mutations hit an unrecovered
    /// transient failure, so `sweeper` finishes them without returning an error
    /// to the client after the commit point.
    #[allow(clippy::too_many_arguments)]
    async fn persist_side_effect_recovery_on_failure(
        &self,
        key_info: &TableKeyInfo,
        data_table: &str,
        base_row_key: &[u8],
        shadow_plans: &[ShadowTargetPlan],
        ttl_attr: Option<&str>,
        existing: Option<&Item>,
        new_item: Option<&Item>,
        new_version: u64,
    ) {
        let mut side_muts = Vec::new();
        Self::shadow_plans_to_participant_mutations(shadow_plans, new_version, &mut side_muts);
        let mut side_ttls = Vec::new();
        if let Some(attr) = ttl_attr {
            let old_exp = existing.and_then(|p| get_ttl_expiry(p, attr));
            let new_exp = new_item.and_then(|n| get_ttl_expiry(n, attr));
            if old_exp.is_some() || new_exp.is_some() {
                side_ttls.push(crate::transact::TxnTtlMutation {
                    participant: crate::transact::ParticipantRow {
                        data_table: data_table.to_owned(),
                        row_key: base_row_key.to_vec(),
                    },
                    account_id: key_info.account_id.clone(),
                    table_name: key_info.table_name.clone(),
                    old_expiry: old_exp,
                    new_expiry: new_exp,
                    new_version,
                });
            }
        }
        if side_muts.is_empty() && side_ttls.is_empty() {
            return;
        }
        let coord = crate::transact::TxnCoordinator::new(
            &self.client,
            Duration::from_secs(self.intent_timeout_secs.max(1)),
        );
        let base_guard = crate::transact::SideEffectBaseGuard {
            participant: crate::transact::ParticipantRow {
                data_table: data_table.to_owned(),
                row_key: base_row_key.to_vec(),
            },
            base_version: new_version,
        };
        let recovery_id = format!("post-{}", Uuid::new_v4().simple());
        if let Err(e) = coord
            .write_committed_side_effects_guarded(
                &recovery_id,
                Some(&base_guard),
                &side_muts,
                &side_ttls,
            )
            .await
        {
            tracing::error!(
                table = %key_info.table_name,
                error = %e,
                "failed to enqueue post-commit side-effect recovery record"
            );
        }
    }

    /// Post-commit slow-write check: when a write took `>= FAST_WRITE_BUDGET`
    /// since reading `table_meta`, refresh uncached `table_meta` and repair any
    /// GSI (`CREATING`/`ACTIVE`) or TTL (`ENABLING`/`ENABLED`) added concurrently.
    #[allow(clippy::too_many_arguments)]
    async fn repair_slow_write_schema_drift(
        &self,
        key_info: &TableKeyInfo,
        data_table: &str,
        base_row_key: &[u8],
        initial_desc: &TableDescription,
        initial_ttl_attr: Option<&str>,
        old_item: Option<&Item>,
        committed_item: Option<&Item>,
        committed_version: u64,
    ) {
        let account_id = key_info.account_id.as_str();
        let table_name = key_info.table_name.as_str();
        self.invalidate_table_cache(account_id, table_name).await;
        let Ok((_, fresh_desc, fresh_ttl_attr)) =
            self.table_full_meta_uncached(account_id, table_name).await
        else {
            return;
        };
        self.metadata_cache
            .insert(
                (account_id.to_owned(), table_name.to_owned()),
                (
                    data_table.to_owned(),
                    fresh_desc.clone(),
                    fresh_ttl_attr.clone(),
                ),
            )
            .await;

        let initial_gsi_names: HashSet<&str> = initial_desc
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|g| g.index_name.as_str())
            .collect();
        let added_gsis: Vec<&GsiDescription> = fresh_desc
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|g| {
                g.index_status != "DELETING" && !initial_gsi_names.contains(g.index_name.as_str())
            })
            .collect();
        let ttl_changed = fresh_ttl_attr.is_some() && fresh_ttl_attr.as_deref() != initial_ttl_attr;

        if added_gsis.is_empty() && !ttl_changed {
            return;
        }

        let budget = self.inline_side_effect_budget();
        let (live_item_owned, target_ver, mut had_repair_err) = match tokio::time::timeout(
            budget,
            self.read_base_row_for_worker(data_table, base_row_key),
        )
        .await
        {
            Ok(Ok((None, None))) => {
                if old_item.is_none() {
                    return;
                }
                (None, committed_version, false)
            }
            Ok(Ok((live_item, live_ver))) => {
                if live_ver.is_some_and(|lv| lv < committed_version) {
                    (committed_item.cloned(), committed_version, false)
                } else {
                    (live_item, live_ver.unwrap_or(committed_version), false)
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    table = %table_name,
                    error = %e,
                    "slow-write schema-drift base read failed; scheduling recovery"
                );
                (committed_item.cloned(), committed_version, true)
            }
            Err(_) => {
                tracing::warn!(
                    table = %table_name,
                    "slow-write schema-drift base read timed out; scheduling recovery"
                );
                (committed_item.cloned(), committed_version, true)
            }
        };
        let target_item = live_item_owned.as_ref();

        let mut repair_plans = Vec::with_capacity(added_gsis.len());
        let mut repair_gsi_schemas = Vec::with_capacity(added_gsis.len());
        for g in added_gsis {
            let shadow_table = crate::gsi::shadow_table_id(data_table, &g.index_name);
            let old_key = old_item.and_then(|o| {
                crate::gsi::shadow_row_key_for_item(o, &g.key_schema, &fresh_desc.key_schema)
                    .ok()
                    .flatten()
            });
            let new_entry = target_item.and_then(|item| {
                crate::gsi::shadow_row_key_for_item(item, &g.key_schema, &fresh_desc.key_schema)
                    .ok()
                    .flatten()
                    .map(|rk| {
                        let projected = crate::gsi::project_for_shadow(
                            item,
                            &g.projection,
                            &fresh_desc.key_schema,
                            &g.key_schema,
                        );
                        (rk, projected)
                    })
            });
            if old_key.is_some() || new_entry.is_some() {
                repair_plans.push(ShadowTargetPlan {
                    shadow_table: shadow_table.clone(),
                    old_key,
                    new_entry,
                });
                repair_gsi_schemas.push((shadow_table, g.key_schema.clone()));
            }
        }

        let repair_ttl_attr = ttl_changed.then_some(fresh_ttl_attr.as_deref()).flatten();
        if repair_plans.is_empty() && repair_ttl_attr.is_none() {
            return;
        }

        if !had_repair_err {
            if !repair_plans.is_empty() {
                match tokio::time::timeout(
                    budget,
                    self.apply_shadow_plans(&repair_plans, target_ver),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(
                            table = %table_name,
                            error = %e,
                            "slow-write schema-drift shadow repair failed; scheduling recovery"
                        );
                        had_repair_err = true;
                    }
                    Err(_) => {
                        tracing::warn!(
                            table = %table_name,
                            "slow-write schema-drift shadow repair timed out; scheduling recovery"
                        );
                        had_repair_err = true;
                    }
                }
            }
            if repair_ttl_attr.is_some() {
                match tokio::time::timeout(
                    budget,
                    self.maintain_ttl_index(
                        key_info,
                        repair_ttl_attr,
                        old_item,
                        target_item,
                        target_ver,
                    ),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(
                            table = %table_name,
                            error = %e,
                            "slow-write schema-drift TTL repair failed; scheduling recovery"
                        );
                        had_repair_err = true;
                    }
                    Err(_) => {
                        tracing::warn!(
                            table = %table_name,
                            "slow-write schema-drift TTL repair timed out; scheduling recovery"
                        );
                        had_repair_err = true;
                    }
                }
            }
        }

        if had_repair_err {
            self.persist_side_effect_recovery_on_failure(
                key_info,
                data_table,
                base_row_key,
                &repair_plans,
                repair_ttl_attr,
                old_item,
                target_item,
                target_ver,
            )
            .await;
            return;
        }

        // Re-verify the base row if we inserted any GSI entry: if a pre-fence fast
        // writer deleted or mutated the row before `SCHEMA_VISIBILITY_FENCE` without
        // writing a GSI tombstone, retire the stale entry immediately.
        if repair_plans.iter().any(|p| p.new_entry.is_some()) {
            match tokio::time::timeout(
                budget,
                self.read_base_row_for_worker(data_table, base_row_key),
            )
            .await
            {
                Ok(Ok((post_item, post_ver)))
                    if post_ver != Some(target_ver) || post_item != live_item_owned =>
                {
                    let retire_ver = post_ver
                        .unwrap_or_else(|| target_ver.saturating_add(1))
                        .max(target_ver.saturating_add(1));
                    let mut retire_plans = Vec::new();
                    for (plan, (_, gsi_schema)) in repair_plans.iter().zip(&repair_gsi_schemas) {
                        if let Some((put_rk, _)) = &plan.new_entry {
                            let cur_rk = post_item.as_ref().and_then(|it| {
                                crate::gsi::shadow_row_key_for_item(
                                    it,
                                    gsi_schema,
                                    &fresh_desc.key_schema,
                                )
                                .ok()
                                .flatten()
                            });
                            if cur_rk.as_ref() != Some(put_rk) {
                                retire_plans.push(ShadowTargetPlan {
                                    shadow_table: plan.shadow_table.clone(),
                                    old_key: Some(put_rk.clone()),
                                    new_entry: None,
                                });
                            }
                        }
                    }
                    if !retire_plans.is_empty() {
                        let retire_res = tokio::time::timeout(
                            budget,
                            self.apply_shadow_plans(&retire_plans, retire_ver),
                        )
                        .await;
                        if !matches!(retire_res, Ok(Ok(()))) {
                            self.persist_side_effect_recovery_on_failure(
                                key_info,
                                data_table,
                                base_row_key,
                                &retire_plans,
                                None,
                                target_item,
                                post_item.as_ref(),
                                retire_ver,
                            )
                            .await;
                        }
                    }
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => {
                    self.persist_side_effect_recovery_on_failure(
                        key_info,
                        data_table,
                        base_row_key,
                        &repair_plans,
                        repair_ttl_attr,
                        old_item,
                        target_item,
                        target_ver,
                    )
                    .await;
                }
            }
        }
    }

    async fn check_idempotency_record(
        &self,
        account_id: &str,
        token: &str,
        fingerprint: &str,
    ) -> Result<(), StorageError> {
        let key = keys::idempotency(account_id, token);
        let Some(prior) = self.cat().get(&key).await? else {
            return Ok(());
        };
        let now_secs = time::OffsetDateTime::now_utc().unix_timestamp();
        let applied_at = prior
            .get("applied_at")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        if now_secs.saturating_sub(applied_at) >= IDEMPOTENCY_WINDOW_SECS {
            let _ = self
                .cat()
                .mutate_json_cas(&key, std::convert::identity, move |cur| {
                    if let Some(c) = cur {
                        let ts = c
                            .get("applied_at")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0);
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
            .and_then(serde_json::Value::as_str)
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
            .mutate_json_cas(&key, std::convert::identity, move |existing| {
                if let Some(cur) = existing {
                    let cur_ts = cur
                        .get("applied_at")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or(0);
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

    fn compute_single_write_post_image(
        &self,
        key_info: &TableKeyInfo,
        desc: &TableDescription,
        op: &SingleWriteOp<'_>,
        existing: Option<&Item>,
        maps: &ExpressionMaps,
    ) -> Result<Option<Option<Item>>, StorageError> {
        match op {
            SingleWriteOp::Put { item } => Ok(Some(Some(item.clone()))),
            SingleWriteOp::Delete { .. } => {
                if existing.is_none() {
                    Ok(None)
                } else {
                    Ok(Some(None))
                }
            }
            SingleWriteOp::Update { key, actions } => {
                let base = existing.cloned().unwrap_or_else(|| (*key).clone());
                let updated = apply_update_actions_validated(
                    &base,
                    actions,
                    maps,
                    key_info,
                    &desc.attribute_definitions,
                )?;
                extenddb_core::validation::validate_update_item_size(
                    &updated,
                    actions,
                    maps,
                    self.max_item_size_bytes,
                )
                .map_err(|e| StorageError::Validation(e.to_string()))?;
                Self::validate_updated_item_index_keys(desc, &updated)?;
                if existing.is_none() && removes_only(actions) {
                    Ok(None)
                } else {
                    Ok(Some(Some(updated)))
                }
            }
        }
    }

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

            let single_wait_deadline =
                Instant::now() + Duration::from_secs(self.intent_timeout_secs.max(2) + 2);
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
                                if Instant::now() >= single_wait_deadline {
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
                                let jitter = rand::random_range(
                                    0..u64::try_from(backoff.as_millis()).unwrap_or(8) + 1,
                                );
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

            let post_stage = match self.compute_single_write_post_image(
                key_info,
                desc,
                &op,
                existing.as_ref(),
                maps,
            ) {
                Ok(Some(new_item)) => new_item,
                Ok(None) => {
                    abort_cleanly(txn_id.clone(), participant).await;
                    return Ok((None, None, txn_id, None));
                }
                Err(e) => {
                    abort_cleanly(txn_id, participant).await;
                    return Err(e);
                }
            };

            let shadow_plans = match Self::prepare_shadow_plan(
                &key_info.key_schema,
                data_table,
                desc,
                existing.as_ref(),
                post_stage.as_ref(),
            ) {
                Ok(plans) => plans,
                Err(e) => {
                    abort_cleanly(txn_id, participant).await;
                    return Err(e);
                }
            };

            let row_existed = existing.is_some();
            let next_version = next_occ_version(read_version);
            let base_payload = match &post_stage {
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
            Self::shadow_plans_to_participant_mutations(
                &shadow_plans,
                next_version,
                &mut txn_mutations,
            );

            let mut ttl_mutations = Vec::new();
            if let Some(attr_name) = ttl_attr {
                let old_exp = existing.as_ref().and_then(|p| get_ttl_expiry(p, attr_name));
                let new_exp = post_stage
                    .as_ref()
                    .and_then(|n| get_ttl_expiry(n, attr_name));
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
            let representative = post_stage
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
                        post_stage.as_ref(),
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
                base_guard: None,
            };

            return Ok((existing, post_stage, txn_id, Some(committed_state)));
        }

        Err(StorageError::TransactionConflict(
            "fenced single-item write retry limit exceeded".to_string(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_fenced_single_item_write(
        &self,
        key_info: &TableKeyInfo,
        data_table: &str,
        row_key: &[u8],
        desc: &TableDescription,
        ttl_attr: Option<&str>,
        meta_read_at: Instant,
        op: SingleWriteOp<'_>,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<(Option<Item>, Option<Item>), StorageError> {
        let (existing, new_item, txn_id, committed_state) = self
            .commit_fenced_single_item_write(
                key_info, data_table, desc, ttl_attr, op, condition, maps, stream,
            )
            .await?;
        let committed_ver = committed_state
            .as_ref()
            .and_then(|st| st.mutations.as_ref())
            .and_then(|ms| ms.first())
            .and_then(|m| m.new_version);
        if let Some(state) = committed_state
            && let Err(e) = self.roll_forward(&txn_id, &state).await
        {
            tracing::warn!(
                txn_id = %txn_id,
                table = %key_info.table_name,
                error = %e,
                "post-commit roll_forward deferred to sweeper"
            );
        }
        if meta_read_at.elapsed() >= FAST_WRITE_BUDGET
            && let Some(ver) = committed_ver
        {
            self.repair_slow_write_schema_drift(
                key_info,
                data_table,
                row_key,
                desc,
                ttl_attr,
                existing.as_ref(),
                new_item.as_ref(),
                ver,
            )
            .await;
        }
        Ok((existing, new_item))
    }

    /// Unified single-item write path (`PutItem`, `DeleteItem`, `UpdateItem`):
    /// - Validates index keys & shadow keys BEFORE committing.
    /// - Uses coordinator-fenced write when stream capture is active OR when a
    ///   single-item write exceeds `max_mutations_per_rpc()`, and direct
    ///   single-row OCC (`put_cas`/`delete_cas`) otherwise.
    /// - Never surfaces post-commit side-effect errors to the client (`#9`),
    ///   delegating any unrecovered post-`put_cas` shadow/TTL retry to
    ///   `__extenddb_txn_log__` (`#7`).
    /// - Performs slow-write schema-drift repair (`#13`) and enqueues row
    ///   tombstone reclamation (`#24`).
    async fn execute_single_item_write(
        &self,
        key_info: &TableKeyInfo,
        op: SingleWriteOp<'_>,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<(Option<Item>, Option<Item>), StorageError> {
        let meta_read_at = Instant::now();
        let (data_table, desc, ttl_attr) = self.table_full_meta_for(key_info).await?;

        match &op {
            SingleWriteOp::Put { item } => Self::validate_put_index_keys(&desc, item)?,
            SingleWriteOp::Update { actions, .. } => {
                Self::validate_update_index_literals(&desc, actions, maps)?;
            }
            SingleWriteOp::Delete { .. } => {}
        }

        let row_key =
            crate::data::encoding::row_key::encode_key(op.lookup_key(), &key_info.key_schema)?;
        let rpc_limit = max_mutations_per_rpc().max(4);
        let exceeds_rpc_limit =
            matches!(&op, SingleWriteOp::Put { item } if item.len().saturating_add(3) > rpc_limit);
        let should_capture_stream =
            Self::active_stream_target_for_capture(&desc, stream, op.lookup_key())?.is_some();

        if should_capture_stream || exceeds_rpc_limit {
            return self
                .finish_fenced_single_item_write(
                    key_info,
                    &data_table,
                    &row_key,
                    &desc,
                    ttl_attr.as_deref(),
                    meta_read_at,
                    op,
                    condition,
                    maps,
                    stream,
                )
                .await;
        }

        let op_label = op.op_label();
        let row_guard = self.acquire_single_write_lock(&data_table, &row_key).await;
        let ops = ItemOps::new(&self.client, &data_table, self.intent_timeout_secs);
        let single_deadline =
            Instant::now() + Duration::from_secs(self.intent_timeout_secs.max(2) + 2);
        let mut multi_conflicts = 0usize;

        for attempt in 0..MAX_OCC_ATTEMPTS {
            let (has_live_intent, existing, read_version) = self
                .read_row_resolving_committed_intents(&ops, &data_table, &row_key)
                .await?;
            check_condition(existing.as_ref(), condition, maps)?;

            let post_stage = match self.compute_single_write_post_image(
                key_info,
                &desc,
                &op,
                existing.as_ref(),
                maps,
            )? {
                Some(stage) => stage,
                None => {
                    if matches!(op, SingleWriteOp::Delete { .. }) && has_live_intent {
                        // A live intent still exists on this missing row; resolve/wait
                        // through `delete_cas` so conflicting 2PC transactions serialize.
                        None
                    } else {
                        return Ok((None, None));
                    }
                }
            };

            if post_stage
                .as_ref()
                .is_some_and(|new_item| new_item.len().saturating_add(3) > rpc_limit)
            {
                drop(row_guard);
                return self
                    .finish_fenced_single_item_write(
                        key_info,
                        &data_table,
                        &row_key,
                        &desc,
                        ttl_attr.as_deref(),
                        meta_read_at,
                        op,
                        condition,
                        maps,
                        stream,
                    )
                    .await;
            }

            // Pre-compute and validate all GSI + LSI shadow keys BEFORE `put_cas` / `delete_cas`.
            let shadow_plans = Self::prepare_shadow_plan(
                &key_info.key_schema,
                &data_table,
                &desc,
                existing.as_ref(),
                post_stage.as_ref(),
            )?;

            let cas_res = match &post_stage {
                Some(new_item) => {
                    ops.put_cas(key_info, new_item, read_version, existing.is_some())
                        .await?
                }
                None => {
                    ops.delete_cas(key_info, op.lookup_key(), read_version, existing.is_some())
                        .await?
                }
            };

            match cas_res {
                CasOutcome::Applied(new_version) => {
                    // Commit point has passed: bound inline side-effect latency strictly
                    // below the tombstone grace horizon and never return `Err` to the client (`#9`).
                    let inline_budget = self.inline_side_effect_budget();
                    let side_outcome = tokio::time::timeout(inline_budget, async {
                        let shadow_res = self.apply_shadow_plans(&shadow_plans, new_version).await;
                        let ttl_res = self
                            .maintain_ttl_index(
                                key_info,
                                ttl_attr.as_deref(),
                                existing.as_ref(),
                                post_stage.as_ref(),
                                new_version,
                            )
                            .await;
                        (shadow_res, ttl_res)
                    })
                    .await;
                    let needs_recovery = match side_outcome {
                        Ok((shadow_res, ttl_res)) => {
                            if let Err(ref e) = shadow_res {
                                tracing::warn!(
                                    table = %key_info.table_name,
                                    error = %e,
                                    "post-commit shadow update failed; scheduling sweeper recovery"
                                );
                            }
                            if let Err(ref e) = ttl_res {
                                tracing::warn!(
                                    table = %key_info.table_name,
                                    error = %e,
                                    "post-commit TTL index update failed; scheduling sweeper recovery"
                                );
                            }
                            shadow_res.is_err() || ttl_res.is_err()
                        }
                        Err(_) => {
                            tracing::warn!(
                                table = %key_info.table_name,
                                "post-commit side-effect budget exceeded; scheduling sweeper recovery"
                            );
                            true
                        }
                    };
                    if needs_recovery {
                        self.persist_side_effect_recovery_on_failure(
                            key_info,
                            &data_table,
                            &row_key,
                            &shadow_plans,
                            ttl_attr.as_deref(),
                            existing.as_ref(),
                            post_stage.as_ref(),
                            new_version,
                        )
                        .await;
                    }
                    if post_stage.is_none() && (existing.is_some() || read_version.is_some()) {
                        self.enqueue_tombstone_gc_best_effort(&data_table, &row_key, new_version)
                            .await;
                    }
                    if meta_read_at.elapsed() >= FAST_WRITE_BUDGET {
                        self.repair_slow_write_schema_drift(
                            key_info,
                            &data_table,
                            &row_key,
                            &desc,
                            ttl_attr.as_deref(),
                            existing.as_ref(),
                            post_stage.as_ref(),
                            new_version,
                        )
                        .await;
                    }
                    return Ok((existing, post_stage));
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

        Err(StorageError::TransactionConflict(format!(
            "OCC contention limit exceeded on {op_label}"
        )))
    }

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
    /// participants, version-guarded GSI + LSI shadow rows, OCC-bounded TTL
    /// index mutations, contiguous stream records, and `ClientRequestToken`
    /// `idem:` persistence. Treats `TableNotFound` on deleted tables as done (`#10`).
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

        let is_side_effect_only = state.participants.as_ref().is_some_and(Vec::is_empty);
        let post_base_still_current =
            if is_side_effect_only && let Some(ref guard) = state.base_guard {
                let base_ops = ItemOps::new(
                    client,
                    &guard.participant.data_table,
                    self.intent_timeout_secs,
                );
                match base_ops
                    .get_row_snapshot_by_raw_key(&guard.participant.row_key)
                    .await
                {
                    Ok(snap) => {
                        snap.item.is_some()
                            && snap.version == Some(guard.base_version)
                            && !snap.in_progress_chunk
                    }
                    Err(StorageError::TableNotFound(_)) => false,
                    Err(e) => return Err(e),
                }
            } else {
                true
            };
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
                        if matches!(m.payload, crate::transact::TxnOpPayload::Delete) {
                            self.enqueue_tombstone_gc_best_effort(
                                &m.participant.data_table,
                                &m.participant.row_key,
                                new_version,
                            )
                            .await;
                        }
                    } else {
                        let ops = ItemOps::new(
                            client,
                            &m.participant.data_table,
                            self.intent_timeout_secs,
                        );
                        match ops
                            .get_row_snapshot_by_raw_key(&m.participant.row_key)
                            .await
                        {
                            Ok(snap) => {
                                let matches_our_write = !snap.in_progress_chunk
                                    && snap.version == Some(new_version)
                                    && match &m.payload {
                                        crate::transact::TxnOpPayload::Put { item } => {
                                            snap.item.as_ref() == Some(item)
                                        }
                                        crate::transact::TxnOpPayload::Delete => {
                                            snap.item.is_none()
                                        }
                                    };
                                last_base_still_current = matches_our_write;
                                if matches_our_write {
                                    materialized_participants.insert(m.participant.clone());
                                }
                            }
                            Err(StorageError::TableNotFound(_)) => {
                                // Table was deleted concurrently; skip its shadow/stream side effects
                                // and continue clearing surviving participants' intents (`#10`).
                                last_base_still_current = false;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                } else {
                    if !is_side_effect_only && !last_base_still_current {
                        continue;
                    }
                    if is_side_effect_only
                        && !post_base_still_current
                        && matches!(m.payload, crate::transact::TxnOpPayload::Put { .. })
                    {
                        continue;
                    }
                    let ops =
                        ItemOps::new(client, &m.participant.data_table, self.intent_timeout_secs);
                    match &m.payload {
                        crate::transact::TxnOpPayload::Put { item } => {
                            match ops
                                .put_shadow_versioned(&m.participant.row_key, item, new_version)
                                .await
                            {
                                Ok(_) | Err(StorageError::TableNotFound(_)) => {}
                                Err(e) => return Err(e),
                            }
                        }
                        crate::transact::TxnOpPayload::Delete => {
                            match ops
                                .delete_shadow_versioned(&m.participant.row_key, new_version)
                                .await
                            {
                                Ok(_) => {
                                    self.enqueue_tombstone_gc_best_effort(
                                        &m.participant.data_table,
                                        &m.participant.row_key,
                                        new_version,
                                    )
                                    .await;
                                }
                                Err(StorageError::TableNotFound(_)) => {}
                                Err(e) => return Err(e),
                            }
                        }
                    }
                }
            }
        }

        if let Some(ttl_muts) = &state.ttl_mutations {
            for tm in ttl_muts {
                if is_side_effect_only
                    || state.mutations.is_none()
                    || materialized_participants.contains(&tm.participant)
                {
                    let effective_new_exp = if is_side_effect_only && !post_base_still_current {
                        None
                    } else {
                        tm.new_expiry
                    };
                    if tm.old_expiry.is_some() || effective_new_exp.is_some() {
                        apply_ttl_mutation_versioned(
                            client,
                            &tm.account_id,
                            &tm.table_name,
                            &tm.participant.row_key,
                            tm.old_expiry,
                            effective_new_exp,
                            tm.new_version,
                        )
                        .await?;
                    }
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
                    let written = crate::streams::write_record_ordered(
                        &cat,
                        &r.shard_id,
                        &r.record,
                        r.dedup_key.as_deref(),
                    )
                    .await?;
                    // If `r.shard_id` was closed after prepare (`written == None`) and the
                    // table was re-enabled with a newer open stream, route to the open shard;
                    // if streams are now disabled, drop cleanly (`#12`).
                    if written.is_none()
                        && let Some(parsed_arn) = extenddb_core::arn::parse(&r.stream_arn)
                        && let Ok((tbl_name, _)) =
                            extenddb_storage::util::parse_stream_arn(&r.stream_arn)
                        && let Ok((_, fresh_desc, _)) = self
                            .table_full_meta_uncached(&parsed_arn.account, &tbl_name)
                            .await
                        && let Ok(Some((_, new_sid))) = Self::active_stream_target_from_desc(
                            &fresh_desc,
                            Some(r.record.dynamodb.stream_view_type),
                            &r.record.dynamodb.keys,
                        )
                        && new_sid != r.shard_id
                    {
                        let _ = crate::streams::write_record_ordered(
                            &cat,
                            &new_sid,
                            &r.record,
                            r.dedup_key.as_deref(),
                        )
                        .await?;
                    }
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
        detail: &str,
    ) -> StorageError {
        tracing::debug!(conflict_idx, detail, "TransactWriteItems conflict");
        let mut reasons: Vec<CancellationReason> =
            (0..total_ops).map(|_| CancellationReason::none()).collect();
        if conflict_idx < reasons.len() {
            reasons[conflict_idx] = CancellationReason::transaction_conflict();
        }
        StorageError::TransactionCanceled(reasons)
    }

    async fn read_row_resolving_committed_intents(
        &self,
        ops: &ItemOps<'_>,
        data_table: &str,
        row_key: &[u8],
    ) -> Result<(bool, Option<Item>, Option<u64>), StorageError> {
        let deadline = Instant::now() + Duration::from_secs(self.intent_timeout_secs.max(5) + 5);
        let coord = crate::transact::TxnCoordinator::new(
            &self.client,
            Duration::from_secs(self.intent_timeout_secs.max(1)),
        );
        let mut sleep_step = 0u64;
        let mut snap = ops.get_row_snapshot_by_raw_key(row_key).await?;
        while Instant::now() < deadline {
            if snap.intents.is_empty() && !snap.in_progress_chunk {
                return Ok((false, snap.item, snap.version));
            }
            if !snap.intents.is_empty() {
                if self
                    .resolve_blocking_intents(data_table, row_key, None)
                    .await?
                    == IntentBlockState::Cleared
                {
                    snap = ops.get_row_snapshot_by_raw_key(row_key).await?;
                    continue;
                }
                if !snap.in_progress_chunk {
                    return Ok((true, snap.item, snap.version));
                }
                // Serve the authoritative post-image from `__extenddb_txn_log__`
                // while a `COMMITTED` coordinator's chunks are still settling.
                for (tid, _) in &snap.intents {
                    if !tid.starts_with(DIRECT_PUT_STAGE_PREFIX)
                        && let Some(st) = coord.get_state(tid).await?
                        && st.state == "COMMITTED"
                        && let Some(muts) = &st.mutations
                    {
                        for m in muts {
                            if m.participant.data_table == data_table
                                && m.participant.row_key == row_key
                            {
                                let committed_item = match &m.payload {
                                    crate::transact::TxnOpPayload::Put { item } => {
                                        Some(item.clone())
                                    }
                                    crate::transact::TxnOpPayload::Delete => None,
                                };
                                return Ok((true, committed_item, m.new_version.or(snap.version)));
                            }
                        }
                    }
                }
            }
            sleep_step = sleep_step.saturating_add(1);
            tokio::time::sleep(Duration::from_millis(1 + sleep_step.min(8))).await;
            snap = ops.get_row_snapshot_by_raw_key(row_key).await?;
        }
        if snap.in_progress_chunk {
            return Err(StorageError::Transient(
                "concurrent multi-chunk write still in progress".to_string(),
            ));
        }
        Ok((!snap.intents.is_empty(), snap.item, snap.version))
    }

    async fn wait_or_fail_occ_intent(
        &self,
        data_table: &str,
        row_key: &[u8],
        single_deadline: Instant,
        multi_conflicts: &mut usize,
        attempt: usize,
    ) -> Result<(), StorageError> {
        match self
            .resolve_blocking_intents(data_table, row_key, None)
            .await?
        {
            IntentBlockState::Cleared => Ok(()),
            IntentBlockState::SingleWriteInFlight => {
                if Instant::now() >= single_deadline {
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

    /// Resume any tables left in `ttl.status == "ENABLING"` whose per-table
    /// backfill lock (`lock:ttl_backfill:<table_id>`) is free.
    pub(crate) async fn resume_enabling_ttl_tables(
        &self,
        shutdown: &CancellationToken,
    ) -> Result<(), StorageError> {
        let rows = self.cat().scan_prefix(keys::TABLE_META_SCAN_PREFIX).await?;
        for (key, val) in rows {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            let (status, Some(attr), _) = parse_ttl_status_and_attr(&val) else {
                continue;
            };
            if status != TimeToLiveStatus::Enabling {
                continue;
            }
            let mut parts = key.splitn(3, ':');
            let _ = parts.next();
            let (Some(acct_id), Some(table_name)) = (parts.next(), parts.next()) else {
                continue;
            };
            if let Err(e) = self
                .run_ttl_backfill_locked(acct_id, table_name, &attr, shutdown)
                .await
            {
                tracing::debug!(
                    account_id = %acct_id,
                    table = %table_name,
                    error = %e,
                    "TTL ENABLING backfill pass skipped or deferred"
                );
            }
        }
        Ok(())
    }

    async fn run_ttl_backfill_locked(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
        shutdown: &CancellationToken,
    ) -> Result<(), StorageError> {
        let meta_key = keys::table_meta(account_id, table_name);
        let Some(row) = self.cat().get(&meta_key).await? else {
            return Ok(());
        };
        let (status, stored_attr, generation) = parse_ttl_status_and_attr(&row);
        if status != TimeToLiveStatus::Enabling {
            return Ok(());
        }
        let target_attr = stored_attr.as_deref().unwrap_or(ttl_attribute);
        let Some(data_table) = row.get("data_table").and_then(serde_json::Value::as_str) else {
            return Ok(());
        };
        let desc = deserialize_table_description_row(&row)?;

        let lock_name = ttl_backfill_lock_name(&desc.table_id);
        let owner = format!("ttl-backfill-{}-{}", self.node_id, Uuid::new_v4());
        if !self
            .cat()
            .try_lock(&lock_name, &owner, TTL_BACKFILL_LEASE)
            .await?
        {
            return Ok(());
        }

        let renew_token = shutdown.child_token();
        let renew_bg = renew_token.clone();
        let renew_client = self.catalog_client.clone();
        let renew_lock = lock_name.clone();
        let renew_owner = owner.clone();
        let renewal = tokio::spawn(async move {
            let every = TTL_BACKFILL_LEASE / 3;
            while sleep_or_shutdown(&renew_bg, every).await {
                let c = Catalog::new(&renew_client);
                if let Ok(false) = c
                    .renew_lock(&renew_lock, &renew_owner, TTL_BACKFILL_LEASE)
                    .await
                {
                    renew_bg.cancel();
                    return;
                }
            }
        });

        let res = async {
            self.await_schema_visibility_fence(data_table, &renew_token)
                .await?;
            if renew_token.is_cancelled() {
                return Ok(());
            }

            let base_key_info =
                crate::gsi_worker::build_reconciler_base_key_info(account_id, table_name, &desc);
            let base_qs = QueryScan::new(&self.client, data_table);
            let mut start_key = None;
            loop {
                if renew_token.is_cancelled() {
                    return Err(StorageError::Transient(
                        "TTL backfill lease cancelled".into(),
                    ));
                }
                let (items, last) = base_qs
                    .scan(
                        &base_key_info,
                        false,
                        Some(TTL_BACKFILL_SCAN_BATCH),
                        start_key.as_ref(),
                        None,
                        None,
                    )
                    .await?;
                if items.is_empty() {
                    break;
                }
                for item in &items {
                    if let Some(exp) = get_ttl_expiry(item, target_attr) {
                        let base_rk =
                            crate::data::encoding::row_key::encode_key(item, &desc.key_schema)?;
                        let (live_opt, live_ver) =
                            self.read_base_row_for_worker(data_table, &base_rk).await?;
                        if let Some(live_item) = live_opt
                            && let Some(live_exp) = get_ttl_expiry(&live_item, target_attr)
                        {
                            let ver = live_ver.unwrap_or_else(|| next_occ_version(None));
                            insert_ttl_index_entry_at(
                                &self.client,
                                account_id,
                                table_name,
                                &base_rk,
                                live_exp.min(exp).max(live_exp),
                                version_to_timestamp_micros(ver),
                            )
                            .await?;
                        }
                    }
                }
                match last {
                    Some(k) => start_key = Some(k),
                    None => break,
                }
            }

            if renew_token.is_cancelled() {
                return Err(StorageError::Transient(
                    "TTL backfill lease lost before promotion".into(),
                ));
            }

            let expected_gen = generation.clone();
            let promoted = self
                .cat()
                .mutate_json_cas(&meta_key, std::convert::identity, move |existing| {
                    let Some(mut cur) = existing else {
                        return Ok((CatalogJsonAction::NoOp, false));
                    };
                    let (cur_status, cur_attr, cur_gen) = parse_ttl_status_and_attr(&cur);
                    if cur_status != TimeToLiveStatus::Enabling || cur_gen != expected_gen {
                        return Ok((CatalogJsonAction::NoOp, false));
                    }
                    if let Some(obj) = cur.as_object_mut() {
                        obj.insert(
                            "ttl".to_string(),
                            json!({
                                "status": "ENABLED",
                                "enabled": true,
                                "attribute": cur_attr,
                                "generation": cur_gen,
                            }),
                        );
                    }
                    Ok((CatalogJsonAction::Put(cur), true))
                })
                .await?;
            if promoted {
                self.invalidate_table_cache(account_id, table_name).await;
            }
            Ok(())
        }
        .await;

        renew_token.cancel();
        renewal.abort();
        let _ = renewal.await;
        let _ = self.cat().release_lock(&lock_name, &owner).await;
        res
    }

    async fn purge_ttl_entries_for_table(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<(), StorageError> {
        let full_index_table = self.client.full_table_name(TTL_INDEX_TABLE);
        for shard_id in 0..NUM_SHARDS {
            let mut start_key = Vec::with_capacity(9);
            start_key.push(shard_id);
            start_key.extend_from_slice(&0i64.to_be_bytes());
            let end_key = vec![shard_id + 1];
            let mut exclude_start = false;

            loop {
                let mut data = self.client.data();
                let req = ReadRowsRequest {
                    table_name: full_index_table.clone(),
                    app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                    rows_limit: CATALOG_GC_PAGE_SIZE,
                    rows: Some(RowSet {
                        row_keys: vec![],
                        row_ranges: vec![RowRange {
                            start_key: Some(if exclude_start {
                                StartKey::StartKeyOpen(start_key.clone())
                            } else {
                                StartKey::StartKeyClosed(start_key.clone())
                            }),
                            end_key: Some(EndKey::EndKeyOpen(end_key.clone())),
                        }],
                    }),
                    filter: Some(RowFilter {
                        filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                    }),
                    ..ReadRowsRequest::default()
                };
                let resp = data
                    .read_rows(req)
                    .await
                    .map_err(|e| map_bigtable_err("ReadRows purge_ttl_entries", e))?;
                if resp.is_empty() {
                    break;
                }
                let batch_len = resp.len();
                let mut last_key = None;
                for (raw_key, cells) in resp {
                    last_key = Some(raw_key.clone());
                    if let Ok((_, _, acct, tbl, _)) = decode_ttl_key(&raw_key)
                        && acct == account_id
                        && tbl == table_name
                    {
                        let max_ts = cells.iter().map(|c| c.timestamp_micros).max().unwrap_or(0);
                        let _ =
                            crate::ttl_worker::delete_ttl_index_entry_up_to(self, &raw_key, max_ts)
                                .await;
                    }
                }
                if i64::try_from(batch_len).unwrap_or(i64::MAX) < CATALOG_GC_PAGE_SIZE {
                    break;
                }
                if let Some(k) = last_key {
                    start_key = k;
                    exclude_start = true;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }
}

fn map_admin_err(msg: String) -> StorageError {
    let lower = msg.to_ascii_lowercase();
    if lower.contains("unavailable")
        || lower.contains("deadline")
        || lower.contains("resourceexhausted")
        || lower.contains("aborted")
        || lower.contains("goaway")
    {
        StorageError::Transient(msg)
    } else {
        StorageError::Internal(msg)
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
            if self.cat().get(&meta_key).await?.is_some() {
                return Err(StorageError::TableAlreadyExists(input.table_name));
            }

            let table_id = Uuid::new_v4().to_string();
            let data_table = data_table_id(&table_id);

            let mut admin = AdminClient::connect(&self.client)
                .await
                .map_err(map_admin_err)?;
            let families = vec![
                ("d", Some(crate::data::admin::gc_max_versions(1))),
                ("m", Some(crate::data::admin::gc_max_versions(1))),
                ("t", None),
            ];
            admin
                .create_table(&data_table, &families)
                .await
                .map_err(map_admin_err)?;

            let mut created_shadows = Vec::new();
            if let Some(gsis) = &input.global_secondary_indexes {
                for g in gsis {
                    let shadow = crate::gsi::shadow_table_id(&data_table, &g.index_name);
                    admin
                        .create_table(&shadow, &families)
                        .await
                        .map_err(map_admin_err)?;
                    created_shadows.push(shadow);
                }
            }
            if let Some(lsis) = &input.local_secondary_indexes {
                for l in lsis {
                    let shadow = crate::gsi::lsi_shadow_table_id(&data_table, &l.index_name);
                    admin
                        .create_table(&shadow, &families)
                        .await
                        .map_err(map_admin_err)?;
                    created_shadows.push(shadow);
                }
            }

            let delay_secs = self.control_plane_delay_secs().await;
            let billing = input.billing_mode.unwrap_or(BillingMode::Provisioned);
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
                        provisioned_throughput: Some(ProvisionedThroughputDescription {
                            read_capacity_units: g
                                .provisioned_throughput
                                .as_ref()
                                .map_or(0, |p| p.read_capacity_units),
                            write_capacity_units: g
                                .provisioned_throughput
                                .as_ref()
                                .map_or(0, |p| p.write_capacity_units),
                            number_of_decreases_today: 0,
                            last_increase_date_time: None,
                            last_decrease_date_time: None,
                        }),
                        index_size_bytes: 0,
                        item_count: 0,
                        index_arn: format!("{table_arn}/index/{}", g.index_name),
                        warm_throughput: None,
                        warm_throughput_record: warm_throughput_record_for_create(
                            g.warm_throughput.as_ref(),
                            delay_secs,
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
                    .unwrap_or(StreamViewType::NewAndOldImages);
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
                warm_throughput_record_for_create(input.warm_throughput.as_ref(), delay_secs);
            let creation_date_time = now_unix_f64();

            let description = TableDescription {
                table_name: input.table_name.clone(),
                key_schema: input.key_schema,
                attribute_definitions: input.attribute_definitions,
                table_status: TableStatus::Active,
                creation_date_time,
                table_size_bytes: 0,
                item_count: 0,
                table_arn,
                table_id: table_id.clone(),
                provisioned_throughput: throughput,
                billing_mode_summary: (billing == BillingMode::PayPerRequest).then_some(
                    BillingModeSummary {
                        billing_mode: BillingMode::PayPerRequest,
                        last_update_to_pay_per_request_date_time: Some(creation_date_time),
                    },
                ),
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
                crate::streams::put_shard(&self.cat(), shard_meta).await?;
            }

            let mut record = json!({
                "data_table": data_table,
                "ttl": {"status": "DISABLED", "enabled": false, "attribute": null, "generation": ""},
            });
            insert_table_description_into_row(&mut record, &description)?;

            let inserted = self.cat().put_if_absent(&meta_key, &record).await?;
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
                .await?
                .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

            let description = deserialize_table_description_row(&row)?;

            if description.deletion_protection_enabled {
                return Err(StorageError::DeletionProtected(input.table_name));
            }

            let data_table = row
                .get("data_table")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    StorageError::Internal("missing data_table in catalog row".into())
                })?;

            let mut admin = AdminClient::connect(&self.client)
                .await
                .map_err(map_admin_err)?;
            if let Some(gsis) = &description.global_secondary_indexes {
                for g in gsis {
                    let shadow = crate::gsi::shadow_table_id(data_table, &g.index_name);
                    let _ = admin.delete_table(&shadow).await;
                }
            }
            if let Some(lsis) = &description.local_secondary_indexes {
                for l in lsis {
                    let shadow = crate::gsi::lsi_shadow_table_id(data_table, &l.index_name);
                    let _ = admin.delete_table(&shadow).await;
                }
            }
            admin
                .delete_table(data_table)
                .await
                .map_err(map_admin_err)?;

            let _ = self
                .purge_ttl_entries_for_table(&account_id, &input.table_name)
                .await;

            let shards =
                crate::streams::load_table_shards(&self.cat(), &description.table_id).await?;
            for shard in &shards {
                self.cat()
                    .delete_prefix(&keys::stream_record_shard_prefix(&shard.shard_id))
                    .await?;
                self.cat()
                    .delete_prefix(&keys::stream_dedup_shard_prefix(&shard.shard_id))
                    .await?;
                self.cat()
                    .delete(&keys::stream_shard(&shard.shard_id))
                    .await?;
            }
            self.cat()
                .delete(&keys::tags(&description.table_arn))
                .await?;

            self.cat()
                .delete(&keys::table_meta(&account_id, &input.table_name))
                .await?;

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
                .await?
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
                .await?;
            let mut names: Vec<String> = rows
                .into_iter()
                .filter_map(|(key, _)| key.rsplit_once(':').map(|(_, last)| last.to_owned()))
                .collect();
            names.sort();

            if let Some(start) = &input.exclusive_start_table_name {
                names.retain(|n| n.as_str() > start.as_str());
            }
            let last_evaluated_table_name = match input.limit {
                Some(l) if l > 0 && usize::try_from(l).unwrap_or(usize::MAX) < names.len() => {
                    names.truncate(usize::try_from(l).unwrap_or(usize::MAX));
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

            let mut precreated_shadows: Vec<String> = Vec::new();
            let mut precreated_stream: Option<(String, String, StreamViewType, Vec<String>)> = None;

            let needs_pre_read = input
                .global_secondary_index_updates
                .as_ref()
                .is_some_and(|u| u.iter().any(|x| x.create.is_some()))
                || input
                    .stream_specification
                    .as_ref()
                    .is_some_and(|s| s.stream_enabled);

            if needs_pre_read {
                let initial_row = self
                    .cat()
                    .get(&meta_key)
                    .await?
                    .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
                let initial_dt = initial_row
                    .get("data_table")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| StorageError::Internal("missing data_table in catalog".into()))?
                    .to_owned();
                let initial_desc = deserialize_table_description_row(&initial_row)?;
                if initial_desc.table_status != TableStatus::Active {
                    return Err(StorageError::TableNotActive(input.table_name.clone()));
                }

                if let Some(updates) = &input.global_secondary_index_updates
                    && updates.iter().any(|u| u.create.is_some())
                {
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
                        .map_err(map_admin_err)?;
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
                            let shadow =
                                crate::gsi::shadow_table_id(&initial_dt, &create.index_name);
                            admin
                                .create_table(&shadow, &families)
                                .await
                                .map_err(map_admin_err)?;
                            precreated_shadows.push(shadow);
                        }
                    }
                }

                // Pre-create the 4 `ShardMeta` rows BEFORE publishing `latest_stream_label`
                // in `table_meta` so a writer can never observe an enabled stream whose
                // shard rows do not exist yet (`#11`).
                if let Some(spec) = &input.stream_specification
                    && spec.stream_enabled
                    && let Ok(extenddb_storage::stream_lineage::StreamChange::Enable(view)) =
                        extenddb_storage::stream_lineage::stream_change(
                            &input.table_name,
                            initial_desc.stream_specification.as_ref(),
                            spec,
                        )
                {
                    let pre_label = extenddb_storage::stream_lineage::next_stream_label(
                        initial_desc.latest_stream_label.as_deref(),
                    );
                    let mut pre_sids = Vec::with_capacity(
                        extenddb_storage::stream_lineage::SHARDS_PER_STREAM as usize,
                    );
                    for idx in 0..extenddb_storage::stream_lineage::SHARDS_PER_STREAM {
                        let sid = extenddb_storage::stream_lineage::shard_id(
                            &initial_desc.table_id,
                            &pre_label,
                            view,
                            idx,
                        )?;
                        let shard_meta = crate::streams::ShardMeta::new_open(
                            initial_desc.table_id.clone(),
                            sid.clone(),
                            None,
                        );
                        crate::streams::put_shard_if_absent(&self.cat(), &shard_meta).await?;
                        pre_sids.push(sid);
                    }
                    precreated_stream =
                        Some((initial_desc.table_id.clone(), pre_label, view, pre_sids));
                }
            }

            let delay_secs = self.control_plane_delay_secs().await;
            let region = self.region.clone();
            let acct_for_cas = account_id.clone();
            let input_for_cas = input.clone();
            let pre_stream_for_cas = precreated_stream
                .as_ref()
                .map(|(tid, lbl, vw, _)| (tid.clone(), lbl.clone(), *vw));

            let cas_result = self
                .cat()
                .mutate_json_cas(&meta_key, std::convert::identity, move |existing| {
                    let mut row = existing.ok_or_else(|| {
                        StorageError::TableNotFound(input_for_cas.table_name.clone())
                    })?;
                    let data_table = row
                        .get("data_table")
                        .and_then(serde_json::Value::as_str)
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
                                        g.provisioned_throughput =
                                            Some(ProvisionedThroughputDescription::default());
                                    }
                                }
                            }
                            BillingMode::Provisioned => {
                                description.billing_mode_summary = None;
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
                        description.warm_throughput_record = Some(
                            warm_throughput_record_for_update(prev_json.as_deref(), wt, delay_secs),
                        );
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

                        let req_attrs = input_for_cas
                            .attribute_definitions
                            .as_deref()
                            .unwrap_or(&[]);

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
                                            delay_secs,
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
                                    provisioned_throughput: Some(
                                        ProvisionedThroughputDescription {
                                            read_capacity_units: create
                                                .provisioned_throughput
                                                .as_ref()
                                                .map_or(0, |p| p.read_capacity_units),
                                            write_capacity_units: create
                                                .provisioned_throughput
                                                .as_ref()
                                                .map_or(0, |p| p.write_capacity_units),
                                            number_of_decreases_today: 0,
                                            last_increase_date_time: None,
                                            last_decrease_date_time: None,
                                        },
                                    ),
                                    index_size_bytes: 0,
                                    item_count: 0,
                                    index_arn: format!(
                                        "{}/index/{}",
                                        description.table_arn, create.index_name
                                    ),
                                    warm_throughput: None,
                                    warm_throughput_record: warm_throughput_record_for_create(
                                        create.warm_throughput.as_ref(),
                                        delay_secs,
                                    ),
                                };
                                description
                                    .global_secondary_indexes
                                    .get_or_insert_with(Vec::new)
                                    .push(gsi_desc.clone());
                                created_gsis.push(gsi_desc);
                            }
                        }

                        let mut surviving_schemas: Vec<Vec<KeySchemaElement>> = Vec::new();
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
                                        let (label, used_precreated) = match &pre_stream_for_cas {
                                            Some((pre_tid, pre_lbl, pre_view))
                                                if *pre_tid == description.table_id
                                                    && *pre_view == view
                                                    && description
                                                        .latest_stream_label
                                                        .as_deref()
                                                        .is_none_or(|cur| {
                                                            cur < pre_lbl.as_str()
                                                        }) =>
                                            {
                                                (pre_lbl.clone(), true)
                                            }
                                            _ => (
                                                extenddb_storage::stream_lineage::next_stream_label(
                                                    description.latest_stream_label.as_deref(),
                                                ),
                                                false,
                                            ),
                                        };
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
                                            used_precreated_shards: used_precreated,
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
                        if let Some((_, _, _, pre_sids)) = precreated_stream {
                            for sid in pre_sids {
                                let _ = self.cat().delete(&keys::stream_shard(&sid)).await;
                            }
                        }
                        return Err(e);
                    }
                };

            let used_precreated_shards = matches!(
                stream_op,
                PendingStreamOp::Enable {
                    used_precreated_shards: true,
                    ..
                }
            );
            if !used_precreated_shards && let Some((_, _, _, pre_sids)) = precreated_stream {
                for sid in pre_sids {
                    let _ = self.cat().delete(&keys::stream_shard(&sid)).await;
                }
            }

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
                    let _ = self
                        .roll_forward_committed_txns_for_table(&data_table)
                        .await;
                    let existing_shards =
                        crate::streams::load_table_shards(&self.cat(), &table_id).await?;
                    let lineage_rows: Vec<_> = existing_shards
                        .iter()
                        .map(crate::streams::ShardMeta::to_lineage_row)
                        .collect();
                    let to_close = extenddb_storage::stream_lineage::open_shard_ids_of(
                        &table_id,
                        &label_to_close,
                        &lineage_rows,
                    );
                    crate::streams::close_shards_by_id(&self.cat(), &to_close).await?;
                    return Err(StorageError::Validation(refusal_message));
                }
                PendingStreamOp::Disable {
                    table_id,
                    label_to_close,
                    label_to_close_prior,
                } => {
                    // Drain any already-committed 2PC writes targeting `data_table`
                    // BEFORE sealing `ending_sequence_number` (`#12`).
                    let _ = self
                        .roll_forward_committed_txns_for_table(&data_table)
                        .await;
                    let existing_shards =
                        crate::streams::load_table_shards(&self.cat(), &table_id).await?;
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
                    crate::streams::close_shards_by_id(&self.cat(), &to_close).await?;
                }
                PendingStreamOp::Enable {
                    table_id,
                    label,
                    view,
                    label_to_close_prior,
                    used_precreated_shards,
                } => {
                    let _ = self
                        .roll_forward_committed_txns_for_table(&data_table)
                        .await;
                    let existing_shards =
                        crate::streams::load_table_shards(&self.cat(), &table_id).await?;
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
                    crate::streams::close_shards_by_id(&self.cat(), &older).await?;

                    let mut inserted_sids = Vec::with_capacity(
                        extenddb_storage::stream_lineage::SHARDS_PER_STREAM as usize,
                    );
                    for idx in 0..extenddb_storage::stream_lineage::SHARDS_PER_STREAM {
                        let sid = extenddb_storage::stream_lineage::shard_id(
                            &table_id, &label, view, idx,
                        )?;
                        if !used_precreated_shards {
                            let shard_meta = crate::streams::ShardMeta::new_open(
                                table_id.clone(),
                                sid.clone(),
                                None,
                            );
                            crate::streams::put_shard_if_absent(&self.cat(), &shard_meta).await?;
                        }
                        inserted_sids.push(sid);
                    }

                    let fresh_row = self.cat().get(&meta_key).await?;
                    let fresh_stream_row = fresh_row
                        .as_ref()
                        .and_then(|r| deserialize_table_description_row(r).ok())
                        .map(|d| Self::table_stream_row(&d));
                    if !extenddb_storage::stream_lineage::enable_still_current(
                        fresh_stream_row.as_ref(),
                        &label,
                    ) {
                        crate::streams::close_shards_by_id(&self.cat(), &inserted_sids).await?;
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
                    let build_lock =
                        gsi_build_lock_name(&staged_desc.table_id, &created_gsi.index_name);
                    let build_owner = format!("update-table-{}-{}", self.node_id, Uuid::new_v4());
                    let _ = self
                        .cat()
                        .try_lock(&build_lock, &build_owner, GSI_BUILD_LEASE)
                        .await;

                    let fence_token = CancellationToken::new();
                    let renew_bg = fence_token.clone();
                    let renew_client = self.catalog_client.clone();
                    let renew_lock = build_lock.clone();
                    let renew_owner = build_owner.clone();
                    let renewal = tokio::spawn(async move {
                        let every = GSI_BUILD_LEASE / 3;
                        while sleep_or_shutdown(&renew_bg, every).await {
                            let c = Catalog::new(&renew_client);
                            if let Ok(false) = c
                                .renew_lock(&renew_lock, &renew_owner, GSI_BUILD_LEASE)
                                .await
                            {
                                renew_bg.cancel();
                                return;
                            }
                        }
                    });

                    let backfill_res = async {
                        self.await_schema_visibility_fence(&data_table, &fence_token)
                            .await?;
                        crate::gsi_worker::reconcile_table_gsi(
                            self,
                            &base_key_info,
                            &data_table,
                            created_gsi,
                        )
                        .await
                    }
                    .await;

                    fence_token.cancel();
                    renewal.abort();
                    let _ = renewal.await;

                    if let Err(e) = backfill_res {
                        let failed_name = created_gsi.index_name.clone();
                        // Roll back ONLY if the index is still `CREATING`; never
                        // drop an index that `gsi_worker` already promoted to `ACTIVE` (`#41`).
                        let removed_creating = self
                            .cat()
                            .mutate_json_cas(&meta_key, std::convert::identity, move |existing| {
                                let Some(mut row) = existing else {
                                    return Ok((CatalogJsonAction::NoOp, false));
                                };
                                let mut desc = deserialize_table_description_row(&row)?;
                                let mut removed = false;
                                if let Some(gsis) = desc.global_secondary_indexes.as_mut() {
                                    let before = gsis.len();
                                    gsis.retain(|g| {
                                        !(g.index_name == failed_name
                                            && g.index_status == "CREATING")
                                    });
                                    removed = gsis.len() < before;
                                    if gsis.is_empty() {
                                        desc.global_secondary_indexes = None;
                                    }
                                }
                                if !removed {
                                    return Ok((CatalogJsonAction::NoOp, false));
                                }
                                insert_table_description_into_row(&mut row, &desc)?;
                                Ok((CatalogJsonAction::Put(row), true))
                            })
                            .await
                            .unwrap_or(false);

                        if removed_creating
                            && let Ok(mut admin) = AdminClient::connect(&self.client).await
                        {
                            let shadow =
                                crate::gsi::shadow_table_id(&data_table, &created_gsi.index_name);
                            let _ = admin.delete_table(&shadow).await;
                        }
                        let _ = self.cat().release_lock(&build_lock, &build_owner).await;
                        self.metadata_cache
                            .invalidate(&(account_id, input.table_name))
                            .await;
                        return Err(e);
                    }

                    let promoted_name = created_gsi.index_name.clone();
                    let promote_res = self
                        .cat()
                        .mutate_json_cas(&meta_key, std::convert::identity, move |existing| {
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
                        .await;
                    let _ = self.cat().release_lock(&build_lock, &build_owner).await;
                    promote_res?;
                }
            }

            if !deleted_gsi_names.is_empty()
                && let Ok(mut admin) = AdminClient::connect(&self.client).await
            {
                let current_desc = self
                    .cat()
                    .get(&meta_key)
                    .await?
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
            let metas = self.cat().scan_prefix(keys::TABLE_META_SCAN_PREFIX).await?;
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
            let (existing, _) = self
                .execute_single_item_write(
                    &key_info,
                    SingleWriteOp::Put { item },
                    condition.as_ref(),
                    &maps,
                    stream.as_ref(),
                )
                .await?;
            Ok(if return_old { existing } else { None })
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
            // Cooperatively resolve any `COMMITTED` 2PC intents on the row (`#40`).
            let (item, _) = self.get_item_with_version(&key_info, &key).await?;
            Ok(item)
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
            let (existing, _) = self
                .execute_single_item_write(
                    &key_info,
                    SingleWriteOp::Delete { key: &key },
                    condition.as_ref(),
                    &maps,
                    stream.as_ref(),
                )
                .await?;
            Ok(if return_old { existing } else { None })
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
            let (existing, new_item) = self
                .execute_single_item_write(
                    &key_info,
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
            Ok((
                if return_old { existing } else { None },
                if return_new { new_item } else { None },
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
                            .query(
                                &gsi_key_info,
                                true,
                                &kc,
                                &maps,
                                forward,
                                limit,
                                esk.as_ref(),
                            )
                            .await
                    } else if let Some(lsi) = lsis.iter().find(|l| l.index_name == idx_name) {
                        let shadow_table =
                            crate::gsi::lsi_shadow_table_id(&data_table, &lsi.index_name);
                        let lsi_key_info = TableKeyInfo {
                            table_name: format!("{}__lsi_{}", key_info.table_name, lsi.index_name),
                            account_id: key_info.account_id.clone(),
                            table_id: format!("{}::{}", key_info.table_id, lsi.index_name),
                            key_schema: lsi.key_schema.clone(),
                            base_key_schema: desc.key_schema.clone(),
                            attribute_definitions: desc.attribute_definitions.clone(),
                            ..TableKeyInfo::default()
                        };
                        QueryScan::new(&self.client, &shadow_table)
                            .query(
                                &lsi_key_info,
                                true,
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
                        .query(&base_ki, false, &kc, &maps, forward, limit, esk.as_ref())
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
                            .scan(
                                &gsi_key_info,
                                true,
                                limit,
                                esk.as_ref(),
                                segment,
                                total_segments,
                            )
                            .await
                    } else if let Some(lsi) = lsis.iter().find(|l| l.index_name == idx_name) {
                        let shadow_table =
                            crate::gsi::lsi_shadow_table_id(&data_table, &lsi.index_name);
                        let lsi_key_info = TableKeyInfo {
                            table_name: format!("{}__lsi_{}", key_info.table_name, lsi.index_name),
                            account_id: key_info.account_id.clone(),
                            table_id: format!("{}::{}", key_info.table_id, lsi.index_name),
                            key_schema: lsi.key_schema.clone(),
                            base_key_schema: desc.key_schema.clone(),
                            attribute_definitions: desc.attribute_definitions.clone(),
                            ..TableKeyInfo::default()
                        };
                        QueryScan::new(&self.client, &shadow_table)
                            .scan(
                                &lsi_key_info,
                                true,
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
                        .scan(
                            &base_ki,
                            false,
                            limit,
                            esk.as_ref(),
                            segment,
                            total_segments,
                        )
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
            let is_index = index_name.is_some();
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
            key_belongs_to_segment(&key, &schema, is_index, segment, total_segments)
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
                        validation_reasons.push(CancellationReason::none());
                        resolved_ops.push((idx, data_table, enc_key));
                    }
                    Err(e) => {
                        any_validation_failed = true;
                        validation_reasons
                            .push(CancellationReason::validation_error(e.to_string()));
                    }
                }
            }
            if any_validation_failed {
                return Err(StorageError::TransactionCanceled(validation_reasons));
            }

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

                if unique_key_count == 1
                    && all_tids.is_empty()
                    && map_1.values().all(|s| !s.in_progress_chunk)
                {
                    let mut out = Vec::with_capacity(total_ops);
                    for (_, dt, rk) in &resolved_ops {
                        let item = map_1
                            .get(&(dt.clone(), rk.clone()))
                            .and_then(|s| s.item.clone());
                        out.push(item);
                    }
                    return Ok(out);
                }

                let mut txns_1: HashMap<String, Option<crate::transact::TxnState>> =
                    HashMap::with_capacity(all_tids.len());
                for tid in all_tids.keys() {
                    if tid.starts_with(DIRECT_PUT_STAGE_PREFIX) {
                        txns_1.insert(tid.clone(), None);
                    } else {
                        let st = coord.get_state(tid).await?;
                        txns_1.insert(tid.clone(), st);
                    }
                }

                let mut torn_unlogged_chunk = false;
                for ((dt, rk), snap) in &map_1 {
                    if snap.in_progress_chunk {
                        let has_committed_post_image = snap.intents.iter().any(|(tid, _)| {
                            txns_1
                                .get(tid)
                                .and_then(Option::as_ref)
                                .is_some_and(|st| st.state == "COMMITTED" && st.mutations.is_some())
                        });
                        if !has_committed_post_image {
                            let _ = self.resolve_blocking_intents(dt, rk, None).await;
                            torn_unlogged_chunk = true;
                        }
                    }
                }
                if torn_unlogged_chunk {
                    let jitter_ms = 1
                        + rand::random_range(0..4u64)
                        + u64::try_from(attempt.min(8)).unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                    continue;
                }

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
                    let jitter_ms = 1
                        + rand::random_range(0..4u64)
                        + u64::try_from(attempt.min(8)).unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                    continue;
                }

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
                    let jitter_ms = 1
                        + rand::random_range(0..4u64)
                        + u64::try_from(attempt.min(8)).unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                    continue;
                }

                let mut txns_match = true;
                for tid in all_tids.keys() {
                    let st_2 = if tid.starts_with(DIRECT_PUT_STAGE_PREFIX) {
                        None
                    } else {
                        coord.get_state(tid).await?
                    };
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
                    let jitter_ms = 1
                        + rand::random_range(0..4u64)
                        + u64::try_from(attempt.min(8)).unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                    continue;
                }

                let mut out: Vec<Option<Item>> = vec![None; total_ops];
                let mut saw_unresolved_chunk = false;
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

                    if resolved_item.is_none() && row_snap.in_progress_chunk {
                        saw_unresolved_chunk = true;
                        break;
                    }

                    out[*idx] = resolved_item.unwrap_or_else(|| row_snap.item.clone());
                }

                if saw_unresolved_chunk {
                    let jitter_ms = 1
                        + rand::random_range(0..4u64)
                        + u64::try_from(attempt.min(8)).unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                    continue;
                }

                return Ok(out);
            }

            let reasons = (0..total_ops)
                .map(|_| CancellationReason::transaction_conflict())
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

            let meta_read_at = Instant::now();
            let mut pre_resolved = Vec::with_capacity(owned.len());
            for op in owned {
                let (data_table, desc, ttl_attr) = self.table_full_meta_for(&op.key_info).await?;
                pre_resolved.push((op, data_table, desc, ttl_attr));
            }

            {
                let mut key_reasons: Vec<CancellationReason> =
                    vec![CancellationReason::none(); pre_resolved.len()];
                let mut any_key_fault = false;
                for (i, (op, _, desc, _)) in pre_resolved.iter().enumerate() {
                    if let Err(r) = Self::transact_op_key_checks(op, desc) {
                        any_key_fault = true;
                        key_reasons[i] = r;
                    }
                }
                if any_key_fault {
                    return Err(match CancellationReason::resolve_key_faults(key_reasons) {
                        Ok(reasons) => StorageError::TransactionCanceled(reasons),
                        Err(message) => StorageError::Validation(message),
                    });
                }
            }

            let mut resolved: Vec<(
                OwnedTxnOp,
                String,
                TableDescription,
                Option<String>,
                crate::transact::ParticipantRow,
            )> = Vec::with_capacity(pre_resolved.len());
            for (op, data_table, desc, ttl_attr) in pre_resolved {
                let row_key =
                    crate::data::encoding::row_key::encode_key(op.lookup_key(), &desc.key_schema)?;
                let participant = crate::transact::ParticipantRow {
                    data_table: data_table.clone(),
                    row_key,
                };
                resolved.push((op, data_table, desc, ttl_attr, participant));
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

                let wait_deadline =
                    Instant::now() + Duration::from_secs(self.intent_timeout_secs.max(2) + 2);
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
                            if let Err(e) = self.roll_forward(&txn_id, &ts).await {
                                tracing::warn!(
                                    txn_id = %txn_id,
                                    error = %e,
                                    "post-commit replay roll_forward deferred to sweeper"
                                );
                            }
                            if ts.token_meta.is_none() {
                                let after_ts = ts.state_ts_micros.max(ts.started_at.unwrap_or(0));
                                let _ = self
                                    .persist_idempotency_if_unexpired(
                                        acct, tok, fp, &txn_id, after_ts,
                                    )
                                    .await;
                            }
                            return Err(StorageError::IdempotentReplay);
                        }
                        "CLEANED" => {
                            let after_ts = ts.state_ts_micros.max(ts.started_at.unwrap_or(0));
                            if let Some(tm) = &ts.token_meta {
                                let _ = self
                                    .persist_idempotency_if_unexpired(
                                        &tm.account_id,
                                        &tm.token,
                                        &tm.fingerprint,
                                        &txn_id,
                                        after_ts,
                                    )
                                    .await;
                            } else {
                                let _ = self
                                    .persist_idempotency_if_unexpired(
                                        acct, tok, fp, &txn_id, after_ts,
                                    )
                                    .await;
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
                            #[allow(clippy::cast_possible_wrap)]
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

                            if Instant::now() >= wait_deadline {
                                return Err(Self::transaction_conflict_canceled(
                                    resolved.len(),
                                    0,
                                    "concurrent transaction with same token is pending",
                                ));
                            }
                            let jitter = rand::random_range(
                                0..u64::try_from(delay.as_millis()).unwrap_or(15) + 1,
                            );
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

                let jitter =
                    rand::random_range(0..u64::try_from(txn_delay.as_millis()).unwrap_or(15) + 1);
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

            let mut reasons: Vec<CancellationReason> = Vec::with_capacity(resolved.len());
            let mut staged_items: Vec<Option<Option<Item>>> = Vec::with_capacity(resolved.len());
            let mut any_failed = false;

            for (idx, (op, _, desc, _, _)) in resolved.iter().enumerate() {
                use extenddb_core::validation;

                let existing = &items_read[idx];
                let cond_reason =
                    match check_condition(existing.as_ref(), op.condition.as_ref(), &op.maps) {
                        Ok(()) => None,
                        Err(StorageError::ConditionFailed(prior)) => {
                            let item_for_reason = match op.return_values_on_ccf {
                                ReturnValuesOnConditionCheckFailure::AllOld => prior,
                                ReturnValuesOnConditionCheckFailure::None => None,
                            };
                            Some(CancellationReason::condition_check_failed_with_item(
                                item_for_reason,
                            ))
                        }
                        Err(StorageError::Validation(msg)) => {
                            Some(CancellationReason::validation_error(msg))
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

                if let Some(r) = cond_reason {
                    any_failed = true;
                    reasons.push(r);
                    staged_items.push(None);
                    continue;
                }

                match &op.kind {
                    OwnedTxnKind::Put { item } => {
                        reasons.push(CancellationReason::none());
                        staged_items.push(Some(Some(item.clone())));
                    }
                    OwnedTxnKind::Delete { .. } => {
                        reasons.push(CancellationReason::none());
                        staged_items.push(Some(None));
                    }
                    OwnedTxnKind::ConditionCheck { .. } => {
                        reasons.push(CancellationReason::none());
                        staged_items.push(None);
                    }
                    OwnedTxnKind::Update { key, actions } => {
                        let base = existing.clone().unwrap_or_else(|| key.clone());
                        let eval_res: Result<Option<Option<Item>>, CancellationReason> = (|| {
                            let updated = apply_update_actions_validated(
                                &base,
                                actions,
                                &op.maps,
                                &op.key_info,
                                &desc.attribute_definitions,
                            )
                            .map_err(|e| match e {
                                StorageError::Validation(m) => {
                                    CancellationReason::validation_error(m)
                                }
                                other => CancellationReason::validation_error(other.to_string()),
                            })?;
                            validation::validate_transact_update_item_size(
                                &updated,
                                self.max_item_size_bytes,
                            )
                            .map_err(|e| CancellationReason::validation_error(e.to_string()))?;
                            let idx_refs = Self::index_key_refs(desc);
                            if !idx_refs.is_empty() {
                                validation::validate_index_key_types_after_update(
                                    &updated,
                                    &idx_refs,
                                    &desc.attribute_definitions,
                                )
                                .map_err(|e| {
                                    CancellationReason::key_type_mismatch(e.to_string())
                                })?;
                                validation::validate_index_key_not_empty(
                                    &updated,
                                    &idx_refs,
                                    validation::SecondaryIndexEmptyContext::UpdateExpression,
                                )
                                .map_err(|e| {
                                    CancellationReason::index_key_empty_abort(e.to_string())
                                })?;
                            }
                            if existing.is_none() && removes_only(actions) {
                                Ok(None)
                            } else {
                                Ok(Some(Some(updated)))
                            }
                        })(
                        );

                        match eval_res {
                            Ok(staged) => {
                                reasons.push(CancellationReason::none());
                                staged_items.push(staged);
                            }
                            Err(r) => {
                                any_failed = true;
                                reasons.push(r);
                                staged_items.push(None);
                            }
                        }
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
                return Err(match CancellationReason::resolve_key_faults(reasons) {
                    Ok(resolved_reasons) => StorageError::TransactionCanceled(resolved_reasons),
                    Err(message) => StorageError::Validation(message),
                });
            }

            let mut log_records: Vec<crate::transact::TxnStreamRecord> = Vec::new();
            let mut ttl_mutations: Vec<crate::transact::TxnTtlMutation> = Vec::new();
            let mut txn_mutations = Vec::with_capacity(resolved.len());
            let mut committed_versions: Vec<Option<u64>> = vec![None; resolved.len()];

            for (idx, (op, data_table, desc, ttl_attr, participant)) in resolved.iter().enumerate()
            {
                let Some(new_item) = staged_items[idx].clone() else {
                    continue;
                };
                let existing = &items_read[idx];

                let shadow_plans = match Self::prepare_shadow_plan(
                    &op.key_info.key_schema,
                    data_table,
                    desc,
                    existing.as_ref(),
                    new_item.as_ref(),
                ) {
                    Ok(plans) => plans,
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

                let next_version = next_occ_version(read_versions[idx]);
                committed_versions[idx] = Some(next_version);
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
                Self::shadow_plans_to_participant_mutations(
                    &shadow_plans,
                    next_version,
                    &mut txn_mutations,
                );

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
                base_guard: None,
            };

            // Commit point has passed: log any post-commit roll_forward failure
            // so `sweeper` finishes it rather than triggering SDK double-retries (`#9`).
            if let Err(e) = self.roll_forward(&txn_id, &committed_state).await {
                tracing::warn!(
                    txn_id = %txn_id,
                    error = %e,
                    "post-commit TransactWriteItems roll_forward deferred to sweeper"
                );
            }

            if meta_read_at.elapsed() >= FAST_WRITE_BUDGET {
                for (idx, (op, data_table, desc, ttl_attr, participant)) in
                    resolved.iter().enumerate()
                {
                    if let (Some(staged_item), Some(cver)) =
                        (staged_items[idx].as_ref(), committed_versions[idx])
                    {
                        self.repair_slow_write_schema_drift(
                            &op.key_info,
                            data_table,
                            &participant.row_key,
                            desc,
                            ttl_attr.as_deref(),
                            items_read[idx].as_ref(),
                            staged_item.as_ref(),
                            cver,
                        )
                        .await;
                    }
                }
            }

            Ok(())
        })
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            let cutoff = time::OffsetDateTime::now_utc().unix_timestamp() - max_age_seconds;
            let prefix = keys::IDEMPOTENCY_SCAN_PREFIX;
            let end_open = "idem;";
            let mut start_key = prefix.to_owned();
            let mut inclusive = true;
            let mut deleted = 0u64;

            loop {
                let rows = self
                    .cat()
                    .scan_str_range(&start_key, inclusive, end_open, CATALOG_GC_PAGE_SIZE)
                    .await?;
                if rows.is_empty() {
                    break;
                }
                let batch_len = rows.len();
                let mut last_key = None;
                for (k, val) in rows {
                    last_key = Some(k.clone());
                    if val
                        .get("applied_at")
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|ts| ts < cutoff)
                    {
                        let removed = self
                            .cat()
                            .mutate_json_cas(&k, std::convert::identity, move |cur| {
                                if cur
                                    .as_ref()
                                    .and_then(|c| c.get("applied_at"))
                                    .and_then(serde_json::Value::as_i64)
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
                if i64::try_from(batch_len).unwrap_or(i64::MAX) < CATALOG_GC_PAGE_SIZE {
                    break;
                }
                if let Some(last) = last_key {
                    start_key = last;
                    inclusive = false;
                } else {
                    break;
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
                .await?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            let (status, attr, _) = parse_ttl_status_and_attr(&row);
            Ok(TimeToLiveDescription {
                time_to_live_status: status,
                attribute_name: match status {
                    TimeToLiveStatus::Enabling | TimeToLiveStatus::Enabled => attr,
                    TimeToLiveStatus::Disabling | TimeToLiveStatus::Disabled => None,
                },
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
            let generation = Uuid::new_v4().to_string();
            self.cat()
                .mutate_json_cas(&meta_key, std::convert::identity, move |existing| {
                    let mut row =
                        existing.ok_or_else(|| StorageError::TableNotFound(tbl_for_err.clone()))?;
                    if let Some(obj) = row.as_object_mut() {
                        if enabled {
                            // Report `ENABLING` until `create_ttl_index` completes
                            // indexing pre-existing items (`#8`).
                            obj.insert(
                                "ttl".to_string(),
                                json!({
                                    "status": "ENABLING",
                                    "enabled": true,
                                    "attribute": attr_for_cas,
                                    "generation": generation,
                                }),
                            );
                        } else {
                            obj.insert(
                                "ttl".to_string(),
                                json!({
                                    "status": "DISABLED",
                                    "enabled": false,
                                    "attribute": serde_json::Value::Null,
                                    "generation": generation,
                                }),
                            );
                        }
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
                .mutate_json_cas(&key, std::convert::identity, move |existing| {
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
                .mutate_json_cas(&key, std::convert::identity, move |existing| {
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
            let Some(cur) = self.cat().get(&key).await? else {
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
                .await?;
            let mut out = Vec::new();
            for (key, value) in rows {
                let (status, Some(attr), _) = parse_ttl_status_and_attr(&value) else {
                    continue;
                };
                if !matches!(
                    status,
                    TimeToLiveStatus::Enabling | TimeToLiveStatus::Enabled
                ) {
                    continue;
                }
                let Some(table_name) = key.rsplit_once(':').map(|(_, n)| n.to_owned()) else {
                    continue;
                };
                out.push((table_name, attr));
            }
            Ok(out)
        })
    }

    fn all_tables_with_ttl(&self) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        Box::pin(async move {
            let rows = self.cat().scan_prefix(keys::TABLE_META_SCAN_PREFIX).await?;
            let mut out = Vec::new();
            for (key, value) in rows {
                let mut parts = key.splitn(3, ':');
                let _ = parts.next();
                let (Some(acct_id), Some(table_name)) = (parts.next(), parts.next()) else {
                    continue;
                };
                let (status, Some(attr), _) = parse_ttl_status_and_attr(&value) else {
                    continue;
                };
                if matches!(
                    status,
                    TimeToLiveStatus::Enabling | TimeToLiveStatus::Enabled
                ) {
                    out.push((acct_id.to_string(), table_name.to_string(), attr));
                }
            }
            Ok(out)
        })
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        Box::pin(async move {
            let rows = self.cat().scan_prefix(keys::TABLE_META_SCAN_PREFIX).await?;
            let mut out = Vec::new();
            for (key, value) in rows {
                let mut parts = key.splitn(3, ':');
                let _ = parts.next();
                let (Some(acct_id), Some(table_name)) = (parts.next(), parts.next()) else {
                    continue;
                };
                let (status, Some(attr), _) = parse_ttl_status_and_attr(&value) else {
                    continue;
                };
                if status == TimeToLiveStatus::Enabled {
                    out.push((acct_id.to_string(), table_name.to_string(), attr));
                }
            }
            Ok(out)
        })
    }

    fn create_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let ttl_attribute = ttl_attribute.to_owned();
        Box::pin(async move {
            let token = CancellationToken::new();
            self.run_ttl_backfill_locked(&account_id, &table_name, &ttl_attribute, &token)
                .await
        })
    }

    fn drop_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            self.purge_ttl_entries_for_table(&account_id, &table_name)
                .await
        })
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
                .await?;
            Ok(rows
                .into_iter()
                .filter_map(|(k, _)| k.rsplit_once(':').map(|(_, n)| n.to_owned()))
                .collect())
        })
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async move {
            let rows = self.cat().scan_prefix(keys::TABLE_META_SCAN_PREFIX).await?;
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
            crate::streams::write_record(&self.cat(), &target_shard, &record).await
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
                .await?
            else {
                return Err(extenddb_storage::util::stream_does_not_exist());
            };
            let desc = deserialize_table_description_row(&table_row)?;

            let Some(shard_val) = self.cat().get(&keys::stream_shard(&shard_id)).await? else {
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
                    let all =
                        crate::streams::load_table_shards(&self.cat(), &desc.table_id).await?;
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

            // Bounded server-side range read starting strictly after `after_sequence` (`#23`),
            // clamped at `shard_meta.ending_sequence_number` when the shard is closed.
            if let (Some(after_seq), Some(end_seq)) = (
                after.as_deref(),
                shard_meta.ending_sequence_number.as_deref(),
            ) && after_seq >= end_seq
            {
                return Ok((Vec::new(), None));
            }
            let prefix = keys::stream_record_shard_prefix(&shard_id);
            let end_open = match shard_meta.ending_sequence_number.as_deref() {
                Some(end_seq) => format!("{prefix}{end_seq}\x00"),
                None => format!("stream_record:{shard_id};"),
            };
            let (start_key, start_inclusive) = match &after {
                Some(seq) => (format!("{prefix}{seq}"), false),
                None => (prefix, true),
            };
            let fetch_limit = if limit > 0 { limit } else { 1_000 };
            let rows = self
                .cat()
                .scan_str_range(&start_key, start_inclusive, &end_open, fetch_limit)
                .await?;

            let mut records: Vec<StreamRecord> = Vec::with_capacity(rows.len());
            for (_key, value) in rows {
                if let Ok(record) = serde_json::from_value::<StreamRecord>(value) {
                    if shard_meta
                        .ending_sequence_number
                        .as_deref()
                        .is_some_and(|end_seq| record.dynamodb.sequence_number.as_str() > end_seq)
                    {
                        continue;
                    }
                    records.push(record);
                }
            }
            records.sort_by(|a, b| a.dynamodb.sequence_number.cmp(&b.dynamodb.sequence_number));
            if limit > 0 && records.len() > usize::try_from(limit).unwrap_or(usize::MAX) {
                records.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
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
                .await?
                .ok_or_else(|| extenddb_storage::util::stream_not_found(&arn))?;
            let desc = deserialize_table_description_row(&row)?;
            let shards = crate::streams::load_table_shards(&self.cat(), &desc.table_id).await?;
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
                .await?;
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
                let shards = crate::streams::load_table_shards(&self.cat(), &desc.table_id).await?;
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
            let mut deleted = 0u64;

            let mut start_key = "stream_record:".to_owned();
            let mut inclusive = true;
            loop {
                let rows = self
                    .cat()
                    .scan_str_range(
                        &start_key,
                        inclusive,
                        "stream_record;",
                        CATALOG_GC_PAGE_SIZE,
                    )
                    .await?;
                if rows.is_empty() {
                    break;
                }
                let batch_len = rows.len();
                let mut last_key = None;
                for (k, value) in rows {
                    last_key = Some(k.clone());
                    if let Ok(rec) = serde_json::from_value::<StreamRecord>(value)
                        && rec.dynamodb.approximate_creation_date_time < cutoff
                    {
                        let _ = crate::streams::ensure_shard_past_record(&self.cat(), &k).await;
                        if self.cat().delete(&k).await.is_ok() {
                            deleted += 1;
                        }
                    }
                }
                if i64::try_from(batch_len).unwrap_or(i64::MAX) < CATALOG_GC_PAGE_SIZE {
                    break;
                }
                if let Some(last) = last_key {
                    start_key = last;
                    inclusive = false;
                } else {
                    break;
                }
            }

            let mut dedup_start = keys::STREAM_DEDUP_SCAN_PREFIX.to_owned();
            let mut dedup_inclusive = true;
            loop {
                let Ok(dedups) = self
                    .cat()
                    .scan_str_range(
                        &dedup_start,
                        dedup_inclusive,
                        "stream_dedup;",
                        CATALOG_GC_PAGE_SIZE,
                    )
                    .await
                else {
                    break;
                };
                if dedups.is_empty() {
                    break;
                }
                let batch_len = dedups.len();
                let mut last_key = None;
                for (dk, dval) in dedups {
                    last_key = Some(dk.clone());
                    if dval
                        .get("created_at")
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|ts| ts < cutoff)
                    {
                        let _ = self
                            .cat()
                            .mutate_json_cas(&dk, std::convert::identity, move |cur| {
                                if cur
                                    .as_ref()
                                    .and_then(|c| c.get("created_at"))
                                    .and_then(serde_json::Value::as_i64)
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
                if i64::try_from(batch_len).unwrap_or(i64::MAX) < CATALOG_GC_PAGE_SIZE {
                    break;
                }
                if let Some(last) = last_key {
                    dedup_start = last;
                    dedup_inclusive = false;
                } else {
                    break;
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
            let shards = crate::streams::load_table_shards(&self.cat(), &desc.table_id).await?;
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
                .await?
            {
                Some(row) => {
                    let desc = deserialize_table_description_row(&row)?;
                    let shards =
                        crate::streams::load_table_shards(&self.cat(), &desc.table_id).await?;
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
        Box::pin(
            async move { crate::streams::latest_sequence_for_shard(&self.cat(), &shard).await },
        )
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
    let ttl_key = encode_ttl_key(account_id, table_name, base_row_key, expiry);
    let mut mutations = Vec::with_capacity(2);
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
        table_name: client.full_table_name(TTL_INDEX_TABLE),
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
    let ttl_key = encode_ttl_key(account_id, table_name, base_row_key, expiry);
    let req = MutateRowRequest {
        table_name: client.full_table_name(TTL_INDEX_TABLE),
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
