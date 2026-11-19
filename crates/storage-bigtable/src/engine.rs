// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BigtableEngine`: implements the `StorageEngine` sub-traits (`TableEngine`,
//! `DataEngine`, `MetadataEngine`, `StreamEngine`, `WorkerStore`, plus backup
//! refusal stubs in `crate::backup_stubs`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction, removes_only};
use extenddb_core::types::{
    AttributeValue, BillingMode, BillingModeSummary, CreateTableInput, DeleteTableInput,
    DescribeStreamInput, DescribeTableInput, GsiDescription, IndexInfo, IndexType, Item,
    ListTablesInput, ListTablesOutput, LsiDescription, ProvisionedThroughputDescription,
    SseDescription, StreamDescription, StreamRecord, TableDescription, TableKeyInfo, TableStatus,
    Tag, TimeToLiveDescription, TimeToLiveStatus, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    DataEngine, ItemPairResult, MetadataEngine, QueryResult, StreamCapture, StreamEngine,
    StreamListResult, StreamRecordsResult, TableEngine, TransactGetOp, TransactWriteOp,
    TtlTableInfo, WorkerStore,
};
use futures::future::BoxFuture;
use moka::future::Cache;
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::catalog::{Catalog, keys};
use crate::data::admin::AdminClient;
use crate::data::client::{
    BigtableClient, map_bigtable_err, map_catalog_err, monotonic_timestamp_micros,
};
use crate::data::item_ops::{
    CasOutcome, ItemOps, apply_update_actions_validated, check_condition, next_occ_version,
    parse_ttl_epoch_secs, version_to_timestamp_micros,
};
use crate::data::query_scan::{QueryScan, key_belongs_to_segment};

const DEFAULT_REGION: &str = "us-east-1";
const MAX_OCC_ATTEMPTS: usize = 16;

/// Maps a logical `(account_id, table_name)` pair's `table_id` UUID to its
/// BigTable short table name (`t` + 16 hex chars).
fn data_table_id(table_id: &str) -> String {
    format!("t{}", &table_id.replace('-', "")[..16])
}

fn now_unix_f64() -> f64 {
    time::OffsetDateTime::now_utc().unix_timestamp() as f64
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
                stream,
                ..
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::Put {
                    item: (*item).clone(),
                },
                condition: condition.cloned(),
                maps: (*maps).clone(),
                stream: stream.clone(),
            },
            TransactWriteOp::Delete {
                key_info,
                key,
                condition,
                maps,
                stream,
                ..
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::Delete {
                    key: (*key).clone(),
                },
                condition: condition.cloned(),
                maps: (*maps).clone(),
                stream: stream.clone(),
            },
            TransactWriteOp::Update {
                key_info,
                key,
                actions,
                condition,
                maps,
                stream,
                ..
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::Update {
                    key: (*key).clone(),
                    actions: actions.to_vec(),
                },
                condition: condition.cloned(),
                maps: (*maps).clone(),
                stream: stream.clone(),
            },
            TransactWriteOp::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                ..
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::ConditionCheck {
                    key: (*key).clone(),
                },
                condition: Some((*condition).clone()),
                maps: (*maps).clone(),
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

pub struct BigtableEngine {
    catalog_client: Arc<BigtableClient>,
    client: Arc<BigtableClient>,
    intent_timeout_secs: u64,
    metadata_cache: Cache<(String, String), (String, TableDescription, Option<String>)>,
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
            node_id,
            region,
        }
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
        let desc: TableDescription = serde_json::from_value(
            row.get("description")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        )
        .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;

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

    /// Write/refresh shadow entries for every GSI at the deterministic OCC
    /// timestamp `version_to_timestamp_micros(new_version)`.
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
        let ts_micros = version_to_timestamp_micros(new_version);
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
                    && let Err(e) = ops
                        .mutate_cells(
                            old,
                            vec![
                                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Mutation {
                                    mutation: Some(
                                        googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
                                            googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromRow {},
                                        ),
                                    ),
                                },
                            ],
                        )
                        .await
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
                    let mutations = ops.item_to_mutations_at(&projected, true, ts_micros)?;
                    if let Err(e) = ops.mutate_cells(row_key, mutations).await {
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
                if let Err(e) = ops
                    .mutate_cells(
                        row_key,
                        vec![
                            googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Mutation {
                                mutation: Some(
                                    googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
                                        googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromRow {},
                                    ),
                                ),
                            },
                        ],
                    )
                    .await
                {
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

    async fn maintain_ttl_index(
        &self,
        key_info: &TableKeyInfo,
        ttl_attr: Option<&str>,
        existing: Option<&Item>,
        new_item: Option<&Item>,
    ) -> Result<(), StorageError> {
        let Some(attr_name) = ttl_attr else {
            return Ok(());
        };
        let old_exp = existing.and_then(|prior| get_ttl_expiry(prior, attr_name));
        let new_exp = new_item.and_then(|next| get_ttl_expiry(next, attr_name));
        if old_exp == new_exp {
            return Ok(());
        }
        if let (Some(old_expiry), Some(prior_item)) = (old_exp, existing) {
            let base_row_key =
                crate::data::encoding::row_key::encode_key(prior_item, &key_info.key_schema)?;
            delete_ttl_index_entry(
                &self.client,
                &key_info.account_id,
                &key_info.table_name,
                &base_row_key,
                old_expiry,
            )
            .await?;
        }
        if let (Some(new_expiry), Some(next_item)) = (new_exp, new_item) {
            let base_row_key =
                crate::data::encoding::row_key::encode_key(next_item, &key_info.key_schema)?;
            insert_ttl_index_entry(
                &self.client,
                &key_info.account_id,
                &key_info.table_name,
                &base_row_key,
                new_expiry,
            )
            .await?;
        }
        Ok(())
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

    /// Execute a single-item write (`PutItem` / `UpdateItem` / `DeleteItem`)
    /// through the `__extenddb_txn_log__` coordinator fence whenever streams
    /// are enabled on the table.
    ///
    /// Guarantees that the base-row mutation AND the stream record share one
    /// atomic commit point (`COMMITTED` in `__extenddb_txn_log__`): a crash
    /// before `COMMITTED` rolls back cleanly with neither applied, and a crash
    /// after `COMMITTED` rolls both forward idempotently via `roll_forward`.
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
        stop_after_commit_for_test: bool,
    ) -> Result<(Option<Item>, Option<Item>, String), StorageError> {
        let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
        let row_key =
            crate::data::encoding::row_key::encode_key(op.lookup_key(), &key_info.key_schema)?;
        let participant = crate::transact::ParticipantRow {
            data_table: data_table.to_owned(),
            row_key,
        };
        let coord = crate::transact::TxnCoordinator::new(
            &self.client,
            Duration::from_secs(self.intent_timeout_secs.max(1)),
        );
        let txn_id = crate::transact::TxnCoordinator::new_txn_id();

        let (_, started_at_micros) = coord
            .open(&txn_id, std::slice::from_ref(&participant))
            .await?;

        let mut outer_attempts = 0;
        let max_outer_attempts = 3;
        let mut backoff = Duration::from_millis(25);
        loop {
            match coord.place_intent(&txn_id, &participant).await {
                Ok(true) => break,
                Ok(false) => {
                    outer_attempts += 1;
                    if outer_attempts >= max_outer_attempts {
                        let _ = coord.drop(&txn_id).await;
                        return Err(StorageError::TransactionConflict(
                            "concurrent transaction holds an intent on this row".to_string(),
                        ));
                    }
                    let jitter = rand::random_range(0..backoff.as_millis() as u64);
                    tokio::time::sleep(backoff + Duration::from_millis(jitter)).await;
                    backoff *= 2;
                }
                Err(e) => {
                    let _ = coord.clear_intent(&txn_id, &participant).await;
                    let _ = coord.drop(&txn_id).await;
                    return Err(e);
                }
            }
        }

        // Helper closure to roll back our placed intent + coordinator row on pre-commit exit.
        let abort_cleanly = |txn_id: String, participant: crate::transact::ParticipantRow| async move {
            if coord.clear_intent(&txn_id, &participant).await.is_ok() {
                let _ = coord.drop(&txn_id).await;
            } else {
                let _ = coord.aborted(&txn_id, started_at_micros).await;
            }
        };

        let ops = ItemOps::new(&self.client, data_table, self.intent_timeout_secs);
        let (existing, read_version) = match ops.get_with_version(key_info, op.lookup_key()).await {
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
                    return Ok((None, None, txn_id));
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
                    return Ok((None, None, txn_id));
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
            payload: base_payload.clone(),
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

        let mut log_records: Vec<crate::transact::TxnStreamRecord> = Vec::new();
        let representative = new_item
            .as_ref()
            .or(existing.as_ref())
            .unwrap_or_else(|| op.lookup_key());
        match Self::active_stream_target_for_capture(desc, stream, representative) {
            Ok(Some((arn, shard_id))) => {
                let seq = crate::streams::next_sequence_number();
                if let Some(record) = crate::streams::build_record(
                    desc.stream_specification.as_ref(),
                    stream,
                    &key_info.key_schema,
                    existing.as_ref(),
                    new_item.as_ref(),
                    &seq,
                    &self.region,
                ) {
                    log_records.push(crate::transact::TxnStreamRecord {
                        stream_arn: arn,
                        shard_id,
                        record,
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
            .commit(
                &txn_id,
                started_at_micros,
                &txn_mutations,
                Some(&log_records),
            )
            .await
        {
            Ok(ts) => ts,
            Err(e) => {
                abort_cleanly(txn_id, participant).await;
                return Err(e);
            }
        };

        if stop_after_commit_for_test {
            return Ok((existing, new_item, txn_id));
        }

        // Phase 2: apply fenced base-row mutation, GSI shadows, TTL index, and
        // durable stream records while m:intent:<txn_id> stays held. Only clear
        // the intent and mark CLEANED after stream records succeed so the 2PC
        // sweeper recovers any interrupted emission without reordering events.
        let applied = coord
            .apply_fenced_mutation(
                &txn_id,
                &participant,
                &base_payload,
                read_version,
                row_existed,
                next_version,
            )
            .await?;
        if !applied {
            return Err(StorageError::TransactionConflict(
                "fenced single-item write superseded during apply".to_string(),
            ));
        }

        match &new_item {
            Some(item) => {
                self.write_shadows_only(
                    key_info,
                    data_table,
                    &gsis,
                    item,
                    existing.as_ref(),
                    next_version,
                )
                .await?;
            }
            None => {
                if let Some(prior) = existing.as_ref() {
                    self.delete_shadows(key_info, data_table, &gsis, prior)
                        .await?;
                }
            }
        }

        self.maintain_ttl_index(key_info, ttl_attr, existing.as_ref(), new_item.as_ref())
            .await?;

        for r in &log_records {
            crate::streams::write_record(&self.cat(), &r.shard_id, &r.record)
                .await
                .map_err(map_catalog_err)?;
        }

        let _ = coord.clear_intent(&txn_id, &participant).await;
        let _ = coord.cleaned(&txn_id, commit_ts_micros).await;
        let _ = coord.drop(&txn_id).await;

        Ok((existing, new_item, txn_id))
    }

    /// Test-only entrypoint simulating a process crash immediately after the
    /// coordinator `COMMITTED` point of a stream-enabled `PutItem` (before base
    /// row or stream record apply). Returns the `txn_id` in `__extenddb_txn_log__`.
    #[doc(hidden)]
    pub async fn debug_fenced_put_commit_only(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        stream: Option<&StreamCapture>,
    ) -> Result<String, StorageError> {
        let (data_table, desc, ttl_attr) = self.table_full_meta_for(key_info).await?;
        let empty_maps = ExpressionMaps::default();
        let (_, _, txn_id) = self
            .execute_fenced_single_item_write(
                key_info,
                &data_table,
                &desc,
                ttl_attr.as_deref(),
                SingleWriteOp::Put { item },
                None,
                &empty_maps,
                stream,
                true,
            )
            .await?;
        Ok(txn_id)
    }

    /// Test-only entrypoint simulating a process crash *before* the coordinator
    /// `COMMITTED` point of a stream-enabled `PutItem` (`PENDING` coordinator
    /// row + `m:intent:<txn_id>` placed, no commit). Returns `txn_id`.
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
        let txn_id = crate::transact::TxnCoordinator::new_txn_id();
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

        if let Some(muts) = &state.mutations {
            let mut last_base_still_current = true;
            for m in muts {
                let is_base_participant = state.participants.as_ref().is_some_and(|parts| {
                    parts.iter().any(|p| {
                        p.data_table == m.participant.data_table
                            && p.row_key == m.participant.row_key
                    })
                });

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
                    } else {
                        let ops = ItemOps::new(
                            client,
                            &m.participant.data_table,
                            self.intent_timeout_secs,
                        );
                        let (_, cur_ver) = ops
                            .get_by_raw_key_with_version(&m.participant.row_key)
                            .await?;
                        last_base_still_current = cur_ver == Some(new_version);
                    }
                } else {
                    if !last_base_still_current {
                        continue;
                    }
                    let ops =
                        ItemOps::new(client, &m.participant.data_table, self.intent_timeout_secs);
                    let ts_micros = version_to_timestamp_micros(new_version);
                    match &m.payload {
                        crate::transact::TxnOpPayload::Put { item } => {
                            let muts_list = ops.item_to_mutations_at(item, true, ts_micros)?;
                            ops.mutate_cells(m.participant.row_key.clone(), muts_list)
                                .await?;
                        }
                        crate::transact::TxnOpPayload::Delete => {
                            ops.mutate_cells(
                                m.participant.row_key.clone(),
                                vec![
                                    googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Mutation {
                                        mutation: Some(
                                            googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
                                                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromRow {},
                                            ),
                                        ),
                                    },
                                ],
                            )
                            .await?;
                        }
                    }
                }
            }
        }

        if let Some(records) = &state.stream_records {
            let cat = self.cat();
            for r in records {
                if !r.shard_id.is_empty() {
                    crate::streams::write_record(&cat, &r.shard_id, &r.record)
                        .await
                        .map_err(map_catalog_err)?;
                }
            }
        }

        if let Some(parts) = &state.participants {
            for p in parts {
                let _ = coord.clear_intent(txn_id, p).await;
            }
        }

        let after_ts = state.state_ts_micros.max(state.started_at.unwrap_or(0));
        coord.cleaned(txn_id, after_ts).await?;
        coord.drop(txn_id).await?;

        Ok(())
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
            if self
                .cat()
                .get(&keys::table_meta(&account_id, &input.table_name))
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

            if let Some(gsis) = &input.global_secondary_indexes {
                for g in gsis {
                    let shadow = crate::gsi::shadow_table_id(&data_table, &g.index_name);
                    admin
                        .create_table(&shadow, &families)
                        .await
                        .map_err(map_catalog_err)?;
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
                        warm_throughput_record: None,
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
                    shards.push(crate::streams::ShardMeta {
                        table_id: table_id.clone(),
                        shard_id: sid,
                        parent_shard_id: None,
                        starting_sequence_number: "0".repeat(crate::streams::SEQUENCE_NUMBER_WIDTH),
                        ending_sequence_number: None,
                    });
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
                ..TableDescription::default()
            };

            for shard_meta in &initial_shards {
                crate::streams::put_shard(&self.cat(), shard_meta)
                    .await
                    .map_err(map_catalog_err)?;
            }

            let record = json!({
                "description": description,
                "data_table": data_table,
                "ttl": {"enabled": false, "attribute": null},
            });
            self.cat()
                .put(&keys::table_meta(&account_id, &input.table_name), &record)
                .await
                .map_err(map_catalog_err)?;

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

            let description: TableDescription = serde_json::from_value(
                row.get("description")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            )
            .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;

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
            let description: TableDescription = serde_json::from_value(
                row.get("description")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            )
            .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;
            Ok(description)
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
            let mut row = self
                .cat()
                .get(&keys::table_meta(&account_id, &input.table_name))
                .await
                .map_err(map_catalog_err)?
                .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
            let mut description: TableDescription = serde_json::from_value(
                row.get("description")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            )
            .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;

            if let Some(b) = input.billing_mode {
                description.billing_mode_summary = Some(BillingModeSummary {
                    billing_mode: b,
                    last_update_to_pay_per_request_date_time: Some(now_unix_f64()),
                });
            }
            if let Some(pt) = input.provisioned_throughput {
                description.provisioned_throughput.read_capacity_units = pt.read_capacity_units;
                description.provisioned_throughput.write_capacity_units = pt.write_capacity_units;
            }
            if let Some(odt) = input.on_demand_throughput {
                description.on_demand_throughput = Some(odt);
            }
            if let Some(d) = input.deletion_protection_enabled {
                description.deletion_protection_enabled = d;
            }
            if let Some(spec) = input.stream_specification {
                let existing_shards =
                    crate::streams::load_table_shards(&self.cat(), &description.table_id)
                        .await
                        .map_err(map_catalog_err)?;
                let lineage_rows: Vec<_> = existing_shards
                    .iter()
                    .map(crate::streams::ShardMeta::to_lineage_row)
                    .collect();

                if let Some(label_to_close) =
                    extenddb_storage::stream_lineage::disabled_stream_to_close(
                        description.stream_specification.as_ref(),
                        description.latest_stream_label.as_deref(),
                        &spec,
                    )
                {
                    let to_close = extenddb_storage::stream_lineage::open_shard_ids_of(
                        &description.table_id,
                        label_to_close,
                        &lineage_rows,
                    );
                    crate::streams::close_shards_by_id(&self.cat(), &to_close)
                        .await
                        .map_err(map_catalog_err)?;
                }

                let change = extenddb_storage::stream_lineage::stream_change(
                    &input.table_name,
                    description.stream_specification.as_ref(),
                    &spec,
                )?;
                let next_spec = extenddb_storage::stream_lineage::stored_specification(
                    change,
                    description.stream_specification.as_ref(),
                    &spec,
                );

                match change {
                    extenddb_storage::stream_lineage::StreamChange::Enable(view) => {
                        let label = extenddb_storage::stream_lineage::next_stream_label(
                            description.latest_stream_label.as_deref(),
                        );
                        let arn = crate::streams::build_stream_arn(
                            &self.region,
                            &account_id,
                            &input.table_name,
                            &label,
                        );
                        let older = extenddb_storage::stream_lineage::open_shard_ids_before(
                            &description.table_id,
                            &label,
                            &lineage_rows,
                        );
                        crate::streams::close_shards_by_id(&self.cat(), &older)
                            .await
                            .map_err(map_catalog_err)?;
                        for idx in 0..extenddb_storage::stream_lineage::SHARDS_PER_STREAM {
                            let sid = extenddb_storage::stream_lineage::shard_id(
                                &description.table_id,
                                &label,
                                view,
                                idx,
                            )?;
                            let shard_meta = crate::streams::ShardMeta {
                                table_id: description.table_id.clone(),
                                shard_id: sid,
                                parent_shard_id: None,
                                starting_sequence_number: "0"
                                    .repeat(crate::streams::SEQUENCE_NUMBER_WIDTH),
                                ending_sequence_number: None,
                            };
                            crate::streams::put_shard(&self.cat(), &shard_meta)
                                .await
                                .map_err(map_catalog_err)?;
                        }
                        description.latest_stream_arn = Some(arn);
                        description.latest_stream_label = Some(label);
                    }
                    extenddb_storage::stream_lineage::StreamChange::Disable => {
                        if let Some(label) = description.latest_stream_label.as_deref() {
                            let to_close = extenddb_storage::stream_lineage::open_shard_ids_of(
                                &description.table_id,
                                label,
                                &lineage_rows,
                            );
                            crate::streams::close_shards_by_id(&self.cat(), &to_close)
                                .await
                                .map_err(map_catalog_err)?;
                        }
                    }
                }
                description.stream_specification = Some(next_spec);
            }

            if let Some(obj) = row.as_object_mut() {
                let desc_val = serde_json::to_value(&description)
                    .map_err(|e| StorageError::Internal(format!("serialize description: {e}")))?;
                obj.insert("description".to_string(), desc_val);
            }
            self.cat()
                .put(&keys::table_meta(&account_id, &input.table_name), &row)
                .await
                .map_err(map_catalog_err)?;
            self.metadata_cache
                .invalidate(&(account_id, input.table_name))
                .await;
            Ok(description)
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
                let Ok(desc) = serde_json::from_value::<TableDescription>(
                    value
                        .get("description")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                ) else {
                    continue;
                };
                if desc.table_id != table_id {
                    continue;
                }
                // `table_meta:<account_id>:<table_name>`
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
                        false,
                    )
                    .await?;
                return Ok(if return_old { existing } else { None });
            }

            let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
            let ops = ItemOps::new(&self.client, &data_table, self.intent_timeout_secs);

            for attempt in 0..MAX_OCC_ATTEMPTS {
                let (existing, read_version) = ops.get_with_version(&key_info, &item).await?;
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
                        )
                        .await?;
                        return Ok(if return_old { existing } else { None });
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
                        false,
                    )
                    .await?;
                return Ok(if return_old { existing } else { None });
            }

            let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
            let ops = ItemOps::new(&self.client, &data_table, self.intent_timeout_secs);

            for attempt in 0..MAX_OCC_ATTEMPTS {
                let (existing, read_version) = ops.get_with_version(&key_info, &key).await?;
                check_condition(existing.as_ref(), condition.as_ref(), &maps)?;
                if existing.is_none() && condition.is_none() {
                    return Ok(None);
                }

                match ops
                    .delete_cas(&key_info, &key, read_version, existing.is_some())
                    .await?
                {
                    CasOutcome::Applied(_) => {
                        if let Some(prior) = existing.as_ref() {
                            self.delete_shadows(&key_info, &data_table, &gsis, prior)
                                .await?;
                            self.maintain_ttl_index(
                                &key_info,
                                ttl_attr.as_deref(),
                                Some(prior),
                                None,
                            )
                            .await?;
                        }
                        return Ok(if return_old { existing } else { None });
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
                        false,
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

            let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
            let ops = ItemOps::new(&self.client, &data_table, self.intent_timeout_secs);

            for attempt in 0..MAX_OCC_ATTEMPTS {
                let (existing, read_version) = ops.get_with_version(&key_info, &key).await?;
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
                        )
                        .await?;
                        return Ok((
                            if return_old { existing } else { None },
                            if return_new { Some(new_item) } else { None },
                        ));
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
            match index_name {
                Some(idx_name) => {
                    if let Some(gsi) = gsis.iter().find(|g| g.index_name == idx_name) {
                        let shadow_table =
                            crate::gsi::shadow_table_id(&data_table, &gsi.index_name);
                        let gsi_key_info = TableKeyInfo {
                            table_name: format!("{}__gsi_{}", key_info.table_name, gsi.index_name),
                            account_id: key_info.account_id.clone(),
                            table_id: format!("{}::{}", key_info.table_id, gsi.index_name),
                            key_schema: gsi.key_schema.clone(),
                            base_key_schema: key_info.key_schema.clone(),
                            ..TableKeyInfo::default()
                        };
                        QueryScan::new(&self.client, &shadow_table)
                            .query(&gsi_key_info, &kc, &maps, forward, limit, esk.as_ref())
                            .await
                    } else if let Some(lsi) = lsis.iter().find(|l| l.index_name == idx_name) {
                        QueryScan::new(&self.client, &data_table)
                            .query_lsi(
                                &key_info,
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
                        .query(&key_info, &kc, &maps, forward, limit, esk.as_ref())
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
            match index_name {
                Some(idx_name) => {
                    let gsis = desc.global_secondary_indexes.unwrap_or_default();
                    let lsis = desc.local_secondary_indexes.unwrap_or_default();
                    if let Some(gsi) = gsis.iter().find(|g| g.index_name == idx_name) {
                        let shadow_table =
                            crate::gsi::shadow_table_id(&data_table, &gsi.index_name);
                        let gsi_key_info = TableKeyInfo {
                            table_name: format!("{}__gsi_{}", key_info.table_name, gsi.index_name),
                            account_id: key_info.account_id.clone(),
                            table_id: format!("{}::{}", key_info.table_id, gsi.index_name),
                            key_schema: gsi.key_schema.clone(),
                            base_key_schema: key_info.key_schema.clone(),
                            ..TableKeyInfo::default()
                        };
                        QueryScan::new(&self.client, &shadow_table)
                            .scan(&gsi_key_info, limit, esk.as_ref(), segment, total_segments)
                            .await
                    } else if let Some(lsi) = lsis.iter().find(|l| l.index_name == idx_name) {
                        let (items, last) = QueryScan::new(&self.client, &data_table)
                            .scan(&key_info, limit, esk.as_ref(), segment, total_segments)
                            .await?;
                        let lsi_sk_name = lsi
                            .key_schema
                            .iter()
                            .find(|k| k.key_type == extenddb_core::types::KeyType::Range)
                            .map(|k| k.attribute_name.clone())
                            .ok_or_else(|| {
                                StorageError::Validation("LSI missing RANGE key".into())
                            })?;
                        let filtered: Vec<Item> = items
                            .into_iter()
                            .filter(|it| it.contains_key(&lsi_sk_name))
                            .collect();
                        Ok((filtered, last))
                    } else {
                        Err(StorageError::IndexNotFound(idx_name))
                    }
                }
                None => {
                    QueryScan::new(&self.client, &data_table)
                        .scan(&key_info, limit, esk.as_ref(), segment, total_segments)
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
                None => key_info.key_schema.clone(),
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

            let resolve_futures =
                owned
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(idx, (key_info, key))| async move {
                        let data_table = self.data_table_for(&key_info).await?;
                        let enc_key =
                            crate::data::encoding::row_key::encode_key(&key, &key_info.key_schema)?;
                        Ok::<_, StorageError>((idx, data_table, key_info, key, enc_key))
                    });
            let resolved_ops = futures::future::try_join_all(resolve_futures).await?;

            type ResolvedOp = (usize, TableKeyInfo, Item, Vec<u8>);
            let mut grouped: HashMap<String, Vec<ResolvedOp>> = HashMap::new();
            for (idx, data_table, key_info, key, enc_key) in resolved_ops {
                grouped
                    .entry(data_table)
                    .or_default()
                    .push((idx, key_info, key, enc_key));
            }

            let mut futures = Vec::new();
            for (data_table, group_ops) in &grouped {
                let client = self.client.clone();
                let intent_timeout_secs = self.intent_timeout_secs;
                let group_ops = group_ops.clone();
                let data_table = data_table.clone();
                futures.push(async move {
                    let ops_helper = ItemOps::new(&client, &data_table, intent_timeout_secs);
                    if let Some((_, key_info, _, _)) = group_ops.first() {
                        let keys: Vec<Item> =
                            group_ops.iter().map(|(_, _, k, _)| k.clone()).collect();
                        let results = ops_helper
                            .batch_get_with_intent_check(key_info, &keys)
                            .await?;
                        Ok::<_, StorageError>((data_table, group_ops, results))
                    } else {
                        Ok::<_, StorageError>((data_table, Vec::new(), Vec::new()))
                    }
                });
            }

            let group_results = futures::future::try_join_all(futures).await?;
            let mut out: Vec<Option<Item>> = vec![None; total_ops];

            let mut txn_states: HashMap<String, Option<crate::transact::TxnState>> = HashMap::new();
            for (_, _, results) in &group_results {
                for (_, active_txn_id) in results {
                    if let Some(tid) = active_txn_id {
                        txn_states.entry(tid.clone()).or_insert(None);
                    }
                }
            }

            if !txn_states.is_empty() {
                let coord = crate::transact::TxnCoordinator::new(
                    &self.client,
                    Duration::from_secs(self.intent_timeout_secs.max(1)),
                );
                for tid in txn_states.keys().cloned().collect::<Vec<_>>() {
                    let state = coord.get_state(&tid).await?;
                    txn_states.insert(tid, state);
                }
            }

            // Snapshot resolution rule:
            // - Intent present + txn COMMITTED -> serve logged post-image from mutation payloads.
            // - Intent present + txn PENDING/CLEANED/ABORTED/absent -> serve current d value.
            // - Intent absent -> serve current d value.
            for (data_table, group_ops, results) in group_results {
                for ((idx, _, _, enc_key), (item_from_d, active_txn_id)) in
                    group_ops.into_iter().zip(results)
                {
                    if let Some(tid) = active_txn_id
                        && let Some(Some(txn_state)) = txn_states.get(&tid)
                        && txn_state.state == "COMMITTED"
                    {
                        let mut resolved_from_log = false;
                        if let Some(mutations) = &txn_state.mutations {
                            for m in mutations {
                                if m.participant.data_table == data_table
                                    && m.participant.row_key == enc_key
                                {
                                    match &m.payload {
                                        crate::transact::TxnOpPayload::Put { item } => {
                                            out[idx] = Some(item.clone());
                                        }
                                        crate::transact::TxnOpPayload::Delete => {
                                            out[idx] = None;
                                        }
                                    }
                                    resolved_from_log = true;
                                    break;
                                }
                            }
                        }
                        if !resolved_from_log {
                            out[idx] = item_from_d;
                        }
                    } else {
                        out[idx] = item_from_d;
                    }
                }
            }

            Ok(out)
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
            if let Some((acct, tok, fp)) = &token
                && let Some(prior) = self
                    .cat()
                    .get(&keys::idempotency(acct, tok))
                    .await
                    .map_err(map_catalog_err)?
            {
                let prior_fp = prior
                    .get("fingerprint")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if prior_fp == fp {
                    return Err(StorageError::IdempotentReplay);
                } else {
                    return Err(StorageError::IdempotentMismatch);
                }
            }

            let intent_max_age = Duration::from_secs(self.intent_timeout_secs.max(1));
            let txn_id = if let Some((acct, tok, _)) = &token {
                derive_txn_id(acct, tok)
            } else {
                crate::transact::TxnCoordinator::new_txn_id()
            };
            let coord = crate::transact::TxnCoordinator::new(&self.client, intent_max_age);

            if token.is_some()
                && let Some(txn_state) = coord.get_state(&txn_id).await?
            {
                match txn_state.state.as_str() {
                    "CLEANED" => {
                        if let Some((acct, tok, fp)) = &token {
                            let record = json!({
                                "fingerprint": fp,
                                "txn_id": txn_id,
                                "applied_at": time::OffsetDateTime::now_utc().unix_timestamp(),
                            });
                            let _ = self.cat().put(&keys::idempotency(acct, tok), &record).await;
                        }
                        return Ok(());
                    }
                    "COMMITTED" => {
                        self.roll_forward(&txn_id, &txn_state).await?;
                        if let Some((acct, tok, fp)) = &token {
                            let record = json!({
                                "fingerprint": fp,
                                "txn_id": txn_id,
                                "applied_at": time::OffsetDateTime::now_utc().unix_timestamp(),
                            });
                            let _ = self.cat().put(&keys::idempotency(acct, tok), &record).await;
                        }
                        return Ok(());
                    }
                    "PENDING" => {
                        let mut attempts = 0;
                        let max_attempts = 5;
                        let mut delay = Duration::from_millis(50);
                        loop {
                            tokio::time::sleep(delay).await;
                            if let Some(ts) = coord.get_state(&txn_id).await? {
                                match ts.state.as_str() {
                                    "CLEANED" => break,
                                    "COMMITTED" => {
                                        self.roll_forward(&txn_id, &ts).await?;
                                        break;
                                    }
                                    "PENDING" => {
                                        attempts += 1;
                                        if attempts >= max_attempts {
                                            return Err(StorageError::TransactionConflict(
                                                "concurrent transaction with same token is pending"
                                                    .to_string(),
                                            ));
                                        }
                                        let jitter =
                                            rand::random_range(0..delay.as_millis() as u64);
                                        delay = delay * 2 + Duration::from_millis(jitter);
                                    }
                                    _ => {
                                        return Err(StorageError::TransactionConflict(format!(
                                            "transaction exists in state: {}",
                                            ts.state
                                        )));
                                    }
                                }
                            } else {
                                if let Some((acct, tok, _)) = &token
                                    && self
                                        .cat()
                                        .get(&keys::idempotency(acct, tok))
                                        .await
                                        .map_err(map_catalog_err)?
                                        .is_some()
                                {
                                    break;
                                }
                                return Err(StorageError::TransactionConflict(
                                    "transaction was rolled back or disappeared".to_string(),
                                ));
                            }
                        }
                        if let Some((acct, tok, fp)) = &token {
                            let record = json!({
                                "fingerprint": fp,
                                "txn_id": txn_id,
                                "applied_at": time::OffsetDateTime::now_utc().unix_timestamp(),
                            });
                            let _ = self.cat().put(&keys::idempotency(acct, tok), &record).await;
                        }
                        return Ok(());
                    }
                    _ => {
                        return Err(StorageError::TransactionConflict(format!(
                            "transaction exists in state: {}",
                            txn_state.state
                        )));
                    }
                }
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

            let mut seen_items = std::collections::HashSet::new();
            for (_, _, _, _, p) in &resolved {
                if !seen_items.insert((p.data_table.clone(), p.row_key.clone())) {
                    return Err(StorageError::Validation(
                        "Transaction request cannot include multiple operations on one item"
                            .to_string(),
                    ));
                }
            }

            let participants: Vec<crate::transact::ParticipantRow> =
                resolved.iter().map(|(_, _, _, _, p)| p.clone()).collect();
            let (_, started_at_micros) = coord.open(&txn_id, &participants).await?;

            let mut intents_placed: Vec<&crate::transact::ParticipantRow> = Vec::new();
            let mut txn_attempts = 0;
            let max_txn_attempts = 3;
            let mut txn_delay = Duration::from_millis(50);

            'outer: loop {
                intents_placed.clear();
                let mut conflict = false;
                for (_, _, _, _, participant) in &resolved {
                    match coord.place_intent(&txn_id, participant).await {
                        Ok(true) => intents_placed.push(participant),
                        Ok(false) => {
                            conflict = true;
                            break;
                        }
                        Err(e) => {
                            let mut rollback_success = true;
                            for p in &intents_placed {
                                if coord.clear_intent(&txn_id, p).await.is_err() {
                                    rollback_success = false;
                                }
                            }
                            if rollback_success {
                                let _ = coord.drop(&txn_id).await;
                            } else {
                                let _ = coord.aborted(&txn_id, started_at_micros).await;
                            }
                            return Err(e);
                        }
                    }
                }

                if !conflict {
                    break 'outer;
                }

                for p in &intents_placed {
                    let _ = coord.clear_intent(&txn_id, p).await;
                }

                txn_attempts += 1;
                if txn_attempts >= max_txn_attempts {
                    let _ = coord.drop(&txn_id).await;
                    let mut reasons: Vec<extenddb_core::types::CancellationReason> = (0..resolved
                        .len())
                        .map(|_| crate::transact::cancellation_reason("None", None, None))
                        .collect();
                    let idx = intents_placed.len();
                    if idx < reasons.len() {
                        reasons[idx] = crate::transact::cancellation_reason(
                            "TransactionConflict",
                            Some("concurrent transaction holds an intent on this row"),
                            None,
                        );
                    }
                    return Err(StorageError::TransactionCanceled(reasons));
                }

                let jitter = rand::random_range(0..txn_delay.as_millis() as u64);
                tokio::time::sleep(txn_delay + Duration::from_millis(jitter)).await;
                txn_delay *= 2;
            }

            let mut items_read: Vec<Option<Item>> = Vec::with_capacity(resolved.len());
            let mut read_versions: Vec<Option<u64>> = Vec::with_capacity(resolved.len());
            for (op, data_table, _, _, _) in &resolved {
                let (existing, version) =
                    ItemOps::new(&self.client, data_table, self.intent_timeout_secs)
                        .get_with_version(&op.key_info, op.lookup_key())
                        .await?;
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
                        reasons.push(crate::transact::cancellation_reason(
                            "ConditionalCheckFailed",
                            Some("The conditional request failed"),
                            prior,
                        ));
                    }
                    Err(e) => {
                        for p in &intents_placed {
                            let _ = coord.clear_intent(&txn_id, p).await;
                        }
                        let _ = coord.drop(&txn_id).await;
                        return Err(e);
                    }
                }
            }
            if any_failed {
                let mut rollback_success = true;
                for p in &intents_placed {
                    if coord.clear_intent(&txn_id, p).await.is_err() {
                        rollback_success = false;
                    }
                }
                if rollback_success {
                    let _ = coord.drop(&txn_id).await;
                } else {
                    let _ = coord.aborted(&txn_id, started_at_micros).await;
                }
                return Err(StorageError::TransactionCanceled(reasons));
            }

            let mut log_records: Vec<crate::transact::TxnStreamRecord> = Vec::new();
            let mut txn_mutations = Vec::with_capacity(resolved.len());
            let mut computed_writes: Vec<Option<(Option<Item>, u64)>> =
                Vec::with_capacity(resolved.len());

            for (idx, (op, data_table, desc, _, participant)) in resolved.iter().enumerate() {
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
                                for p in &intents_placed {
                                    let _ = coord.clear_intent(&txn_id, p).await;
                                }
                                let _ = coord.drop(&txn_id).await;
                                return Err(e);
                            }
                        };
                        if existing.is_none() && removes_only(actions) {
                            computed_writes.push(None);
                            continue;
                        }
                        Some(updated)
                    }
                    OwnedTxnKind::ConditionCheck { .. } => {
                        computed_writes.push(None);
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
                    for p in &intents_placed {
                        let _ = coord.clear_intent(&txn_id, p).await;
                    }
                    let _ = coord.drop(&txn_id).await;
                    return Err(e);
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
                        let seq = crate::streams::next_sequence_number();
                        if let Some(record) = crate::streams::build_record(
                            desc.stream_specification.as_ref(),
                            op.stream.as_ref(),
                            &op.key_info.key_schema,
                            existing.as_ref(),
                            new_item.as_ref(),
                            &seq,
                            &self.region,
                        ) {
                            log_records.push(crate::transact::TxnStreamRecord {
                                stream_arn: arn,
                                shard_id,
                                record,
                            });
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        for p in &intents_placed {
                            let _ = coord.clear_intent(&txn_id, p).await;
                        }
                        let _ = coord.drop(&txn_id).await;
                        return Err(e);
                    }
                }

                computed_writes.push(Some((new_item, next_version)));
            }

            let commit_ts_micros = coord
                .commit(
                    &txn_id,
                    started_at_micros,
                    &txn_mutations,
                    Some(&log_records),
                )
                .await?;

            for (idx, (op, data_table, desc, ttl_attr, participant)) in resolved.iter().enumerate()
            {
                let Some((new_item, next_version)) = &computed_writes[idx] else {
                    continue;
                };
                let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
                let prior = &items_read[idx];

                if let Some(item) = new_item {
                    let payload = crate::transact::TxnOpPayload::Put { item: item.clone() };
                    let applied = coord
                        .apply_fenced_mutation(
                            &txn_id,
                            participant,
                            &payload,
                            read_versions[idx],
                            prior.is_some(),
                            *next_version,
                        )
                        .await?;
                    if !applied {
                        return Err(StorageError::TransactionConflict(
                            "concurrent transaction superseded this transaction (fencing triggered)"
                                .to_string(),
                        ));
                    }
                    self.write_shadows_only(
                        &op.key_info,
                        data_table,
                        &gsis,
                        item,
                        prior.as_ref(),
                        *next_version,
                    )
                    .await?;
                    self.maintain_ttl_index(
                        &op.key_info,
                        ttl_attr.as_deref(),
                        prior.as_ref(),
                        Some(item),
                    )
                    .await?;
                } else if let OwnedTxnKind::Delete { .. } = &op.kind {
                    let payload = crate::transact::TxnOpPayload::Delete;
                    let applied = coord
                        .apply_fenced_mutation(
                            &txn_id,
                            participant,
                            &payload,
                            read_versions[idx],
                            prior.is_some(),
                            *next_version,
                        )
                        .await?;
                    if !applied {
                        return Err(StorageError::TransactionConflict(
                            "concurrent transaction superseded this transaction (fencing triggered)"
                                .to_string(),
                        ));
                    }
                    if let Some(prior_item) = prior.as_ref() {
                        self.delete_shadows(&op.key_info, data_table, &gsis, prior_item)
                            .await?;
                        self.maintain_ttl_index(
                            &op.key_info,
                            ttl_attr.as_deref(),
                            Some(prior_item),
                            None,
                        )
                        .await?;
                    }
                }
            }

            for r in &log_records {
                crate::streams::write_record(&self.cat(), &r.shard_id, &r.record)
                    .await
                    .map_err(map_catalog_err)?;
            }

            for (_, _, _, _, participant) in &resolved {
                let _ = coord.clear_intent(&txn_id, participant).await;
            }
            let _ = coord.cleaned(&txn_id, commit_ts_micros).await;
            let _ = coord.drop(&txn_id).await;

            if let Some((acct, tok, fp)) = &token {
                let now_secs = time::OffsetDateTime::now_utc().unix_timestamp();
                let _ = self
                    .cat()
                    .put(
                        &keys::idempotency(acct, tok),
                        &json!({
                            "fingerprint": fp,
                            "txn_id": txn_id,
                            "applied_at": now_secs,
                        }),
                    )
                    .await;
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
            let rows = self
                .cat()
                .scan_prefix(keys::IDEMPOTENCY_SCAN_PREFIX)
                .await
                .map_err(map_catalog_err)?;
            let mut deleted = 0u64;
            for (k, val) in rows {
                if let Some(ts) = val.get("applied_at").and_then(|v| v.as_i64())
                    && ts < cutoff
                    && self.cat().delete(&k).await.is_ok()
                {
                    deleted += 1;
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
            let mut row = self
                .cat()
                .get(&keys::table_meta(&account_id, &table_name))
                .await
                .map_err(map_catalog_err)?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            if let Some(obj) = row.as_object_mut() {
                obj.insert(
                    "ttl".to_string(),
                    json!({"enabled": enabled, "attribute": attribute_name}),
                );
            }
            self.cat()
                .put(&keys::table_meta(&account_id, &table_name), &row)
                .await
                .map_err(map_catalog_err)?;
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
            let cur = self
                .cat()
                .get(&key)
                .await
                .map_err(map_catalog_err)?
                .unwrap_or_else(|| json!({}));
            let mut map: serde_json::Map<String, serde_json::Value> =
                serde_json::from_value(cur).unwrap_or_default();
            for t in tags {
                map.insert(t.key, serde_json::Value::String(t.value));
            }
            self.cat()
                .put(&key, &serde_json::Value::Object(map))
                .await
                .map_err(map_catalog_err)?;
            Ok(())
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
            let Some(cur) = self.cat().get(&key).await.map_err(map_catalog_err)? else {
                return Ok(());
            };
            let mut map: serde_json::Map<String, serde_json::Value> =
                serde_json::from_value(cur).unwrap_or_default();
            for k in tag_keys {
                map.remove(&k);
            }
            self.cat()
                .put(&key, &serde_json::Value::Object(map))
                .await
                .map_err(map_catalog_err)?;
            Ok(())
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
            let desc: TableDescription = serde_json::from_value(
                table_row
                    .get("description")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            )
            .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;

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
            let desc: TableDescription = serde_json::from_value(
                row.get("description")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            )
            .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;
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
                let Ok(desc) = serde_json::from_value::<TableDescription>(
                    value
                        .get("description")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                ) else {
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
                    && self.cat().delete(&k).await.is_ok()
                {
                    deleted += 1;
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
                    let desc: TableDescription = serde_json::from_value(
                        row.get("description")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null),
                    )
                    .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;
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

async fn insert_ttl_index_entry(
    client: &BigtableClient,
    account_id: &str,
    table_name: &str,
    base_row_key: &[u8],
    expiry: i64,
) -> Result<(), StorageError> {
    let mut data = client.data();
    let ttl_key = crate::data::encoding::ttl_key::encode_ttl_key(
        account_id,
        table_name,
        base_row_key,
        expiry,
    );
    let ts_micros = monotonic_timestamp_micros();
    let req = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::MutateRowRequest {
        table_name: client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE),
        app_profile_id: client.app_profile_id.clone().unwrap_or_default(),
        row_key: ttl_key,
        mutations: vec![
            googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Mutation {
                mutation: Some(
                    googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::SetCell(
                        googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::SetCell {
                            family_name: "d".to_string(),
                            column_qualifier: vec![],
                            timestamp_micros: ts_micros,
                            value: vec![],
                        },
                    ),
                ),
            },
        ],
        ..Default::default()
    };
    data.mutate_row(req)
        .await
        .map_err(|e| map_bigtable_err("insert TTL index", e))?;
    Ok(())
}

async fn delete_ttl_index_entry(
    client: &BigtableClient,
    account_id: &str,
    table_name: &str,
    base_row_key: &[u8],
    expiry: i64,
) -> Result<(), StorageError> {
    let mut data = client.data();
    let ttl_key = crate::data::encoding::ttl_key::encode_ttl_key(
        account_id,
        table_name,
        base_row_key,
        expiry,
    );
    let req = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::MutateRowRequest {
        table_name: client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE),
        app_profile_id: client.app_profile_id.clone().unwrap_or_default(),
        row_key: ttl_key,
        mutations: vec![
            googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Mutation {
                mutation: Some(
                    googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
                        googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromRow {},
                    ),
                ),
            },
        ],
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
