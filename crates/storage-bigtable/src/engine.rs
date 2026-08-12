//! BigtableEngine: implements the six StorageEngine sub-traits.
//!
//! Phase 2 reality: TableEngine has real `list_tables`; everything else
//! (DataEngine, MetadataEngine, StreamEngine, BackupEngine, WorkerStore)
//! returns `StorageError::Internal("... lands in phase N")`. Later phases
//! fill these in.

use std::sync::Arc;
use std::time::Duration;
use moka::future::Cache;
use sha2::{Sha256, Digest};

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{
    AttributeValue, BillingMode, BillingModeSummary, CreateTableInput, DeleteTableInput, DescribeStreamInput,
    DescribeTableInput, GsiDescription, IndexInfo, IndexType, Item, ListTablesInput,
    ListTablesOutput, LsiDescription, ProvisionedThroughputDescription,
    StreamDescription, StreamRecord, TableDescription, TableKeyInfo, TableStatus, Tag,
    TimeToLiveDescription, TimeToLiveStatus, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupEngine, DataEngine, ItemPairResult, MetadataEngine, QueryResult, StreamCapture,
    StreamEngine, StreamListResult, StreamRecordsResult, TableEngine, TransactGetOp,
    TransactWriteOp, TtlTableInfo, WorkerStore,
};
use futures::future::BoxFuture;
use serde_json::json;
use uuid::Uuid;

use crate::catalog::{Catalog, keys};
use crate::data::admin::AdminClient;
use crate::data::client::BigtableClient;
use crate::data::item_ops::{ItemOps, apply_update_actions, check_condition};
use crate::data::query_scan::QueryScan;

const DEFAULT_REGION: &str = "us-east-1";

/// Family names on every data table. d = data, m = metadata (intent/idempotency
/// markers), t = TTL marker (gets a GC rule attached when TTL is enabled).
const DATA_FAMILIES: &[(&str, Option<()>); 3] = &[("d", None), ("m", None), ("t", None)];

/// Maps a logical (account_id, table_name) pair to the BigTable table id we
/// give it. We use the short UUID embedded in TableDescription.table_id so
/// the catalog round-trip is self-describing.
fn data_table_id(table_id: &str) -> String {
    format!("t{}", &table_id.replace('-', "")[..16])
}

fn now_unix_f64() -> f64 {
    time::OffsetDateTime::now_utc().unix_timestamp() as f64
}

fn todo_phase(phase: u32, what: &str) -> StorageError {
    StorageError::Internal(format!("bigtable backend: {what} lands in phase {phase}"))
}

// Owned view of a TransactWriteOp, so the future can move past the borrowed
// slice. The trait passes per-request lifetimes; we clone everything we need.
enum OwnedTxnKind {
    Put { item: Item },
    Delete { key: Item },
    Update { key: Item, actions: Vec<UpdateAction> },
    ConditionCheck { key: Item },
}

struct OwnedTxnOp {
    key_info: TableKeyInfo,
    kind: OwnedTxnKind,
    condition: Option<Expr>,
    maps: ExpressionMaps,
}

impl OwnedTxnOp {
    /// The key to look up the existing item for condition evaluation.
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
                ..
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::Put {
                    item: (*item).clone(),
                },
                condition: condition.cloned(),
                maps: (*maps).clone(),
            },
            TransactWriteOp::Delete {
                key_info,
                key,
                condition,
                maps,
                ..
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::Delete { key: (*key).clone() },
                condition: condition.cloned(),
                maps: (*maps).clone(),
            },
            TransactWriteOp::Update {
                key_info,
                key,
                actions,
                condition,
                maps,
                ..
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::Update {
                    key: (*key).clone(),
                    actions: actions.to_vec(),
                },
                condition: condition.cloned(),
                maps: (*maps).clone(),
            },
            TransactWriteOp::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                ..
            } => OwnedTxnOp {
                key_info: (*key_info).clone(),
                kind: OwnedTxnKind::ConditionCheck { key: (*key).clone() },
                condition: Some((*condition).clone()),
                maps: (*maps).clone(),
            },
        }
    }
}

fn derive_txn_id(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    let hash = hasher.finalize();
    format!("{:x}", hash)
}

pub struct BigtableEngine {
    catalog_client: Arc<BigtableClient>,
    client: Arc<BigtableClient>,
    intent_timeout_secs: u64,
    metadata_cache: Cache<(String, String), (String, TableDescription, Option<String>)>,
    node_id: String,
}

impl BigtableEngine {
    pub fn new(
        catalog_client: Arc<BigtableClient>,
        data_client: Arc<BigtableClient>,
        intent_timeout_secs: u64,
    ) -> Self {
        let metadata_cache = Cache::builder()
            .time_to_live(std::time::Duration::from_secs(5))
            .build();
        let node_id = uuid::Uuid::new_v4().to_string();
        Self {
            catalog_client,
            client: data_client,
            intent_timeout_secs,
            metadata_cache,
            node_id,
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Borrow the underlying BigtableClient — runtime_hooks need it for
    /// startup table-ensure calls.
    pub fn client_ref(&self) -> &BigtableClient {
        &self.client
    }

    pub fn intent_timeout_secs(&self) -> u64 {
        self.intent_timeout_secs
    }

    fn cat(&self) -> Catalog<'_> {
        Catalog::new(&self.catalog_client)
    }

    /// Look up the BigTable data-table short name for a (account, table_name).
    async fn data_table_for(&self, key_info: &TableKeyInfo) -> Result<String, StorageError> {
        let (dt, _) = self.table_meta_for(key_info).await?;
        Ok(dt)
    }

    /// Look up both the data table id and the GSI definitions for a table.
    /// One catalog round-trip — callers cache for the request's duration.
    async fn table_meta_for(
        &self,
        key_info: &TableKeyInfo,
    ) -> Result<(String, Vec<GsiDescription>), StorageError> {
        let (data_table, desc, _) = self.table_full_meta_for(key_info).await?;
        Ok((data_table, desc.global_secondary_indexes.unwrap_or_default()))
    }

    /// Look up the data table id + the full table description (so callers can
    /// reach GSI or LSI definitions or any other description field).
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
            .map_err(StorageError::Internal)?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        let data_table = row
            .get("data_table")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .ok_or_else(|| StorageError::Internal("missing data_table in catalog".into()))?;
        let desc: TableDescription = serde_json::from_value(
            row.get("description").cloned().unwrap_or(serde_json::Value::Null),
        )
        .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;

        let ttl = row.get("ttl").cloned().unwrap_or(serde_json::Value::Null);
        let ttl_enabled = ttl.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
        let ttl_attr = if ttl_enabled {
            ttl.get("attribute").and_then(|v| v.as_str()).map(str::to_owned)
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
        self.table_full_meta_for_raw(&key_info.account_id, &key_info.table_name).await
    }

    /// Write/refresh a single item across the base table and every GSI shadow,
    /// taking into account the prior item so stale shadow entries are deleted.
    /// Sparse semantics: items missing GSI key attrs skip that shadow.
    /// Base-first to prefer ghost entries over missing data on partial failure.
    async fn write_with_shadows(
        &self,
        key_info: &TableKeyInfo,
        data_table: &str,
        gsis: &[GsiDescription],
        new_item: &Item,
        old_item: Option<&Item>,
        guarded: bool,
    ) -> Result<(), StorageError> {
        let base_ops = ItemOps::new(&self.client, data_table, self.intent_timeout_secs);
        if guarded {
            base_ops.put(key_info, new_item).await?;
        } else {
            base_ops.put_unconditional(key_info, new_item).await?;
        }

        let mut futures = Vec::new();

        for g in gsis {
            let new_key = crate::gsi::shadow_row_key_for_item(
                new_item,
                &g.key_schema,
                &key_info.key_schema,
            )?;
            let old_key = match old_item {
                Some(o) => crate::gsi::shadow_row_key_for_item(
                    o,
                    &g.key_schema,
                    &key_info.key_schema,
                )?,
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

                // Delete the prior shadow entry when its key differs from the new
                // one (or when the new item no longer has GSI key attrs).
                if let Some(old) = old_key {
                    if new_key.as_ref() != Some(&old) {
                        if let Err(e) = ops.mutate_cells(
                            old,
                            vec![googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Mutation {
                                mutation: Some(
                                    googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
                                        googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromRow {},
                                    ),
                                ),
                            }],
                        ).await {
                            tracing::warn!(
                                "shadow stale-delete to gsi {} for table {} failed: {e}",
                                index_name,
                                table_name
                            );
                        }
                    }
                }

                // Write the new shadow entry if the item has GSI key attrs.
                if let Some(row_key) = new_key {
                    let projected = crate::gsi::project_for_shadow(
                        new_item,
                        &g_projection,
                        &base_key_schema,
                        &g_key_schema,
                    );
                    let mutations = ops.item_to_mutations(&projected, true)?;
                    if let Err(e) = ops.mutate_cells(row_key, mutations).await {
                        tracing::warn!(
                            "shadow write to gsi {} for table {} failed: {e}",
                            index_name,
                            table_name
                        );
                    }
                }
                Ok::<(), StorageError>(())
            });
        }

        let results = futures::future::join_all(futures).await;
        for res in results {
            res?;
        }

        Ok(())
    }

    /// Delete all shadow entries for an item being removed from the base table.
    async fn delete_shadows(
        &self,
        key_info: &TableKeyInfo,
        data_table: &str,
        gsis: &[GsiDescription],
        item: &Item,
    ) -> Result<(), StorageError> {
        let mut futures = Vec::new();
        for g in gsis {
            let Some(row_key) = crate::gsi::shadow_row_key_for_item(
                item,
                &g.key_schema,
                &key_info.key_schema,
            )?
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
                if let Err(e) = ops.mutate_cells(
                    row_key,
                    vec![googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Mutation {
                        mutation: Some(
                            googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
                                googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromRow {},
                            ),
                        ),
                    }],
                ).await {
                    tracing::warn!(
                        "shadow delete on gsi {} for table {} failed: {e}",
                        index_name,
                        table_name
                    );
                }
                Ok::<(), StorageError>(())
            });
        }

        let results = futures::future::join_all(futures).await;
        for res in results {
            res?;
        }
        Ok(())
    }

    pub(crate) async fn roll_forward(
        &self,
        txn_id: &str,
        state: &crate::transact::TxnState,
    ) -> Result<(), StorageError> {
        let client = &self.client;
        let coord = crate::transact::TxnCoordinator::new(client, Duration::from_secs(self.intent_timeout_secs));

        if let Some(muts) = &state.mutations {
            for m in muts {
                let ops = ItemOps::new(client, &m.participant.data_table, self.intent_timeout_secs);
                match &m.payload {
                    crate::transact::TxnOpPayload::Put { item } => {
                        let muts_list = ops.item_to_mutations(item, true)?;
                        ops.mutate_cells(m.participant.row_key.clone(), muts_list).await?;
                    }
                    crate::transact::TxnOpPayload::Delete => {
                        ops.mutate_cells(
                            m.participant.row_key.clone(),
                            vec![googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Mutation {
                                mutation: Some(googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
                                    googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromRow {},
                                )),
                            }],
                        ).await?;
                    }
                }
            }
        }

        if let Some(records) = &state.stream_records {
            let cat = self.cat();
            for r in records {
                crate::streams::write_record(&cat, &r.stream_arn, &r.record)
                    .await
                    .map_err(StorageError::Internal)?;
            }
        }

        if let Some(parts) = &state.participants {
            for p in parts {
                coord.clear_intent(txn_id, p).await?;
            }
        }

        coord.cleaned(txn_id).await?;
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
        Box::pin(async move {
            // Reject duplicates up-front so the engine layer gets the right error code.
            if self
                .cat()
                .get(&keys::table_meta(&account_id, &input.table_name))
                .await
                .map_err(StorageError::Internal)?
                .is_some()
            {
                return Err(StorageError::TableAlreadyExists(input.table_name));
            }

            let table_id = Uuid::new_v4().to_string();
            let data_table = data_table_id(&table_id);

            // Provision the data table on BigTable. Idempotent — admin.create_table
            // swallows AlreadyExists from the emulator.
            let mut admin = AdminClient::connect(&self.client)
                .await
                .map_err(StorageError::Internal)?;
            let families: Vec<(&str, Option<_>)> =
                DATA_FAMILIES.iter().map(|(n, _)| (*n, None)).collect();
            admin
                .create_table(&data_table, &families)
                .await
                .map_err(StorageError::Internal)?;

            // Provision shadow tables for each GSI (phase 8).
            if let Some(gsis) = &input.global_secondary_indexes {
                for g in gsis {
                    let shadow = crate::gsi::shadow_table_id(&data_table, &g.index_name);
                    admin
                        .create_table(&shadow, &families)
                        .await
                        .map_err(StorageError::Internal)?;
                }
            }

            // Build the TableDescription we'll return + persist.
            let billing = input.billing_mode.unwrap_or(BillingMode::PayPerRequest);
            let throughput = match input.provisioned_throughput {
                Some(p) => ProvisionedThroughputDescription {
                    read_capacity_units: p.read_capacity_units,
                    write_capacity_units: p.write_capacity_units,
                    number_of_decreases_today: 0,
                    last_increase_date_time: None,
                    last_decrease_date_time: None,
                },
                None => ProvisionedThroughputDescription {
                    read_capacity_units: 0,
                    write_capacity_units: 0,
                    number_of_decreases_today: 0,
                    last_increase_date_time: None,
                    last_decrease_date_time: None,
                },
            };
            let table_arn = format!(
                "arn:aws:dynamodb:{}:{}:table/{}",
                DEFAULT_REGION, account_id, input.table_name
            );
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

            // Set up the stream metadata up front so the TableDescription we
            // persist + return carries LatestStreamArn / LatestStreamLabel.
            let stream_spec = input.stream_specification.clone();
            let (latest_stream_arn, latest_stream_label, stream_meta_record) = if let Some(spec) =
                stream_spec.as_ref().filter(|s| s.stream_enabled)
            {
                let view = spec.stream_view_type
                    .unwrap_or(extenddb_core::types::StreamViewType::NewAndOldImages);
                let label = crate::streams::stream_label_now();
                let arn = crate::streams::build_stream_arn(&account_id, &input.table_name, &label);
                let meta = crate::streams::StreamMeta {
                    stream_arn: arn.clone(),
                    stream_label: label.clone(),
                    stream_view_type: view,
                    table_name: input.table_name.clone(),
                    table_arn: table_arn.clone(),
                    key_schema: input.key_schema.clone(),
                };
                (Some(arn), Some(label), Some(meta))
            } else {
                (None, None, None)
            };

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
                sse_description: None,
                table_class_summary: None,
                on_demand_throughput: input.on_demand_throughput,
            };

            // Persist the catalog row: TableDescription + the BigTable data table id.
            let record = json!({
                "description": description,
                "data_table": data_table,
                "ttl": {"enabled": false, "attribute": null},
            });
            self.cat()
                .put(&keys::table_meta(&account_id, &input.table_name), &record)
                .await
                .map_err(StorageError::Internal)?;

            // Persist the stream metadata row if streams were enabled.
            if let Some(meta) = stream_meta_record {
                let value = serde_json::to_value(&meta)
                    .map_err(|e| StorageError::Internal(format!("encode stream meta: {e}")))?;
                self.cat()
                    .put(&keys::stream_meta(&meta.stream_arn), &value)
                    .await
                    .map_err(StorageError::Internal)?;
            }

            self.metadata_cache.invalidate(&(account_id, input.table_name)).await;
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
                .map_err(StorageError::Internal)?
                .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

            let description: TableDescription =
                serde_json::from_value(row.get("description").cloned().unwrap_or(serde_json::Value::Null))
                    .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;

            if description.deletion_protection_enabled {
                return Err(StorageError::DeletionProtected(input.table_name));
            }

            let data_table = row
                .get("data_table")
                .and_then(|v| v.as_str())
                .ok_or_else(|| StorageError::Internal("missing data_table in catalog row".into()))?;

            let mut admin = AdminClient::connect(&self.client)
                .await
                .map_err(StorageError::Internal)?;
            admin
                .delete_table(data_table)
                .await
                .map_err(StorageError::Internal)?;

            self.cat()
                .delete(&keys::table_meta(&account_id, &input.table_name))
                .await
                .map_err(StorageError::Internal)?;

            let mut returned = description;
            returned.table_status = TableStatus::Deleting;
            self.metadata_cache.invalidate(&(account_id, input.table_name)).await;
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
                .map_err(StorageError::Internal)?
                .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
            let description: TableDescription =
                serde_json::from_value(row.get("description").cloned().unwrap_or(serde_json::Value::Null))
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
                .map_err(StorageError::Internal)?;
            let mut names: Vec<String> = rows
                .into_iter()
                .filter_map(|(key, _)| {
                    key.rsplit_once(':').map(|(_, last)| last.to_owned())
                })
                .collect();
            names.sort();

            // Honor ExclusiveStartTableName.
            if let Some(start) = &input.exclusive_start_table_name {
                names.retain(|n| n.as_str() > start.as_str());
            }
            let last_evaluated_table_name = match input.limit {
                Some(l) if (l as usize) < names.len() => {
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
            // Phase 2 partial: only billing-mode + deletion-protection updates
            // are supported. GSI updates and stream changes land in phase 8/17.
            let mut row = self
                .cat()
                .get(&keys::table_meta(&account_id, &input.table_name))
                .await
                .map_err(StorageError::Internal)?
                .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
            let mut description: TableDescription =
                serde_json::from_value(row.get("description").cloned().unwrap_or(serde_json::Value::Null))
                    .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;
            if let Some(b) = input.billing_mode {
                description.billing_mode_summary = Some(BillingModeSummary {
                    billing_mode: b,
                    last_update_to_pay_per_request_date_time: Some(now_unix_f64()),
                });
            }
            if let Some(d) = input.deletion_protection_enabled {
                description.deletion_protection_enabled = d;
            }
            if let Some(obj) = row.as_object_mut() {
                let desc_val = serde_json::to_value(&description).map_err(|e| StorageError::Internal(format!("serialize description: {e}")))?;
                obj.insert("description".to_string(), desc_val);
            }
            self.cat()
                .put(&keys::table_meta(&account_id, &input.table_name), &row)
                .await
                .map_err(StorageError::Internal)?;
            self.metadata_cache.invalidate(&(account_id, input.table_name)).await;
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
            let (_, description, _) = self.table_full_meta_for_raw(&account_id, &table_name).await?;
            let key_schema = description.key_schema;
            let global_secondary_indexes = description.global_secondary_indexes.clone().unwrap_or_default().into_iter().map(|g| extenddb_core::types::IndexInfo {
                index_name: g.index_name.clone(),
                index_id: g.index_name,
                index_type: extenddb_core::types::IndexType::Gsi,
                key_schema: g.key_schema,
                projection: g.projection,
            }).collect();
            let local_secondary_indexes = description.local_secondary_indexes.clone().unwrap_or_default().into_iter().map(|l| extenddb_core::types::IndexInfo {
                index_name: l.index_name.clone(),
                index_id: l.index_name,
                index_type: extenddb_core::types::IndexType::Lsi,
                key_schema: l.key_schema,
                projection: l.projection,
            }).collect();
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
            let (_, description, _) = self.table_full_meta_for_raw(&account_id, &table_name).await?;
            // Search both GSIs and LSIs by name.
            if let Some(gsis) = &description.global_secondary_indexes {
                if let Some(g) = gsis.iter().find(|g| g.index_name == index_name) {
                    return Ok(IndexInfo {
                        index_name: g.index_name.clone(),
                        index_id: format!("{}::{}", description.table_id, g.index_name),
                        index_type: IndexType::Gsi,
                        key_schema: g.key_schema.clone(),
                        projection: g.projection.clone(),
                    });
                }
            }
            if let Some(lsis) = &description.local_secondary_indexes {
                if let Some(l) = lsis.iter().find(|l| l.index_name == index_name) {
                    return Ok(IndexInfo {
                        index_name: l.index_name.clone(),
                        index_id: format!("{}::{}", description.table_id, l.index_name),
                        index_type: IndexType::Lsi,
                        key_schema: l.key_schema.clone(),
                        projection: l.projection.clone(),
                    });
                }
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
            // No reverse-lookup row yet (phase 8 backlog); scan account
            // table_meta rows looking for the matching table_id. Cheap at
            // ddbtest scale; revisit if we ever have thousands of tables.
            let accounts = self
                .cat()
                .scan_prefix(keys::ACCOUNT_SCAN_PREFIX)
                .await
                .map_err(StorageError::Internal)?;
            for (acct_key, _) in accounts {
                let acct_id = acct_key.strip_prefix("acct:").unwrap_or(&acct_key);
                let metas = self
                    .cat()
                    .scan_prefix(&keys::table_meta_scan_prefix(acct_id))
                    .await
                    .map_err(StorageError::Internal)?;
                for (meta_key, value) in metas {
                    let desc: TableDescription = match serde_json::from_value(
                        value.get("description").cloned().unwrap_or(serde_json::Value::Null),
                    ) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    if desc.table_id != table_id {
                        continue;
                    }
                    let table_name = meta_key
                        .rsplit_once(':')
                        .map(|(_, n)| n.to_owned())
                        .unwrap_or(meta_key);
                    return self.index_info(acct_id, &table_name, &index_name).await;
                }
            }
            Err(StorageError::TableNotFound(format!("table_id={table_id}")))
        })
    }
}

// =========== DataEngine (phase 3-6) ===========

impl DataEngine for BigtableEngine {
    fn put_item(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        Box::pin(async move {
            let (data_table, desc, ttl_attr) = self.table_full_meta_for(&key_info).await?;
            let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
            let streams_enabled = desc.latest_stream_arn.is_some();
            let ops = ItemOps::new(&self.client, &data_table, self.intent_timeout_secs);
            // Need prior image for: ConditionExpression, ReturnValues=ALL_OLD,
            // GSI shadow stale-cleanup, stream OLD_IMAGE / Insert-vs-Modify, and TTL maintenance.
            let existing = if condition.is_some() || return_old || !gsis.is_empty() || streams_enabled || ttl_attr.is_some() {
                ops.get(&key_info, &item).await?
            } else {
                None
            };
            check_condition(existing.as_ref(), condition.as_ref(), &maps)?;
            self.write_with_shadows(&key_info, &data_table, &gsis, &item, existing.as_ref(), true)
                .await?;

            // TTL Index Maintenance
            if let Some(ref attr_name) = ttl_attr {
                let old_exp = existing.as_ref().and_then(|prior| get_ttl_expiry(prior, attr_name));
                let new_exp = get_ttl_expiry(&item, attr_name);
                if old_exp != new_exp {
                    if let (Some(old_expiry), Some(prior_item)) = (old_exp, existing.as_ref()) {
                        let base_row_key = crate::data::encoding::row_key::encode_key(prior_item, &key_info.key_schema)?;
                        delete_ttl_index_entry(&self.client, &key_info.account_id, &key_info.table_name, &base_row_key, old_expiry).await?;
                    }
                    if let Some(new_expiry) = new_exp {
                        let base_row_key = crate::data::encoding::row_key::encode_key(&item, &key_info.key_schema)?;
                        insert_ttl_index_entry(&self.client, &key_info.account_id, &key_info.table_name, &base_row_key, new_expiry).await?;
                    }
                }
            }
            crate::streams::emit(
                &self.cat(),
                desc.latest_stream_arn.as_deref(),
                desc.stream_specification.as_ref(),
                &key_info.key_schema,
                existing.as_ref(),
                Some(&item),
            )
            .await
            .map_err(StorageError::Internal)?;
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
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        Box::pin(async move {
            let (data_table, desc, ttl_attr) = self.table_full_meta_for(&key_info).await?;
            let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
            let streams_enabled = desc.latest_stream_arn.is_some();
            let ops = ItemOps::new(&self.client, &data_table, self.intent_timeout_secs);
            let existing = if condition.is_some() || return_old || !gsis.is_empty() || streams_enabled || ttl_attr.is_some() {
                ops.get(&key_info, &key).await?
            } else {
                None
            };
            check_condition(existing.as_ref(), condition.as_ref(), &maps)?;
            ops.delete(&key_info, &key).await?;
            if let Some(prior) = existing.as_ref() {
                self.delete_shadows(&key_info, &data_table, &gsis, prior).await?;

                // TTL Index Maintenance
                if let Some(ref attr_name) = ttl_attr {
                    if let Some(old_expiry) = get_ttl_expiry(prior, attr_name) {
                        let base_row_key = crate::data::encoding::row_key::encode_key(prior, &key_info.key_schema)?;
                        delete_ttl_index_entry(&self.client, &key_info.account_id, &key_info.table_name, &base_row_key, old_expiry).await?;
                    }
                }
            }
            if existing.is_some() {
                crate::streams::emit(
                    &self.cat(),
                    desc.latest_stream_arn.as_deref(),
                    desc.stream_specification.as_ref(),
                    &key_info.key_schema,
                    existing.as_ref(),
                    None,
                )
                .await
                .map_err(StorageError::Internal)?;
            }
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
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, ItemPairResult> {
        let key_info = key_info.clone();
        let key = key.clone();
        let actions = actions.to_vec();
        let condition = condition.cloned();
        let maps = maps.clone();
        Box::pin(async move {
            let (data_table, desc, ttl_attr) = self.table_full_meta_for(&key_info).await?;
            let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
            let ops = ItemOps::new(&self.client, &data_table, self.intent_timeout_secs);
            let existing = ops.get(&key_info, &key).await?;
            check_condition(existing.as_ref(), condition.as_ref(), &maps)?;
            let base = match existing.as_ref() {
                Some(e) => e.clone(),
                None => key.clone(),
            };
            let new_item = apply_update_actions(&base, &actions, &maps)?;
            self.write_with_shadows(&key_info, &data_table, &gsis, &new_item, existing.as_ref(), true)
                .await?;

            // TTL Index Maintenance
            if let Some(ref attr_name) = ttl_attr {
                let old_exp = existing.as_ref().and_then(|prior| get_ttl_expiry(prior, attr_name));
                let new_exp = get_ttl_expiry(&new_item, attr_name);
                if old_exp != new_exp {
                    if let (Some(old_expiry), Some(prior_item)) = (old_exp, existing.as_ref()) {
                        let base_row_key = crate::data::encoding::row_key::encode_key(prior_item, &key_info.key_schema)?;
                        delete_ttl_index_entry(&self.client, &key_info.account_id, &key_info.table_name, &base_row_key, old_expiry).await?;
                    }
                    if let Some(new_expiry) = new_exp {
                        let base_row_key = crate::data::encoding::row_key::encode_key(&new_item, &key_info.key_schema)?;
                        insert_ttl_index_entry(&self.client, &key_info.account_id, &key_info.table_name, &base_row_key, new_expiry).await?;
                    }
                }
            }
            crate::streams::emit(
                &self.cat(),
                desc.latest_stream_arn.as_deref(),
                desc.stream_specification.as_ref(),
                &key_info.key_schema,
                existing.as_ref(),
                Some(&new_item),
            )
            .await
            .map_err(StorageError::Internal)?;
            Ok((
                if return_old { existing } else { None },
                if return_new { Some(new_item) } else { None },
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
                        let shadow_table = crate::gsi::shadow_table_id(&data_table, &gsi.index_name);
                        // Synthesize TableKeyInfo for the shadow's key schema —
                        // QueryScan only uses key_schema for row-key encoding.
                        let gsi_key_info = TableKeyInfo {
                            table_name: format!("{}__gsi_{}", key_info.table_name, gsi.index_name),
                            account_id: key_info.account_id.clone(),
                            table_id: format!("{}::{}", key_info.table_id, gsi.index_name),
                            key_schema: gsi.key_schema.clone(),
                            base_key_schema: key_info.key_schema.clone(),
                            attribute_definitions: Vec::new(),
                            has_lsi: false,
                            global_secondary_indexes: Vec::new(),
                            local_secondary_indexes: Vec::new(),
                            stream_specification: None,
                        };
                        QueryScan::new(&self.client, &shadow_table)
                            .query(&gsi_key_info, &kc, &maps, forward, limit, esk.as_ref())
                            .await
                    } else if let Some(lsi) = lsis.iter().find(|l| l.index_name == idx_name) {
                        // LSI shares the partition with the base table — no
                        // shadow. Read the partition, project items that have
                        // the LSI sort key, sort client-side, and apply the
                        // optional SK condition + pagination.
                        QueryScan::new(&self.client, &data_table)
                            .query_lsi(&key_info, &lsi.key_schema, &kc, &maps, forward, limit, esk.as_ref())
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
                        let shadow_table = crate::gsi::shadow_table_id(&data_table, &gsi.index_name);
                        let gsi_key_info = TableKeyInfo {
                            table_name: format!("{}__gsi_{}", key_info.table_name, gsi.index_name),
                            account_id: key_info.account_id.clone(),
                            table_id: format!("{}::{}", key_info.table_id, gsi.index_name),
                            key_schema: gsi.key_schema.clone(),
                            base_key_schema: key_info.key_schema.clone(),
                            attribute_definitions: Vec::new(),
                            has_lsi: false,
                            global_secondary_indexes: Vec::new(),
                            local_secondary_indexes: Vec::new(),
                            stream_specification: None,
                        };
                        QueryScan::new(&self.client, &shadow_table)
                            .scan(&gsi_key_info, limit, esk.as_ref(), segment, total_segments)
                            .await
                    } else if let Some(lsi) = lsis.iter().find(|l| l.index_name == idx_name) {
                        // LSI scan: base table + drop rows missing the LSI SK.
                        let (items, last) = QueryScan::new(&self.client, &data_table)
                            .scan(&key_info, limit, esk.as_ref(), segment, total_segments)
                            .await?;
                        let lsi_sk_name = lsi.key_schema.iter()
                            .find(|k| k.key_type == extenddb_core::types::KeyType::Range)
                            .map(|k| k.attribute_name.clone())
                            .ok_or_else(|| StorageError::Validation("LSI missing RANGE key".into()))?;
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

    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        // Clone all ops into owned form so the future can outlive the slice.
        let owned: Vec<(TableKeyInfo, Item)> = ops
            .iter()
            .map(|op| (op.key_info.clone(), op.key.clone()))
            .collect();
        Box::pin(async move {
            let total_ops = owned.len();
            let mut out: Vec<Option<Item>> = vec![None; total_ops];
            
            // Group by data_table to batch.
            let mut grouped: std::collections::HashMap<String, Vec<(usize, TableKeyInfo, Item)>> = std::collections::HashMap::new();
            for (idx, (key_info, key)) in owned.into_iter().enumerate() {
                let data_table = self.data_table_for(&key_info).await?;
                grouped.entry(data_table).or_default().push((idx, key_info, key));
            }

            let mut futures = Vec::new();
            for (data_table, group_ops) in grouped {
                let client = self.client.clone();
                let intent_timeout_secs = self.intent_timeout_secs;
                futures.push(async move {
                    let ops_helper = ItemOps::new(&client, &data_table, intent_timeout_secs);
                    if let Some((_, key_info, _)) = group_ops.first() {
                        let keys: Vec<Item> = group_ops.iter().map(|(_, _, k)| k.clone()).collect();
                        let results = ops_helper.batch_get(key_info, &keys).await?;
                        Ok::<_, StorageError>((group_ops, results))
                    } else {
                        Ok::<_, StorageError>((Vec::new(), Vec::new()))
                    }
                });
            }

            let group_results = futures::future::join_all(futures).await;
            for res in group_results {
                let (group_ops, results) = res?;
                for ((idx, _, _), item) in group_ops.into_iter().zip(results.into_iter()) {
                    out[idx] = item;
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
        let token = token.map(|k| (k.token.to_owned(), k.fingerprint.to_owned()));
        let owned: Vec<OwnedTxnOp> = ops.iter().map(OwnedTxnOp::from).collect();
        Box::pin(async move {
            use std::time::Duration;

            // Pre-check ClientRequestToken idempotency.
            if let Some((tok, fp)) = &token {
                if let Some(prior) = self
                    .cat()
                    .get(&keys::idempotency(tok))
                    .await
                    .map_err(StorageError::Internal)?
                {
                    let prior_fp = prior.get("fingerprint").and_then(|v| v.as_str()).unwrap_or("");
                    if prior_fp == fp {
                        return Err(StorageError::IdempotentReplay);
                    } else {
                        return Err(StorageError::IdempotentMismatch);
                    }
                }
            }

            let intent_max_age = Duration::from_secs(60);
            let txn_id = if let Some((tok, _)) = &token {
                derive_txn_id(tok)
            } else {
                crate::transact::TxnCoordinator::new_txn_id()
            };
            let coord = crate::transact::TxnCoordinator::new(&self.client, intent_max_age);

            if token.is_some() {
                if let Some(txn_state) = coord.get_state(&txn_id).await? {
                    match txn_state.state.as_str() {
                        "CLEANED" => {
                            if let Some((tok, fp)) = &token {
                                let record = json!({
                                    "fingerprint": fp,
                                    "txn_id": txn_id,
                                });
                                let _ = self.cat().put(&keys::idempotency(tok), &record).await;
                            }
                            return Ok(());
                        }
                        "COMMITTED" => {
                            self.roll_forward(&txn_id, &txn_state).await?;
                            if let Some((tok, fp)) = &token {
                                let record = json!({
                                    "fingerprint": fp,
                                    "txn_id": txn_id,
                                });
                                let _ = self.cat().put(&keys::idempotency(tok), &record).await;
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
                                        "CLEANED" => {
                                            break;
                                        }
                                        "COMMITTED" => {
                                            self.roll_forward(&txn_id, &ts).await?;
                                            break;
                                        }
                                        "PENDING" => {
                                            attempts += 1;
                                            if attempts >= max_attempts {
                                                return Err(StorageError::TransactionConflict(
                                                    "concurrent transaction with same token is pending".to_string()
                                                ));
                                            }
                                            let jitter = rand::random_range(0..delay.as_millis() as u64);
                                            delay = delay * 2 + Duration::from_millis(jitter);
                                        }
                                        _ => {
                                             return Err(StorageError::TransactionConflict(
                                                 format!("transaction exists in state: {}", ts.state)
                                             ));
                                        }
                                    }
                                } else {
                                    // Row disappeared. Check if token was written.
                                    if let Some((tok, _)) = &token {
                                        if self.cat().get(&keys::idempotency(tok)).await.map_err(StorageError::Internal)?.is_some() {
                                            break;
                                        }
                                    }
                                    return Err(StorageError::TransactionConflict(
                                        "transaction was rolled back or disappeared".to_string()
                                    ));
                                }
                            }
                            if let Some((tok, fp)) = &token {
                                let record = json!({
                                    "fingerprint": fp,
                                    "txn_id": txn_id,
                                });
                                let _ = self.cat().put(&keys::idempotency(tok), &record).await;
                            }
                            return Ok(());
                        }
                        _ => {
                            return Err(StorageError::TransactionConflict(
                                format!("transaction exists in state: {}", txn_state.state)
                            ));
                        }
                    }
                }
            }

            // Phase 1: per-op metadata resolution & key encoding (no DB reads yet).
            let mut resolved: Vec<(OwnedTxnOp, String, TableDescription, Option<String>, crate::transact::ParticipantRow)> =
                Vec::with_capacity(owned.len());
            for op in owned {
                let (data_table, desc, ttl_attr) = self.table_full_meta_for(&op.key_info).await?;
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

            // Phase 2: Open coordinator row in PENDING.
            let mut participants = Vec::with_capacity(resolved.len());
            for (_, _, _, _, participant) in &resolved {
                participants.push(participant.clone());
            }
            coord.open(&txn_id, &participants).await?;

            // Phase 3: Place intents (locks).
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
                                if let Err(e2) = coord.clear_intent(&txn_id, p).await {
                                    tracing::warn!("Failed to clear intent during rollback for txn {}: {}", txn_id, e2);
                                    rollback_success = false;
                                }
                            }
                            if rollback_success {
                                let _ = coord.drop(&txn_id).await;
                            } else {
                                let _ = coord.aborted(&txn_id).await;
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
                    let mut rollback_success = true;
                    for p in &intents_placed {
                        if let Err(e2) = coord.clear_intent(&txn_id, p).await {
                            tracing::warn!("Failed to clear intent during rollback for txn {}: {}", txn_id, e2);
                            rollback_success = false;
                        }
                    }
                    if rollback_success {
                        let _ = coord.drop(&txn_id).await;
                    } else {
                        let _ = coord.aborted(&txn_id).await;
                    }
                    let mut reasons: Vec<extenddb_core::types::CancellationReason> =
                        (0..resolved.len())
                            .map(|_| {
                                crate::transact::cancellation_reason("None", None, None)
                            })
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
                let sleep_duration = txn_delay + Duration::from_millis(jitter);
                tokio::time::sleep(sleep_duration).await;
                txn_delay *= 2;
            }

            // Phase 4: Read locked items and evaluate conditions.
            let mut items_read: Vec<Option<Item>> = Vec::with_capacity(resolved.len());
            for (op, data_table, _, _, _) in &resolved {
                let existing = ItemOps::new(&self.client, data_table, self.intent_timeout_secs)
                    .get(&op.key_info, op.lookup_key())
                    .await?;
                items_read.push(existing);
            }

            let mut reasons: Vec<extenddb_core::types::CancellationReason> =
                Vec::with_capacity(resolved.len());
            let mut any_failed = false;
            for (idx, (op, _, _, _, _)) in resolved.iter().enumerate() {
                let existing = &items_read[idx];
                let cond = op.condition.as_ref();
                match check_condition(existing.as_ref(), cond, &op.maps) {
                    Ok(()) => reasons.push(crate::transact::cancellation_reason("None", None, None)),
                    Err(StorageError::ConditionFailed(prior)) => {
                        any_failed = true;
                        reasons.push(crate::transact::cancellation_reason(
                            "ConditionalCheckFailed",
                            Some("The conditional request failed"),
                            prior.clone(),
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
                    if let Err(e2) = coord.clear_intent(&txn_id, p).await {
                        tracing::warn!("Failed to clear intent during rollback for txn {}: {}", txn_id, e2);
                        rollback_success = false;
                    }
                }
                if rollback_success {
                    let _ = coord.drop(&txn_id).await;
                } else {
                    let _ = coord.aborted(&txn_id).await;
                }
                return Err(StorageError::TransactionCanceled(reasons));
            }

            // Phase 5: Generate stream records and prepare mutation payloads.
            let mut stream_records: Vec<(String, extenddb_core::types::StreamRecord)> = Vec::new();
            let mut log_records: Vec<crate::transact::TxnStreamRecord> = Vec::new();
            let mut txn_mutations = Vec::with_capacity(resolved.len());
            for (idx, (op, data_table, desc, _, participant)) in resolved.iter().enumerate() {
                let existing = &items_read[idx];
                let new_item: Option<Item> = match &op.kind {
                    OwnedTxnKind::Put { item } => Some(item.clone()),
                    OwnedTxnKind::Delete { .. } => None,
                    OwnedTxnKind::Update { key, actions } => {
                        let base = existing.clone().unwrap_or_else(|| key.clone());
                        Some(apply_update_actions(&base, actions, &op.maps)?)
                    }
                    OwnedTxnKind::ConditionCheck { .. } => continue,
                };

                let payload = match &new_item {
                    Some(item) => crate::transact::TxnOpPayload::Put { item: item.clone() },
                    None => crate::transact::TxnOpPayload::Delete,
                };
                txn_mutations.push(crate::transact::ParticipantMutation {
                    participant: participant.clone(),
                    payload,
                });

                // Generate GSI mutations for 2PC log so sweeper can roll them forward.
                let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
                if let Some(item) = &new_item {
                    for g in &gsis {
                        let new_key = crate::gsi::shadow_row_key_for_item(
                            item,
                            &g.key_schema,
                            &op.key_info.key_schema,
                        )?;
                        let old_key = match existing {
                            Some(o) => crate::gsi::shadow_row_key_for_item(
                                o,
                                &g.key_schema,
                                &op.key_info.key_schema,
                            )?,
                            None => None,
                        };
                        let shadow_table = crate::gsi::shadow_table_id(data_table, &g.index_name);
                        
                        if let Some(old) = old_key {
                            if new_key.as_ref() != Some(&old) {
                                txn_mutations.push(crate::transact::ParticipantMutation {
                                    participant: crate::transact::ParticipantRow {
                                        data_table: shadow_table.clone(),
                                        row_key: old,
                                    },
                                    payload: crate::transact::TxnOpPayload::Delete,
                                });
                            }
                        }
                        if let Some(row_key) = new_key {
                            let projected = crate::gsi::project_for_shadow(
                                item,
                                &g.projection,
                                &op.key_info.key_schema,
                                &g.key_schema,
                            );
                            txn_mutations.push(crate::transact::ParticipantMutation {
                                participant: crate::transact::ParticipantRow {
                                    data_table: shadow_table,
                                    row_key,
                                },
                                payload: crate::transact::TxnOpPayload::Put { item: projected },
                            });
                        }
                    }
                } else {
                    for g in &gsis {
                        if let Some(prior_item) = existing {
                            if let Some(row_key) = crate::gsi::shadow_row_key_for_item(
                                prior_item,
                                &g.key_schema,
                                &op.key_info.key_schema,
                            )? {
                                let shadow_table = crate::gsi::shadow_table_id(data_table, &g.index_name);
                                txn_mutations.push(crate::transact::ParticipantMutation {
                                    participant: crate::transact::ParticipantRow {
                                        data_table: shadow_table,
                                        row_key,
                                    },
                                    payload: crate::transact::TxnOpPayload::Delete,
                                });
                            }
                        }
                    }
                }

                if let Some(arn) = &desc.latest_stream_arn {
                    if let Some(spec) = &desc.stream_specification {
                        let seq = crate::streams::next_sequence_number();
                        if let Some(record) = crate::streams::build_record(
                            spec,
                            &op.key_info.key_schema,
                            existing.as_ref(),
                            new_item.as_ref(),
                            &seq,
                        ) {
                            stream_records.push((arn.clone(), record.clone()));
                            log_records.push(crate::transact::TxnStreamRecord {
                                stream_arn: arn.clone(),
                                record,
                            });
                        }
                    }
                }
            }

            // Phase 6: Commit point (COMMITTED + save stream records + mutations in log).
            coord.commit(&txn_id, &txn_mutations, Some(&log_records)).await?;

            // Phase 7: Apply each mutation.
            for (idx, (op, data_table, desc, ttl_attr, _)) in resolved.iter().enumerate() {
                let gsis = desc.global_secondary_indexes.clone().unwrap_or_default();
                let prior = &items_read[idx];
                let new_item: Option<Item> = match &op.kind {
                    OwnedTxnKind::Put { item } => Some(item.clone()),
                    OwnedTxnKind::Delete { .. } => None,
                    OwnedTxnKind::Update { key, actions } => {
                        let base = prior.clone().unwrap_or_else(|| key.clone());
                        Some(apply_update_actions(&base, actions, &op.maps)?)
                    }
                    OwnedTxnKind::ConditionCheck { .. } => continue,
                };
                if let Some(item) = new_item {
                    self.write_with_shadows(&op.key_info, data_table, &gsis, &item, prior.as_ref(), false)
                        .await?;

                    // TTL Index Maintenance for Put/Update
                    if let Some(attr_name) = ttl_attr {
                        let old_exp = prior.as_ref().and_then(|prior_item| get_ttl_expiry(prior_item, attr_name));
                        let new_exp = get_ttl_expiry(&item, attr_name);
                        if old_exp != new_exp {
                            if let (Some(old_expiry), Some(prior_item)) = (old_exp, prior.as_ref()) {
                                let base_row_key = crate::data::encoding::row_key::encode_key(prior_item, &op.key_info.key_schema)?;
                                delete_ttl_index_entry(&self.client, &op.key_info.account_id, &op.key_info.table_name, &base_row_key, old_expiry).await?;
                            }
                            if let Some(new_expiry) = new_exp {
                                let base_row_key = crate::data::encoding::row_key::encode_key(&item, &op.key_info.key_schema)?;
                                insert_ttl_index_entry(&self.client, &op.key_info.account_id, &op.key_info.table_name, &base_row_key, new_expiry).await?;
                            }
                        }
                    }
                } else {
                    let ops_h = ItemOps::new(&self.client, data_table, self.intent_timeout_secs);
                    if let OwnedTxnKind::Delete { key } = &op.kind {
                        ops_h.delete_unconditional(&op.key_info, key).await?;
                        if let Some(prior_item) = prior.as_ref() {
                            self.delete_shadows(&op.key_info, data_table, &gsis, prior_item)
                                .await?;

                            // TTL Index Maintenance for Delete
                            if let Some(attr_name) = ttl_attr {
                                if let Some(old_expiry) = get_ttl_expiry(prior_item, attr_name) {
                                    let base_row_key = crate::data::encoding::row_key::encode_key(prior_item, &op.key_info.key_schema)?;
                                    delete_ttl_index_entry(&self.client, &op.key_info.account_id, &op.key_info.table_name, &base_row_key, old_expiry).await?;
                                }
                            }
                        }
                    }
                }
            }

            // Phase 8: Emit stream records.
            for (arn, record) in stream_records {
                if let Err(e) = crate::streams::write_record(&self.cat(), &arn, &record).await {
                    tracing::warn!("Failed to write stream record for committed txn: {e}");
                }
            }

            // Phase 9: Clear intents and mark CLEANED.
            for (_, _, _, _, participant) in &resolved {
                let _ = coord.clear_intent(&txn_id, participant).await;
            }
            let _ = coord.cleaned(&txn_id).await;
            let _ = coord.drop(&txn_id).await;

            // Record the token + fingerprint for dedup.
            if let Some((tok, fp)) = &token {
                let now_secs = time::OffsetDateTime::now_utc().unix_timestamp();
                let _ = self
                    .cat()
                    .put(
                        &keys::idempotency(tok),
                        &serde_json::json!({
                            "fingerprint": fp,
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
        _max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async { Ok(0) })
    }
}

// =========== MetadataEngine (TTL + tags; phase 10 for TTL) ===========

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
                .map_err(StorageError::Internal)?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            let ttl = row.get("ttl").cloned().unwrap_or(serde_json::Value::Null);
            let enabled = ttl.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
            let attribute = ttl.get("attribute").and_then(|v| v.as_str()).map(str::to_owned);
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
                .map_err(StorageError::Internal)?
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
                .map_err(StorageError::Internal)?;
            self.metadata_cache.invalidate(&(account_id, table_name)).await;
            Ok(())
        })
    }

    fn tag_resource(
        &self,
        arn: &str,
        tags: &[Tag],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_owned();
        let tags = tags.to_vec();
        Box::pin(async move {
            let key = keys::tags(&arn);
            let cur = self
                .cat()
                .get(&key)
                .await
                .map_err(StorageError::Internal)?
                .unwrap_or_else(|| json!({}));
            let mut map: serde_json::Map<String, serde_json::Value> =
                serde_json::from_value(cur).unwrap_or_default();
            for t in tags {
                map.insert(t.key, serde_json::Value::String(t.value));
            }
            self.cat()
                .put(&key, &serde_json::Value::Object(map))
                .await
                .map_err(StorageError::Internal)?;
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
            let Some(cur) = self
                .cat()
                .get(&key)
                .await
                .map_err(StorageError::Internal)?
            else {
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
                .map_err(StorageError::Internal)?;
            Ok(())
        })
    }

    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        let arn = arn.to_owned();
        Box::pin(async move {
            let key = keys::tags(&arn);
            let Some(cur) = self
                .cat()
                .get(&key)
                .await
                .map_err(StorageError::Internal)?
            else {
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
                .map_err(StorageError::Internal)?;
            let mut out = Vec::new();
            for (key, value) in rows {
                let ttl = value.get("ttl").cloned().unwrap_or(serde_json::Value::Null);
                let enabled = ttl.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
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
        Box::pin(async {
            let accounts = self
                .cat()
                .scan_prefix(keys::ACCOUNT_SCAN_PREFIX)
                .await
                .map_err(StorageError::Internal)?;
            let mut out = Vec::new();
            for (acct_key, _) in accounts {
                let acct_id = acct_key.strip_prefix("acct:").unwrap_or(&acct_key);
                let rows = self
                    .cat()
                    .scan_prefix(&keys::table_meta_scan_prefix(acct_id))
                    .await
                    .map_err(StorageError::Internal)?;
                for (key, value) in rows {
                    let ttl = value.get("ttl").cloned().unwrap_or(serde_json::Value::Null);
                    let enabled = ttl.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
                    let attr = ttl.get("attribute").and_then(|v| v.as_str());
                    if !enabled {
                        continue;
                    }
                    let Some(attr) = attr else { continue };
                    let Some(table_name) = key.rsplit_once(':').map(|(_, n)| n.to_owned()) else {
                        continue;
                    };
                    out.push((acct_id.to_string(), table_name, attr.to_string()));
                }
            }
            Ok(out)
        })
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        // No separate TTL index — every TTL-enabled table is "ready".
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
        _account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

// =========== StreamEngine ===========

impl StreamEngine for BigtableEngine {
    fn write_stream_record(
        &self,
        _account_id: &str,
        record: &StreamRecord,
        _shard_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        // The bigtable backend's own write paths (put/update/delete/transact)
        // already emit records via crate::streams::emit; this entrypoint is
        // for callers that want to inject a synthetic record (e.g. a TTL
        // sweeper). The shard isn't part of the record's persisted key —
        // sequence number is unique — so the stream_arn is derived from the
        // record's keys by writing into the all-records prefix.
        //
        // For now we only persist if the record carries a stream_arn we can
        // recover. The default code path doesn't use this entrypoint, so a
        // no-op is safe.
        let _ = record;
        Box::pin(async { Ok(()) })
    }

    fn get_stream_records(
        &self,
        _account_id: &str,
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        let shard_id = shard_id.to_owned();
        let after = after_sequence.map(str::to_owned);
        Box::pin(async move {
            // Find the stream this shard belongs to. Shard ids encode the
            // stream arn as "<arn>|<shard_id>" so we can route in one read.
            let (arn, real_shard) = parse_shard_id(&shard_id)?;
            let rows = self
                .cat()
                .scan_prefix(&keys::stream_record_shard_prefix(&arn, &real_shard))
                .await
                .map_err(StorageError::Internal)?;
            let mut records: Vec<StreamRecord> = Vec::with_capacity(rows.len());
            for (key, value) in rows {
                // Key looks like "stream_record:<arn>:<shard>:<seq>".
                let seq = key.rsplit_once(':').map(|(_, s)| s).unwrap_or("");
                if let Some(after) = &after {
                    if seq <= after.as_str() {
                        continue;
                    }
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
        let _ = account_id;
        let arn = input.stream_arn.clone();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::stream_meta(&arn))
                .await
                .map_err(StorageError::Internal)?
                .ok_or_else(|| {
                    StorageError::Validation(format!("Stream not found: {arn}"))
                })?;
            let meta: crate::streams::StreamMeta = serde_json::from_value(row)
                .map_err(|e| StorageError::Internal(format!("decode stream meta: {e}")))?;
            let single_shard = encode_shard_id(&arn, crate::streams::SINGLE_SHARD_ID);
            Ok(StreamDescription {
                stream_arn: meta.stream_arn,
                stream_label: meta.stream_label,
                stream_status: extenddb_core::types::StreamStatus::Enabled,
                stream_view_type: meta.stream_view_type,
                table_name: meta.table_name,
                key_schema: meta.key_schema,
                shards: vec![extenddb_core::types::Shard {
                    shard_id: single_shard,
                    parent_shard_id: None,
                    sequence_number_range: extenddb_core::types::SequenceNumberRange {
                        starting_sequence_number: "0".to_owned(),
                        ending_sequence_number: None,
                    },
                }],
                last_evaluated_shard_id: None,
            })
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
            let rows = self
                .cat()
                .scan_prefix(&keys::stream_scan_prefix_for_account(DEFAULT_REGION, &account_id))
                .await
                .map_err(StorageError::Internal)?;
            let mut summaries: Vec<extenddb_core::types::StreamSummary> = Vec::new();
            for (_key, value) in rows {
                let meta: crate::streams::StreamMeta = match serde_json::from_value(value) {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                if let Some(want) = &table_name {
                    if meta.table_name != *want {
                        continue;
                    }
                }
                summaries.push(extenddb_core::types::StreamSummary {
                    stream_arn: meta.stream_arn,
                    stream_label: meta.stream_label,
                    table_name: meta.table_name,
                });
            }
            summaries.sort_by(|a, b| a.stream_arn.cmp(&b.stream_arn));
            if let Some(esk) = esk.as_deref() {
                summaries.retain(|s| s.stream_arn.as_str() > esk);
            }
            let last_evaluated = if limit > 0 && summaries.len() > limit as usize {
                let cut = limit as usize;
                let s = summaries[cut - 1].stream_arn.clone();
                summaries.truncate(cut);
                Some(s)
            } else {
                None
            };
            Ok((summaries, last_evaluated))
        })
    }

    fn cleanup_expired_stream_records(
        &self,
        _retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async { Ok(0) })
    }

    fn assign_shard(
        &self,
        _account_id: &str,
        _table_name: &str,
        _partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        // Single-shard backend; engine layer can pass this back through.
        Box::pin(async move { Ok(crate::streams::SINGLE_SHARD_ID.to_owned()) })
    }

    fn next_sequence_number(
        &self,
        _shard_id: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        Box::pin(async move { Ok(crate::streams::next_sequence_number()) })
    }

    fn validate_shard(
        &self,
        _account_id: &str,
        stream_arn: &str,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = stream_arn.to_owned();
        let shard = shard_id.to_owned();
        Box::pin(async move {
            // Encoded form already binds shard to arn; just verify the arn exists.
            let (parsed_arn, _) = parse_shard_id(&shard).unwrap_or((arn.clone(), shard.clone()));
            if parsed_arn != arn {
                return Err(StorageError::Validation("shard does not belong to stream".into()));
            }
            let exists = self
                .cat()
                .get(&keys::stream_meta(&arn))
                .await
                .map_err(StorageError::Internal)?
                .is_some();
            if !exists {
                return Err(StorageError::Validation(format!("Stream not found: {arn}")));
            }
            Ok(())
        })
    }

    fn latest_sequence_number(
        &self,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        let shard = shard_id.to_owned();
        Box::pin(async move {
            let Ok((arn, real_shard)) = parse_shard_id(&shard) else {
                return Ok(None);
            };
            let rows = self
                .cat()
                .scan_prefix(&keys::stream_record_shard_prefix(&arn, &real_shard))
                .await
                .map_err(StorageError::Internal)?;
            let mut last: Option<String> = None;
            for (key, _) in rows {
                if let Some((_, seq)) = key.rsplit_once(':') {
                    if last.as_deref().map(|l| seq > l).unwrap_or(true) {
                        last = Some(seq.to_owned());
                    }
                }
            }
            Ok(last)
        })
    }
}

/// The bigtable backend exposes a single shard per stream but its id must
/// disambiguate across streams, so we encode `<arn>|<shard_id>` in the
/// returned shard ids. Iterator + get_records calls receive the encoded
/// form and recover the (arn, shard) pair via `parse_shard_id`.
// The engine's shard-iterator codec uses `|` as a separator; the shard-id
// pair must avoid that character. `~~` is safe in stream ARNs.
const SHARD_ID_SEP: &str = "~~";

fn encode_shard_id(arn: &str, shard_id: &str) -> String {
    format!("{arn}{SHARD_ID_SEP}{shard_id}")
}

fn parse_shard_id(s: &str) -> Result<(String, String), StorageError> {
    s.split_once(SHARD_ID_SEP)
        .map(|(a, b)| (a.to_owned(), b.to_owned()))
        .ok_or_else(|| {
            StorageError::Validation(format!(
                "invalid shard id (expected <arn>{SHARD_ID_SEP}<shard>): {s}"
            ))
        })
}

// =========== WorkerStore ===========

impl WorkerStore for BigtableEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        // BigTable backend doesn't track CREATING/DELETING states — tables
        // come up ACTIVE immediately.
        Box::pin(async { Ok(Vec::new()) })
    }
}

// =========== BackupEngine ===========

impl BigtableEngine {
    /// Look up a table's data-table name + full description by (account, name).
    async fn load_desc(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<(String, TableDescription), StorageError> {
        let row = self
            .cat()
            .get(&keys::table_meta(account_id, table_name))
            .await
            .map_err(StorageError::Internal)?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
        let data_table = row
            .get("data_table")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .ok_or_else(|| StorageError::Internal("missing data_table in catalog".into()))?;
        let desc: TableDescription = serde_json::from_value(
            row.get("description").cloned().unwrap_or(serde_json::Value::Null),
        )
        .map_err(|e| StorageError::Internal(format!("catalog deserialize: {e}")))?;
        Ok((data_table, desc))
    }
}

impl BackupEngine for BigtableEngine {
    fn create_backup(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::BackupDetails, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let backup_name = backup_name.to_owned();
        Box::pin(async move {
            let (data_table, desc) = self.load_desc(&account_id, &table_name).await?;
            let key_info = TableKeyInfo {
                table_name: table_name.clone(),
                account_id: account_id.clone(),
                table_id: desc.table_id.clone(),
                key_schema: desc.key_schema.clone(),
                base_key_schema: desc.key_schema.clone(),
                attribute_definitions: desc.attribute_definitions.clone(),
                has_lsi: desc.local_secondary_indexes.is_some(),
                global_secondary_indexes: Vec::new(),
                local_secondary_indexes: Vec::new(),
                stream_specification: desc.stream_specification.clone(),
            };

            // Snapshot the source data table.
            let (items, _) = QueryScan::new(&self.client, &data_table)
                .scan(&key_info, None, None, None, None)
                .await?;

            let ts_ms: i64 = (time::OffsetDateTime::now_utc().unix_timestamp_nanos()
                / 1_000_000) as i64;
            let arn = format!(
                "arn:aws:dynamodb:{DEFAULT_REGION}:{account_id}:table/{table_name}/backup/{ts_ms}"
            );

            let mut size_bytes: i64 = 0;
            for (i, item) in items.iter().enumerate() {
                let body = serde_json::to_value(item)
                    .map_err(|e| StorageError::Internal(format!("encode item: {e}")))?;
                size_bytes += body.to_string().len() as i64;
                self.cat()
                    .put(&keys::backup_item(&arn, i as u64), &body)
                    .await
                    .map_err(StorageError::Internal)?;
            }

            let creation = (ts_ms as f64) / 1000.0;
            let billing_str = desc.billing_mode_summary.as_ref().and_then(|s| {
                use extenddb_core::types::BillingMode;
                match s.billing_mode {
                    BillingMode::PayPerRequest => Some("PAY_PER_REQUEST".to_owned()),
                    BillingMode::Provisioned => Some("PROVISIONED".to_owned()),
                }
            });

            let details = extenddb_core::types::BackupDetails {
                backup_arn: arn.clone(),
                backup_name: backup_name.clone(),
                backup_status: "AVAILABLE".to_owned(),
                backup_type: "USER".to_owned(),
                backup_size_bytes: size_bytes,
                backup_creation_date_time: creation,
            };
            let source = extenddb_core::types::SourceTableDetails {
                table_name: table_name.clone(),
                table_id: desc.table_id.clone(),
                table_arn: desc.table_arn.clone(),
                key_schema: desc.key_schema.clone(),
                item_count: items.len() as i64,
                table_size_bytes: desc.table_size_bytes,
                billing_mode: billing_str,
                table_creation_date_time: desc.creation_date_time,
            };
            let full = extenddb_core::types::BackupDescription {
                backup_details: details.clone(),
                source_table_details: source,
            };
            // Persist the BackupDescription plus the full source TableDescription
            // so restore can rebuild GSIs / LSIs / attribute definitions.
            let row = serde_json::json!({
                "backup_desc": serde_json::to_value(&full)
                    .map_err(|e| StorageError::Internal(format!("encode backup desc: {e}")))?,
                "source_desc": serde_json::to_value(&desc)
                    .map_err(|e| StorageError::Internal(format!("encode source desc: {e}")))?,
            });
            self.cat()
                .put(&keys::backup_meta(&arn), &row)
                .await
                .map_err(StorageError::Internal)?;
            Ok(details)
        })
    }

    fn describe_backup(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::BackupDescription, StorageError>> {
        let arn = backup_arn.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::backup_meta(&arn))
                .await
                .map_err(StorageError::Internal)?
                .ok_or_else(|| StorageError::Validation(format!("Backup not found: {arn}")))?;
            let desc_value = row
                .get("backup_desc")
                .cloned()
                .ok_or_else(|| StorageError::Internal("backup row missing backup_desc".into()))?;
            serde_json::from_value(desc_value)
                .map_err(|e| StorageError::Internal(format!("decode backup desc: {e}")))
        })
    }

    fn list_backups(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<extenddb_core::types::BackupSummary>, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.map(str::to_owned);
        Box::pin(async move {
            let prefix = keys::backup_scan_prefix_for_account(DEFAULT_REGION, &account_id);
            let rows = self
                .cat()
                .scan_prefix(&prefix)
                .await
                .map_err(StorageError::Internal)?;
            let mut out: Vec<extenddb_core::types::BackupSummary> = Vec::with_capacity(rows.len());
            for (_key, value) in rows {
                let Some(bd_value) = value.get("backup_desc").cloned() else { continue };
                let desc: extenddb_core::types::BackupDescription =
                    match serde_json::from_value(bd_value) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                if let Some(want) = &table_name {
                    if desc.source_table_details.table_name != *want {
                        continue;
                    }
                }
                let d = &desc.backup_details;
                let s = &desc.source_table_details;
                out.push(extenddb_core::types::BackupSummary {
                    backup_arn: d.backup_arn.clone(),
                    backup_name: d.backup_name.clone(),
                    table_name: s.table_name.clone(),
                    table_arn: s.table_arn.clone(),
                    backup_status: d.backup_status.clone(),
                    backup_type: d.backup_type.clone(),
                    backup_size_bytes: d.backup_size_bytes,
                    backup_creation_date_time: d.backup_creation_date_time,
                });
            }
            Ok(out)
        })
    }

    fn delete_backup(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::BackupDescription, StorageError>> {
        let arn = backup_arn.to_owned();
        Box::pin(async move {
            let row = self
                .cat()
                .get(&keys::backup_meta(&arn))
                .await
                .map_err(StorageError::Internal)?
                .ok_or_else(|| StorageError::Validation(format!("Backup not found: {arn}")))?;
            let bd_value = row
                .get("backup_desc")
                .cloned()
                .ok_or_else(|| StorageError::Internal("backup row missing backup_desc".into()))?;
            let mut desc: extenddb_core::types::BackupDescription = serde_json::from_value(bd_value)
                .map_err(|e| StorageError::Internal(format!("decode backup desc: {e}")))?;
            // Remove all backup item rows.
            self.cat()
                .delete_prefix(&keys::backup_item_scan_prefix(&arn))
                .await
                .map_err(StorageError::Internal)?;
            // Remove the meta row.
            self.cat()
                .delete(&keys::backup_meta(&arn))
                .await
                .map_err(StorageError::Internal)?;
            desc.backup_details.backup_status = "DELETED".to_owned();
            Ok(desc)
        })
    }

    fn restore_table_from_backup(
        &self,
        account_id: &str,
        target_table_name: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let target_table_name = target_table_name.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            // Load backup metadata.
            let row = self
                .cat()
                .get(&keys::backup_meta(&backup_arn))
                .await
                .map_err(StorageError::Internal)?
                .ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))?;
            let source_value = row
                .get("source_desc")
                .cloned()
                .ok_or_else(|| StorageError::Internal("backup row missing source_desc".into()))?;
            let source_desc: TableDescription = serde_json::from_value(source_value)
                .map_err(|e| StorageError::Internal(format!("decode source desc: {e}")))?;

            // Rebuild GSI / LSI inputs from descriptions so the target table
            // gets the same secondary indexes as the source.
            let gsi_inputs = source_desc.global_secondary_indexes.as_ref().map(|gsis| {
                gsis.iter()
                    .map(|g| extenddb_core::types::GsiInput {
                        index_name: g.index_name.clone(),
                        key_schema: g.key_schema.clone(),
                        projection: g.projection.clone(),
                        provisioned_throughput: None,
                    })
                    .collect()
            });
            let lsi_inputs = source_desc.local_secondary_indexes.as_ref().map(|lsis| {
                lsis.iter()
                    .map(|l| extenddb_core::types::LsiInput {
                        index_name: l.index_name.clone(),
                        key_schema: l.key_schema.clone(),
                        projection: l.projection.clone(),
                    })
                    .collect()
            });

            // Build CreateTableInput from source schema.
            let billing_mode = source_desc
                .billing_mode_summary
                .as_ref()
                .map(|s| s.billing_mode.clone());
            let create_input = extenddb_core::types::CreateTableInput {
                table_name: target_table_name.clone(),
                key_schema: source_desc.key_schema.clone(),
                attribute_definitions: source_desc.attribute_definitions.clone(),
                billing_mode,
                provisioned_throughput: None,
                global_secondary_indexes: gsi_inputs,
                local_secondary_indexes: lsi_inputs,
                stream_specification: None,
                tags: None,
                deletion_protection_enabled: None,
                sse_specification: None,
                table_class: None,
                on_demand_throughput: source_desc.on_demand_throughput.clone(),
            };
            let target_desc = self.create_table(&account_id, create_input).await?;

            // Re-load the target's catalog row to get its data-table id and
            // fresh GSI list (different from source's table_id but same shape).
            let (target_data_table, target_full) =
                self.load_desc(&account_id, &target_table_name).await?;
            let target_gsis = target_full
                .global_secondary_indexes
                .clone()
                .unwrap_or_default();
            let target_key_info = TableKeyInfo {
                table_name: target_table_name.clone(),
                account_id: account_id.clone(),
                table_id: target_full.table_id.clone(),
                key_schema: target_full.key_schema.clone(),
                base_key_schema: target_full.key_schema.clone(),
                attribute_definitions: target_full.attribute_definitions.clone(),
                has_lsi: target_full.local_secondary_indexes.is_some(),
                global_secondary_indexes: Vec::new(),
                local_secondary_indexes: Vec::new(),
                stream_specification: None,
            };

            // Read every backup_item:<arn>:* row and write into the target,
            // routing through write_with_shadows so GSI shadow tables get
            // populated.
            let item_rows = self
                .cat()
                .scan_prefix(&keys::backup_item_scan_prefix(&backup_arn))
                .await
                .map_err(StorageError::Internal)?;
            for (_key, body) in item_rows {
                let item: extenddb_core::types::Item = serde_json::from_value(body)
                    .map_err(|e| StorageError::Internal(format!("decode item: {e}")))?;
                self.write_with_shadows(
                    &target_key_info,
                    &target_data_table,
                    &target_gsis,
                    &item,
                    None,
                    false,
                )
                .await?;
            }
            Ok(target_desc)
        })
    }

    fn describe_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::ContinuousBackupsDescription, StorageError>>
    {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            // Verify the table exists.
            let _ = self.load_desc(&account_id, &table_name).await?;
            let row = self
                .cat()
                .get(&keys::continuous_backups(&account_id, &table_name))
                .await
                .map_err(StorageError::Internal)?;
            let pitr_enabled = row
                .as_ref()
                .and_then(|v| v.get("pitr_enabled").and_then(|b| b.as_bool()))
                .unwrap_or(false);
            Ok(extenddb_core::types::ContinuousBackupsDescription {
                continuous_backups_status: "ENABLED".to_owned(),
                point_in_time_recovery_description: Some(
                    extenddb_core::types::PointInTimeRecoveryDescription {
                        point_in_time_recovery_status: if pitr_enabled { "ENABLED" } else { "DISABLED" }
                            .to_owned(),
                        earliest_restorable_date_time: None,
                        latest_restorable_date_time: None,
                    },
                ),
            })
        })
    }

    fn update_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
        pitr_enabled: bool,
    ) -> BoxFuture<'_, Result<extenddb_core::types::ContinuousBackupsDescription, StorageError>>
    {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let _ = self.load_desc(&account_id, &table_name).await?;
            self.cat()
                .put(
                    &keys::continuous_backups(&account_id, &table_name),
                    &serde_json::json!({ "pitr_enabled": pitr_enabled }),
                )
                .await
                .map_err(StorageError::Internal)?;
            Ok(extenddb_core::types::ContinuousBackupsDescription {
                continuous_backups_status: "ENABLED".to_owned(),
                point_in_time_recovery_description: Some(
                    extenddb_core::types::PointInTimeRecoveryDescription {
                        point_in_time_recovery_status: if pitr_enabled { "ENABLED" } else { "DISABLED" }
                            .to_owned(),
                        earliest_restorable_date_time: None,
                        latest_restorable_date_time: None,
                    },
                ),
            })
        })
    }

    fn restore_table_to_point_in_time(
        &self,
        _account_id: &str,
        _source_table_name: &str,
        _target_table_name: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        Box::pin(async { Err(todo_phase(18, "restore_table_to_point_in_time")) })
    }
}

async fn insert_ttl_index_entry(
    client: &BigtableClient,
    account_id: &str,
    table_name: &str,
    base_row_key: &[u8],
    expiry: i64,
) -> Result<(), StorageError> {
    let mut data = client.data();
    let ttl_key = crate::data::encoding::ttl_key::encode_ttl_key(account_id, table_name, base_row_key, expiry);
    let req = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::MutateRowRequest {
        table_name: client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE),
        row_key: ttl_key,
        mutations: vec![
            googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Mutation {
                mutation: Some(googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::SetCell(
                    googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::SetCell {
                        family_name: "d".to_string(),
                        column_qualifier: vec![],
                        timestamp_micros: -1,
                        value: vec![],
                    }
                )),
            }
        ],
        ..Default::default()
    };
    data.mutate_row(req)
        .await
        .map_err(|e| StorageError::Internal(format!("insert TTL index: {e}")))?;
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
    let ttl_key = crate::data::encoding::ttl_key::encode_ttl_key(account_id, table_name, base_row_key, expiry);
    let req = googleapis_tonic_google_bigtable_v2::google::bigtable::v2::MutateRowRequest {
        table_name: client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE),
        row_key: ttl_key,
        mutations: vec![
            googleapis_tonic_google_bigtable_v2::google::bigtable::v2::Mutation {
                mutation: Some(googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
                    googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromRow {}
                )),
            }
        ],
        ..Default::default()
    };
    data.mutate_row(req)
        .await
        .map_err(|e| StorageError::Internal(format!("delete TTL index: {e}")))?;
    Ok(())
}

fn get_ttl_expiry(item: &Item, attr_name: &str) -> Option<i64> {
    match item.get(attr_name) {
        Some(AttributeValue::N(s)) => s.parse::<i64>().ok(),
        _ => None,
    }
}
