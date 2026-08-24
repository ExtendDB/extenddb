// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TableEngine` trait implementation for `MongoEngine`.

use bson::{Document, doc};
use futures::future::BoxFuture;
use mongodb::IndexModel;
use mongodb::options::{Collation, IndexOptions};

use extenddb_core::types::{
    AttributeDefinition, BillingMode, BillingModeSummary, CreateTableInput, DeleteTableInput,
    DescribeTableInput, GsiDescription, IndexInfo, IndexType, KeySchemaElement, ListTablesInput,
    ListTablesOutput, LsiDescription, OnDemandThroughput, ProvisionedThroughputDescription,
    ScalarAttributeType, SseDescription, SseType, TableDescription, TableKeyInfo, TableStatus,
    UpdateTableInput,
};
use extenddb_storage::TableEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    effective_attribute_definitions, index_arn, merge_attribute_definitions, sk_info, stream_arn,
    table_arn,
};

use crate::MongoEngine;
use crate::data::data_collection_name;

/// Format a timestamp as a DynamoDB-style stream label:
/// `YYYY-MM-DDThh:mm:ss` (second precision, no timezone).
///
/// Matches the postgres backend's
/// `to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS')` output byte-for-byte
/// so a stream ARN issued by one backend is parseable by tooling that
/// only ever saw the other. The `time` crate's `Iso8601::DEFAULT`
/// emits nanoseconds with a trailing `Z` — pushing that through AWS-
/// SDK parsers or postgres-shaped tests failed unpredictably. D-m8.
fn format_stream_label(now: time::OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

impl TableEngine for MongoEngine {
    fn create_table(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move { self.create_table_impl(&account_id, input, false).await })
    }

    fn delete_table(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move { self.delete_table_impl(&account_id, input).await })
    }

    fn describe_table(
        &self,
        account_id: &str,
        input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move {
            self.describe_table_impl(&account_id, &input.table_name)
                .await
        })
    }

    fn list_tables(
        &self,
        account_id: &str,
        input: ListTablesInput,
    ) -> BoxFuture<'_, Result<ListTablesOutput, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move { self.list_tables_impl(&account_id, input).await })
    }

    fn update_table(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move { self.update_table_impl(&account_id, input).await })
    }

    fn table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableKeyInfo, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move { self.table_key_info_impl(&account_id, &table_name).await })
    }

    fn index_info(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let index_name = index_name.to_string();
        Box::pin(async move {
            self.index_info_impl(&account_id, &table_name, &index_name)
                .await
        })
    }

    fn index_info_by_table_id(
        &self,
        table_id: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        let table_id = table_id.to_string();
        let index_name = index_name.to_string();
        Box::pin(async move {
            self.index_info_by_table_id_impl(&table_id, &index_name)
                .await
        })
    }
}

impl MongoEngine {
    /// Create a table. When `defer_active` is set (the restore path), the row
    /// is written `CREATING` with **no** scheduled transition, so the
    /// background worker will not flip it to `ACTIVE`; the caller schedules the
    /// transition only after it has finished populating the table. Normal
    /// `CreateTable` passes `false` and gets the usual timed transition.
    pub(crate) async fn create_table_impl(
        &self,
        account_id: &str,
        input: CreateTableInput,
        defer_active: bool,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;

        let table_id = uuid::Uuid::new_v4().to_string();
        let table_arn_val = table_arn(&self.region, account_id, &input.table_name);
        let billing_mode = input.billing_mode.unwrap_or(BillingMode::Provisioned);
        let deletion_protection = input.deletion_protection_enabled.unwrap_or(false);

        let now = time::OffsetDateTime::now_utc();
        let creation_epoch = now.unix_timestamp() as f64;

        // Build the table metadata document
        let key_schema_bson =
            bson::to_bson(&input.key_schema).map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs_bson = bson::to_bson(&input.attribute_definitions)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let billing_str = match billing_mode {
            BillingMode::Provisioned => "PROVISIONED",
            BillingMode::PayPerRequest => "PAY_PER_REQUEST",
        };
        let pt_bson = input
            .provisioned_throughput
            .as_ref()
            .map(bson::to_bson)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let stream_bson = input
            .stream_specification
            .as_ref()
            .map(bson::to_bson)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Compute stream label early so it's stored in the table document
        let stream_label_opt = if input
            .stream_specification
            .as_ref()
            .is_some_and(|ss| ss.stream_enabled)
        {
            Some(format_stream_label(now))
        } else {
            None
        };
        let stream_label_bson = stream_label_opt
            .as_ref()
            .map_or(bson::Bson::Null, |l| bson::Bson::String(l.clone()));

        // Persist TableClass / SSESpecification / OnDemandThroughput on the
        // catalog doc so DescribeTable can return TableClassSummary,
        // SSEDescription, and OnDemandThroughput respectively. Mirrors the
        // postgres backend at storage-postgres/src/create_table.rs:100-102.
        let table_class_bson = input
            .table_class
            .as_deref()
            .map_or(bson::Bson::Null, |tc| bson::Bson::String(tc.to_owned()));
        let sse_spec_bson = input.sse_specification.as_ref().map_or_else(
            || bson::Bson::Null,
            |v| bson::to_bson(v).unwrap_or(bson::Bson::Null),
        );
        let on_demand_bson = input.on_demand_throughput.as_ref().map_or_else(
            || bson::Bson::Null,
            |v| bson::to_bson(v).unwrap_or(bson::Bson::Null),
        );

        // Enter CREATING with a scheduled transition to ACTIVE, unless
        // control_plane_delay_seconds is 0 (then go straight to ACTIVE). The
        // background control_plane_worker flips CREATING -> ACTIVE once the
        // transition time passes; during the window data-plane ops on the
        // table return ResourceNotFound, matching DynamoDB and the postgres
        // backend.
        let delay_secs = self.control_plane_delay_seconds().await;
        let (table_status, status_transition_at): (&str, bson::Bson) = if defer_active {
            // Restore path: enter CREATING with no scheduled transition. The
            // caller flips the table to ACTIVE (or schedules the transition)
            // only after the data copy completes, so ACTIVE never precedes a
            // populated table.
            ("CREATING", bson::Bson::Null)
        } else if delay_secs <= 0.0 {
            ("ACTIVE", bson::Bson::Null)
        } else {
            let at = bson::DateTime::now().timestamp_millis() + (delay_secs * 1000.0) as i64;
            (
                "CREATING",
                bson::Bson::DateTime(bson::DateTime::from_millis(at)),
            )
        };

        let table_doc = doc! {
            "_id": { "account_id": account_id, "table_name": &input.table_name },
            "key_schema": key_schema_bson,
            "attribute_definitions": attr_defs_bson,
            "billing_mode": billing_str,
            "provisioned_throughput": pt_bson.unwrap_or(bson::Bson::Null),
            "stream_specification": stream_bson.unwrap_or(bson::Bson::Null),
            "table_status": table_status,
            "status_transition_at": status_transition_at,
            "creation_date_time": bson::DateTime::from_millis((creation_epoch * 1000.0) as i64),
            "table_size_bytes": 0_i64,
            "item_count": 0_i64,
            "table_arn": &table_arn_val,
            "table_id": &table_id,
            "deletion_protection_enabled": deletion_protection,
            "ttl_attribute": bson::Bson::Null,
            "stream_label": stream_label_bson,
            "table_class": table_class_bson,
            "sse_specification": sse_spec_bson,
            "on_demand_throughput": on_demand_bson,
        };

        let tables_coll = self.catalog_db.collection::<Document>("tables");
        tables_coll.insert_one(table_doc).await.map_err(|e| {
            if e.to_string().contains("E11000") {
                StorageError::TableAlreadyExists(input.table_name.clone())
            } else {
                StorageError::Internal(e.to_string())
            }
        })?;

        // Create the data collection with appropriate indexes
        let coll_name = data_collection_name(&table_id);
        self.data_db
            .create_collection(&coll_name)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let data_coll = self.data_db.collection::<Document>(&coll_name);

        // Create index based on sort key type
        if let Some((_, sk_type)) = sk_info(&input.key_schema, &input.attribute_definitions) {
            let sk_field = match sk_type {
                ScalarAttributeType::S => "sk_s",
                ScalarAttributeType::N => "sk_n",
                ScalarAttributeType::B => "sk_b",
            };
            let index_keys = doc! { "pk": 1, sk_field: 1 };
            let mut index_opts = IndexOptions::builder().unique(true).build();
            // Use simple collation for string sort keys (byte-order)
            if sk_type == ScalarAttributeType::S {
                index_opts.collation =
                    Some(Collation::builder().locale("simple".to_string()).build());
            }
            let index = IndexModel::builder()
                .keys(index_keys)
                .options(index_opts)
                .build();
            data_coll
                .create_index(index)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        } else {
            // PK-only index
            let index = IndexModel::builder()
                .keys(doc! { "pk": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build();
            data_coll
                .create_index(index)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Initialize stream shards if streaming is enabled. shard_id is
        // derived from table_id (UUID), never table_name — see
        // stream_engine::build_shard_id for the security rationale.
        if stream_label_opt.is_some() {
            self.init_stream_shards(&table_id).await?;
        }

        // Handle GSI creation
        let gsi_descriptions = if let Some(ref gsis) = input.global_secondary_indexes {
            let mut descs = Vec::new();
            for gsi in gsis {
                let index_id = uuid::Uuid::new_v4().to_string();
                let index_arn_val =
                    index_arn(&self.region, account_id, &input.table_name, &gsi.index_name);

                // Store index metadata in catalog
                let key_schema_bson = bson::to_bson(&gsi.key_schema)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let projection_bson = bson::to_bson(&gsi.projection)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let index_pt_bson = gsi
                    .provisioned_throughput
                    .as_ref()
                    .map(bson::to_bson)
                    .transpose()
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let index_doc = doc! {
                    "_id": { "table_id": &table_id, "index_name": &gsi.index_name },
                    "index_id": &index_id,
                    "index_type": "GSI",
                    "key_schema": key_schema_bson,
                    "projection": projection_bson,
                    "index_status": "ACTIVE",
                    "provisioned_throughput": index_pt_bson.unwrap_or(bson::Bson::Null),
                };

                self.catalog_db
                    .collection::<Document>("indexes")
                    .insert_one(index_doc)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                self.create_index_data_collection(
                    &index_id,
                    &gsi.key_schema,
                    &input.key_schema,
                    &input.attribute_definitions,
                )
                .await?;

                descs.push(GsiDescription {
                    index_name: gsi.index_name.clone(),
                    key_schema: gsi.key_schema.clone(),
                    projection: gsi.projection.clone(),
                    index_status: "ACTIVE".to_string(),
                    provisioned_throughput: gsi.provisioned_throughput.as_ref().map(|pt| {
                        ProvisionedThroughputDescription {
                            read_capacity_units: pt.read_capacity_units,
                            write_capacity_units: pt.write_capacity_units,
                            number_of_decreases_today: 0,
                            last_increase_date_time: None,
                            last_decrease_date_time: None,
                        }
                    }),
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn: index_arn_val,
                });
            }
            Some(descs)
        } else {
            None
        };

        // Handle LSI creation
        let lsi_descriptions = if let Some(ref lsis) = input.local_secondary_indexes {
            let mut descs = Vec::new();
            for lsi in lsis {
                let index_id = uuid::Uuid::new_v4().to_string();
                let index_arn_val =
                    index_arn(&self.region, account_id, &input.table_name, &lsi.index_name);

                let key_schema_bson = bson::to_bson(&lsi.key_schema)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let projection_bson = bson::to_bson(&lsi.projection)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let index_doc = doc! {
                    "_id": { "table_id": &table_id, "index_name": &lsi.index_name },
                    "index_id": &index_id,
                    "index_type": "LSI",
                    "key_schema": key_schema_bson,
                    "projection": projection_bson,
                    "index_status": "ACTIVE",
                    "provisioned_throughput": bson::Bson::Null,
                };

                self.catalog_db
                    .collection::<Document>("indexes")
                    .insert_one(index_doc)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                self.create_index_data_collection(
                    &index_id,
                    &lsi.key_schema,
                    &input.key_schema,
                    &input.attribute_definitions,
                )
                .await?;

                descs.push(LsiDescription {
                    index_name: lsi.index_name.clone(),
                    key_schema: lsi.key_schema.clone(),
                    projection: lsi.projection.clone(),
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn: index_arn_val,
                });
            }
            Some(descs)
        } else {
            None
        };

        // Build stream ARN from pre-computed label
        let stream_arn_opt = stream_label_opt
            .as_ref()
            .map(|label| stream_arn(&self.region, account_id, &input.table_name, label));

        let pt_desc = match &input.provisioned_throughput {
            Some(pt) => ProvisionedThroughputDescription {
                read_capacity_units: pt.read_capacity_units,
                write_capacity_units: pt.write_capacity_units,
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

        let billing_summary = if billing_mode == BillingMode::PayPerRequest {
            Some(BillingModeSummary {
                billing_mode: BillingMode::PayPerRequest,
                last_update_to_pay_per_request_date_time: Some(creation_epoch),
            })
        } else {
            None
        };

        // Store initial tags if provided
        if let Some(ref tags) = input.tags {
            let tags_coll = self.catalog_db.collection::<Document>("tags");
            for tag in tags {
                tags_coll
                    .update_one(
                        doc! { "resource_arn": &table_arn_val, "tag_key": &tag.key },
                        doc! { "$set": { "resource_arn": &table_arn_val, "tag_key": &tag.key, "tag_value": &tag.value } },
                    )
                    .upsert(true)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        // Derive the SSEDescription from the SSESpecification, mirroring the
        // postgres backend (table_helpers.rs:329-346). The specification's
        // `Enabled: true` becomes a KMS-status ENABLED description with a
        // synthesized ARN. Anything else omits the field.
        let sse_description = input.sse_specification.as_ref().and_then(|spec| {
            let enabled = spec
                .get("Enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if enabled {
                Some(SseDescription {
                    status: "ENABLED".to_owned(),
                    sse_type: Some(SseType::KMS),
                    kms_master_key_arn: Some(format!(
                        "arn:aws:kms:{}:{}:key/default",
                        self.region, account_id
                    )),
                })
            } else {
                None
            }
        });
        let table_class_summary = input
            .table_class
            .as_deref()
            .map(|tc| serde_json::json!({ "TableClass": tc }));

        Ok(TableDescription {
            table_name: input.table_name,
            key_schema: input.key_schema,
            attribute_definitions: input.attribute_definitions,
            table_status: if table_status == "CREATING" {
                TableStatus::Creating
            } else {
                TableStatus::Active
            },
            creation_date_time: creation_epoch,
            table_size_bytes: 0,
            item_count: 0,
            table_arn: table_arn_val,
            table_id,
            provisioned_throughput: pt_desc,
            billing_mode_summary: billing_summary,
            global_secondary_indexes: gsi_descriptions,
            local_secondary_indexes: lsi_descriptions,
            stream_specification: input.stream_specification,
            latest_stream_arn: stream_arn_opt,
            latest_stream_label: stream_label_opt,
            deletion_protection_enabled: deletion_protection,
            sse_description,
            table_class_summary,
            on_demand_throughput: input.on_demand_throughput,
            // Fields for features this backend does not implement, vector
            // indexes today, take their defaults. Adding one to
            // TableDescription then does not break this build.
            ..Default::default()
        })
    }

    async fn delete_table_impl(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;

        // Fetch the table first
        let desc = self
            .describe_table_impl(account_id, &input.table_name)
            .await?;

        // Check deletion protection
        if desc.deletion_protection_enabled {
            return Err(StorageError::DeletionProtected(input.table_name.clone()));
        }

        // Mark as DELETING
        let tables_coll = self.catalog_db.collection::<Document>("tables");
        tables_coll
            .update_one(
                doc! { "_id": { "account_id": account_id, "table_name": &input.table_name } },
                doc! { "$set": { "table_status": "DELETING" } },
            )
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Drop the data collection
        let coll_name = data_collection_name(&desc.table_id);
        self.data_db
            .collection::<Document>(&coll_name)
            .drop()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Drop each physical GSI/LSI collection before deleting its catalog
        // metadata. The index documents are stored separately from the table
        // document, so dropping the base collection alone does not remove
        // `_ddb_<index_id>` collections.
        self.drop_index_collections_for_table(&desc.table_id)
            .await?;

        // Tags are keyed by the table ARN rather than table_id. Remove them
        // before deleting the table metadata so a table recreated with the
        // same name cannot inherit the previous table's tags.
        self.catalog_db
            .collection::<Document>("tags")
            .delete_many(doc! { "resource_arn": &desc.table_arn })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        self.gsi_cache_invalidate(&desc.table_id);

        // Delete stream_shards, stream_records, and their sequence counters
        // for this table. Prevents a table recreated with the same name
        // (which will get a fresh table_id) from inheriting the deleted
        // table's stream history. RFC-0003 §8.2 (table-name reuse).
        self.cleanup_stream_state_for_table(&desc.table_id).await?;

        // Delete the table metadata
        tables_coll
            .delete_one(
                doc! { "_id": { "account_id": account_id, "table_name": &input.table_name } },
            )
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(TableDescription {
            table_status: TableStatus::Deleting,
            ..desc
        })
    }

    pub(crate) async fn describe_table_impl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;

        let tables_coll = self.catalog_db.collection::<Document>("tables");
        let table_doc = tables_coll
            .find_one(doc! { "_id": { "account_id": account_id, "table_name": table_name } })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        self.doc_to_table_description(&table_doc).await
    }

    async fn list_tables_impl(
        &self,
        account_id: &str,
        input: ListTablesInput,
    ) -> Result<ListTablesOutput, StorageError> {
        Self::validate_account_id(account_id)?;

        use futures::TryStreamExt;

        let limit = i64::from(input.limit.unwrap_or(100));
        let tables_coll = self.catalog_db.collection::<Document>("tables");

        let mut filter = doc! { "_id.account_id": account_id };
        if let Some(ref start) = input.exclusive_start_table_name {
            filter.insert("_id.table_name", doc! { "$gt": start });
        }

        let opts = mongodb::options::FindOptions::builder()
            .sort(doc! { "_id.table_name": 1 })
            .limit(limit + 1)
            .projection(doc! { "_id.table_name": 1 })
            .build();

        let cursor = tables_coll
            .find(filter)
            .with_options(opts)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let docs: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let names: Vec<String> = docs
            .iter()
            .filter_map(|d| {
                d.get_document("_id")
                    .ok()
                    .and_then(|id| id.get_str("table_name").ok())
                    .map(std::string::ToString::to_string)
            })
            .collect();

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let limit_usize = limit as usize;

        if names.len() > limit_usize {
            Ok(ListTablesOutput {
                last_evaluated_table_name: Some(names[limit_usize - 1].clone()),
                table_names: names[..limit_usize].to_vec(),
            })
        } else {
            Ok(ListTablesOutput {
                table_names: names,
                last_evaluated_table_name: None,
            })
        }
    }

    async fn update_table_impl(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;

        let tables_coll = self.catalog_db.collection::<Document>("tables");

        // Reject ProvisionedThroughput when the effective billing mode is
        // PAY_PER_REQUEST. The effective mode is the requested billing_mode
        // when the request changes it, otherwise the table's current mode.
        // Real DynamoDB returns "Neither ReadCapacityUnits nor WriteCapacityUnits
        // can be specified when BillingMode is PAY_PER_REQUEST". Postgres does
        // this same check under a FOR UPDATE row lock in update_table.rs; mongo
        // reads the current billing_mode via find_one and relies on the fact
        // that any concurrent billing-mode change would then be rejected by its
        // own no-op check (not yet implemented — see R-8 followup).
        if input.provisioned_throughput.is_some() {
            let effective_ppr = match input.billing_mode {
                Some(BillingMode::PayPerRequest) => true,
                Some(BillingMode::Provisioned) => false,
                None => {
                    let table_doc = tables_coll
                        .find_one(doc! {
                            "_id": { "account_id": account_id, "table_name": &input.table_name },
                        })
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    table_doc
                        .and_then(|d| d.get_str("billing_mode").ok().map(str::to_owned))
                        .as_deref()
                        == Some("PAY_PER_REQUEST")
                }
            };
            if effective_ppr {
                return Err(StorageError::Validation(
                    "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST".to_owned(),
                ));
            }
        }

        // Build update document
        let mut update_doc = Document::new();

        if let Some(billing_mode) = &input.billing_mode {
            let billing_str = match billing_mode {
                BillingMode::Provisioned => "PROVISIONED",
                BillingMode::PayPerRequest => "PAY_PER_REQUEST",
            };
            update_doc.insert("billing_mode", billing_str);
        }

        if let Some(pt) = &input.provisioned_throughput {
            let pt_bson = bson::to_bson(pt).map_err(|e| StorageError::Internal(e.to_string()))?;
            update_doc.insert("provisioned_throughput", pt_bson);
        }

        if let Some(dp) = input.deletion_protection_enabled {
            update_doc.insert("deletion_protection_enabled", dp);
        }

        if let Some(tc) = &input.table_class {
            update_doc.insert("table_class", tc);
        }

        if let Some(odt) = &input.on_demand_throughput {
            let odt_bson = bson::to_bson(odt).map_err(|e| StorageError::Internal(e.to_string()))?;
            update_doc.insert("on_demand_throughput", odt_bson);
        }

        if let Some(ss) = &input.stream_specification {
            let ss_bson = bson::to_bson(ss).map_err(|e| StorageError::Internal(e.to_string()))?;
            update_doc.insert("stream_specification", ss_bson);
            if ss.stream_enabled {
                let table_doc = tables_coll
                    .find_one(doc! { "_id": { "account_id": account_id, "table_name": &input.table_name } })
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                    .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
                let table_id = table_doc
                    .get_str("table_id")
                    .map_err(|_| StorageError::Internal("missing table_id".to_string()))?;

                // Idempotent re-enable: if shards already exist for this
                // table, reuse them and preserve the existing
                // stream_label. Otherwise a repeat UpdateTable would
                // insert duplicate shards (DescribeStream would then
                // report N × k) and rotate stream_label, invalidating
                // stream ARNs previously handed out to consumers.
                let shards_coll = self.data_db.collection::<Document>("stream_shards");
                let existing_shard = shards_coll
                    .find_one(doc! { "table_id": table_id })
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                if existing_shard.is_none() {
                    let label = format_stream_label(time::OffsetDateTime::now_utc());
                    update_doc.insert("stream_label", &label);
                    self.init_stream_shards(table_id).await?;
                } else if table_doc
                    .get_str("stream_label")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .is_none()
                {
                    // Shards exist but the label was cleared by a
                    // previous disable — restore a fresh label so the
                    // ARN resolves again.
                    let label = format_stream_label(time::OffsetDateTime::now_utc());
                    update_doc.insert("stream_label", &label);
                }
            }
        }

        if !update_doc.is_empty() {
            tables_coll
                .update_one(
                    doc! { "_id": { "account_id": account_id, "table_name": &input.table_name } },
                    doc! { "$set": &update_doc },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Handle GSI updates
        if let Some(gsi_updates) = &input.global_secondary_index_updates {
            // Persist the request's attribute definitions, merged into the stored
            // set. This backend previously never wrote them at all, so a created
            // index's key attributes were missing from the catalog and `sk_info`
            // resolved the index's own sort key to `None`, making it behave as
            // hash-only. Merging (rather than replacing) is what keeps the base
            // table's pk/sk definitions intact, which is issue #259 on the SQL
            // backends. Read-modify-write without a transaction, matching the rest
            // of this method; the catalog row is only written by UpdateTable.
            let merged_attr_defs = if let Some(new_attr_defs) = &input.attribute_definitions {
                // Read just the stored definitions rather than a full describe_table_impl,
                // which would also load every index. Two concurrent UpdateTables on one
                // table can still interleave here and lose one side's additions; that is
                // inherent to this backend's UpdateTable, which is non-transactional
                // throughout (the index inserts and collection creation below are not
                // atomic with this write either), so the window is narrowed rather than
                // closed.
                let current_defs: Vec<extenddb_core::types::AttributeDefinition> = tables_coll
                    .find_one(
                        doc! { "_id": { "account_id": account_id, "table_name": &input.table_name } },
                    )
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                    .as_ref()
                    .and_then(|d| d.get("attribute_definitions"))
                    .map(|b| bson::from_bson(b.clone()))
                    .transpose()
                    .map_err(|e| StorageError::Internal(format!("attr_defs parse error: {e}")))?
                    .unwrap_or_default();
                let merged = merge_attribute_definitions(&current_defs, new_attr_defs);
                let merged_bson =
                    bson::to_bson(&merged).map_err(|e| StorageError::Internal(e.to_string()))?;
                tables_coll
                    .update_one(
                        doc! { "_id": { "account_id": account_id, "table_name": &input.table_name } },
                        doc! { "$set": { "attribute_definitions": merged_bson } },
                    )
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Some(merged)
            } else {
                None
            };
            for update in gsi_updates {
                if let Some(create) = &update.create {
                    // Fetch table_id
                    let desc = self
                        .describe_table_impl(account_id, &input.table_name)
                        .await?;
                    let index_id = uuid::Uuid::new_v4().to_string();

                    let key_schema_bson = bson::to_bson(&create.key_schema)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let projection_bson = bson::to_bson(&create.projection)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let pt_bson = create
                        .provisioned_throughput
                        .as_ref()
                        .map(bson::to_bson)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;

                    // Enter CREATING; the background gsi_backfill_worker
                    // in ttl_worker.rs discovers this row, backfills the
                    // base table, and flips the status to ACTIVE. Matches
                    // DDB's async UpdateTable contract (§2.4 in RFC-0003).
                    // Live writes during the backfill window sync through
                    // sync_indexes (upserts) so the eventual state is
                    // convergent regardless of interleaving.
                    let index_doc = doc! {
                        "_id": { "table_id": &desc.table_id, "index_name": &create.index_name },
                        "index_id": &index_id,
                        "index_type": "GSI",
                        "key_schema": key_schema_bson,
                        "projection": projection_bson,
                        "index_status": "CREATING",
                        "provisioned_throughput": pt_bson.unwrap_or(bson::Bson::Null),
                    };

                    self.catalog_db
                        .collection::<Document>("indexes")
                        .insert_one(index_doc)
                        .await
                        .map_err(|e| {
                            if e.to_string().contains("E11000") {
                                StorageError::IndexAlreadyExists(create.index_name.clone())
                            } else {
                                StorageError::Internal(e.to_string())
                            }
                        })?;

                    // Pre-create the mongo collection + query indexes
                    // before the backfill worker starts writing — the
                    // worker's upserts would work on an un-indexed
                    // collection but subsequent GetItem/Query traffic
                    // on the CREATING index would run coll-scans. D-m7.
                    self.create_index_data_collection(
                        &index_id,
                        &create.key_schema,
                        &desc.key_schema,
                        merged_attr_defs
                            .as_deref()
                            .unwrap_or(&desc.attribute_definitions),
                    )
                    .await?;

                    self.gsi_cache_set(&desc.table_id, true);
                }

                if let Some(delete) = &update.delete {
                    let desc = self
                        .describe_table_impl(account_id, &input.table_name)
                        .await?;
                    let indexes_coll = self.catalog_db.collection::<Document>("indexes");
                    let index_filter = doc! { "_id": { "table_id": &desc.table_id, "index_name": &delete.index_name } };
                    let index_doc = indexes_coll
                        .find_one(index_filter.clone())
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let index_id = index_doc
                        .as_ref()
                        .and_then(|doc| doc.get_str("index_id").ok())
                        .map(str::to_owned)
                        .ok_or_else(|| StorageError::IndexNotFound(delete.index_name.clone()))?;

                    // Remove the catalog entry before dropping the physical
                    // collection. Otherwise a concurrent Query can observe
                    // an ACTIVE index in the catalog after its collection has
                    // already been dropped; MongoDB treats a missing
                    // collection as an empty result rather than a missing
                    // resource.
                    indexes_coll
                        .delete_one(index_filter)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;

                    // Invalidate cache — may still have other GSIs
                    self.gsi_cache_invalidate(&desc.table_id);

                    // The physical cleanup is still reported to the caller if
                    // it fails, but readers no longer observe the deleted
                    // index as available while that cleanup is in progress.
                    self.drop_index_collection(&index_id).await?;
                }
            }

            // Prune definitions that no key and no surviving index references.
            //
            // The merge above is only half of DynamoDB's behaviour: the stored set
            // is also pruned to the attributes still referenced by the table key
            // schema or by an index that survives this update, so an unused
            // definition supplied with a GSI add is not stored and deleting a GSI
            // drops the definitions only that index used. See
            // effective_attribute_definitions.
            //
            // This runs after the create/delete loop, which is what makes a
            // deletion prune: describe_table_impl reports exactly the indexes that
            // exist now. It is a second write rather than being folded into the
            // merge above because index creation inside the loop needs the merged
            // set before the surviving set is known. This backend's UpdateTable is
            // non-transactional throughout, so this widens no window the merge did
            // not already have.
            let desc = self
                .describe_table_impl(account_id, &input.table_name)
                .await?;
            let mut surviving_index_key_schemas: Vec<Vec<KeySchemaElement>> = Vec::new();
            for gsi in desc.global_secondary_indexes.iter().flatten() {
                surviving_index_key_schemas.push(gsi.key_schema.clone());
            }
            for lsi in desc.local_secondary_indexes.iter().flatten() {
                surviving_index_key_schemas.push(lsi.key_schema.clone());
            }

            // The request's definitions are already folded into the stored set by
            // the merge above, so nothing further is contributed here.
            let effective = effective_attribute_definitions(
                &desc.attribute_definitions,
                &[],
                &desc.key_schema,
                &surviving_index_key_schemas,
            );
            if effective != desc.attribute_definitions {
                let effective_bson =
                    bson::to_bson(&effective).map_err(|e| StorageError::Internal(e.to_string()))?;
                tables_coll
                    .update_one(
                        doc! { "_id": { "account_id": account_id, "table_name": &input.table_name } },
                        doc! { "$set": { "attribute_definitions": effective_bson } },
                    )
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        self.describe_table_impl(account_id, &input.table_name)
            .await
    }

    pub(crate) async fn table_key_info_impl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableKeyInfo, StorageError> {
        Self::validate_account_id(account_id)?;

        let tables_coll = self.catalog_db.collection::<Document>("tables");
        let table_doc = tables_coll
            .find_one(doc! { "_id": { "account_id": account_id, "table_name": table_name } })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        self.table_key_info_from_doc(&table_doc, true).await
    }

    /// Load `TableKeyInfo` by `table_id`. The tables catalog has a
    /// unique index on `table_id`, so this is a single-doc lookup.
    /// Used by the GSI backfill worker, which discovers work items
    /// keyed by `table_id`. Skips the ACTIVE-status guard so a table
    /// that is temporarily in a transient state (CREATING, UPDATING)
    /// can still be backfilled — backfill is decoupled from data-plane
    /// availability.
    pub(crate) async fn table_key_info_by_table_id_impl(
        &self,
        table_id: &str,
    ) -> Result<TableKeyInfo, StorageError> {
        let tables_coll = self.catalog_db.collection::<Document>("tables");
        let table_doc = tables_coll
            .find_one(doc! { "table_id": table_id })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::TableNotFound(table_id.to_string()))?;

        self.table_key_info_from_doc(&table_doc, false).await
    }

    async fn table_key_info_from_doc(
        &self,
        table_doc: &Document,
        require_active: bool,
    ) -> Result<TableKeyInfo, StorageError> {
        let id_doc = table_doc
            .get_document("_id")
            .map_err(|_| StorageError::Internal("missing _id".to_string()))?;
        let table_name = id_doc
            .get_str("table_name")
            .map_err(|_| StorageError::Internal("missing _id.table_name".to_string()))?
            .to_string();
        let account_id = id_doc
            .get_str("account_id")
            .map_err(|_| StorageError::Internal("missing _id.account_id".to_string()))?
            .to_string();

        let status = table_doc.get_str("table_status").unwrap_or("ACTIVE");
        if require_active && status != "ACTIVE" {
            // This guard gates data-plane key-schema resolution. DynamoDB
            // returns ResourceNotFoundException (not ResourceInUse) for a
            // data-plane op against a table that is not yet ACTIVE, matching
            // the postgres backend; TableNotActive would map to ResourceInUse.
            return Err(StorageError::TableNotFound(table_name));
        }

        let table_id = table_doc
            .get_str("table_id")
            .map_err(|_| StorageError::Internal("missing table_id".to_string()))?
            .to_string();

        let key_schema_bson = table_doc
            .get("key_schema")
            .ok_or_else(|| StorageError::Internal("missing key_schema".to_string()))?;
        let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
            bson::from_bson(key_schema_bson.clone())
                .map_err(|e| StorageError::Internal(format!("key_schema parse error: {e}")))?;

        let attr_defs_bson = table_doc
            .get("attribute_definitions")
            .ok_or_else(|| StorageError::Internal("missing attribute_definitions".to_string()))?;
        let attribute_definitions: Vec<extenddb_core::types::AttributeDefinition> =
            bson::from_bson(attr_defs_bson.clone())
                .map_err(|e| StorageError::Internal(format!("attr_defs parse error: {e}")))?;

        let stream_spec_bson = table_doc.get("stream_specification");
        let stream_specification = stream_spec_bson.and_then(|b| {
            if b.as_null().is_some() {
                None
            } else {
                bson::from_bson(b.clone()).ok()
            }
        });

        // Load all secondary indexes so per-index consumed capacity can be
        // computed from the cached TableKeyInfo without an extra describe_table
        // round-trip per write (matches the fields upstream added).
        use futures::TryStreamExt;
        let indexes_coll = self.catalog_db.collection::<Document>("indexes");
        let mut idx_cursor = indexes_coll
            .find(doc! { "_id.table_id": &table_id })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let mut infos = Vec::new();
        while let Some(idx_doc) = idx_cursor
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            infos.push(index_info_from_doc(&idx_doc)?);
        }
        // Grouped by core rather than matched here, so a new IndexType variant
        // does not break this backend. `index_info_from_doc` already rejects any
        // kind this backend cannot have created.
        let grouped = extenddb_core::types::partition_indexes(infos);
        let global_secondary_indexes = grouped.gsis;
        let local_secondary_indexes = grouped.lsis;
        let has_lsi = !local_secondary_indexes.is_empty();

        let key_info = TableKeyInfo {
            table_name,
            account_id,
            table_id,
            base_key_schema: key_schema.clone(),
            key_schema,
            attribute_definitions,
            has_lsi,
            global_secondary_indexes,
            local_secondary_indexes,
            stream_specification,
            // Fields for features this backend does not implement, vector
            // indexes today, take their defaults. Adding one to TableKeyInfo
            // then does not break this build.
            ..Default::default()
        };
        // Catalog metadata that cannot describe its own sort key would make the
        // keyed read paths fall back to a partition-only lookup and return the
        // wrong item, so refuse it here rather than serve a wrong answer (#259).
        key_info
            .validate_sort_key_definitions()
            .map_err(StorageError::Internal)?;
        Ok(key_info)
    }

    async fn index_info_impl(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<IndexInfo, StorageError> {
        // First, get the table_id
        let key_info = self.table_key_info_impl(account_id, table_name).await?;
        self.index_info_by_table_id_impl(&key_info.table_id, index_name)
            .await
    }

    pub(crate) async fn index_info_by_table_id_impl(
        &self,
        table_id: &str,
        index_name: &str,
    ) -> Result<IndexInfo, StorageError> {
        let indexes_coll = self.catalog_db.collection::<Document>("indexes");
        let index_doc = indexes_coll
            .find_one(doc! { "_id": { "table_id": table_id, "index_name": index_name } })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        let index_id = index_doc
            .get_str("index_id")
            .map_err(|_| StorageError::Internal("missing index_id".to_string()))?
            .to_string();
        let index_type_str = index_doc
            .get_str("index_type")
            .map_err(|_| StorageError::Internal("missing index_type".to_string()))?;
        let index_type = match index_type_str {
            "GSI" => IndexType::Gsi,
            "LSI" => IndexType::Lsi,
            _ => {
                return Err(StorageError::Internal(format!(
                    "unknown index type: {index_type_str}"
                )));
            }
        };

        let key_schema_bson = index_doc
            .get("key_schema")
            .ok_or_else(|| StorageError::Internal("missing key_schema in index".to_string()))?;
        let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
            bson::from_bson(key_schema_bson.clone())
                .map_err(|e| StorageError::Internal(format!("index key_schema parse: {e}")))?;

        let projection_bson = index_doc
            .get("projection")
            .ok_or_else(|| StorageError::Internal("missing projection in index".to_string()))?;
        let projection: extenddb_core::types::Projection = bson::from_bson(projection_bson.clone())
            .map_err(|e| StorageError::Internal(format!("index projection parse: {e}")))?;

        Ok(IndexInfo {
            index_name: index_name.to_string(),
            index_id,
            index_type,
            key_schema,
            projection,
        })
    }

    /// Convert a catalog table document to a `TableDescription`.
    async fn doc_to_table_description(
        &self,
        doc: &Document,
    ) -> Result<TableDescription, StorageError> {
        let id_doc = doc
            .get_document("_id")
            .map_err(|_| StorageError::Internal("missing _id".to_string()))?;
        let table_name = id_doc
            .get_str("table_name")
            .map_err(|_| StorageError::Internal("missing table_name".to_string()))?
            .to_string();
        let account_id = id_doc
            .get_str("account_id")
            .map_err(|_| StorageError::Internal("missing account_id".to_string()))?;

        let table_id = doc
            .get_str("table_id")
            .map_err(|_| StorageError::Internal("missing table_id".to_string()))?
            .to_string();
        let table_arn_val = doc
            .get_str("table_arn")
            .map_err(|_| StorageError::Internal("missing table_arn".to_string()))?
            .to_string();

        let status_str = doc.get_str("table_status").unwrap_or("ACTIVE");
        let table_status = match status_str {
            "CREATING" => TableStatus::Creating,
            "ACTIVE" => TableStatus::Active,
            "DELETING" => TableStatus::Deleting,
            "UPDATING" => TableStatus::Updating,
            _ => TableStatus::Active,
        };

        let creation_dt = doc
            .get_datetime("creation_date_time")
            .map(|dt| dt.timestamp_millis() as f64 / 1000.0)
            .unwrap_or(0.0);

        let table_size_bytes = doc.get_i64("table_size_bytes").unwrap_or(0);
        let item_count = doc.get_i64("item_count").unwrap_or(0);
        let deletion_protection = doc.get_bool("deletion_protection_enabled").unwrap_or(false);

        let key_schema_bson = doc
            .get("key_schema")
            .ok_or_else(|| StorageError::Internal("missing key_schema".to_string()))?;
        let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
            bson::from_bson(key_schema_bson.clone())
                .map_err(|e| StorageError::Internal(format!("key_schema: {e}")))?;

        let attr_defs_bson = doc
            .get("attribute_definitions")
            .ok_or_else(|| StorageError::Internal("missing attribute_definitions".to_string()))?;
        let attribute_definitions: Vec<extenddb_core::types::AttributeDefinition> =
            bson::from_bson(attr_defs_bson.clone())
                .map_err(|e| StorageError::Internal(format!("attr_defs: {e}")))?;

        let stream_specification = doc.get("stream_specification").and_then(|b| {
            if b.as_null().is_some() {
                None
            } else {
                bson::from_bson(b.clone()).ok()
            }
        });

        let billing_str = doc.get_str("billing_mode").unwrap_or("PROVISIONED");
        let billing_mode = match billing_str {
            "PAY_PER_REQUEST" => BillingMode::PayPerRequest,
            _ => BillingMode::Provisioned,
        };

        let pt_desc = doc
            .get("provisioned_throughput")
            .and_then(|b| {
                if b.as_null().is_some() {
                    None
                } else {
                    bson::from_bson::<extenddb_core::types::ProvisionedThroughput>(b.clone()).ok()
                }
            })
            .map_or(
                ProvisionedThroughputDescription {
                    read_capacity_units: 0,
                    write_capacity_units: 0,
                    number_of_decreases_today: 0,
                    last_increase_date_time: None,
                    last_decrease_date_time: None,
                },
                |pt| ProvisionedThroughputDescription {
                    read_capacity_units: pt.read_capacity_units,
                    write_capacity_units: pt.write_capacity_units,
                    number_of_decreases_today: 0,
                    last_increase_date_time: None,
                    last_decrease_date_time: None,
                },
            );

        let billing_summary = if billing_mode == BillingMode::PayPerRequest {
            Some(BillingModeSummary {
                billing_mode: BillingMode::PayPerRequest,
                last_update_to_pay_per_request_date_time: Some(creation_dt),
            })
        } else {
            None
        };

        // Fetch indexes
        let indexes_coll = self.catalog_db.collection::<Document>("indexes");
        use futures::TryStreamExt;
        let index_cursor = indexes_coll
            .find(doc! { "_id.table_id": &table_id })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let index_docs: Vec<Document> = index_cursor
            .try_collect()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut gsis = Vec::new();
        let mut lsis = Vec::new();

        for idx_doc in &index_docs {
            let idx_id_doc = idx_doc
                .get_document("_id")
                .map_err(|_| StorageError::Internal("missing index _id".to_string()))?;
            let idx_name = idx_id_doc
                .get_str("index_name")
                .map_err(|_| StorageError::Internal("missing index_name".to_string()))?
                .to_string();
            let idx_type = idx_doc.get_str("index_type").unwrap_or("GSI");

            let idx_ks_bson = idx_doc
                .get("key_schema")
                .ok_or_else(|| StorageError::Internal("missing index key_schema".to_string()))?;
            let idx_key_schema: Vec<extenddb_core::types::KeySchemaElement> =
                bson::from_bson(idx_ks_bson.clone())
                    .map_err(|e| StorageError::Internal(format!("index key_schema: {e}")))?;

            let idx_proj_bson = idx_doc
                .get("projection")
                .ok_or_else(|| StorageError::Internal("missing index projection".to_string()))?;
            let idx_projection: extenddb_core::types::Projection =
                bson::from_bson(idx_proj_bson.clone())
                    .map_err(|e| StorageError::Internal(format!("index projection: {e}")))?;

            let idx_arn = index_arn(&self.region, account_id, &table_name, &idx_name);

            match idx_type {
                "GSI" => {
                    let idx_pt = idx_doc
                        .get("provisioned_throughput")
                        .and_then(|b| {
                            if b.as_null().is_some() {
                                None
                            } else {
                                bson::from_bson::<extenddb_core::types::ProvisionedThroughput>(
                                    b.clone(),
                                )
                                .ok()
                            }
                        })
                        .map(|pt| ProvisionedThroughputDescription {
                            read_capacity_units: pt.read_capacity_units,
                            write_capacity_units: pt.write_capacity_units,
                            number_of_decreases_today: 0,
                            last_increase_date_time: None,
                            last_decrease_date_time: None,
                        });

                    gsis.push(GsiDescription {
                        index_name: idx_name,
                        key_schema: idx_key_schema,
                        projection: idx_projection,
                        index_status: idx_doc
                            .get_str("index_status")
                            .unwrap_or("ACTIVE")
                            .to_string(),
                        provisioned_throughput: idx_pt,
                        index_size_bytes: 0,
                        item_count: 0,
                        index_arn: idx_arn,
                    });
                }
                "LSI" => {
                    lsis.push(LsiDescription {
                        index_name: idx_name,
                        key_schema: idx_key_schema,
                        projection: idx_projection,
                        index_size_bytes: 0,
                        item_count: 0,
                        index_arn: idx_arn,
                    });
                }
                _ => {}
            }
        }

        // Stream info
        let stream_label = doc
            .get_str("stream_label")
            .ok()
            .map(std::string::ToString::to_string);
        let stream_arn_opt = stream_label
            .as_ref()
            .map(|label| stream_arn(&self.region, account_id, &table_name, label));

        // TableClass / SSEDescription / OnDemandThroughput — read back the
        // fields persisted at CreateTable time. Same shape as postgres'
        // table_helpers.rs:329-353.
        let table_class_summary = doc
            .get_str("table_class")
            .ok()
            .map(|tc| serde_json::json!({ "TableClass": tc }));
        let sse_description = doc.get("sse_specification").and_then(|b| {
            let spec: serde_json::Value = bson::from_bson(b.clone()).ok()?;
            let enabled = spec
                .get("Enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if enabled {
                Some(SseDescription {
                    status: "ENABLED".to_owned(),
                    sse_type: Some(SseType::KMS),
                    kms_master_key_arn: Some(format!(
                        "arn:aws:kms:{}:{}:key/default",
                        self.region, account_id
                    )),
                })
            } else {
                None
            }
        });
        let on_demand_throughput: Option<OnDemandThroughput> = doc
            .get("on_demand_throughput")
            .and_then(|b| bson::from_bson(b.clone()).ok());

        Ok(TableDescription {
            table_name,
            key_schema,
            attribute_definitions,
            table_status,
            creation_date_time: creation_dt,
            table_size_bytes,
            item_count,
            table_arn: table_arn_val,
            table_id,
            provisioned_throughput: pt_desc,
            billing_mode_summary: billing_summary,
            global_secondary_indexes: if gsis.is_empty() { None } else { Some(gsis) },
            local_secondary_indexes: if lsis.is_empty() { None } else { Some(lsis) },
            stream_specification,
            latest_stream_arn: stream_arn_opt,
            latest_stream_label: stream_label,
            deletion_protection_enabled: deletion_protection,
            sse_description,
            table_class_summary,
            on_demand_throughput,
            // Fields for features this backend does not implement, vector
            // indexes today, take their defaults. Adding one to
            // TableDescription then does not break this build.
            ..Default::default()
        })
    }

    /// Create the mongo collection for a GSI/LSI and add the indexes
    /// its query path relies on. Every read against the index goes
    /// through `find` predicates on `(pk, sk_*, base_pk, base_sk_*)`
    /// with sorts on the same fields — without indexes those queries
    /// devolve to a collection scan per read. Called from
    /// `create_table_impl` (initial GSI/LSI) and the UpdateTable GSI-
    /// create path (D-m7).
    pub(crate) async fn create_index_data_collection(
        &self,
        index_id: &str,
        index_key_schema: &[KeySchemaElement],
        base_key_schema: &[KeySchemaElement],
        attribute_definitions: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let coll_name = data_collection_name(index_id);
        // create_collection is idempotent on recent MongoDB; a duplicate
        // means we retried through a crash after the first success. Log
        // + continue rather than surfacing an error to the caller.
        if let Err(e) = self.data_db.create_collection(&coll_name).await {
            tracing::debug!("index collection {coll_name} pre-exists or race: {e}");
        }
        let coll = self.data_db.collection::<Document>(&coll_name);

        // Sort/paginate key: (pk, sk?, base_pk, base_sk?). Same tuple
        // that scan_impl / query_impl sort by post-D-C1. Not unique —
        // GSI keys are non-unique across base items; index docs are
        // disambiguated by base-key components in the _id.
        let idx_sk_field = sk_info(index_key_schema, attribute_definitions).map(|(_, t)| match t {
            ScalarAttributeType::S => ("sk_s", true),
            ScalarAttributeType::N => ("sk_n", false),
            ScalarAttributeType::B => ("sk_b", false),
        });
        let base_sk_field = sk_info(base_key_schema, attribute_definitions).map(|(_, t)| match t {
            ScalarAttributeType::S => "base_sk_s",
            ScalarAttributeType::N => "base_sk_n",
            ScalarAttributeType::B => "base_sk_b",
        });

        let mut keys = doc! { "pk": 1 };
        if let Some((sk_f, _)) = idx_sk_field {
            keys.insert(sk_f, 1);
        }
        keys.insert("base_pk", 1);
        if let Some(base_sk_f) = base_sk_field {
            keys.insert(base_sk_f, 1);
        }

        // String sort keys need the `simple` collation so range
        // comparisons behave as byte-wise, matching the query path.
        let uses_string_sort =
            matches!(idx_sk_field, Some((_, true))) || matches!(base_sk_field, Some("base_sk_s"));
        let mut opts = IndexOptions::builder().build();
        if uses_string_sort {
            opts.collation = Some(Collation::builder().locale("simple".to_string()).build());
        }

        coll.create_index(IndexModel::builder().keys(keys).options(opts).build())
            .await
            .map_err(|e| StorageError::Internal(format!("index-coll index: {e}")))?;

        Ok(())
    }

    /// Drop the physical collection backing one secondary index.
    async fn drop_index_collection(&self, index_id: &str) -> Result<(), StorageError> {
        let coll_name = data_collection_name(index_id);
        let collections = self
            .data_db
            .list_collection_names()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if collections.iter().any(|name| name == &coll_name) {
            self.data_db
                .collection::<Document>(&coll_name)
                .drop()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        Ok(())
    }

    /// Drop all physical secondary-index collections for a table before its
    /// catalog index documents are removed.
    async fn drop_index_collections_for_table(&self, table_id: &str) -> Result<(), StorageError> {
        use futures::TryStreamExt;

        let indexes_coll = self.catalog_db.collection::<Document>("indexes");
        let mut cursor = indexes_coll
            .find(doc! { "_id.table_id": table_id })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut index_ids = Vec::new();
        while let Some(index_doc) = cursor
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            let index_id = index_doc.get_str("index_id").map_err(|_| {
                StorageError::Internal("index document missing index_id".to_owned())
            })?;
            index_ids.push(index_id.to_owned());
        }

        for index_id in index_ids {
            self.drop_index_collection(&index_id).await?;
        }

        indexes_coll
            .delete_many(doc! { "_id.table_id": table_id })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }
}

/// Build an `IndexInfo` from an `indexes` catalog document whose `_id` is
/// `{ table_id, index_name }`. Used to populate the GSI/LSI lists carried on
/// `TableKeyInfo` for per-index consumed-capacity computation.
fn index_info_from_doc(index_doc: &Document) -> Result<IndexInfo, StorageError> {
    let index_name = index_doc
        .get_document("_id")
        .ok()
        .and_then(|id| id.get_str("index_name").ok())
        .ok_or_else(|| StorageError::Internal("missing _id.index_name".to_string()))?
        .to_string();
    let index_id = index_doc
        .get_str("index_id")
        .map_err(|_| StorageError::Internal("missing index_id".to_string()))?
        .to_string();
    let index_type = match index_doc.get_str("index_type") {
        Ok("GSI") => IndexType::Gsi,
        Ok("LSI") => IndexType::Lsi,
        other => {
            return Err(StorageError::Internal(format!(
                "unknown index type: {other:?}"
            )));
        }
    };
    let key_schema_bson = index_doc
        .get("key_schema")
        .ok_or_else(|| StorageError::Internal("missing key_schema in index".to_string()))?;
    let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
        bson::from_bson(key_schema_bson.clone())
            .map_err(|e| StorageError::Internal(format!("index key_schema parse: {e}")))?;
    let projection_bson = index_doc
        .get("projection")
        .ok_or_else(|| StorageError::Internal("missing projection in index".to_string()))?;
    let projection: extenddb_core::types::Projection = bson::from_bson(projection_bson.clone())
        .map_err(|e| StorageError::Internal(format!("index projection parse: {e}")))?;
    Ok(IndexInfo {
        index_name,
        index_id,
        index_type,
        key_schema,
        projection,
    })
}
