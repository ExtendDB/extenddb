// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BatchWriteItem` operation handler.

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    BatchWriteItemInput, BatchWriteItemOutput, Item, KeySchemaElement, ReturnItemCollectionMetrics,
    WriteRequest, extract_key, item_size_bytes,
};
use extenddb_core::validation::{
    validate_attribute_name_sizes, validate_batch_item_keys, validate_batch_key_only,
    validate_item_nesting_depth, validate_item_size, validate_key_sizes,
};

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::serialize_output;
use crate::stream_capture;
use crate::{DispatchMetrics, DispatchResult};

/// Maximum number of write requests across all tables in a single `BatchWriteItem`.
const MAX_BATCH_WRITE_ITEMS: usize = 25;

/// Handle a `BatchWriteItem` request.
///
/// Writes (put or delete) items across one or more tables. Each operation is
/// executed individually — no cross-item conditions. `DynamoDB` limits: max 25
/// items total, max 16 MB request size, max 400 KB per item.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
pub async fn handle_batch_write_item(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    let input: BatchWriteItemInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    // Validate: RequestItems must not be empty
    if input.request_items.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "The requestItems parameter is required for BatchWriteItem".to_owned(),
        ));
    }

    // Validate: per-table operations <= 25
    for (table_name, reqs) in &input.request_items {
        if reqs.len() > MAX_BATCH_WRITE_ITEMS {
            let items_repr = reqs
                .iter()
                .map(|_| "WriteRequest")
                .collect::<Vec<_>>()
                .join(", ");
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '{{{table_name}=[{items_repr}]}}' at 'requestItems' failed to satisfy constraint: \
                 Map value must satisfy constraint: [Member must have length less than or equal to 25, \
                 Member must have length greater than or equal to 1]"
            )));
        }
    }

    // Validate: total operations across all tables <= 25
    let total_ops: usize = input.request_items.values().map(|r| r.len()).sum();
    if total_ops > MAX_BATCH_WRITE_ITEMS {
        return Err(DynamoDbError::ValidationException(
            "Too many items requested for the BatchWriteItem call".to_owned(),
        ));
    }

    // Validate: each table must have at least one request
    for (table_name, reqs) in &input.request_items {
        if reqs.is_empty() {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '[]' at 'requestItems.{table_name}.member' failed to satisfy constraint: Member must have length greater than or equal to 1"
            )));
        }
    }

    // Validate: each WriteRequest has exactly one of PutRequest or DeleteRequest
    for reqs in input.request_items.values() {
        for wr in reqs {
            match (&wr.put_request, &wr.delete_request) {
                (None, None) | (Some(_), Some(_)) => {
                    return Err(DynamoDbError::ValidationException(
                        "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes".to_owned(),
                    ));
                }
                _ => {}
            }
        }
    }

    let empty_maps = ExpressionMaps::default();
    let mut all_icm: HashMap<String, Vec<extenddb_core::types::ItemCollectionMetrics>> =
        HashMap::new();
    let mut total_wcu: f64 = 0.0;
    let mut per_table_wcu: HashMap<String, f64> = HashMap::new();

    for (table_name, reqs) in &input.request_items {
        let key_info = ctx
            .storage
            .table_key_info(&ctx.account_id, table_name)
            .await
            .map_err(storage_err_to_dynamo)?;

        // Validate: no duplicate keys within the same table (using key schema)
        validate_no_duplicate_keys(reqs, &key_info.key_schema)?;

        let view_type = stream_capture::stream_view_type(&key_info);

        for wr in reqs {
            if let Some(put) = &wr.put_request {
                validate_batch_item_keys(
                    &put.item,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )?;
                validate_item_nesting_depth(&put.item)?;
                validate_item_size(&put.item, ctx.limits.max_item_size_bytes)?;
                validate_attribute_name_sizes(&put.item, &ctx.limits)?;
                validate_key_sizes(&put.item, &key_info.key_schema, &ctx.limits)?;

                collect_icm_if_needed(
                    input.return_item_collection_metrics,
                    &key_info,
                    &put.item,
                    table_name,
                    &mut all_icm,
                );

                let stream = view_type.map(|vt| extenddb_storage::StreamCapture {
                    view_type: vt,
                    user_identity: None,
                    region: ctx.region.clone(),
                });
                let need_old_for_stream = stream.is_some();
                let item_wcu = capacity_helpers::write_capacity_units(item_size_bytes(&put.item));
                total_wcu += item_wcu;
                *per_table_wcu.entry(table_name.clone()).or_default() += item_wcu;
                let _old_item = ctx
                    .storage
                    .put_item(
                        &key_info,
                        put.item.clone(),
                        need_old_for_stream,
                        None,
                        &empty_maps,
                        stream.as_ref(),
                    )
                    .await
                    .map_err(storage_err_to_dynamo)?;
            } else if let Some(del) = &wr.delete_request {
                validate_batch_key_only(
                    &del.key,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                )?;

                collect_icm_if_needed(
                    input.return_item_collection_metrics,
                    &key_info,
                    &del.key,
                    table_name,
                    &mut all_icm,
                );

                let stream = view_type.map(|vt| extenddb_storage::StreamCapture {
                    view_type: vt,
                    user_identity: None,
                    region: ctx.region.clone(),
                });
                let need_old_for_stream = stream.is_some();
                // TODO(fidelity): DynamoDB charges WCU based on old item size for deletes,
                // but old item size is not available here. Using key size as lower bound.
                let item_wcu = capacity_helpers::write_capacity_units(item_size_bytes(&del.key));
                total_wcu += item_wcu;
                *per_table_wcu.entry(table_name.clone()).or_default() += item_wcu;
                let _old_item = ctx
                    .storage
                    .delete_item(
                        &key_info,
                        &del.key,
                        need_old_for_stream,
                        None,
                        &empty_maps,
                        stream.as_ref(),
                    )
                    .await
                    .map_err(storage_err_to_dynamo)?;
            }
        }
    }

    let consumed_capacity = capacity_helpers::batch_write_capacity(
        input.return_consumed_capacity,
        per_table_wcu.iter().map(|(t, cu)| (t.as_str(), *cu)),
    );

    // Per-item WCU already accumulated above (M-1: DynamoDB rounds per item, then sums).
    let wcu = total_wcu;

    let output = BatchWriteItemOutput {
        unprocessed_items: HashMap::new(),
        consumed_capacity,
        item_collection_metrics: if all_icm.is_empty() {
            None
        } else {
            Some(all_icm)
        },
    };
    let body = serialize_output(&output)?;
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            write_capacity_units: wcu,
            ..Default::default()
        },
    })
}

/// Validate that no two write requests in the same table target the same key.
///
/// `DynamoDB` returns `ValidationException` for duplicate keys within a single
/// `BatchWriteItem` request for the same table.
///
/// Uses `Vec::contains()` (O(n²)) because `AttributeValue` does not implement
/// `Hash` or `Ord`, and max 25 items makes this negligible.
fn validate_no_duplicate_keys(
    reqs: &[WriteRequest],
    key_schema: &[KeySchemaElement],
) -> Result<(), DynamoDbError> {
    let mut seen: Vec<Item> = Vec::with_capacity(reqs.len());
    for wr in reqs {
        let key = extract_write_key(wr, key_schema);
        if seen.contains(&key) {
            return Err(DynamoDbError::ValidationException(
                "Provided list of item keys contains duplicates".to_owned(),
            ));
        }
        seen.push(key);
    }
    Ok(())
}

/// Extract the key attributes from a `WriteRequest` for duplicate detection.
fn extract_write_key(wr: &WriteRequest, key_schema: &[KeySchemaElement]) -> Item {
    if let Some(put) = &wr.put_request {
        extract_key(&put.item, key_schema)
    } else if let Some(del) = &wr.delete_request {
        del.key.clone()
    } else {
        Item::new()
    }
}

/// Collect `ItemCollectionMetrics` for a single item if requested and the table has an LSI.
fn collect_icm_if_needed(
    ricm: ReturnItemCollectionMetrics,
    key_info: &extenddb_core::types::TableKeyInfo,
    item_or_key: &Item,
    table_name: &str,
    all_icm: &mut HashMap<String, Vec<extenddb_core::types::ItemCollectionMetrics>>,
) {
    if let Some(m) =
        capacity_helpers::item_metrics(ricm, &key_info.key_schema, item_or_key, key_info.has_lsi)
    {
        all_icm.entry(table_name.to_owned()).or_default().push(m);
    }
}
