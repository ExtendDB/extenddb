// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BatchWriteItem` operation handler.

use std::collections::HashMap;
use std::time::Instant;

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
use extenddb_storage::error::StorageError;

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::request_limits;
use crate::serialize_output;
use crate::stream_capture;
use crate::{DispatchMetrics, DispatchResult};

/// Handle a `BatchWriteItem` request.
///
/// Writes (put or delete) items across one or more tables. Each operation is
/// executed individually — no cross-item conditions. `DynamoDB` limits: max 25
/// items total, max 16 MB request size, max 400 KB per item.
///
/// A write the backend cannot apply now (see `is_deferrable`) does not stop
/// the batch: its request is returned in UnprocessedItems and the requests
/// after it are still tried.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
/// A write the backend cannot apply now fails the batch only when no request
/// of the batch was applied.
pub async fn handle_batch_write_item(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    // The storage calls of the batch run in one request clock scope, so a
    // backend that bounds how long a contended write keeps retrying measures
    // one bound for the whole batch instead of one per write request.
    extenddb_storage::request_clock::scope(Instant::now(), write_batch(body, ctx)).await
}

/// The body of [`handle_batch_write_item`], which runs it inside the batch's
/// request clock scope.
async fn write_batch(body: Value, ctx: &OperationContext) -> Result<DispatchResult, DynamoDbError> {
    let input: BatchWriteItemInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    // Validate the table and request counts: RequestItems not empty, at most
    // 25 tables, at most 25 requests per table and in total, at least one
    // request per table. The server runs the same checks before authorization.
    let counts: Vec<(&str, Option<usize>)> = input
        .request_items
        .iter()
        .map(|(table_name, reqs)| (table_name.as_str(), Some(reqs.len())))
        .collect();
    request_limits::check_batch_counts("BatchWriteItem", &counts)?;

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
    // Per-table per-index write units, populated only when INDEXES is
    // requested: attributing them needs each write's old image, which is
    // an extra read the aggregate-only shapes never pay for.
    let want_index_cc =
        input.return_consumed_capacity == extenddb_core::types::ReturnConsumedCapacity::Indexes;
    let mut per_table_gsi: HashMap<String, HashMap<String, f64>> = HashMap::new();
    let mut per_table_lsi: HashMap<String, HashMap<String, f64>> = HashMap::new();
    // The write requests the backend could not apply now (see `is_deferrable`),
    // returned to the client in UnprocessedItems, and the first such error,
    // which the batch answers when none of its requests was applied.
    let mut unprocessed: HashMap<String, Vec<WriteRequest>> = HashMap::new();
    let mut first_deferral: Option<StorageError> = None;
    let mut applied = 0usize;

    for (table_name, reqs) in &input.request_items {
        let key_info = ctx
            .table_key_info_uncached(table_name)
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
                extenddb_core::validation::validate_vector_write(
                    &put.item,
                    &key_info.vector_indexes,
                    &key_info.attribute_definitions,
                )?;

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
                let written = ctx
                    .storage
                    .put_item(
                        &key_info,
                        put.item.clone(),
                        need_old_for_stream || want_index_cc,
                        None,
                        &empty_maps,
                        stream.as_ref(),
                    )
                    .await;
                let old_item = match written {
                    Ok(old_item) => old_item,
                    Err(e) if is_deferrable(&e) => {
                        tracing::warn!(
                            table = %table_name,
                            error = %e,
                            "BatchWriteItem: a put request was not applied; it is returned in UnprocessedItems"
                        );
                        unprocessed
                            .entry(table_name.clone())
                            .or_default()
                            .push(wr.clone());
                        first_deferral.get_or_insert(e);
                        continue;
                    }
                    Err(e) => return Err(storage_err_to_dynamo(e)),
                };
                applied += 1;
                if want_index_cc {
                    let (gsi, lsi) = capacity_helpers::index_write_units(
                        old_item.as_ref(),
                        Some(&put.item),
                        false,
                        &key_info,
                    );
                    let g = per_table_gsi.entry(table_name.clone()).or_default();
                    for (name, cu) in gsi {
                        *g.entry(name).or_default() += cu;
                    }
                    let l = per_table_lsi.entry(table_name.clone()).or_default();
                    for (name, cu) in lsi {
                        *l.entry(name).or_default() += cu;
                    }
                }
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
                let written = ctx
                    .storage
                    .delete_item(
                        &key_info,
                        &del.key,
                        need_old_for_stream || want_index_cc,
                        None,
                        &empty_maps,
                        stream.as_ref(),
                    )
                    .await;
                let old_item = match written {
                    Ok(old_item) => old_item,
                    Err(e) if is_deferrable(&e) => {
                        tracing::warn!(
                            table = %table_name,
                            error = %e,
                            "BatchWriteItem: a delete request was not applied; it is returned in UnprocessedItems"
                        );
                        unprocessed
                            .entry(table_name.clone())
                            .or_default()
                            .push(wr.clone());
                        first_deferral.get_or_insert(e);
                        continue;
                    }
                    Err(e) => return Err(storage_err_to_dynamo(e)),
                };
                applied += 1;
                if want_index_cc {
                    let (gsi, lsi) = capacity_helpers::index_write_units(
                        old_item.as_ref(),
                        None,
                        false,
                        &key_info,
                    );
                    let g = per_table_gsi.entry(table_name.clone()).or_default();
                    for (name, cu) in gsi {
                        *g.entry(name).or_default() += cu;
                    }
                    let l = per_table_lsi.entry(table_name.clone()).or_default();
                    for (name, cu) in lsi {
                        *l.entry(name).or_default() += cu;
                    }
                }
            }
        }
    }

    // No request of the batch was applied, so answering an error is safe: a
    // client that retries it resends requests none of which took effect.
    if applied == 0
        && let Some(e) = first_deferral
    {
        return Err(storage_err_to_dynamo(e));
    }

    // With INDEXES, each per-table entry carries the arms the batch actually
    // touched: base table plus every affected index, aggregate = their sum
    // (pinned by the ground-truth runs of 2026-08-24: an indexed put and a
    // sparse put in one batch answer total 3 = table 2 + gsi 1). TOTAL and
    // NONE keep the aggregate-only shapes.
    let consumed_capacity = if want_index_cc {
        Some(
            per_table_wcu
                .iter()
                .map(|(t, cu)| {
                    extenddb_core::types::ConsumedCapacity::write_indexed(
                        t,
                        *cu,
                        per_table_gsi.remove(t).unwrap_or_default(),
                        per_table_lsi.remove(t).unwrap_or_default(),
                        true,
                    )
                })
                .collect(),
        )
    } else {
        capacity_helpers::batch_write_capacity(
            input.return_consumed_capacity,
            per_table_wcu.iter().map(|(t, cu)| (t.as_str(), *cu)),
        )
    };

    // Per-item WCU already accumulated above (M-1: DynamoDB rounds per item, then sums).
    let wcu = total_wcu;

    let output = BatchWriteItemOutput {
        unprocessed_items: unprocessed,
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

/// Whether a failed write leaves its request for the client to retry, in
/// UnprocessedItems, instead of failing the batch.
///
/// Neither error applied the write. `Transient` is a write the backend could
/// not apply now and expects to apply on a retry; on MongoDB a write that kept
/// losing to concurrent writers until its contention deadline or its attempt
/// budget ran out answers it. `TransactionConflict` is a write that lost to a
/// transaction holding the item. Every other error still fails the batch.
///
/// Amazon DynamoDB answers a request it cannot apply this way. Measured in
/// us-east-1 on 2026-11-03 with client retries off, while TransactWriteItems
/// calls kept updating one item: of 80 batches that put the held item and then
/// a free item, 46 answered 200 with the held item's request in
/// UnprocessedItems and the free item written, and 53 of 80 batches with the
/// two requests the other way round did the same. Each of those 99 batches
/// reported 2 write capacity units, the units of both requests, which is why
/// the capacity of a request returned unprocessed is still counted here. A
/// batch whose only request was on the held item answered 500
/// InternalServerError in 54 of 80 calls, never 200 with its request
/// unprocessed.
///
/// Failing the batch after an earlier request was applied is what this
/// avoids: an SDK that retries the error resends the whole batch, so the
/// requests already applied are applied again, after any write another
/// client made to the same items in between.
fn is_deferrable(e: &StorageError) -> bool {
    matches!(
        e,
        StorageError::Transient(_) | StorageError::TransactionConflict(_)
    )
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
