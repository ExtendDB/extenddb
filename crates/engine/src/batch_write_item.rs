// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BatchWriteItem` operation handler.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    BatchWriteItemInput, BatchWriteItemOutput, Item, KeySchemaElement, ReturnItemCollectionMetrics,
    TableKeyInfo, WriteRequest, extract_key, item_size_bytes,
};
use extenddb_core::validation::{
    validate_attribute_name_sizes, validate_batch_item_keys, validate_batch_key_only,
    validate_item_nesting_depth, validate_item_size, validate_key_sizes,
};

use extenddb_storage::DataEngine;
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
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
pub async fn handle_batch_write_item(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
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

    // Read every table's key info before anything is written. A lookup that
    // failed is reported where `check_batch` reaches its table, so the first
    // fault reported is the one the checks reach first.
    let tables: Vec<(&String, &Vec<WriteRequest>)> = input.request_items.iter().collect();
    let mut lookups = Vec::with_capacity(tables.len());
    for (table_name, _) in &tables {
        lookups.push(ctx.table_key_info_uncached(table_name).await);
    }

    write_batch(
        &*ctx.storage,
        &ctx.limits,
        &ctx.region,
        &input,
        &tables,
        lookups,
    )
    .await
}

/// Check the whole batch, then write it.
///
/// The table lookups, the duplicate-key check and the validation of every
/// request run before the first write, in the order the tables and requests
/// are listed. A fault in any of them rejects the batch with nothing applied,
/// wherever the faulty request sits, which is what the `BatchWriteItem` API
/// reference documents for these faults. The writes run only after that, so a
/// write that cannot be applied now never leaves a later request unchecked,
/// and a request that is applied is never followed by a rejection of the
/// batch.
async fn write_batch(
    data: &dyn DataEngine,
    limits: &LimitsConfig,
    region: &Arc<str>,
    input: &BatchWriteItemInput,
    tables: &[(&String, &Vec<WriteRequest>)],
    lookups: Vec<Result<TableKeyInfo, StorageError>>,
) -> Result<DispatchResult, DynamoDbError> {
    let key_infos = check_batch(tables, lookups, limits)?;

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

    // Requests the batch did not apply: each write that failed because the
    // backend could not apply it now. Amazon DynamoDB tolerates the failure of
    // individual requests in a batch and reports them in UnprocessedItems for
    // the client to replay: "If any requested operations fail because the
    // table's provisioned throughput is exceeded or an internal processing
    // failure occurs, the failed operations are returned in the
    // UnprocessedItems response parameter." A failed request does not stop the
    // others. Measured on 2026-10-27 in us-east-1, with a TransactWriteItems
    // holding the items: a batch of two requests in which one was on a held
    // item answered 200 with that request in UnprocessedItems and the other
    // request applied, whichever came first. So every request is tried, and
    // the answer to one contended request does not depend on where it sat in
    // the batch.
    //
    // A batch of which no request applied is an error. The same measurement
    // answered 500 InternalServerError for batches whose requests were all on
    // held items (one request or two), and never 200 with every request
    // unprocessed. This server answers 503 ServiceUnavailable, which the SDKs
    // retry with backoff, so a client's UnprocessedItems loop cannot spin on a
    // batch that makes no progress. Each failed request spends its own retry
    // budget first, so a batch of 25 contended requests can spend 25 budgets
    // before it answers.
    //
    // The whole batch is rejected only for the request faults the same page
    // lists (a missing table, a key that does not match the schema, duplicate
    // keys, too many requests, an item or request over the size limit).
    // `check_batch` has found every one of those before the first write,
    // wherever the faulty request sits, so a deferral below never hides one
    // and a rejected batch never has an applied request.
    let mut unprocessed: HashMap<String, Vec<WriteRequest>> = HashMap::new();
    let mut applied = 0usize;

    'tables: for (table_index, ((table_name, reqs), key_info)) in
        tables.iter().zip(&key_infos).enumerate()
    {
        let table_name = *table_name;
        let view_type = stream_capture::stream_view_type(key_info);

        for (request_index, wr) in reqs.iter().enumerate() {
            let stream = view_type.map(|vt| extenddb_storage::StreamCapture {
                view_type: vt,
                user_identity: None,
                region: region.clone(),
            });
            let need_old_for_stream = stream.is_some();

            let (item_or_key, item_wcu, written) = if let Some(put) = &wr.put_request {
                let item_wcu = capacity_helpers::write_capacity_units(item_size_bytes(&put.item));
                let written = data
                    .put_item(
                        key_info,
                        put.item.clone(),
                        need_old_for_stream || want_index_cc,
                        None,
                        &empty_maps,
                        stream.as_ref(),
                    )
                    .await;
                (
                    &put.item,
                    item_wcu,
                    written.map(|old| (old, Some(&put.item))),
                )
            } else if let Some(del) = &wr.delete_request {
                // TODO(fidelity): DynamoDB charges WCU based on old item size for deletes,
                // but old item size is not available here. Using key size as lower bound.
                let item_wcu = capacity_helpers::write_capacity_units(item_size_bytes(&del.key));
                let written = data
                    .delete_item(
                        key_info,
                        &del.key,
                        need_old_for_stream || want_index_cc,
                        None,
                        &empty_maps,
                        stream.as_ref(),
                    )
                    .await;
                (&del.key, item_wcu, written.map(|old| (old, None)))
            } else {
                // Rejected by the shape check in `handle_batch_write_item`.
                continue;
            };

            let (old_item, new_item) = match written {
                Ok(outcome) => outcome,
                Err(e) if is_deferrable(&e) => {
                    tracing::warn!(
                        table = %table_name,
                        error = %e,
                        "BatchWriteItem: write not applied after {applied} committed; it is reported in UnprocessedItems if another request of the batch applies"
                    );
                    if matches!(e, StorageError::Connection(_)) {
                        // The backend is unreachable and the requests after
                        // this one would wait on it as well: leave them, and
                        // this one, for the client.
                        for (name, rest) in remaining_requests(tables, table_index, request_index) {
                            unprocessed.entry(name).or_default().extend(rest);
                        }
                        break 'tables;
                    }
                    unprocessed
                        .entry(table_name.clone())
                        .or_default()
                        .push(wr.clone());
                    continue;
                }
                Err(e) => return Err(storage_err_to_dynamo(e)),
            };
            applied += 1;

            collect_icm_if_needed(
                input.return_item_collection_metrics,
                key_info,
                item_or_key,
                table_name,
                &mut all_icm,
            );
            total_wcu += item_wcu;
            *per_table_wcu.entry(table_name.clone()).or_default() += item_wcu;
            if want_index_cc {
                let (gsi, lsi) = capacity_helpers::index_write_units(
                    old_item.as_ref(),
                    new_item,
                    false,
                    key_info,
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

    if applied == 0 && !unprocessed.is_empty() {
        tracing::warn!("BatchWriteItem: no request of the batch could be applied");
        return Err(DynamoDbError::ServiceUnavailable(
            "Service is temporarily unavailable".to_owned(),
        ));
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

/// Whether a failed write leaves that request for the client to replay
/// instead of failing the batch.
///
/// These are the errors a replay may clear: the backend could not apply the
/// request now, because the write lost every retry against concurrent writers
/// or because the connection failed. A fault in the request itself
/// (validation, a missing table, a condition) fails the batch as before, and
/// so does an internal error: the documentation lists an internal processing
/// failure among the causes of UnprocessedItems, but a storage fault here is
/// not cleared by replaying the request, and a client's UnprocessedItems loop
/// would spin on it. An internal error keeps answering HTTP 500, which is
/// what it answered before this handler deferred anything.
///
/// `TransactionConflict` is deferred because the service treats it that way.
/// Measured on 2026-10-27 in us-east-1 with client retries off, while a
/// TransactWriteItems held the items: a batch of two requests in which one
/// was on a held item answered 200 with that request in UnprocessedItems and
/// the other request applied, whichever of the two came first, and no
/// BatchWriteItem call answered TransactionConflictException.
fn is_deferrable(e: &StorageError) -> bool {
    matches!(
        e,
        StorageError::TransactionConflict(_)
            | StorageError::Transient(_)
            | StorageError::Connection(_)
    )
}

/// The requests from `tables[table_index][request_index]` on, in batch order:
/// the request that was not applied and every request after it, in its table
/// and in the tables that follow. These are the requests left unattempted
/// when a connection error ends the batch.
fn remaining_requests(
    tables: &[(&String, &Vec<WriteRequest>)],
    table_index: usize,
    request_index: usize,
) -> HashMap<String, Vec<WriteRequest>> {
    let mut out: HashMap<String, Vec<WriteRequest>> = HashMap::new();
    for (i, (table_name, reqs)) in tables.iter().enumerate().skip(table_index) {
        let from = if i == table_index { request_index } else { 0 };
        let rest = &reqs[from..];
        if !rest.is_empty() {
            out.insert((*table_name).clone(), rest.to_vec());
        }
    }
    out
}

/// Take every table's key info and validate every request, before the first
/// write. Returns the key info per table, in the order of `tables`.
///
/// Faults are reported in the order the writes used to reach them: for each
/// table in turn, its lookup, the duplicate-key check, then its requests in
/// order. The first fault rejects the whole batch.
fn check_batch(
    tables: &[(&String, &Vec<WriteRequest>)],
    lookups: Vec<Result<TableKeyInfo, StorageError>>,
    limits: &LimitsConfig,
) -> Result<Vec<TableKeyInfo>, DynamoDbError> {
    let mut key_infos = Vec::with_capacity(tables.len());
    for ((_, reqs), lookup) in tables.iter().zip(lookups) {
        let key_info = lookup.map_err(storage_err_to_dynamo)?;
        validate_no_duplicate_keys(reqs, &key_info.key_schema)?;
        for wr in reqs.iter() {
            validate_request(wr, &key_info, limits)?;
        }
        key_infos.push(key_info);
    }
    Ok(key_infos)
}

/// Validate one write request against its table.
fn validate_request(
    wr: &WriteRequest,
    key_info: &TableKeyInfo,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    if let Some(put) = &wr.put_request {
        validate_batch_item_keys(
            &put.item,
            &key_info.key_schema,
            &key_info.attribute_definitions,
        )?;
        validate_item_nesting_depth(&put.item)?;
        validate_item_size(&put.item, limits.max_item_size_bytes)?;
        validate_attribute_name_sizes(&put.item, limits)?;
        validate_key_sizes(&put.item, &key_info.key_schema, limits)?;
        extenddb_core::validation::validate_vector_write(
            &put.item,
            &key_info.vector_indexes,
            &key_info.attribute_definitions,
        )?;
    } else if let Some(del) = &wr.delete_request {
        validate_batch_key_only(
            &del.key,
            &key_info.key_schema,
            &key_info.attribute_definitions,
        )?;
    }
    Ok(())
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

#[cfg(test)]
mod scripted;
#[cfg(test)]
mod write_batch_deferral_tests;
#[cfg(test)]
mod write_batch_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{AttributeValue, DeleteRequest, PutRequest};

    fn put(pk: &str) -> WriteRequest {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
        WriteRequest {
            put_request: Some(PutRequest { item }),
            delete_request: None,
        }
    }

    fn delete(pk: &str) -> WriteRequest {
        let mut key = Item::new();
        key.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
        WriteRequest {
            put_request: None,
            delete_request: Some(DeleteRequest { key }),
        }
    }

    fn pks(reqs: &[WriteRequest]) -> Vec<String> {
        reqs.iter()
            .map(|wr| {
                let item = wr
                    .put_request
                    .as_ref()
                    .map(|p| &p.item)
                    .or(wr.delete_request.as_ref().map(|d| &d.key))
                    .expect("one of put or delete");
                match item.get("pk") {
                    Some(AttributeValue::S(s)) => s.clone(),
                    other => panic!("unexpected key {other:?}"),
                }
            })
            .collect()
    }

    #[test]
    fn remaining_requests_start_at_the_failed_request_and_cover_later_tables() {
        let t1 = "t1".to_owned();
        let t2 = "t2".to_owned();
        let r1 = vec![put("a"), delete("b"), put("c")];
        let r2 = vec![put("d")];
        let tables = vec![(&t1, &r1), (&t2, &r2)];

        let rest = remaining_requests(&tables, 0, 1);
        assert_eq!(pks(&rest["t1"]), ["b", "c"]);
        assert_eq!(pks(&rest["t2"]), ["d"]);

        // Failed on the last request of the last table: only that request.
        let rest = remaining_requests(&tables, 1, 0);
        assert!(!rest.contains_key("t1"));
        assert_eq!(pks(&rest["t2"]), ["d"]);

        // Failed on the last request of an earlier table: no entry for that
        // table's later requests, which do not exist.
        let rest = remaining_requests(&tables, 0, 2);
        assert_eq!(pks(&rest["t1"]), ["c"]);
        assert_eq!(pks(&rest["t2"]), ["d"]);
    }

    #[test]
    fn only_errors_a_replay_may_clear_are_deferred() {
        assert!(is_deferrable(&StorageError::TransactionConflict(
            String::new()
        )));
        assert!(is_deferrable(&StorageError::Transient(String::new())));
        assert!(is_deferrable(&StorageError::Connection(String::new())));
        // A storage fault is not cleared by a replay: it keeps answering 500
        // rather than being handed to the client's UnprocessedItems loop.
        assert!(!is_deferrable(&StorageError::Internal(String::new())));
        assert!(!is_deferrable(&StorageError::Validation(String::new())));
        assert!(!is_deferrable(&StorageError::TableNotFound(String::new())));
        assert!(!is_deferrable(&StorageError::ConditionFailed(None)));
        assert!(!is_deferrable(&StorageError::LimitExceeded(String::new())));
    }
}
