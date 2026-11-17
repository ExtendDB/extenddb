// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The two pure pieces of a transacted mutation's stream record: which old
//! image it carries, and how its statement is batched with the index rows.
//! `transactions.rs` calls both from its commit phase.

use std::collections::HashMap;

use cdrs_tokio::frame::message_batch::{BatchQuery, BatchQuerySubj, BodyReqBatch};
use cdrs_tokio::query::{QueryBatch, QueryValues};
use cdrs_tokio::types::value::Value;
use extenddb_core::types::Item;

/// The most bytes, counting statement text and bound values, that the index
/// rows and the stream record may total while they share one batch.
///
/// Cassandra fails a LOGGED BATCH that writes more than one partition once its
/// data passes `batch_size_fail_threshold`, which is 50 KiB (51,200 bytes)
/// unless the operator changed it. The code does not read that setting.
/// 24,000 bytes is under half of the default, which leaves room for the
/// difference between this count and the size Cassandra measures, and for a
/// server configured with a lower threshold.
const SHARED_BATCH_BUDGET_BYTES: usize = 24_000;

/// The old image a transacted mutation's stream record carries, from the row as
/// the commit phase read it.
///
/// `created_to_prepare` marks a row that exists only because a PREPARE planted
/// it. Such a row's `item_data` is the request's own item for a Put and the
/// bare key for an Update, so there is no old image: the mutation is an insert.
/// Otherwise the old image is the row's `item_data`, which is itself absent
/// when the row holds a marker with no item, as a concurrent single-item delete
/// inside the PREPARE window leaves it.
pub(super) fn commit_old_image(row: Option<Item>, created_to_prepare: bool) -> Option<Item> {
    if created_to_prepare { None } else { row }
}

/// The batches one committed transactional mutation's secondary effects run as,
/// in the order they run.
///
/// While the index rows and the record total at most
/// `SHARED_BATCH_BUDGET_BYTES` they share one batch, which applies all of its
/// statements or none of them. Past that the index rows run first and the
/// record runs as a batch of its own. A LOGGED BATCH that writes a single
/// partition is not subject to the size limit, so a record of any size
/// commits alone.
///
/// The base row has already committed by the time these batches run, so a
/// batch Cassandra rejects leaves the item written without its index rows or
/// its record, and the caller answers with an error. Splitting keeps the
/// record from being the statement that pushes a batch over the limit. It does
/// not bound the index batch, which is sent whole: when the index rows write
/// more than one partition and pass the limit by themselves (two synchronous
/// GSIs, or one synchronous and one asynchronous GSI, from about 26 KB), the
/// item is written without them. The split costs the atomicity of the index
/// rows and the record for a record too large to share their batch: a failure
/// between the two batches leaves the index rows written and the record
/// unwritten, and nothing writes the record afterwards.
///
/// The caller executes exactly what this returns, in order, so the grouping is
/// asserted in unit tests rather than resting on the shape of the calling code.
pub(super) fn secondary_effect_batches(
    mut index_batch: QueryBatch,
    stream_stmt: Option<String>,
) -> Vec<QueryBatch> {
    let Some(stmt) = stream_stmt else {
        return vec![index_batch];
    };
    let record = BatchQuery::new(
        BatchQuerySubj::QueryString(stmt),
        QueryValues::SimpleValues(vec![]),
    );
    let index_bytes: usize = index_batch
        .request
        .queries
        .iter()
        .map(statement_bytes)
        .sum();
    if index_batch.request.queries.is_empty()
        || index_bytes + statement_bytes(&record) <= SHARED_BATCH_BUDGET_BYTES
    {
        index_batch.request.queries.push(record);
        return vec![index_batch];
    }

    let request = &index_batch.request;
    let record_batch = QueryBatch::new(
        BodyReqBatch::new(
            request.batch_type,
            vec![record],
            request.consistency,
            request.serial_consistency,
            request.timestamp,
            request.keyspace.clone(),
            request.now_in_seconds,
        ),
        HashMap::new(),
    );
    vec![index_batch, record_batch]
}

/// What a statement adds to a batch: its text (or prepared id) and the bytes of
/// every value bound to it.
fn statement_bytes(query: &BatchQuery) -> usize {
    let subject = match &query.subject {
        BatchQuerySubj::QueryString(cql) => cql.len(),
        BatchQuerySubj::PreparedId(id) => id.serialized_len(),
    };
    let values: usize = match &query.values {
        QueryValues::SimpleValues(values) => values.iter().map(value_bytes).sum(),
        QueryValues::NamedValues(values) => values.values().map(value_bytes).sum(),
    };
    subject + values
}

fn value_bytes(value: &Value) -> usize {
    match value {
        Value::Some(bytes) => bytes.len(),
        Value::Null | Value::NotSet => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::query::BatchQueryBuilder;
    use extenddb_core::types::AttributeValue;

    fn built_statements(batch: &QueryBatch) -> Vec<String> {
        batch
            .request
            .queries
            .iter()
            .map(|query| match &query.subject {
                BatchQuerySubj::QueryString(cql) => cql.clone(),
                BatchQuerySubj::PreparedId(_) => {
                    panic!("secondary effects are built as CQL strings")
                }
            })
            .collect()
    }

    /// An index batch with every setting the record batch has to carry over set
    /// to a value other than its default.
    fn index_batch() -> QueryBatch {
        BatchQueryBuilder::new()
            .with_consistency(Consistency::LocalQuorum)
            .with_serial_consistency(Consistency::LocalSerial)
            .with_timestamp(1_700_000_000_000_000)
            .with_keyspace("ks".to_owned())
            .with_now_in_seconds(1_700_000_000)
            .add_query(
                "INSERT INTO ks.index_rows (a) VALUES (1)".to_owned(),
                QueryValues::SimpleValues(vec![]),
            )
            .add_query(
                "DELETE FROM ks.index_rows WHERE a = 2".to_owned(),
                QueryValues::SimpleValues(vec![]),
            )
            .build()
            .unwrap()
    }

    fn batch_bytes(batch: &QueryBatch) -> usize {
        batch.request.queries.iter().map(statement_bytes).sum()
    }

    const RECORD: &str = "INSERT INTO ks.stream_records (shard_id) VALUES ('s')";

    /// A record statement of exactly `len` bytes.
    fn record_of_len(len: usize) -> String {
        let (head, tail) = (
            "INSERT INTO ks.stream_records (record_data) VALUES ('",
            "')",
        );
        format!("{head}{}{tail}", "x".repeat(len - head.len() - tail.len()))
    }

    /// The index rows and the record commit together or not at all, which is
    /// what makes a transacted record as atomic with its index rows as the
    /// single-item path's record is.
    #[test]
    fn index_rows_and_the_stream_record_share_one_batch() {
        let batches = secondary_effect_batches(index_batch(), Some(RECORD.to_owned()));
        assert_eq!(
            batches.len(),
            1,
            "a small record must not run as its own batch"
        );
        let statements = built_statements(&batches[0]);
        assert_eq!(statements.len(), 3, "got {statements:?}");
        assert!(statements[0].contains("INSERT INTO ks.index_rows"));
        assert!(statements[1].contains("DELETE FROM ks.index_rows"));
        assert_eq!(statements[2], RECORD);
    }

    #[test]
    fn a_table_with_no_index_runs_the_record_in_one_batch() {
        let empty = BatchQueryBuilder::new().build().unwrap();
        let batches = secondary_effect_batches(empty, Some(RECORD.to_owned()));
        assert_eq!(batches.len(), 1);
        assert_eq!(built_statements(&batches[0]), vec![RECORD.to_owned()]);
    }

    /// With no index rows there is nothing to split from, so a record of any
    /// size is one batch holding the record alone, never an empty batch
    /// followed by the record.
    #[test]
    fn a_large_record_with_no_index_rows_is_one_batch() {
        let empty = BatchQueryBuilder::new().build().unwrap();
        let record = record_of_len(300_000);
        let batches = secondary_effect_batches(empty, Some(record.clone()));
        assert_eq!(batches.len(), 1);
        assert_eq!(built_statements(&batches[0]), vec![record]);
    }

    #[test]
    fn index_rows_without_a_record_run_unchanged() {
        let batches = secondary_effect_batches(index_batch(), None);
        assert_eq!(batches.len(), 1);
        assert_eq!(built_statements(&batches[0]).len(), 2);
    }

    /// A record too large to share a batch with the index rows runs after
    /// them, alone, with the index batch's type and settings: consistency,
    /// serial consistency, timestamp, keyspace and now_in_seconds.
    #[test]
    fn a_30000_byte_record_runs_as_a_second_batch_after_the_index_rows() {
        let record = record_of_len(30_000);
        let batches = secondary_effect_batches(index_batch(), Some(record.clone()));
        assert_eq!(batches.len(), 2, "the record must run as its own batch");

        let index_statements = built_statements(&batches[0]);
        assert_eq!(index_statements.len(), 2, "got {index_statements:?}");
        assert!(index_statements[0].contains("INSERT INTO ks.index_rows"));
        assert!(index_statements[1].contains("DELETE FROM ks.index_rows"));

        assert_eq!(
            built_statements(&batches[1]),
            vec![record],
            "the record is last and alone"
        );
        let index = &batches[0].request;
        let record_batch = &batches[1].request;
        assert!(
            index.serial_consistency.is_some()
                && index.timestamp.is_some()
                && index.keyspace.is_some()
                && index.now_in_seconds.is_some(),
            "the index batch must set every field, or the comparisons below match on defaults"
        );
        assert_eq!(record_batch.consistency, Consistency::LocalQuorum);
        assert_eq!(record_batch.batch_type, index.batch_type);
        assert_eq!(
            record_batch.serial_consistency, index.serial_consistency,
            "the record batch must keep the serial consistency"
        );
        assert_eq!(
            record_batch.timestamp, index.timestamp,
            "the record batch must keep the timestamp"
        );
        assert_eq!(
            record_batch.keyspace, index.keyspace,
            "the record batch must keep the keyspace"
        );
        assert_eq!(
            record_batch.now_in_seconds, index.now_in_seconds,
            "the record batch must keep now_in_seconds"
        );
    }

    /// The record shares the batch up to the budget and runs alone one byte
    /// past it, counting the index statements' own size.
    #[test]
    fn the_record_shares_the_batch_up_to_the_budget_and_not_past_it() {
        let index_bytes = batch_bytes(&index_batch());

        let at_budget = record_of_len(SHARED_BATCH_BUDGET_BYTES - index_bytes);
        let batches = secondary_effect_batches(index_batch(), Some(at_budget));
        assert_eq!(batches.len(), 1);
        assert_eq!(batch_bytes(&batches[0]), SHARED_BATCH_BUDGET_BYTES);

        let past_budget = record_of_len(SHARED_BATCH_BUDGET_BYTES - index_bytes + 1);
        let batches = secondary_effect_batches(index_batch(), Some(past_budget));
        assert_eq!(batches.len(), 2);
    }

    /// Values bound to the index statements count toward the budget. An async
    /// GSI's queue row carries the whole new item as a bound value, so a small
    /// record can still be the statement that tips a batch over.
    #[test]
    fn bound_values_count_toward_the_budget() {
        let queue_row = BatchQueryBuilder::new()
            .with_consistency(Consistency::LocalQuorum)
            .add_query(
                "INSERT INTO ks.gsi_pending (new_item) VALUES (?)".to_owned(),
                QueryValues::SimpleValues(vec![Value::from("x".repeat(23_000).as_str())]),
            )
            .build()
            .unwrap();
        assert!(batch_bytes(&queue_row) > 23_000);

        let record = record_of_len(2_000);
        let batches = secondary_effect_batches(queue_row, Some(record.clone()));
        assert_eq!(
            batches.len(),
            2,
            "23,000 bound bytes and a 2,000-byte record pass the budget"
        );
        assert_eq!(built_statements(&batches[1]), vec![record]);
    }

    #[test]
    fn a_prepare_created_row_has_no_old_image() {
        let mut item = Item::new();
        item.insert("id".to_owned(), AttributeValue::S("a".to_owned()));

        assert_eq!(
            commit_old_image(Some(item.clone()), true),
            None,
            "a row PREPARE planted holds the request's own item, not an old image"
        );
        assert_eq!(commit_old_image(Some(item.clone()), false), Some(item));
        assert_eq!(commit_old_image(None, false), None);
        assert_eq!(commit_old_image(None, true), None);
    }
}
