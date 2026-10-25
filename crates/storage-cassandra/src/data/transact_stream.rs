// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The two pure pieces of a transacted mutation's stream record: which old
//! image it carries, and how its statement is batched with the index rows.
//! `transactions.rs` calls both from its commit phase.

use cdrs_tokio::query::BatchQueryBuilder;
use extenddb_core::types::Item;

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

/// The batches one committed transactional mutation's secondary effects run as.
///
/// There is one, holding the index rows and the stream record together: a
/// reader must not see an index row for a mutation whose record was not
/// written, and a crash must not leave the index rows standing without it. The
/// caller executes exactly what this returns, so the one-batch property is
/// asserted in a unit test rather than resting on the shape of the calling
/// code, which no integration test can see.
pub(super) fn secondary_effect_batches(
    index_batch: BatchQueryBuilder,
    stream_stmt: Option<String>,
) -> Vec<BatchQueryBuilder> {
    let batch = match stream_stmt {
        Some(stmt) => {
            index_batch.add_query(stmt, cdrs_tokio::query::QueryValues::SimpleValues(vec![]))
        }
        None => index_batch,
    };
    vec![batch]
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::AttributeValue;

    fn built_statements(batch: BatchQueryBuilder) -> Vec<String> {
        use cdrs_tokio::frame::message_batch::BatchQuerySubj;

        batch
            .build()
            .unwrap()
            .request
            .queries
            .into_iter()
            .map(|query| match query.subject {
                BatchQuerySubj::QueryString(cql) => cql.to_string(),
                BatchQuerySubj::PreparedId(_) => {
                    panic!("secondary effects are built as CQL strings")
                }
            })
            .collect()
    }

    fn index_batch() -> BatchQueryBuilder {
        BatchQueryBuilder::new()
            .add_query(
                "INSERT INTO ks.index_rows (a) VALUES (1)".to_owned(),
                cdrs_tokio::query::QueryValues::SimpleValues(vec![]),
            )
            .add_query(
                "DELETE FROM ks.index_rows WHERE a = 2".to_owned(),
                cdrs_tokio::query::QueryValues::SimpleValues(vec![]),
            )
    }

    const RECORD: &str = "INSERT INTO ks.stream_records (shard_id) VALUES ('s')";

    /// The index rows and the record commit together or not at all, which is
    /// what makes a transacted record as atomic with its item as the
    /// single-item path's record is.
    #[test]
    fn index_rows_and_the_stream_record_share_one_batch() {
        let batches = secondary_effect_batches(index_batch(), Some(RECORD.to_owned()));
        assert_eq!(batches.len(), 1, "the record must not run as its own batch");
        let statements = built_statements(batches.into_iter().next().unwrap());
        assert_eq!(statements.len(), 3, "got {statements:?}");
        assert!(statements[0].contains("INSERT INTO ks.index_rows"));
        assert!(statements[1].contains("DELETE FROM ks.index_rows"));
        assert_eq!(statements[2], RECORD);
    }

    #[test]
    fn a_table_with_no_index_runs_the_record_in_one_batch() {
        let batches = secondary_effect_batches(BatchQueryBuilder::new(), Some(RECORD.to_owned()));
        assert_eq!(batches.len(), 1);
        assert_eq!(
            built_statements(batches.into_iter().next().unwrap()),
            vec![RECORD.to_owned()]
        );
    }

    #[test]
    fn index_rows_without_a_record_run_unchanged() {
        let batches = secondary_effect_batches(index_batch(), None);
        assert_eq!(batches.len(), 1);
        assert_eq!(
            built_statements(batches.into_iter().next().unwrap()).len(),
            2
        );
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
