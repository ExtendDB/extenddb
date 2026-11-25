// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pins the `ReadOptions` of the three reads the PartiQL layer makes on its
//! own, outside the classic handlers: the base-table read under an LSI read
//! that reaches back carries the client's `ConsistentRead`; the read that
//! evaluates an `UPDATE` whose value nests past the item limit and the read
//! that sizes the charge of a batch write that failed at its item are
//! consistent whatever the client sent.

use std::sync::Arc;

use serde_json::{Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::error::StorageError;

use super::fixtures::{EVENTS, LSI, ORDERS};
use super::recording::{self, Fake, ReadCall};
use super::{SENT_AND_EXPECTED, context, with_consistent_read};
use crate::OperationContext;
use crate::partiql::PartiqlExecutor;
use crate::partiql::test_context::Fixed;

/// The context of the other read-options tests with an authorizer that
/// allows every statement; without one the PartiQL layer refuses them all.
fn partiql_context(storage: &Arc<Fake>) -> OperationContext {
    OperationContext {
        item_authorizer: Some(Fixed::allowing()),
        ..context(storage)
    }
}

/// An index row of `Events`: the table key and the LSI sort key.
fn events_row(sk: &str) -> Item {
    Item::from([
        ("pk".to_owned(), AttributeValue::S("device-1".to_owned())),
        ("sk".to_owned(), AttributeValue::S(sk.to_owned())),
        (
            "created".to_owned(),
            AttributeValue::S("2026-01-01".to_owned()),
        ),
    ])
}

/// A value of `levels` maps around a string leaf, in wire form.
fn nested(levels: usize) -> Value {
    let mut value = json!({"S": "leaf"});
    for _ in 0..levels {
        value = json!({"M": {"n": value}});
    }
    value
}

/// The single storage read a write that failed at its item made.
fn single_read_of_failed_write(storage: &Fake) -> ReadCall {
    let reads = storage.reads();
    assert_eq!(
        reads.len(),
        1,
        "the write made {} storage reads: {reads:?}",
        reads.len()
    );
    reads.into_iter().next().unwrap()
}

/// `SELECT` of an attribute the LSI does not project reads each walked row's
/// item back from the table; that read carries the client's `ConsistentRead`,
/// as the LSI read it follows does.
#[tokio::test]
async fn lsi_read_that_reaches_back_carries_the_clients_consistent_read() {
    for (sent, expected) in SENT_AND_EXPECTED {
        let storage = recording::storage();
        storage
            .read_pages
            .lock()
            .unwrap()
            .push_back((vec![events_row("evt-1"), events_row("evt-2")], None));
        let ctx = partiql_context(&storage);
        let body = with_consistent_read(
            json!({"Statement": format!("SELECT payload FROM \"{EVENTS}\".\"{LSI}\" WHERE pk = 'device-1'")}),
            sent,
        );
        let _ = ctx
            .execute_statement(body)
            .await
            .unwrap_or_else(|e| panic!("the read failed before storage: {e:?}"));

        let reads = storage.reads();
        assert_eq!(
            reads.len(),
            3,
            "one index read and one item read per row: {reads:?}"
        );
        assert_eq!(reads[0].op, "Query");
        assert_eq!(reads[0].index_name.as_deref(), Some(LSI));
        for read in &reads[1..] {
            assert_eq!(read.op, "GetItem");
            assert_eq!(read.table, EVENTS);
            assert_eq!(read.index_name, None);
            assert_eq!(
                read.opts.consistent_read, expected,
                "ConsistentRead {sent:?} reached the base-table read as {}",
                read.opts.consistent_read
            );
        }
    }
}

/// An `UPDATE` whose `SET` value nests past the item limit is evaluated
/// against the stored item before anything is written; that read is
/// consistent whatever the client sent, because a write condition is
/// evaluated against the item every acknowledged write has reached.
#[tokio::test]
async fn an_update_past_the_item_limit_reads_its_item_consistent() {
    for sent in [Some(false), None] {
        let storage = recording::storage();
        let ctx = partiql_context(&storage);
        let body = with_consistent_read(
            json!({
                "Statement": format!("UPDATE \"{ORDERS}\" SET payload = ? WHERE pk = 'order-1'"),
                "Parameters": [nested(32)]
            }),
            sent,
        );
        let result = ctx.execute_statement(body).await.map(|r| r.body);
        assert!(
            matches!(
                result,
                Err(DynamoDbError::ConditionalCheckFailedException(..))
            ),
            "a missing item fails the update's condition: {result:?}"
        );
        let call = single_read_of_failed_write(&storage);
        assert_eq!(call.op, "GetItem");
        assert_eq!(call.table, ORDERS);
        assert!(
            call.opts.consistent_read,
            "ConsistentRead {sent:?}: the item check read the item eventually consistent"
        );
    }
}

/// A batch write that failed at its item is charged the write units of the
/// stored item; the read that sizes the charge is consistent whatever the
/// client sent.
#[tokio::test]
async fn a_failed_batch_write_sizes_its_charge_from_a_consistent_read() {
    for sent in [Some(false), None] {
        let storage = recording::storage();
        *storage.update_error.lock().unwrap() = Some(StorageError::ConditionFailed(None));
        let ctx = partiql_context(&storage);
        let body = json!({
            "Statements": [with_consistent_read(
                json!({"Statement": format!("UPDATE \"{ORDERS}\" SET payload = 'x' WHERE pk = 'order-1'")}),
                sent
            )],
            "ReturnConsumedCapacity": "TOTAL"
        });
        let response = ctx
            .batch_execute_statement(body)
            .await
            .unwrap_or_else(|e| panic!("the batch was refused: {e:?}"));
        assert_eq!(
            response.body["Responses"][0]["Error"]["Code"],
            json!("ConditionalCheckFailed"),
            "the member must fail at its item: {}",
            response.body
        );
        let call = single_read_of_failed_write(&storage);
        assert_eq!(call.op, "GetItem");
        assert_eq!(call.table, ORDERS);
        assert!(
            call.opts.consistent_read,
            "ConsistentRead {sent:?}: the charge was sized from an eventually consistent read"
        );
    }
}
