// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests of the `ClientRequestToken` that a cancelled
//! `TransactWriteItems` keeps. The service keeps a cancelled token used for 10
//! minutes from its last cancellation (measured 2026-11-05): another request
//! with it answers `IdempotentParameterMismatchException`, and the same
//! request runs again. The tests set `created_at` back in the token
//! collection instead of waiting.
//!
//! The tests read the replica set URI from `EXTENDDB_TEST_MONGODB_URI` and are
//! `#[ignore]`d, like the tests in the parent module.

use bson::{Document, doc};
use extenddb_core::expression::{Expr, ExpressionMaps, PathElement};
use extenddb_core::types::{
    AttributeValue, Item, ReturnValuesOnConditionCheckFailure, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, IdempotencyKey, TransactWriteOp};
use mongodb::IndexModel;
use mongodb::options::IndexOptions;

use super::{finish, scratch_engine, table};
use crate::MongoEngine;

const ACCOUNT: &str = "000000000000";

/// The token collection with the unique index of the baseline migration, so
/// an insert of a held token fails as it does on a migrated deployment.
async fn token_collection(engine: &MongoEngine) -> mongodb::Collection<Document> {
    let coll = engine.data_db.collection::<Document>("idempotency_tokens");
    let model = IndexModel::builder()
        .keys(doc! { "account_id": 1, "token": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();
    coll.create_index(model).await.expect("unique token index");
    coll
}

/// The fingerprint of `token` and the age of its document in milliseconds.
async fn token_document(coll: &mongodb::Collection<Document>, token: &str) -> (String, i64) {
    let found = coll
        .find_one(doc! { "account_id": ACCOUNT, "token": token })
        .await
        .expect("read the token")
        .expect("a token document");
    let created_at = found.get_datetime("created_at").expect("created_at");
    let age = bson::DateTime::now()
        .timestamp_millis()
        .saturating_sub(created_at.timestamp_millis());
    (
        found
            .get_str("fingerprint")
            .expect("fingerprint")
            .to_owned(),
        age,
    )
}

/// Set the `created_at` of `token` `minutes` back.
async fn age_token(coll: &mongodb::Collection<Document>, token: &str, minutes: i64) {
    let then =
        bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - minutes * 60_000);
    let updated = coll
        .update_one(
            doc! { "account_id": ACCOUNT, "token": token },
            doc! { "$set": { "created_at": then } },
        )
        .await
        .expect("age the token");
    assert_eq!(updated.matched_count, 1);
}

fn item(id: &str) -> Item {
    Item::from([("pk".to_owned(), AttributeValue::S(id.to_owned()))])
}

/// A cancelled request keeps its token used, and a cancellation of the same
/// request sent again starts the 10 minutes again (Q1c): the token document
/// reads `cancelled:<fingerprint>` with the time of the last cancellation.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_cancellation_keeps_the_token_used_from_its_last_cancellation() {
    let engine = scratch_engine().await;
    let coll = token_collection(&engine).await;
    let key_info = TableKeyInfo {
        account_id: ACCOUNT.to_owned(),
        ..table(&engine, "token-window", false)
    };
    let maps = ExpressionMaps::default();
    engine
        .put_item(&key_info, item("c1"), false, None, &maps, None)
        .await
        .expect("seed c1");
    let not_exists = Expr::Function {
        name: "attribute_not_exists".to_owned(),
        args: vec![Expr::Path(vec![PathElement::Attribute("pk".to_owned())])],
    };
    let c1 = item("c1");
    let failing = [TransactWriteOp::Put {
        key_info: &key_info,
        item: &c1,
        condition: Some(&not_exists),
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let c2 = item("c2");
    let other = [TransactWriteOp::Put {
        key_info: &key_info,
        item: &c2,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let token = format!("tok-window-{}", uuid::Uuid::new_v4().simple());
    let key = |fingerprint: &'static str| IdempotencyKey {
        account_id: ACCOUNT,
        token: &token,
        fingerprint,
    };
    let cancelled = |result: &Result<(), StorageError>| {
        matches!(result, Err(StorageError::TransactionCanceled(r))
            if r.len() == 1 && r[0].code == "ConditionalCheckFailed")
    };

    let first = engine
        .transact_write_items(&failing, Some(key("fp-a")))
        .await;
    assert!(cancelled(&first), "{first:?}");
    let (fingerprint, age) = token_document(&coll, &token).await;
    assert_eq!(fingerprint, "cancelled:fp-a");
    assert!(age < 60_000, "age {age} ms");

    let mismatch = engine.transact_write_items(&other, Some(key("fp-b"))).await;
    assert!(
        matches!(mismatch, Err(StorageError::IdempotentMismatch)),
        "{mismatch:?}"
    );
    assert!(
        engine
            .get_item(&key_info, &c2)
            .await
            .expect("get c2")
            .is_none(),
        "c2 was written"
    );

    age_token(&coll, &token, 9).await;
    let again = engine
        .transact_write_items(&failing, Some(key("fp-a")))
        .await;
    assert!(cancelled(&again), "{again:?}");
    let (fingerprint, age) = token_document(&coll, &token).await;
    assert_eq!(fingerprint, "cancelled:fp-a");
    assert!(age < 60_000, "age {age} ms after the second cancellation");

    finish(engine).await;
}

/// A cancellation replaces a token document older than the 10-minute read
/// window, as the SQL backends replace an expired row, and keeps a document
/// of another request stored less than 10 minutes ago.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_cancellation_replaces_only_a_token_document_older_than_ten_minutes() {
    let engine = scratch_engine().await;
    let coll = token_collection(&engine).await;
    let token = format!("tok-replace-{}", uuid::Uuid::new_v4().simple());

    engine
        .record_cancelled_token(ACCOUNT, &token, "fp-a")
        .await
        .expect("record fp-a");
    engine
        .record_cancelled_token(ACCOUNT, &token, "fp-b")
        .await
        .expect("record fp-b over a young document");
    assert_eq!(token_document(&coll, &token).await.0, "cancelled:fp-a");

    age_token(&coll, &token, 11).await;
    engine
        .record_cancelled_token(ACCOUNT, &token, "fp-b")
        .await
        .expect("record fp-b over an old document");
    let (fingerprint, age) = token_document(&coll, &token).await;
    assert_eq!(fingerprint, "cancelled:fp-b");
    assert!(age < 60_000, "age {age} ms");
    assert_eq!(
        coll.count_documents(doc! { "token": &token })
            .await
            .expect("count"),
        1
    );

    finish(engine).await;
}
