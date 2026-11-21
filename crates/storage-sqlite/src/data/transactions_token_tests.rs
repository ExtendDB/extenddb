//! The `ClientRequestToken` of a request cancelled after the key checks.

use std::collections::BTreeMap;

use extenddb_core::expression::{self, ExpressionMaps};
use extenddb_core::types::{AttributeValue, Item, ReturnValuesOnConditionCheckFailure};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, IdempotencyKey, TableEngine, TransactWriteOp};
use serde_json::json;

use crate::SqliteEngine;

const ACCOUNT: &str = "123456789012";

/// A failed write of the cancelled token leaves the answer as it is:
/// nothing was applied, so the request answers its cancellation, not an
/// internal error.
#[tokio::test]
async fn a_failed_token_write_keeps_the_cancellation() {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    sqlx::query(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0')",
    )
    .execute(&engine.pool)
    .await
    .expect("delay");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
    let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
        "TableName": "t_token",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input");
    engine.create_table(ACCOUNT, input).await.expect("create");
    let key_info = engine
        .table_key_info(ACCOUNT, "t_token")
        .await
        .expect("key info");
    let maps = ExpressionMaps::default();
    let seeded: Item = BTreeMap::from([("pk".to_owned(), AttributeValue::S("bp".to_owned()))]);
    engine
        .put_item(&key_info, seeded.clone(), false, None, &maps, None)
        .await
        .expect("seed bp");
    // Every write of a cancelled token fails from here on.
    sqlx::query(
        "CREATE TRIGGER fail_cancelled_tokens BEFORE INSERT ON idempotency_tokens \
         WHEN NEW.fingerprint LIKE 'cancelled:%' \
         BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&engine.pool)
    .await
    .expect("trigger");

    let condition = expression::parse_condition(
        &expression::tokenize("attribute_not_exists(pk)").expect("tokenize"),
    )
    .expect("parse");
    let ops = [TransactWriteOp::Put {
        key_info: &key_info,
        item: &seeded,
        condition: Some(&condition),
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let key = IdempotencyKey {
        account_id: ACCOUNT,
        token: "tok-failed-write",
        fingerprint: "fp",
    };
    match engine.transact_write_items(&ops, Some(key)).await {
        Err(StorageError::TransactionCanceled(reasons)) => {
            assert_eq!(reasons.len(), 1);
            assert_eq!(reasons[0].code, "ConditionalCheckFailed", "{reasons:?}");
        }
        other => panic!("expected the cancellation, got {other:?}"),
    }
}
