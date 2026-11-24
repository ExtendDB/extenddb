// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The ClientRequestToken of TransactWriteItems: its token row after a key
//! fault, a cancellation, a commit and two sends at once.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item, ReturnValuesOnConditionCheckFailure};
use extenddb_storage::{DataEngine, IdempotencyKey, TransactWriteOp};

use crate::helpers::{TestTable, setup_engine};

#[path = "transact_write_tokens/concurrent_sends.rs"]
mod concurrent_sends;

/// A Put of `{id}` with `token`, whose fingerprint differs from every other
/// request in `test_idempotency_token_after_a_key_fault`.
async fn put_with_token(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    key_info: &extenddb_core::types::TableKeyInfo,
    id: &str,
    token: &str,
) -> Result<(), extenddb_storage::error::StorageError> {
    let mut item = Item::new();
    item.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
    let maps = ExpressionMaps::default();
    let ops = [TransactWriteOp::Put {
        key_info,
        item: &item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let fingerprint = format!("fp-put-{id}");
    engine
        .transact_write_items(
            &ops,
            Some(IdempotencyKey {
                account_id: &key_info.account_id,
                token,
                fingerprint: &fingerprint,
            }),
        )
        .await
}

/// A token beside a key fault, as the service keeps it (measured
/// 2026-11-01): a fault of the request-level checks (a wrong-typed table key)
/// leaves the token unused, so another request with it commits. A fault found
/// after them (an empty index-key literal, a failed condition) leaves it used:
/// the same request with it answers that fault again, and another request
/// with it answers the mismatch and writes nothing.
#[tokio::test]
async fn test_idempotency_token_after_a_key_fault() {
    use extenddb_core::expression::{self, Expr, PathElement};
    use extenddb_storage::TableEngine;
    use extenddb_storage::error::StorageError;
    use std::collections::HashMap;

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_gsi(&engine, "TxnTokenKeyFault", "GkIndex", "gk").await;
    // The index is in the key info only once the table is active.
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("process table transitions");
    let key_info = engine
        .table_key_info(&table.key_info.account_id, "TxnTokenKeyFault")
        .await
        .expect("key info");
    assert_eq!(key_info.global_secondary_indexes.len(), 1, "{key_info:?}");
    let present = |id: &'static str| {
        let engine = &engine;
        let key_info = &key_info;
        async move {
            let key = Item::from([("id".to_owned(), AttributeValue::S(id.to_owned()))]);
            engine
                .get_item(key_info, &key)
                .await
                .expect("get_item")
                .is_some()
        }
    };
    let token = |name: &str| format!("tok-{name}-{}", uuid::Uuid::new_v4().simple());
    let plain = ExpressionMaps::default();

    // A wrong-typed table key: answered by the request-level checks, and the
    // token stays unused.
    let bad_key = Item::from([("id".to_owned(), AttributeValue::N("1".to_owned()))]);
    let t1 = token("key");
    let ops = [TransactWriteOp::Put {
        key_info: &key_info,
        item: &bad_key,
        condition: None,
        maps: &plain,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let key_fault = IdempotencyKey {
        account_id: &key_info.account_id,
        token: &t1,
        fingerprint: "fp-key",
    };
    let result = engine.transact_write_items(&ops, Some(key_fault)).await;
    assert!(
        matches!(&result, Err(StorageError::TransactionCanceled(r)) if r[0].code == "ValidationError"),
        "{result:?}"
    );
    put_with_token(&engine, &key_info, "after-key", &t1)
        .await
        .expect("the token is unused after a request-level fault");
    assert!(present("after-key").await);

    // An empty index-key literal: found after the token is checked, so the
    // token stays used.
    let empty = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("e".to_owned(), AttributeValue::S(String::new()))]),
    );
    let tokens = expression::tokenize("SET gk = :e").expect("tokenize");
    let actions = expression::parse_update(&tokens).expect("parse");
    let key_u = Item::from([("id".to_owned(), AttributeValue::S("u".to_owned()))]);
    let t2 = token("empty");
    let ops = [TransactWriteOp::Update {
        key_info: &key_info,
        key: &key_u,
        actions: &actions,
        condition: None,
        maps: &empty,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let empty_fault = IdempotencyKey {
        account_id: &key_info.account_id,
        token: &t2,
        fingerprint: "fp-empty",
    };
    for send in ["first", "again"] {
        let result = engine.transact_write_items(&ops, Some(empty_fault)).await;
        assert!(
            matches!(&result, Err(StorageError::Validation(m)) if m.ends_with("cannot contain an empty string value.")),
            "{send}: {result:?}"
        );
    }
    let result = put_with_token(&engine, &key_info, "after-empty", &t2).await;
    assert!(
        matches!(result, Err(StorageError::IdempotentMismatch)),
        "{result:?}"
    );
    assert!(!present("after-empty").await);

    // A failed condition: the same.
    engine
        .put_item(
            &key_info,
            Item::from([("id".to_owned(), AttributeValue::S("c1".to_owned()))]),
            false,
            None,
            &plain,
            None,
        )
        .await
        .expect("seed c1");
    let c1 = Item::from([("id".to_owned(), AttributeValue::S("c1".to_owned()))]);
    let not_exists = Expr::Function {
        name: "attribute_not_exists".to_owned(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".to_owned())])],
    };
    let t3 = token("ccf");
    let ops = [TransactWriteOp::Put {
        key_info: &key_info,
        item: &c1,
        condition: Some(&not_exists),
        maps: &plain,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let ccf = IdempotencyKey {
        account_id: &key_info.account_id,
        token: &t3,
        fingerprint: "fp-ccf",
    };
    for send in ["first", "again"] {
        let result = engine.transact_write_items(&ops, Some(ccf)).await;
        assert!(
            matches!(&result, Err(StorageError::TransactionCanceled(r)) if r[0].code == "ConditionalCheckFailed"),
            "{send}: {result:?}"
        );
    }
    let result = put_with_token(&engine, &key_info, "after-ccf", &t3).await;
    assert!(
        matches!(result, Err(StorageError::IdempotentMismatch)),
        "{result:?}"
    );
    assert!(!present("after-ccf").await);
}

/// The fingerprint of the token row of `token` and the seconds it has left
/// (`TTL(fingerprint)`, `None` when the row never expires), or `None` when
/// the token has no row.
async fn token_row_if_any(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    account_id: &str,
    token: &str,
) -> Option<(String, Option<i32>)> {
    use cdrs_tokio::types::IntoRustByName;
    let query = format!(
        "SELECT fingerprint, TTL(fingerprint) AS ttl FROM {}.idempotency_tokens_by_account \
         WHERE account_id = ? AND \"token\" = ?",
        engine.catalog_keyspace()
    );
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<
        extenddb_storage::error::StorageError,
    >(
        &engine.session_arc(),
        &query,
        cdrs_tokio::query_values!(account_id, token),
        "read the token row",
    )
    .await
    .expect("read the token row");
    let row = rows.first()?;
    let fingerprint: Option<String> = row.get_by_name("fingerprint").ok().flatten();
    let ttl: Option<i32> = row.get_by_name("ttl").ok().flatten();
    Some((fingerprint.expect("a fingerprint"), ttl))
}

/// [`token_row_if_any`] of a token that has a row.
async fn token_row(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    account_id: &str,
    token: &str,
) -> (String, Option<i32>) {
    token_row_if_any(engine, account_id, token)
        .await
        .expect("a token row")
}

/// A table with the item `c1`, and a Put of `c1` with
/// `attribute_not_exists(id)`, which `c1` fails.
async fn table_with_a_failing_put(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    name: &str,
) -> TestTable {
    let table = TestTable::new(engine, name, false).await;
    let seeded = Item::from([("id".to_owned(), AttributeValue::S("c1".to_owned()))]);
    engine
        .put_item(
            &table.key_info,
            seeded,
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("seed c1");
    table
}

fn condition_failed(result: &Result<(), extenddb_storage::error::StorageError>) -> bool {
    matches!(result, Err(extenddb_storage::error::StorageError::TransactionCanceled(r))
        if r.len() == 1 && r[0].code == "ConditionalCheckFailed")
}

/// The service keeps a cancelled token used for 10 minutes from its last
/// cancellation (measured 2026-11-05, Q1a to Q1c). Cassandra keeps that
/// window as the TTL of the token row: a cancellation writes it with 600
/// seconds, and a cancellation of the same request sent again starts it again.
#[tokio::test]
async fn test_a_cancelled_token_row_expires_ten_minutes_after_its_last_cancellation() {
    use extenddb_core::expression::{Expr, PathElement};

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = table_with_a_failing_put(&engine, "TxnTokenTtl").await;
    let key_info = &table.key_info;
    let not_exists = Expr::Function {
        name: "attribute_not_exists".to_owned(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".to_owned())])],
    };
    let maps = ExpressionMaps::default();
    let ccf_item = Item::from([
        ("id".to_owned(), AttributeValue::S("c1".to_owned())),
        ("v".to_owned(), AttributeValue::S("x".to_owned())),
    ]);
    let ops = [TransactWriteOp::Put {
        key_info,
        item: &ccf_item,
        condition: Some(&not_exists),
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let token = format!("tok-ttl-{}", uuid::Uuid::new_v4().simple());
    let key = IdempotencyKey {
        account_id: &key_info.account_id,
        token: &token,
        fingerprint: "fp-ttl",
    };

    let first = engine.transact_write_items(&ops, Some(key)).await;
    assert!(condition_failed(&first), "{first:?}");
    let (fingerprint, ttl) = token_row(&engine, &key_info.account_id, &token).await;
    assert_eq!(fingerprint, "cancelled:fp-ttl");
    let first_ttl = ttl.expect("the cancelled token row has a TTL");
    assert!((590..=600).contains(&first_ttl), "TTL {first_ttl}");

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let aged_ttl = token_row(&engine, &key_info.account_id, &token)
        .await
        .1
        .expect("a TTL");
    assert!(
        aged_ttl < first_ttl,
        "TTL {aged_ttl} after 3 s, {first_ttl} before"
    );

    let again = engine.transact_write_items(&ops, Some(key)).await;
    assert!(condition_failed(&again), "{again:?}");
    let (fingerprint, ttl) = token_row(&engine, &key_info.account_id, &token).await;
    assert_eq!(fingerprint, "cancelled:fp-ttl");
    let renewed_ttl = ttl.expect("a TTL");
    assert!(
        renewed_ttl > aged_ttl,
        "TTL {renewed_ttl} after the second cancellation, {aged_ttl} before it"
    );
}

/// The seconds the `created_at` cell of the token row of `token` has left.
async fn created_at_ttl(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    account_id: &str,
    token: &str,
) -> Option<i32> {
    use cdrs_tokio::types::IntoRustByName;
    let query = format!(
        "SELECT TTL(created_at) AS ttl FROM {}.idempotency_tokens_by_account \
         WHERE account_id = ? AND \"token\" = ?",
        engine.catalog_keyspace()
    );
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<
        extenddb_storage::error::StorageError,
    >(
        &engine.session_arc(),
        &query,
        cdrs_tokio::query_values!(account_id, token),
        "read the TTL of created_at",
    )
    .await
    .expect("read the TTL of created_at");
    rows.first()
        .expect("a token row")
        .get_by_name("ttl")
        .ok()
        .flatten()
}

/// A send that fails after the reservation leaves the running row it
/// reserved, and the same request answers TransactionInProgressException
/// while the row lives. The reservation writes the row with a TTL of 600
/// seconds, so the row expires 10 minutes after it. The test commits a Put
/// and reads the TTL of `created_at`, which on that path only the
/// reservation writes.
#[tokio::test]
async fn test_a_reserved_token_row_expires_ten_minutes_after_its_reservation() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnTokenReservedTtl", false).await;
    let key_info = &table.key_info;
    let token = format!("tok-reserved-ttl-{}", uuid::Uuid::new_v4().simple());
    put_with_token(&engine, key_info, "reserved", &token)
        .await
        .expect("the Put commits");
    let ttl = created_at_ttl(&engine, &key_info.account_id, &token)
        .await
        .expect("the reservation has a TTL");
    assert!((590..=600).contains(&ttl), "TTL(created_at) {ttl}");
}

/// The service keeps the token of a committed request used 9 minutes after
/// the request and treats it as new 11 minutes after (measured 2026-11-05).
/// Cassandra keeps that window as the TTL of the commit mark, the write of
/// the plain fingerprint.
#[tokio::test]
async fn test_a_committed_token_row_expires_ten_minutes_after_its_commit() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnTokenCommittedTtl", false).await;
    let key_info = &table.key_info;
    let token = format!("tok-committed-ttl-{}", uuid::Uuid::new_v4().simple());
    put_with_token(&engine, key_info, "committed", &token)
        .await
        .expect("the Put commits");
    let (fingerprint, ttl) = token_row(&engine, &key_info.account_id, &token).await;
    assert_eq!(fingerprint, "fp-put-committed");
    let ttl = ttl.expect("the committed token row has a TTL");
    assert!((590..=600).contains(&ttl), "TTL {ttl}");
}
