// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The ClientRequestToken of TransactWriteItems: its token row after a key
//! fault, a cancellation, a commit and two sends at once.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item, ReturnValuesOnConditionCheckFailure};
use extenddb_storage::{DataEngine, IdempotencyKey, TransactWriteOp};

use crate::helpers::{TestTable, setup_engine};

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

/// A send of a request whose token another send of the same request holds
/// while it runs answers TransactionInProgressException, as the service
/// does (measured 2026-11-05), and a different request with the token
/// answers the mismatch. The running send's row is written by hand, as the
/// reservation leaves it.
#[tokio::test]
async fn test_a_send_that_meets_a_running_send_of_its_request_answers_in_progress() {
    use extenddb_storage::error::StorageError;

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnTokenRunning", false).await;
    let key_info = &table.key_info;
    let token = format!("tok-running-{}", uuid::Uuid::new_v4().simple());
    let reserve = format!(
        "INSERT INTO {}.idempotency_tokens_by_account \
         (account_id, \"token\", fingerprint, created_at) VALUES (?, ?, ?, ?) USING TTL 600",
        engine.catalog_keyspace()
    );
    extenddb_storage_cassandra::cassandra_util::execute::<StorageError>(
        &engine.session_arc(),
        &reserve,
        cdrs_tokio::query_values!(
            key_info.account_id.as_str(),
            token.as_str(),
            "running:fp-put-run1",
            extenddb_storage_cassandra::cassandra_util::now_millis()
        ),
        "reserve the token as a running send",
    )
    .await
    .expect("write the running row");

    let same = put_with_token(&engine, key_info, "run1", &token).await;
    assert!(
        matches!(same, Err(StorageError::TransactionInProgress)),
        "{same:?}"
    );
    let other = put_with_token(&engine, key_info, "other", &token).await;
    assert!(
        matches!(other, Err(StorageError::IdempotentMismatch)),
        "{other:?}"
    );
    for id in ["run1", "other"] {
        let key = Item::from([("id".to_owned(), AttributeValue::S(id.to_owned()))]);
        assert!(
            engine
                .get_item(key_info, &key)
                .await
                .expect("get")
                .is_none(),
            "{id} was written"
        );
    }
    let (fingerprint, _) = token_row(&engine, &key_info.account_id, &token).await;
    assert_eq!(fingerprint, "running:fp-put-run1");
}

/// Two sends of one request with one token at the same moment (measured
/// 2026-11-05, Q9a and Q9b): the service answers one with the request's own
/// answer and the other with TransactionInProgressException, or both with
/// the request's answer. Never a replay of a write that did not happen, and
/// never IdempotentParameterMismatchException for the request's own token.
/// Each send runs in a task of its own on a runtime with two worker threads,
/// and both start when a barrier opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_two_sends_of_one_request_answer_its_cancellation_or_in_progress() {
    use extenddb_storage::error::StorageError;
    use std::sync::Arc;

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = table_with_a_failing_put(&engine, "TxnTokenRace").await;
    let key_info = &table.key_info;
    // One send of the Put of c1 that fails its condition, with `token`, from
    // a task of its own that waits on `barrier` first.
    let send = |token: &str, barrier: &Arc<tokio::sync::Barrier>| {
        let engine = engine.clone();
        let key_info = key_info.clone();
        let token = token.to_owned();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            use extenddb_core::expression::{Expr, PathElement};
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
                key_info: &key_info,
                item: &ccf_item,
                condition: Some(&not_exists),
                maps: &maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: None,
            }];
            let key = IdempotencyKey {
                account_id: &key_info.account_id,
                token: &token,
                fingerprint: "fp-race",
            };
            barrier.wait().await;
            engine.transact_write_items(&ops, Some(key)).await
        })
    };

    let mut canceled = 0;
    let mut in_progress = 0;
    for round in 0..10 {
        // First two first sends of a fresh token, then a cancelled request
        // sent twice at once.
        for cancelled_before in [false, true] {
            let token = format!(
                "tok-race-{round}-{cancelled_before}-{}",
                uuid::Uuid::new_v4().simple()
            );
            if cancelled_before {
                let first = send(&token, &Arc::new(tokio::sync::Barrier::new(1)))
                    .await
                    .expect("the first send");
                assert!(condition_failed(&first), "{first:?}");
            }
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let (a, b) = (send(&token, &barrier), send(&token, &barrier));
            let (a, b) = (a.await.expect("send a"), b.await.expect("send b"));
            for answer in [a, b] {
                if condition_failed(&answer) {
                    canceled += 1;
                } else if matches!(answer, Err(StorageError::TransactionInProgress)) {
                    in_progress += 1;
                } else {
                    panic!("round {round} (cancelled before: {cancelled_before}): {answer:?}");
                }
            }
        }
    }
    eprintln!("40 answers: {canceled} cancelled, {in_progress} in progress");
    let c1 = Item::from([("id".to_owned(), AttributeValue::S("c1".to_owned()))]);
    let stored = engine
        .get_item(key_info, &c1)
        .await
        .expect("get c1")
        .expect("c1");
    assert!(!stored.contains_key("v"), "c1 was written: {stored:?}");
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

/// The cancellation of a send writes the cancelled token only where the
/// running row of that send was. Every write of a token row is an LWT, so a
/// row that another request reserved while the send ran stays as it is, and
/// a row that is gone gets the cancelled token. Each case changes the row
/// while the send prepares its 40 members, as a row looks when it expired
/// during the send. A different request with the token then answers the
/// mismatch and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_a_cancellation_leaves_a_token_row_that_changed_while_the_send_ran() {
    use extenddb_core::expression::{Expr, PathElement};
    use extenddb_storage::error::StorageError;

    // (case, the LWT that changes the send's row in keyspace `{ks}`, the
    // fingerprint of the row after the cancellation)
    const CASES: [(&str, &str, &str); 2] = [
        (
            "another request reserved the token",
            "UPDATE {ks}.idempotency_tokens_by_account USING TTL 600 \
             SET fingerprint = 'running:fp-b' \
             WHERE account_id = ? AND \"token\" = ? IF fingerprint = 'running:fp-a'",
            "running:fp-b",
        ),
        (
            "the row is gone",
            "DELETE FROM {ks}.idempotency_tokens_by_account \
             WHERE account_id = ? AND \"token\" = ? IF fingerprint = 'running:fp-a'",
            "cancelled:fp-a",
        ),
    ];

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = table_with_a_failing_put(&engine, "TxnTokenChanged").await;
    let key_info = &table.key_info;
    let account = key_info.account_id.as_str();
    // A send of 39 Puts that prepare, then the Put of c1 that fails.
    let send = |n: usize, token: &str| {
        let engine = engine.clone();
        let key_info = key_info.clone();
        let token = token.to_owned();
        tokio::spawn(async move {
            let maps = ExpressionMaps::default();
            let not_exists = Expr::Function {
                name: "attribute_not_exists".to_owned(),
                args: vec![Expr::Path(vec![PathElement::Attribute("id".to_owned())])],
            };
            let mut items: Vec<Item> = (0..39)
                .map(|i| Item::from([("id".to_owned(), AttributeValue::S(format!("m{n}-{i:02}")))]))
                .collect();
            items.push(Item::from([
                ("id".to_owned(), AttributeValue::S("c1".to_owned())),
                ("v".to_owned(), AttributeValue::S("x".to_owned())),
            ]));
            let ops: Vec<TransactWriteOp<'_>> = items
                .iter()
                .enumerate()
                .map(|(i, item)| TransactWriteOp::Put {
                    key_info: &key_info,
                    item,
                    condition: (i == 39).then_some(&not_exists),
                    maps: &maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                    stream: None,
                })
                .collect();
            let key = IdempotencyKey {
                account_id: &key_info.account_id,
                token: &token,
                fingerprint: "fp-a",
            };
            engine.transact_write_items(&ops, Some(key)).await
        })
    };

    for (n, (case, change, expected)) in CASES.into_iter().enumerate() {
        let token = format!("tok-changed-{n}-{}", uuid::Uuid::new_v4().simple());
        let sent = send(n, &token);
        // Wait for the reservation of the send, then change its row.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while token_row_if_any(&engine, account, &token).await.is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "{case}: the send reserved no token"
            );
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let changed = extenddb_storage_cassandra::cassandra_util::apply_lwt::<StorageError>(
            &engine.session_arc(),
            &change.replace("{ks}", &engine.catalog_keyspace()),
            cdrs_tokio::query_values!(account, token.as_str()),
            "change the token row",
        )
        .await
        .expect("change the token row");
        assert!(
            changed,
            "{case}: the send was cancelled before the row changed"
        );

        let answer = sent.await.expect("the send");
        assert!(
            matches!(&answer, Err(StorageError::TransactionCanceled(r))
                if r.len() == 40 && r[39].code == "ConditionalCheckFailed"),
            "{case}: {answer:?}"
        );
        let row = token_row_if_any(&engine, account, &token).await;
        assert_eq!(row.as_ref().map(|r| r.0.as_str()), Some(expected), "{case}");
        assert!(
            matches!(row, Some((_, Some(590..=600)))),
            "{case}: TTL of {row:?}"
        );
        let other = format!("d{n}");
        let result = put_with_token(&engine, key_info, &other, &token).await;
        assert!(
            matches!(result, Err(StorageError::IdempotentMismatch)),
            "{case}: {result:?}"
        );
        let key = Item::from([("id".to_owned(), AttributeValue::S(other))]);
        let written = engine.get_item(key_info, &key).await.expect("get");
        assert!(written.is_none(), "{case}: the different request wrote");
    }
}
