// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Sends of a TransactWriteItems that meet another send with the same
//! ClientRequestToken: a send of a request that another send of it runs,
//! two sends at once, and a token row that changes while a send runs.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item, ReturnValuesOnConditionCheckFailure};
use extenddb_storage::{DataEngine, IdempotencyKey, TransactWriteOp};

use super::{
    condition_failed, put_with_token, table_with_a_failing_put, token_row, token_row_if_any,
};
use crate::helpers::{TestTable, setup_engine};

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
