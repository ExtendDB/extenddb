// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Batched writes through put_items_batch: ordering, failure, and rollback.

use super::*;

fn batch_plain(pk: &str, v: &str) -> BatchRow {
    BatchRow {
        item: item(pk, v),
        condition: None,
        marker: None,
        return_old: false,
    }
}

fn batch_conditional(pk: &str, v: &str, condition: &str, m: BulkRowMarker) -> BatchRow {
    let tokens = expression::tokenize(condition).expect("tokenize");
    let expr = expression::parse_condition(&tokens).expect("parse the condition");
    BatchRow {
        item: item(pk, v),
        condition: Some((expr, ExpressionMaps::new(HashMap::new(), HashMap::new()))),
        marker: Some(m),
        return_old: false,
    }
}

async fn stored_v<E: StorageEngine>(
    fx: &Fixture<E>,
    ki: &extenddb_core::types::TableKeyInfo,
    pk: &str,
) -> Option<String> {
    fx.engine
        .get_item(
            ki,
            &Item::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))]),
        )
        .await
        .expect("get_item")
        .and_then(|stored| match stored.get("v") {
            Some(AttributeValue::S(v)) => Some(v.clone()),
            _ => None,
        })
}

/// One batch applies every row and answers one result per row in input
/// order. A failed condition on one row is its own result: the rows before
/// and after it still land, the failure's ledger row says `ConditionFailed`,
/// the passing condition row's says `Applied`, and plain rows sent without
/// a marker leave no ledger row.
pub async fn a_batch_applies_every_row_and_reports_each_outcome_in_order<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "batch_outcomes").await;
    let ki = key_info(fx, &table.table_name).await;
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    fx.engine
        .put_item(&ki, item("a", "before"), false, None, &maps, None, None)
        .await
        .expect("seed the target item");
    take_over(fx, "op-batch").await;

    let results = fx
        .engine
        .put_items_batch(
            &ki,
            vec![
                batch_plain("b", "1"),
                batch_conditional(
                    "a",
                    "after",
                    "attribute_not_exists(pk)",
                    marker(&fx.account_a, "op-batch", 1),
                ),
                batch_conditional(
                    "c",
                    "1",
                    "attribute_not_exists(pk)",
                    marker(&fx.account_a, "op-batch", 2),
                ),
                batch_plain("d", "1"),
            ],
            None,
        )
        .await
        .expect("the batch applies");

    assert_eq!(results.len(), 4, "one result per row");
    assert_eq!(results[0], BatchRowResult::Written { old: None });
    match &results[1] {
        BatchRowResult::ConditionFailed { old: Some(old) } => assert_eq!(
            old.get("v"),
            Some(&AttributeValue::S("before".to_owned())),
            "the failure carries the image the condition was evaluated against"
        ),
        other => panic!("row 1 must be a condition failure with its old image, got {other:?}"),
    }
    assert_eq!(results[2], BatchRowResult::Written { old: None });
    assert_eq!(results[3], BatchRowResult::Written { old: None });

    assert_eq!(stored_v(fx, &ki, "a").await.as_deref(), Some("before"));
    assert_eq!(stored_v(fx, &ki, "b").await.as_deref(), Some("1"));
    assert_eq!(stored_v(fx, &ki, "c").await.as_deref(), Some("1"));
    assert_eq!(stored_v(fx, &ki, "d").await.as_deref(), Some("1"));

    let outcome = |row: i64| {
        fx.engine
            .bulk_row_outcome(&fx.account_a, "op-batch", 0, row)
    };
    assert_eq!(
        outcome(1).await.expect("ledger"),
        Some(RowOutcome::ConditionFailed),
        "the failed condition is recorded in the batch's transaction"
    );
    assert_eq!(
        outcome(2).await.expect("ledger"),
        Some(RowOutcome::Applied),
        "the passing condition row is recorded with its write"
    );
    assert_eq!(
        outcome(0).await.expect("ledger"),
        None,
        "plain rows carry no marker"
    );
    assert_eq!(
        outcome(3).await.expect("ledger"),
        None,
        "plain rows carry no marker"
    );
}

/// Two rows on one key inside a batch behave as two sequential puts: the
/// second sees the first's write, so a condition on the key's existence
/// passes, and the replaced image it reports is the first row's.
pub async fn a_batch_applies_two_rows_on_one_key_in_order<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "batch_same_key").await;
    let ki = key_info(fx, &table.table_name).await;
    take_over(fx, "op-same").await;

    let mut second = batch_conditional(
        "a",
        "second",
        "attribute_exists(pk)",
        marker(&fx.account_a, "op-same", 1),
    );
    second.return_old = true;
    let results = fx
        .engine
        .put_items_batch(&ki, vec![batch_plain("a", "first"), second], None)
        .await
        .expect("the batch applies");

    assert_eq!(results[0], BatchRowResult::Written { old: None });
    match &results[1] {
        BatchRowResult::Written { old: Some(old) } => assert_eq!(
            old.get("v"),
            Some(&AttributeValue::S("first".to_owned())),
            "the second row replaced the first row's image"
        ),
        other => panic!("row 1 must be written over the first row, got {other:?}"),
    }
    assert_eq!(stored_v(fx, &ki, "a").await.as_deref(), Some("second"));
}

/// An error on one row (here a row with no partition key) fails the whole
/// batch, and nothing from the batch is visible afterwards: the rows before
/// the bad one were in the same transaction and rolled back with it.
pub async fn a_batch_error_leaves_nothing_written<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "batch_rollback").await;
    let ki = key_info(fx, &table.table_name).await;
    take_over(fx, "op-rollback").await;

    let no_key = BatchRow {
        item: Item::from([("v".to_owned(), AttributeValue::S("orphan".to_owned()))]),
        condition: None,
        marker: None,
        return_old: false,
    };
    let result = fx
        .engine
        .put_items_batch(
            &ki,
            vec![
                batch_plain("a", "1"),
                batch_conditional(
                    "c",
                    "1",
                    "attribute_not_exists(pk)",
                    marker(&fx.account_a, "op-rollback", 1),
                ),
                no_key,
                batch_plain("b", "1"),
            ],
            None,
        )
        .await;

    assert!(result.is_err(), "a row without its key fails the batch");
    assert_eq!(
        stored_v(fx, &ki, "a").await,
        None,
        "the row before the error rolled back"
    );
    assert_eq!(
        stored_v(fx, &ki, "c").await,
        None,
        "the condition row rolled back"
    );
    assert_eq!(
        stored_v(fx, &ki, "b").await,
        None,
        "the row after the error never ran"
    );
    assert_eq!(
        fx.engine
            .bulk_row_outcome(&fx.account_a, "op-rollback", 0, 1)
            .await
            .expect("ledger"),
        None,
        "the marker rolled back with the write it rode"
    );
}

/// An empty batch answers an empty result and touches nothing.
pub async fn an_empty_batch_is_a_no_op<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "batch_empty").await;
    let ki = key_info(fx, &table.table_name).await;
    let results = fx
        .engine
        .put_items_batch(&ki, Vec::new(), None)
        .await
        .expect("an empty batch applies");
    assert!(results.is_empty());
}

/// Two operations writing the same keys in opposite orders, each as one
/// batch, both complete. A batch takes its row locks in input order, so two
/// concurrent batches can wait on each other's locks; the backend must
/// resolve that (PostgreSQL aborts one transaction and the batch is
/// retried; SQLite serialises writers before the transaction opens) rather
/// than surface it as a failed operation. Repeated so the interleaving is
/// reached.
pub async fn opposite_order_batches_on_one_key_set_both_complete<E: StorageEngine + 'static>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "batch_deadlock").await;
    let ki = Arc::new(key_info(fx, &table.table_name).await);
    let keys: Vec<String> = (0..60).map(|i| format!("k{i:03}")).collect();
    for round in 0..4 {
        let forward: Vec<BatchRow> = keys
            .iter()
            .map(|k| batch_plain(k, &format!("f{round}")))
            .collect();
        let backward: Vec<BatchRow> = keys
            .iter()
            .rev()
            .map(|k| batch_plain(k, &format!("b{round}")))
            .collect();
        let (engine_a, ki_a) = (Arc::clone(&fx.engine), Arc::clone(&ki));
        let (engine_b, ki_b) = (Arc::clone(&fx.engine), Arc::clone(&ki));
        let a = tokio::spawn(async move { engine_a.put_items_batch(&ki_a, forward, None).await });
        let b = tokio::spawn(async move { engine_b.put_items_batch(&ki_b, backward, None).await });
        let (ra, rb) = (a.await.expect("task a"), b.await.expect("task b"));
        assert!(
            ra.is_ok() && rb.is_ok(),
            "round {round}: both opposite-order batches must complete, got a={ra:?} b={rb:?}"
        );
        assert_eq!(ra.expect("a").len(), 60);
        assert_eq!(rb.expect("b").len(), 60);
    }
    // Every key holds one of the two last-round values: both batches landed.
    for k in &keys {
        let v = stored_v(fx, &ki, k).await.expect("every key written");
        assert!(v == "f3" || v == "b3", "key {k} holds {v}");
    }
}

/// An overwrite inside a batch reports the replaced item whenever the row
/// asks for it, on a table with no index as well, so the runner can charge
/// the table the larger of the old and new item sizes on every overwrite
/// without a second read. An insert reports no old image.
pub async fn a_batch_overwrite_reports_the_replaced_item_for_sizing<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "batch_overwrite_size").await;
    let ki = key_info(fx, &table.table_name).await;
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    fx.engine
        .put_item(&ki, item("a", "old"), false, None, &maps, None, None)
        .await
        .expect("seed the target item");

    let mut overwrite = batch_plain("a", "new");
    overwrite.return_old = true;
    let mut insert = batch_plain("z", "new");
    insert.return_old = true;
    let results = fx
        .engine
        .put_items_batch(&ki, vec![overwrite, insert], None)
        .await
        .expect("the batch applies");

    match &results[0] {
        BatchRowResult::Written { old: Some(old) } => assert_eq!(
            old.get("v"),
            Some(&AttributeValue::S("old".to_owned())),
            "the overwrite reports the replaced item for sizing"
        ),
        other => panic!("row 0 must report the replaced image, got {other:?}"),
    }
    assert_eq!(
        results[1],
        BatchRowResult::Written { old: None },
        "an insert reports no old image"
    );
}

/// A table and a two-row plain batch for a backend's own fault cases: the
/// `SQLite` suite holds the database's write lock from a second connection
/// while the batch runs, a fault the shared contract cannot induce.
pub async fn plain_batch_on_a_new_table<E: StorageEngine>(
    fx: &Fixture<E>,
    name: &str,
) -> (extenddb_core::types::TableKeyInfo, Vec<BatchRow>) {
    let table = create_table(fx, &fx.account_a, name).await;
    let ki = key_info(fx, &table.table_name).await;
    (ki, vec![batch_plain("a", "1"), batch_plain("b", "1")])
}
