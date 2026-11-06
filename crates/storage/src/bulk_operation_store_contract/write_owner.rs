// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The write owner and the per-row ledger a resumed run reads back: every
//! marked batch row recorded with what it replaced, the owner check that
//! keeps an earlier claim's batch out, the prune at the checkpoint, and the
//! count of resumes that found the checkpoint where the last one left it.

use super::*;

fn marked(pk: &str, v: &str, m: BulkRowMarker) -> BatchRow {
    BatchRow {
        item: item(pk, v),
        condition: None,
        marker: Some(m),
        return_old: true,
    }
}

fn marked_conditional(pk: &str, v: &str, condition: &str, m: BulkRowMarker) -> BatchRow {
    let tokens = expression::tokenize(condition).expect("tokenize");
    let expr = expression::parse_condition(&tokens).expect("parse the condition");
    BatchRow {
        item: item(pk, v),
        condition: Some((expr, ExpressionMaps::new(HashMap::new(), HashMap::new()))),
        marker: Some(m),
        return_old: true,
    }
}

fn at(operation_id: &str, file_index: i64, row_index: i64, fx_account: &str) -> BulkRowMarker {
    BulkRowMarker {
        file_index,
        ..marker(fx_account, operation_id, row_index)
    }
}

async fn recorded<E: StorageEngine>(
    fx: &Fixture<E>,
    operation_id: &str,
    file_index: i64,
) -> Vec<crate::RecordedRow> {
    fx.engine
        .bulk_rows_recorded(&fx.account_a, operation_id, file_index, 0, i64::MAX)
        .await
        .expect("bulk_rows_recorded")
}

async fn stored(
    fx: &Fixture<impl StorageEngine>,
    ki: &extenddb_core::types::TableKeyInfo,
    pk: &str,
) -> Option<Item> {
    fx.engine
        .get_item(
            ki,
            &Item::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))]),
        )
        .await
        .expect("get_item")
}

fn size(item: &Item) -> Option<i64> {
    Some(i64::try_from(extenddb_core::types::item_size_bytes(item)).expect("item size fits"))
}

/// Every marked row of a batch is recorded in the batch's transaction with
/// its outcome and the size of the item it replaced or was evaluated
/// against. The item itself is kept when the marker asks for it and for
/// every failed condition, whose report record carries it.
pub async fn every_marked_batch_row_is_recorded_with_what_it_replaced<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "owner_recorded").await;
    let ki = key_info(fx, &table.table_name).await;
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    fx.engine
        .put_item(&ki, item("a", "before"), false, None, &maps, None, None)
        .await
        .expect("seed");
    take_over(fx, "op-rec").await;

    let keep = |row| BulkRowMarker {
        keep_old: true,
        ..marker(&fx.account_a, "op-rec", row)
    };
    let results = fx
        .engine
        .put_items_batch(
            &ki,
            vec![
                marked("a", "new", marker(&fx.account_a, "op-rec", 0)),
                marked("b", "1", keep(1)),
                marked_conditional(
                    "a",
                    "cond",
                    "attribute_not_exists(pk)",
                    marker(&fx.account_a, "op-rec", 2),
                ),
                marked("a", "last", keep(3)),
            ],
            None,
        )
        .await
        .expect("the batch applies");
    assert_eq!(results.len(), 4);

    let rows = recorded(fx, "op-rec", 0).await;
    let summary: Vec<(i64, RowOutcome, Option<i64>, Option<Item>)> = rows
        .into_iter()
        .map(|r| (r.row_index, r.outcome, r.old_size, r.old_image))
        .collect();
    assert_eq!(
        summary,
        vec![
            (0, RowOutcome::Applied, size(&item("a", "before")), None),
            (1, RowOutcome::Applied, None, None),
            (
                2,
                RowOutcome::ConditionFailed,
                size(&item("a", "new")),
                Some(item("a", "new")),
            ),
            (
                3,
                RowOutcome::Applied,
                size(&item("a", "new")),
                Some(item("a", "new")),
            ),
        ],
        "one entry per row, in order, with the replaced item's size and the item where kept"
    );
}

/// A batch deletes the operation's ledger rows before its `settled` cursor,
/// the last checkpoint: rows of earlier files and rows of the cursor's file
/// before the cursor row. Rows at or after it stay.
pub async fn a_batch_prunes_the_ledger_rows_its_checkpoint_covers<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "owner_prune").await;
    let ki = key_info(fx, &table.table_name).await;
    let account = fx.account_a.clone();
    take_over(fx, "op-prune").await;

    let first: Vec<BatchRow> = (0..4)
        .map(|row| marked(&format!("k{row}"), "1", at("op-prune", 0, row, &account)))
        .collect();
    fx.engine
        .put_items_batch(&ki, first, None)
        .await
        .expect("the first batch");
    let second: Vec<BatchRow> = (4..6)
        .map(|row| {
            let m = BulkRowMarker {
                settled: (0, 3),
                ..at("op-prune", 0, row, &account)
            };
            marked(&format!("k{row}"), "1", m)
        })
        .collect();
    fx.engine
        .put_items_batch(&ki, second, None)
        .await
        .expect("the second batch");
    let left: Vec<i64> = recorded(fx, "op-prune", 0)
        .await
        .iter()
        .map(|r| r.row_index)
        .collect();
    assert_eq!(
        left,
        [3, 4, 5],
        "rows before the settled cursor are deleted"
    );

    let next_file = BulkRowMarker {
        settled: (1, 0),
        ..at("op-prune", 1, 0, &account)
    };
    fx.engine
        .put_items_batch(&ki, vec![marked("n0", "1", next_file)], None)
        .await
        .expect("a batch of the next file");
    assert!(
        recorded(fx, "op-prune", 0).await.is_empty(),
        "the earlier file's rows are covered by the checkpoint that passed it"
    );
    assert_eq!(recorded(fx, "op-prune", 1).await.len(), 1);
}

/// Once another owner is recorded for the operation, a marked batch of the
/// earlier owner writes nothing and answers a transient error, and so does a
/// marked batch of an operation no owner was recorded for.
pub async fn a_batch_of_an_earlier_owner_writes_nothing<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "owner_fence").await;
    let ki = key_info(fx, &table.table_name).await;
    take_over(fx, "op-fence").await;
    fx.engine
        .bulk_write_takeover(&fx.account_a, "op-fence", "owner-2", (0, 0))
        .await
        .expect("the next claim takes the operation over");

    let late = fx
        .engine
        .put_items_batch(
            &ki,
            vec![marked("a", "late", marker(&fx.account_a, "op-fence", 0))],
            None,
        )
        .await;
    assert!(
        matches!(late, Err(StorageError::Transient(_))),
        "the earlier owner's batch is refused, got {late:?}"
    );
    assert_eq!(stored(fx, &ki, "a").await, None, "nothing was written");
    assert!(recorded(fx, "op-fence", 0).await.is_empty());

    let current = BulkRowMarker {
        owner: "owner-2".to_owned(),
        ..marker(&fx.account_a, "op-fence", 0)
    };
    fx.engine
        .put_items_batch(&ki, vec![marked("a", "current", current)], None)
        .await
        .expect("the current owner's batch applies");
    assert_eq!(stored(fx, &ki, "a").await, Some(item("a", "current")));

    let unowned = fx
        .engine
        .put_items_batch(
            &ki,
            vec![marked("b", "1", marker(&fx.account_a, "op-unowned", 0))],
            None,
        )
        .await;
    assert!(
        matches!(unowned, Err(StorageError::Transient(_))),
        "a batch with no recorded owner is refused, got {unowned:?}"
    );
    assert_eq!(stored(fx, &ki, "b").await, None);
}

/// A batch carrying a row whose position the ledger already holds fails as a
/// whole: nothing of it is visible and the recorded row keeps its outcome.
pub async fn a_batch_row_at_a_recorded_position_fails_the_batch<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "owner_twice").await;
    let ki = key_info(fx, &table.table_name).await;
    take_over(fx, "op-twice").await;
    fx.engine
        .put_items_batch(
            &ki,
            vec![marked("a", "1", marker(&fx.account_a, "op-twice", 0))],
            None,
        )
        .await
        .expect("the first batch");

    let again = fx
        .engine
        .put_items_batch(
            &ki,
            vec![
                marked("b", "1", marker(&fx.account_a, "op-twice", 1)),
                marked("a", "2", marker(&fx.account_a, "op-twice", 0)),
            ],
            None,
        )
        .await;
    assert!(again.is_err(), "the batch fails");
    assert_eq!(stored(fx, &ki, "a").await, Some(item("a", "1")));
    assert_eq!(stored(fx, &ki, "b").await, None, "the batch rolled back");
    assert_eq!(recorded(fx, "op-twice", 0).await.len(), 1);
}

/// Two rows of one batch at one position fail the batch as a row at a
/// recorded position does: the ledger key refuses the second outcome, and
/// nothing of the batch is visible, its first row at that position
/// included.
pub async fn two_rows_of_one_batch_at_one_position_fail_the_batch<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "owner_one_position").await;
    let ki = key_info(fx, &table.table_name).await;
    take_over(fx, "op-one-position").await;

    let result = fx
        .engine
        .put_items_batch(
            &ki,
            vec![
                marked("a", "1", marker(&fx.account_a, "op-one-position", 0)),
                marked("b", "1", marker(&fx.account_a, "op-one-position", 1)),
                marked("c", "1", marker(&fx.account_a, "op-one-position", 0)),
            ],
            None,
        )
        .await;
    assert!(result.is_err(), "the batch fails, got {result:?}");
    for pk in ["a", "b", "c"] {
        assert_eq!(
            stored(fx, &ki, pk).await,
            None,
            "{pk}: the batch rolled back"
        );
    }
    assert!(
        recorded(fx, "op-one-position", 0).await.is_empty(),
        "no outcome of the batch is recorded"
    );
}

/// Marked rows in one batch: more than one statement can carry at seven
/// bound values a row on either backend (PostgreSQL binds at most 65,535
/// values to a statement, the bundled SQLite at most 32,766).
const ROWS_PAST_ONE_STATEMENT: i64 = 9_400;

/// A batch of more marked rows than one statement can bind records every
/// row, each once, and writes every item.
pub async fn a_batch_past_one_statement_of_rows_records_every_row<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "owner_many_rows").await;
    let ki = key_info(fx, &table.table_name).await;
    take_over(fx, "op-many-rows").await;

    let rows: Vec<BatchRow> = (0..ROWS_PAST_ONE_STATEMENT)
        .map(|row| {
            marked(
                &format!("k{row}"),
                "1",
                marker(&fx.account_a, "op-many-rows", row),
            )
        })
        .collect();
    let results = fx
        .engine
        .put_items_batch(&ki, rows, None)
        .await
        .expect("the batch applies");
    assert_eq!(
        i64::try_from(results.len()).expect("fits"),
        ROWS_PAST_ONE_STATEMENT
    );

    let recorded = recorded(fx, "op-many-rows", 0).await;
    let indexes: Vec<i64> = recorded.iter().map(|r| r.row_index).collect();
    assert_eq!(
        indexes,
        (0..ROWS_PAST_ONE_STATEMENT).collect::<Vec<_>>(),
        "every row is recorded once, in order"
    );
    assert!(
        recorded
            .iter()
            .all(|r| r.outcome == RowOutcome::Applied && r.old_size.is_none()),
        "every row is an applied insert"
    );
    let last = format!("k{}", ROWS_PAST_ONE_STATEMENT - 1);
    assert_eq!(stored(fx, &ki, &last).await, Some(item(&last, "1")));
}

/// Each takeover answers how many claims in a row, itself included, found
/// the checkpoint cursor where the claim before them found it: zero for the
/// first claim and for one that finds the cursor moved. Deleting the
/// operation's ledger rows deletes its owner record too.
pub async fn a_takeover_counts_the_resumes_that_find_the_checkpoint_unmoved<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let mut answers = Vec::new();
    for (owner, cursor) in [
        ("c1", (0, 0)),
        ("c2", (0, 0)),
        ("c3", (0, 0)),
        ("c4", (0, 7)),
        ("c5", (0, 7)),
        ("c6", (1, 0)),
    ] {
        answers.push(
            fx.engine
                .bulk_write_takeover(&fx.account_a, "op-stall", owner, cursor)
                .await
                .expect("takeover"),
        );
    }
    assert_eq!(answers, [0, 1, 2, 0, 1, 0]);

    fx.engine
        .delete_bulk_row_outcomes(&fx.account_a, "op-stall")
        .await
        .expect("delete the ledger");
    assert_eq!(
        fx.engine
            .bulk_write_takeover(&fx.account_a, "op-stall", "c7", (1, 0))
            .await
            .expect("takeover"),
        0,
        "the owner record went with the ledger"
    );
}

/// A takeover issued while a batch of the earlier owner is in flight
/// returns only once that batch has committed or been refused: the rows a
/// resumed run reads right after its takeover include every row of a batch
/// that commits, so none is applied a second time. Repeated so a takeover
/// meets a batch inside its transaction.
pub async fn a_takeover_waits_for_a_batch_in_flight<E: StorageEngine + 'static>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "owner_in_flight").await;
    let ki = Arc::new(key_info(fx, &table.table_name).await);
    for round in 0..4_i64 {
        let op = format!("op-flight-{round}");
        take_over(fx, &op).await;
        let rows: Vec<BatchRow> = (0..200)
            .map(|row| {
                marked(
                    &format!("r{round}k{row}"),
                    "1",
                    marker(&fx.account_a, &op, row),
                )
            })
            .collect();
        let (engine, batch_ki) = (Arc::clone(&fx.engine), Arc::clone(&ki));
        let batch =
            tokio::spawn(async move { engine.put_items_batch(&batch_ki, rows, None).await });
        tokio::time::sleep(Duration::from_millis(u64::try_from(round).unwrap_or(0) * 3)).await;
        fx.engine
            .bulk_write_takeover(&fx.account_a, &op, "owner-next", (0, 0))
            .await
            .expect("takeover");
        let seen = recorded(fx, &op, 0).await.len();
        let outcome = batch.await.expect("batch task");
        match outcome {
            Ok(results) => {
                assert_eq!(results.len(), 200);
                assert_eq!(
                    seen, 200,
                    "round {round}: a batch that committed is fully recorded when the takeover returns"
                );
            }
            Err(StorageError::Transient(_)) => {
                assert_eq!(seen, 0, "round {round}: a refused batch records nothing");
                let first = stored(fx, &ki, &format!("r{round}k0")).await;
                assert_eq!(first, None, "round {round}: a refused batch writes nothing");
            }
            Err(other) => panic!("round {round}: unexpected batch error {other:?}"),
        }
    }
}
