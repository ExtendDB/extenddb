// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The row-outcome ledger: marked batch rows, condition failures, deletion
//! scope, and a second write at a recorded position.

use super::*;

/// The size `bulk_rows_recorded` reports for `item`.
fn recorded_size(item: &Item) -> Option<i64> {
    Some(i64::try_from(extenddb_core::types::item_size_bytes(item)).expect("item size fits"))
}

/// A marked plain row commits the item and the Applied ledger row together,
/// with the size of the item it replaced, and that item only when the
/// marker asks to keep it.
pub async fn a_marked_write_records_applied_with_the_item<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "ledger_applied").await;
    let ki = key_info(fx, &table.table_name).await;
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());

    fx.engine
        .put_item(&ki, item("a", "before"), false, None, &maps, None)
        .await
        .expect("seed the replaced item");
    take_over(fx, "op-1").await;
    fx.engine
        .put_items_batch(
            &ki,
            vec![marked("a", "1", marker(&fx.account_a, "op-1", 5))],
            None,
        )
        .await
        .expect("a marked batch");
    fx.engine
        .put_items_batch(
            &ki,
            vec![marked(
                "a",
                "2",
                BulkRowMarker {
                    keep_old: true,
                    ..marker(&fx.account_a, "op-1", 6)
                },
            )],
            None,
        )
        .await
        .expect("a marked batch that keeps the replaced item");
    let recorded: Vec<(i64, RowOutcome, Option<i64>, Option<Item>)> = fx
        .engine
        .bulk_rows_recorded(&fx.account_a, "op-1", 0, 0, i64::MAX)
        .await
        .expect("bulk_rows_recorded")
        .into_iter()
        .map(|r| (r.row_index, r.outcome, r.old_size, r.old_image))
        .collect();
    assert_eq!(
        recorded,
        vec![
            (
                5,
                RowOutcome::Applied,
                recorded_size(&item("a", "before")),
                None
            ),
            (
                6,
                RowOutcome::Applied,
                recorded_size(&item("a", "1")),
                Some(item("a", "1"))
            ),
        ],
        "each write records the size of the item it replaced, and that item where kept"
    );

    let stored = fx
        .engine
        .get_item(
            &ki,
            &Item::from([("pk".to_owned(), AttributeValue::S("a".to_owned()))]),
        )
        .await
        .expect("get_item");
    assert!(stored.is_some(), "the item landed");
    assert_eq!(
        fx.engine
            .bulk_row_outcome(&fx.account_a, "op-1", 0, 5)
            .await
            .expect("bulk_row_outcome"),
        Some(RowOutcome::Applied)
    );
}

/// A marked conditional row whose condition fails records `ConditionFailed`
/// with the item it was evaluated against, writes no item change, and
/// records no Applied row.
pub async fn a_marked_condition_failure_records_the_failure_and_writes_nothing<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "ledger_condition").await;
    let ki = key_info(fx, &table.table_name).await;
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());

    fx.engine
        .put_item(&ki, item("a", "before"), false, None, &maps, None)
        .await
        .expect("seed the target item");

    take_over(fx, "op-2").await;
    let results = fx
        .engine
        .put_items_batch(
            &ki,
            vec![marked_conditional(
                "a",
                "after",
                "attribute_not_exists(pk)",
                marker(&fx.account_a, "op-2", 9),
            )],
            None,
        )
        .await
        .expect("the batch applies");
    assert!(
        matches!(results[..], [BatchRowResult::ConditionFailed { .. }]),
        "the condition must fail against the existing item, got {results:?}"
    );

    let stored = fx
        .engine
        .get_item(
            &ki,
            &Item::from([("pk".to_owned(), AttributeValue::S("a".to_owned()))]),
        )
        .await
        .expect("get_item")
        .expect("the item is still there");
    assert_eq!(
        stored.get("v"),
        Some(&AttributeValue::S("before".to_owned())),
        "the refused write must not have touched the item"
    );
    assert_eq!(
        fx.engine
            .bulk_row_outcome(&fx.account_a, "op-2", 0, 9)
            .await
            .expect("bulk_row_outcome"),
        Some(RowOutcome::ConditionFailed),
        "the failure is recorded so a resumed run does not re-evaluate the row"
    );
    let recorded: Vec<(i64, RowOutcome, Option<i64>, Option<Item>)> = fx
        .engine
        .bulk_rows_recorded(&fx.account_a, "op-2", 0, 0, i64::MAX)
        .await
        .expect("bulk_rows_recorded")
        .into_iter()
        .map(|r| (r.row_index, r.outcome, r.old_size, r.old_image))
        .collect();
    assert_eq!(
        recorded,
        vec![(
            9,
            RowOutcome::ConditionFailed,
            recorded_size(&item("a", "before")),
            Some(item("a", "before"))
        )],
        "a failed condition records the item it was evaluated against"
    );
}

/// Deleting one operation's ledger rows leaves another operation's alone and
/// reports how many went.
pub async fn ledger_deletion_is_scoped_to_one_operation<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "ledger_scope").await;
    let ki = key_info(fx, &table.table_name).await;

    for (op, rows) in [
        ("op-a", vec![("a1", 1_i64), ("a2", 2)]),
        ("op-b", vec![("b1", 1)]),
    ] {
        take_over(fx, op).await;
        let batch = rows
            .into_iter()
            .map(|(pk, row)| marked(pk, "1", marker(&fx.account_a, op, row)))
            .collect();
        fx.engine
            .put_items_batch(&ki, batch, None)
            .await
            .expect("a marked batch");
    }

    let deleted = fx
        .engine
        .delete_bulk_row_outcomes(&fx.account_a, "op-a")
        .await
        .expect("delete_bulk_row_outcomes");
    assert_eq!(deleted, 2);
    assert_eq!(
        fx.engine
            .bulk_row_outcome(&fx.account_a, "op-a", 0, 1)
            .await
            .expect("read"),
        None
    );
    assert_eq!(
        fx.engine
            .bulk_row_outcome(&fx.account_a, "op-a", 0, 2)
            .await
            .expect("read"),
        None
    );
    assert_eq!(
        fx.engine
            .bulk_row_outcome(&fx.account_a, "op-b", 0, 1)
            .await
            .expect("read"),
        Some(RowOutcome::Applied),
        "the other operation's ledger survives"
    );
}

/// A marked row at a position the ledger already records is refused and
/// its batch changes nothing: every applied row is recorded once, so a
/// second outcome for one position means the row was sent after it was
/// applied.
pub async fn a_marked_write_at_a_recorded_position_is_refused_and_changes_nothing<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "ledger_recorded").await;
    let ki = key_info(fx, &table.table_name).await;
    let m = marker(&fx.account_a, "op-recorded", 4);
    take_over(fx, "op-recorded").await;

    fx.engine
        .put_items_batch(&ki, vec![marked("a", "first", m.clone())], None)
        .await
        .expect("the first marked batch");
    let again = fx
        .engine
        .put_items_batch(&ki, vec![marked("a", "second", m)], None)
        .await;
    assert!(
        again.is_err(),
        "the second write at the position is refused"
    );

    let stored = fx
        .engine
        .get_item(
            &ki,
            &Item::from([("pk".to_owned(), AttributeValue::S("a".to_owned()))]),
        )
        .await
        .expect("get_item")
        .expect("the item is there");
    assert_eq!(
        stored.get("v"),
        Some(&AttributeValue::S("first".to_owned())),
        "the refused write rolled back with its ledger row"
    );
    assert_eq!(
        fx.engine
            .bulk_row_outcome(&fx.account_a, "op-recorded", 0, 4)
            .await
            .expect("bulk_row_outcome"),
        Some(RowOutcome::Applied),
        "one ledger row, the first outcome"
    );
}
