// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A start whose table `DeleteTable` set `DELETING`, or removed, after the
//! start handler's lookup. The handler refuses a `DELETING` table at its
//! lookup; the start transaction refuses it the same way when the status
//! changed or the row went away after that lookup, before its other checks
//! and before any insert.

use extenddb_core::types::{DeleteTableInput, DescribeTableInput};

use crate::bulk_operation_store::TABLE_DELETING_MESSAGE;

use super::*;

/// Calls the storage start and requires the refusal the handler gives for a
/// table being deleted.
async fn refused_as_deleting<E: StorageEngine>(
    fx: &Fixture<E>,
    req: NewBulkOperation,
    label: &str,
) {
    match fx.engine.start_bulk_operation(&fx.account_a, req, 50).await {
        Err(StorageError::Validation(message)) => {
            assert_eq!(message, TABLE_DELETING_MESSAGE, "{label}");
        }
        Ok(outcome) => panic!("{label}: the start was answered {}", start_kind(&outcome)),
        Err(other) => panic!("{label}: unexpected error {other:?}"),
    }
}

/// A table set `DELETING` after the lookup: a new start is refused with the
/// handler's answer and inserts nothing, and a start that would replay a
/// token of an operation started before the deletion is refused the same
/// way, as the handler's lookup refuses it before its token replay.
pub async fn a_start_on_a_table_set_deleting_after_the_lookup_is_refused<E: StorageEngine>(
    fx: &Fixture<E>,
    editor: &dyn RowEditor,
) {
    let fresh = create_table(fx, &fx.account_a, "deleting_fresh").await;
    editor
        .mark_table_deleting(&fx.account_a, &fresh.table_id)
        .await;
    refused_as_deleting(
        fx,
        new_op(fx, &fx.account_a, &fresh, None, "fp", 1000),
        "a new start",
    )
    .await;
    let (records, cursor) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 10, None)
        .await
        .expect("list");
    assert!(
        records.is_empty() && cursor.is_none(),
        "the refused start inserted nothing, got {:?}",
        records.iter().map(|r| &r.operation_id).collect::<Vec<_>>()
    );

    let replayed = create_table(fx, &fx.account_a, "deleting_replayed").await;
    let first = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &replayed, Some("tok"), "fp", 2000),
            50,
        )
        .await,
    );
    editor
        .mark_table_deleting(&fx.account_a, &replayed.table_id)
        .await;
    refused_as_deleting(
        fx,
        new_op(fx, &fx.account_a, &replayed, Some("tok"), "fp", 3000),
        "a token replay",
    )
    .await;
    let (records, _) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 10, None)
        .await
        .expect("list");
    let ids: Vec<&str> = records.iter().map(|r| r.operation_id.as_str()).collect();
    assert_eq!(
        ids,
        vec![first.operation_id.as_str()],
        "only the operation started before the deletion exists"
    );
}

/// A table `DeleteTable` removed after the lookup, as it does when its
/// control-plane delay is under a second: the start is refused as for a
/// table being deleted and inserts nothing.
pub async fn a_start_on_a_table_removed_after_the_lookup_is_refused<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let removed = create_table(fx, &fx.account_a, "removed_after_lookup").await;
    fx.engine
        .delete_table(
            &fx.account_a,
            DeleteTableInput {
                table_name: removed.table_name.clone(),
            },
        )
        .await
        .expect("delete the table");
    let described = fx
        .engine
        .describe_table(
            &fx.account_a,
            DescribeTableInput {
                table_name: removed.table_name.clone(),
            },
        )
        .await;
    assert!(
        matches!(described, Err(StorageError::TableNotFound(_))),
        "the fixture's DeleteTable removes the table row at once, got {:?}",
        described.map(|d| d.table_status)
    );
    refused_as_deleting(
        fx,
        new_op(fx, &fx.account_a, &removed, None, "fp", 1000),
        "a start on a removed table",
    )
    .await;
    let (records, cursor) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 10, None)
        .await
        .expect("list");
    assert!(
        records.is_empty() && cursor.is_none(),
        "the refused start inserted nothing, got {:?}",
        records.iter().map(|r| &r.operation_id).collect::<Vec<_>>()
    );
}
