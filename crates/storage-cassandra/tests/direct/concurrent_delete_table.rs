// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct tests for two `DeleteTable` calls on one table.
//!
//! Amazon DynamoDB answers the second of two concurrent `DeleteTable` calls on
//! an `ACTIVE` table with `ResourceInUseException` and the sentence `Attempt to
//! change a resource which is still in use: Table is being deleted: <name>`
//! (measured 2026-10-25, five rounds of five). On this backend `DeleteTable`
//! claims the row as `DELETING` with a lightweight transaction and then drops
//! the data and the catalog row inside the same call; these tests run against
//! the window that call opens.
//!
//! `create_table` in this harness leaves a fresh table `CREATING` with a 250 ms
//! activation deadline, so the tests activate their tables first (`activate`)
//! and the claim they reach is the one on an `ACTIVE` row; one round of the
//! first test deletes a `CREATING` table, which the claim also admits.
//!
//! The claim sets the row's `status_transition_at` five minutes ahead, which
//! the control-plane sweep (`process_control_plane_transitions`, run by other
//! tests in this binary against the shared catalog keyspace) waits for before
//! it removes a `DELETING` row. The stopped-claim test moves that deadline into
//! the past by hand and runs the sweep itself. Each test runs inside
//! `removing_rows_after`, so a test that fails leaves no row behind for a later
//! sweep to reach after its account keyspace is gone.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::types::{TableStatus, Tag};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, MetadataEngine, TableEngine};

use crate::delete_table_support::{
    activate, catalog_rows, create_input, create_one, data_table_exists, delete_input,
    move_deadline_into_the_past, pitr_row, removing_rows_after, row_deadline_ms, row_status,
    row_table_id, wait_for_row_gone,
};
use crate::helpers::{TestAccount, setup_engine, test_config};

/// The round of `two_concurrent_delete_tables_have_one_winner` that deletes a
/// table still `CREATING`. The claim admits a `CREATING` row on purpose: this
/// backend has always deleted one, and refusing it is a separate change.
const CREATING_ROUND: usize = 9;

/// Two concurrent `DeleteTable` calls on one `ACTIVE` table: the claim applies
/// for one of them, the other answers `TableBeingDeleted`, which is the
/// service's answer for the second of two concurrent calls (measured
/// 2026-10-25, five rounds of five). Ten rounds, a fresh table each;
/// afterwards neither the catalog row nor the data table remains.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_delete_tables_have_one_winner() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, account_id) = (engine.clone(), id.clone());
        async move {
            let shared = Arc::new(engine.clone());
            for round in 0..10 {
                let name = format!("ConcurrentDelete{round}");
                let created = create_one(&engine, &account_id, &name).await;
                if round != CREATING_ROUND {
                    activate(&engine, &account_id, &name).await;
                }

                let tasks: Vec<_> = (0..2)
                    .map(|_| {
                        let engine = Arc::clone(&shared);
                        let account_id = account_id.clone();
                        let name = name.clone();
                        tokio::spawn(async move {
                            engine.delete_table(&account_id, delete_input(&name)).await
                        })
                    })
                    .collect();

                let mut ok = 0;
                let mut being_deleted = 0;
                for task in tasks {
                    match task.await.expect("task") {
                        Ok(description) => {
                            assert_eq!(
                                description.table_status,
                                TableStatus::Deleting,
                                "round {round}: the winner reports DELETING"
                            );
                            ok += 1;
                        }
                        Err(StorageError::TableBeingDeleted(reported)) if reported == name => {
                            being_deleted += 1;
                        }
                        Err(other) => panic!("round {round}: unexpected {other:?}"),
                    }
                }
                assert_eq!((ok, being_deleted), (1, 1), "round {round}");
                assert_eq!(
                    row_status(&engine, &account_id, &name).await,
                    None,
                    "round {round}: the catalog row is gone"
                );
                assert!(
                    !data_table_exists(&engine, &account_id, &created.table_id).await,
                    "round {round}: the data table is gone"
                );
            }
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// The second half of #553. The deleted table's continuous-backup row is keyed
/// on the table name and its tags on the table ARN, and a table created into
/// the same name holds both under the same keys. Here a `CreateTable` takes the
/// name back as soon as the winner releases it and tags the new table at once,
/// while a second `DeleteTable` is still in flight; the new table keeps its
/// tags and its PITR row, because the second call is refused at the claim and
/// runs none of the deletes.
///
/// The PITR flag is read from the stored row as well as through
/// `describe_continuous_backups`, whose `continuous_backups_status` is the
/// constant `ENABLED`. The new table enables PITR on even rounds, which a
/// missing row fails, and leaves it off on odd rounds, which a row inherited
/// from the first table fails; its tags must not include the first table's
/// `first-only` key.
///
/// The second call is started a little behind the first so that it meets a
/// delete already under way; under the create loop's load either of the two
/// can reach the claim first, so the test counts the outcomes rather than
/// naming a winner. The call that loses answers `TableBeingDeleted` when it
/// meets the claim and `TableNotFound` when the other one had already
/// finished, and both mean it ran no deletes of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_created_during_a_delete_keeps_its_tags_and_pitr_row() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, account_id) = (engine.clone(), id.clone());
        async move {
            let shared = Arc::new(engine.clone());
            for round in 0..5 {
                recreate_during_a_delete(&shared, &account_id, round).await;
            }
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// One round of `a_table_created_during_a_delete_keeps_its_tags_and_pitr_row`.
async fn recreate_during_a_delete(
    shared: &Arc<extenddb_storage_cassandra::CassandraEngine>,
    account_id: &str,
    round: usize,
) {
    let engine = shared.as_ref();
    let name = format!("RecreatedDuringDelete{round}");
    let first = create_one(engine, account_id, &name).await;
    activate(engine, account_id, &name).await;
    engine
        .tag_resource(
            &first.table_arn,
            &[
                Tag {
                    key: "owner".to_owned(),
                    value: "first".to_owned(),
                },
                Tag {
                    key: "first-only".to_owned(),
                    value: "yes".to_owned(),
                },
            ],
        )
        .await
        .expect("tag the first table");
    engine
        .update_continuous_backups(account_id, &name, true)
        .await
        .expect("enable PITR on the first table");

    let deletes: Vec<_> = (0..2)
        .map(|index| {
            let engine = Arc::clone(shared);
            let account_id = account_id.to_owned();
            let name = name.clone();
            tokio::spawn(async move {
                // The second call starts a little behind so it meets a
                // delete already under way rather than an untouched table.
                if index == 1 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                engine.delete_table(&account_id, delete_input(&name)).await
            })
        })
        .collect();
    // Takes the name back the moment the winner releases it, and writes the
    // new table's tags and PITR row while the second call may still be
    // running. A delete that is not fenced takes both away from here.
    let creator = {
        let engine = Arc::clone(shared);
        let account_id = account_id.to_owned();
        let name = name.clone();
        tokio::spawn(async move {
            for _ in 0..3_000 {
                match engine.create_table(&account_id, create_input(&name)).await {
                    Ok(description) => {
                        engine
                            .tag_resource(
                                &description.table_arn,
                                &[Tag {
                                    key: "owner".to_owned(),
                                    value: "second".to_owned(),
                                }],
                            )
                            .await
                            .expect("tag the new table");
                        if round.is_multiple_of(2) {
                            engine
                                .update_continuous_backups(&account_id, &name, true)
                                .await
                                .expect("enable PITR on the new table");
                        }
                        return Some(description);
                    }
                    Err(StorageError::TableAlreadyExists(_)) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(other) => panic!("recreating {name}: {other:?}"),
                }
            }
            None
        })
    };

    let mut ok = 0;
    let mut refused = 0;
    for task in deletes {
        match task.await.expect("task") {
            Ok(_) => ok += 1,
            Err(StorageError::TableBeingDeleted(reported)) if reported == name => refused += 1,
            Err(StorageError::TableNotFound(_)) => refused += 1,
            Err(other) => panic!("round {round}: unexpected {other:?}"),
        }
    }
    assert_eq!(
        (ok, refused),
        (1, 1),
        "round {round}: one call deletes the table and the other runs no deletes"
    );
    let recreated = creator
        .await
        .expect("task")
        .expect("the name became free again");
    assert_ne!(
        recreated.table_id, first.table_id,
        "round {round}: a new incarnation"
    );

    let tags = engine
        .list_tags(&recreated.table_arn)
        .await
        .expect("read the new table's tags");
    assert_eq!(
        tags.iter()
            .map(|tag| (tag.key.as_str(), tag.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("owner", "second")],
        "round {round}: the new table keeps its tags"
    );
    let backups = engine
        .describe_continuous_backups(account_id, &name)
        .await
        .expect("read the new table's continuous backups");
    let pitr_status = backups
        .point_in_time_recovery_description
        .map(|pitr| pitr.point_in_time_recovery_status);
    let stored = pitr_row(engine, account_id, &name).await;
    if round.is_multiple_of(2) {
        assert_eq!(
            (pitr_status.as_deref(), stored),
            (Some("ENABLED"), Some(true)),
            "round {round}: the new table keeps its PITR row"
        );
    } else {
        assert_eq!(
            (pitr_status.as_deref(), stored),
            (Some("DISABLED"), None),
            "round {round}: the new table inherits no PITR row"
        );
    }
    assert_eq!(
        row_table_id(engine, account_id, &name).await.as_deref(),
        Some(recreated.table_id.as_str()),
        "round {round}: the catalog row is the new table's"
    );

    engine
        .delete_table(account_id, delete_input(&name))
        .await
        .expect("remove the new table");
}

/// The claim is conditioned on the table id as well as on the status, so a
/// call holding the id of a table that has since been deleted and created
/// again under the name does not claim the newer table.
#[tokio::test]
async fn a_claim_carrying_another_table_id_does_not_apply() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, account_id) = (engine.clone(), id.clone());
        async move {
            let name = "ClaimedWithAnotherId";
            create_one(&engine, &account_id, name).await;
            activate(&engine, &account_id, name).await;
            let other_table_id = uuid::Uuid::new_v4().to_string();
            assert!(
                !engine
                    .claim_table_for_delete(&account_id, name, &other_table_id)
                    .await
                    .expect("claim"),
                "a claim carrying another table id does not apply"
            );
            assert_eq!(
                row_status(&engine, &account_id, name).await.as_deref(),
                Some("ACTIVE")
            );
            engine
                .delete_table(&account_id, delete_input(name))
                .await
                .expect("remove the table");
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// The lease is renewed with a compare-and-set on the deadline the holder
/// last wrote. A renewal that carries an older deadline belongs to an actor
/// whose row was taken over since, and does not apply.
#[tokio::test]
async fn a_renewal_against_a_stale_deadline_does_not_apply() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, account_id) = (engine.clone(), id.clone());
        async move {
            let name = "RenewedWithStaleDeadline";
            let created = create_one(&engine, &account_id, name).await;
            activate(&engine, &account_id, name).await;
            assert!(
                engine
                    .claim_table_for_delete(&account_id, name, &created.table_id)
                    .await
                    .expect("claim"),
                "the claim applies to an ACTIVE row"
            );
            let first = row_deadline_ms(&engine, &account_id, name)
                .await
                .expect("the claim sets a deadline");
            let second = engine
                .renew_delete_lease(&account_id, name, &created.table_id, first)
                .await
                .expect("renew")
                .expect("a renewal carrying the current deadline applies");
            assert!(second >= first, "a renewal moves the deadline forward");
            assert_eq!(
                engine
                    .renew_delete_lease(&account_id, name, &created.table_id, first)
                    .await
                    .expect("renew again"),
                None,
                "a renewal carrying the deadline that was already replaced does not apply"
            );
            assert_eq!(
                row_deadline_ms(&engine, &account_id, name).await,
                Some(second),
                "the row keeps the deadline of the renewal that applied"
            );
            move_deadline_into_the_past(&engine, &account_id, name).await;
            engine
                .process_control_plane_transitions()
                .await
                .expect("sweep the claimed row");
            wait_for_row_gone(&engine, &account_id, name).await;
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

// ── A claim whose call stopped ──────────────────────────────────────────────

/// A `DeleteTable` that stops after its claim, because its process dies or a
/// statement after the claim fails, leaves the row `DELETING` with the claim's
/// deadline ahead of it. Until that deadline the control-plane sweep leaves the
/// row alone, so it does not run the removal beside a call that is still
/// dropping; once it has passed, the sweep removes the row, the data table, the
/// tags and the continuous-backup row, and the name can be created again. The
/// test claims the row and stops there, moves the deadline into the past rather
/// than waiting five minutes, and runs the sweep itself.
#[tokio::test]
async fn a_claim_left_by_a_stopped_call_is_swept_after_its_deadline() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, account_id) = (engine.clone(), id.clone());
        async move { claim_then_sweep(&engine, &account_id).await }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// The body of `a_claim_left_by_a_stopped_call_is_swept_after_its_deadline`.
async fn claim_then_sweep(engine: &extenddb_storage_cassandra::CassandraEngine, account_id: &str) {
    let name = "ClaimedThenStopped";
    let created = create_one(engine, account_id, name).await;
    activate(engine, account_id, name).await;
    engine
        .tag_resource(
            &created.table_arn,
            &[Tag {
                key: "owner".to_owned(),
                value: "first".to_owned(),
            }],
        )
        .await
        .expect("tag the table");
    engine
        .update_continuous_backups(account_id, name, true)
        .await
        .expect("enable PITR");

    let claimed_at_ms = chrono::Utc::now().timestamp_millis();
    assert!(
        engine
            .claim_table_for_delete(account_id, name, &created.table_id)
            .await
            .expect("claim"),
        "the claim applies to an ACTIVE row"
    );
    assert!(
        !engine
            .claim_table_for_delete(account_id, name, &created.table_id)
            .await
            .expect("second claim"),
        "a second claim does not apply"
    );
    assert_eq!(
        row_status(engine, account_id, name).await.as_deref(),
        Some("DELETING")
    );
    let lease_ms = row_deadline_ms(engine, account_id, name)
        .await
        .expect("the claim sets a deadline")
        - claimed_at_ms;
    assert!(
        (299_000..=310_000).contains(&lease_ms),
        "the deadline is five minutes after the claim, got {lease_ms} ms"
    );

    engine
        .process_control_plane_transitions()
        .await
        .expect("sweep before the deadline");
    assert_eq!(
        row_status(engine, account_id, name).await.as_deref(),
        Some("DELETING"),
        "before the deadline the sweep leaves the row to the call"
    );
    assert!(
        data_table_exists(engine, account_id, &created.table_id).await,
        "the data table is still there"
    );

    move_deadline_into_the_past(engine, account_id, name).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("sweep after the deadline");
    // Another test's sweep may have taken the row first; this sweep then
    // leaves it to that one, so wait for the removal rather than assume it.
    wait_for_row_gone(engine, account_id, name).await;
    assert!(
        !data_table_exists(engine, account_id, &created.table_id).await,
        "the data table is gone"
    );
    let catalog = engine.catalog_keyspace();
    assert_eq!(
        catalog_rows(
            engine,
            &format!("SELECT tag_key FROM {catalog}.tags WHERE resource_arn = ?"),
            cdrs_tokio::query_values!(created.table_arn.as_str()),
        )
        .await,
        0,
        "the tags are gone"
    );
    assert_eq!(
        catalog_rows(
            engine,
            &format!(
                "SELECT table_name FROM {catalog}.continuous_backups \
                 WHERE account_id = ? AND table_name = ?"
            ),
            cdrs_tokio::query_values!(account_id, name),
        )
        .await,
        0,
        "the continuous-backup row is gone"
    );

    let again = create_one(engine, account_id, name).await;
    assert_ne!(again.table_id, created.table_id, "a new incarnation");
    engine
        .delete_table(account_id, delete_input(name))
        .await
        .expect("remove the new table");
}
