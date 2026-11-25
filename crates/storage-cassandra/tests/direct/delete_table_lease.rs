// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct tests for who may finish a `DeleteTable` on this backend.
//!
//! A `DeleteTable` claims the catalog row as `DELETING` with a deadline five
//! minutes ahead. The control-plane sweep takes a row whose deadline has
//! passed with a compare-and-set on that deadline. The actor that holds the
//! row renews it with the same swap, deletes the tags (the delete keyed on
//! the name), and last deletes the row on its table id. These tests put a
//! second actor beside the one that holds the row, and a table created into
//! the name in between, and check that the newer table keeps its catalog row
//! and its tags. An actor whose removal fails moves the deadline to the
//! current time, so the sweep's next pass finishes the removal. A
//! `DeleteTable` whose request is dropped after its claim still finishes it.
//!
//! Rows with a deadline in the past are visible to every sweep in this binary,
//! so the tests wait on the state they expect rather than on the sweep they
//! started, and each runs inside `removing_rows_after`, which removes the
//! account's catalog rows whether the test passes or fails.

use std::time::Duration;

use extenddb_core::types::Tag;
use extenddb_storage::{MetadataEngine, TableEngine};
use extenddb_storage_cassandra::CassandraEngine;

use crate::delete_table_support::{
    activate, catalog_rows, create_input, create_one, data_table_exists, delete_input, gsi_input,
    move_deadline_into_the_past, removing_rows_after, row_deadline_ms, row_status, row_table_id,
    tags_of, wait_for_data_table_gone, wait_for_row_gone, wait_for_status,
};
use crate::helpers::{TestAccount, setup_engine, test_config};

/// A claim whose call stopped, its deadline passed, and two servers' sweeps
/// started 300 ms apart. The first sweep takes the row by moving its deadline,
/// so the second one's select no longer returns it, and the first holds the
/// row until it has deleted it. A table created into the name once the row is
/// gone keeps its catalog row, its tag and its data table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sweeps_over_a_stopped_claim_leave_a_newer_table_alone() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let other_server = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, id) = (engine.clone(), id.clone());
        async move {
            let name = "SweptByTwoServers";
            let first = engine
                .create_table(&id, gsi_input(name, 6))
                .await
                .expect("create the first table");
            activate(&engine, &id, name).await;
            assert!(
                engine
                    .claim_table_for_delete(&id, name, &first.table_id)
                    .await
                    .expect("claim"),
                "the claim applies"
            );
            move_deadline_into_the_past(&engine, &id, name).await;

            let sweep_a = {
                let engine = engine.clone();
                tokio::spawn(async move { engine.process_control_plane_transitions().await })
            };
            tokio::time::sleep(Duration::from_millis(300)).await;
            let sweep_b =
                tokio::spawn(async move { other_server.process_control_plane_transitions().await });

            wait_for_row_gone(&engine, &id, name).await;
            let second = create_one(&engine, &id, name).await;
            tag_second(&engine, &second.table_arn).await;
            sweep_a.await.expect("task").expect("first sweep");
            sweep_b.await.expect("task").expect("second sweep");

            assert_eq!(
                row_table_id(&engine, &id, name).await.as_deref(),
                Some(second.table_id.as_str()),
                "the new table keeps its catalog row"
            );
            assert_eq!(
                tags_of(&engine, &second.table_arn).await,
                second_tag(),
                "the new table keeps its tag"
            );
            assert!(data_table_exists(&engine, &id, &second.table_id).await);
            assert!(!data_table_exists(&engine, &id, &first.table_id).await);
            engine
                .delete_table(&id, delete_input(name))
                .await
                .expect("remove the new table");
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// A sweep holds a row from its select, and while it drops the index tables
/// the row is removed and the name created again (here the row delete is run
/// by hand, the last statement of whichever actor finished first). The sweep's
/// renewal then finds the row holding another table id, so it stops before the
/// delete keyed on the name and the row delete, and does not report the
/// table as removed; the newer table keeps its catalog row and its tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sweep_whose_row_was_replaced_leaves_the_newer_table_alone() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, id) = (engine.clone(), id.clone());
        async move {
            let name = "ReplacedUnderTheSweep";
            let first = engine
                .create_table(&id, gsi_input(name, 6))
                .await
                .expect("create the first table");
            activate(&engine, &id, name).await;
            assert!(
                engine
                    .claim_table_for_delete(&id, name, &first.table_id)
                    .await
                    .expect("claim")
            );
            move_deadline_into_the_past(&engine, &id, name).await;

            let sweep = {
                let engine = engine.clone();
                tokio::spawn(async move { engine.process_control_plane_transitions().await })
            };
            // The sweep is past its select once its take has moved the
            // deadline, or once the table's index metadata is gone.
            let catalog = engine.catalog_keyspace();
            let started = std::time::Instant::now();
            loop {
                let taken = row_deadline_ahead(&engine, &id, name).await;
                let metadata_gone = catalog_rows(
                    &engine,
                    &format!("SELECT index_id FROM {catalog}.indexes WHERE table_id = ?"),
                    cdrs_tokio::query_values!(first.table_id.as_str()),
                )
                .await
                    == 0;
                if taken || metadata_gone {
                    break;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(60),
                    "the sweep never started"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let row_delete = format!(
                "DELETE FROM {catalog}.tables WHERE account_id = ? AND table_name = ? \
                 IF table_id = ?"
            );
            extenddb_storage_cassandra::cassandra_util::query_lwt(
                engine.session(),
                &row_delete,
                cdrs_tokio::query_values!(id.as_str(), name, first.table_id.as_str()),
            )
            .await
            .expect("remove the row");
            let second = create_one(&engine, &id, name).await;
            tag_second(&engine, &second.table_arn).await;

            let swept = sweep.await.expect("task").expect("the sweep");
            assert_eq!(
                row_table_id(&engine, &id, name).await.as_deref(),
                Some(second.table_id.as_str()),
                "the new table keeps its catalog row"
            );
            assert_eq!(
                tags_of(&engine, &second.table_arn).await,
                second_tag(),
                "the new table keeps its tag"
            );
            assert!(data_table_exists(&engine, &id, &second.table_id).await);
            assert!(
                !swept.iter().any(|(table, _)| table == name),
                "the sweep stopped at its renewal: {swept:?}"
            );
            engine
                .delete_table(&id, delete_input(name))
                .await
                .expect("remove the new table");
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// True when the row's deadline is more than four minutes away, which only a
/// fresh claim or a take by a sweep writes.
async fn row_deadline_ahead(engine: &CassandraEngine, id: &str, name: &str) -> bool {
    row_deadline_ms(engine, id, name)
        .await
        .is_some_and(|deadline| deadline > chrono::Utc::now().timestamp_millis() + 240_000)
}

/// The tag of the table created into a deleted table's name. The tags are the
/// one delete keyed on the name rather than the table id, so a stale actor
/// that runs its name-keyed delete after this table exists takes them away.
const SECOND_TAG: (&str, &str) = ("owner", "second");

async fn tag_second(engine: &CassandraEngine, arn: &str) {
    engine
        .tag_resource(
            arn,
            &[Tag {
                key: SECOND_TAG.0.to_owned(),
                value: SECOND_TAG.1.to_owned(),
            }],
        )
        .await
        .expect("tag the new table");
}

/// [`SECOND_TAG`] as [`tags_of`] reads it back.
fn second_tag() -> Vec<(String, String)> {
    vec![(SECOND_TAG.0.to_owned(), SECOND_TAG.1.to_owned())]
}

/// A `DeleteTable` of a table with eight indexes, and, once its base data
/// table is gone and while it drops the index tables, a tag written at the
/// storage layer, below the engine's validation. The delete keyed on the name
/// runs after the drops, so the row goes with the table, and a table created
/// into the name afterwards has no tags.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tag_written_during_the_drops_goes_with_the_table() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, id) = (engine.clone(), id.clone());
        async move {
            let name = "WrittenDuringTheDrops";
            let first = engine
                .create_table(&id, gsi_input(name, 8))
                .await
                .expect("create the table");
            activate(&engine, &id, name).await;
            let call = {
                let (engine, id) = (engine.clone(), id.clone());
                tokio::spawn(async move { engine.delete_table(&id, delete_input(name)).await })
            };
            wait_for_status(&engine, &id, name, "DELETING").await;
            wait_for_data_table_gone(&engine, &id, &first.table_id).await;
            engine
                .tag_resource(
                    &first.table_arn,
                    &[Tag {
                        key: "left-by".to_owned(),
                        value: "the deleted table".to_owned(),
                    }],
                )
                .await
                .expect("write a tag row");
            call.await.expect("task").expect("DeleteTable");

            let second = engine
                .create_table(&id, create_input(name))
                .await
                .expect("create the name again");
            assert_eq!(
                tags_of(&engine, &second.table_arn).await,
                Vec::<(String, String)>::new(),
                "the new table has no tags"
            );
            engine
                .delete_table(&id, delete_input(name))
                .await
                .expect("remove the new table");
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// Two claims whose calls stopped, in two accounts, both past their deadline;
/// one account's keyspace is gone, so removing its table fails at the TTL
/// clear. The sweep logs that row and goes on: it returns `Ok`, and the other
/// account's row is removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_row_the_sweep_cannot_remove_does_not_stop_the_others() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let broken = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let healthy = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let (broken_id, healthy_id) = (broken.account_id.clone(), healthy.account_id.clone());
    let test = {
        let (engine, broken_id, healthy_id) =
            (engine.clone(), broken_id.clone(), healthy_id.clone());
        let inner_engine = engine.clone();
        let healthy_guard = healthy_id.clone();
        async move {
            let inner = async move {
                for (id, name) in [(&broken_id, "Unremovable"), (&healthy_id, "Removable")] {
                    let table = create_one(&engine, id, name).await;
                    activate(&engine, id, name).await;
                    assert!(
                        engine
                            .claim_table_for_delete(id, name, &table.table_id)
                            .await
                            .expect("claim")
                    );
                }
                // Dropped before either deadline passes, so no sweep can remove
                // the first table while its keyspace is still there.
                let keyspace = engine.account_keyspace(&broken_id);
                engine
                    .session()
                    .query(format!("DROP KEYSPACE IF EXISTS {keyspace}"))
                    .await
                    .expect("drop the account keyspace");
                move_deadline_into_the_past(&engine, &broken_id, "Unremovable").await;
                move_deadline_into_the_past(&engine, &healthy_id, "Removable").await;

                engine
                    .process_control_plane_transitions()
                    .await
                    .expect("the sweep logs the row it cannot remove and goes on");
                wait_for_row_gone(&engine, &healthy_id, "Removable").await;
                assert_eq!(
                    row_status(&engine, &broken_id, "Unremovable")
                        .await
                        .as_deref(),
                    Some("DELETING"),
                    "the row whose removal failed is left for a later pass"
                );
            };
            removing_rows_after(&inner_engine, &healthy_guard, inner).await;
        }
    };
    removing_rows_after(&engine, &broken.account_id, test).await;
}

/// The order of a `DeleteTable`: the delete keyed on the name runs after the
/// drops. Eight rounds, each a tagged table with one index; while the call
/// runs, the moment its base data table is gone the table's tag row is still
/// there (the index table is dropped between the two), and once the call
/// returns it is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_tag_rows_outlive_the_drops_and_go_with_the_row() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, id) = (engine.clone(), id.clone());
        async move {
            for round in 0..8 {
                let name = format!("NameKeyedLast{round}");
                let table = engine
                    .create_table(&id, gsi_input(&name, 1))
                    .await
                    .expect("create the table");
                activate(&engine, &id, &name).await;
                engine
                    .tag_resource(
                        &table.table_arn,
                        &[Tag {
                            key: "owner".to_owned(),
                            value: "deleted".to_owned(),
                        }],
                    )
                    .await
                    .expect("tag the table");
                let call = {
                    let (engine, id, name) = (engine.clone(), id.clone(), name.clone());
                    tokio::spawn(async move { engine.delete_table(&id, delete_input(&name)).await })
                };
                wait_for_data_table_gone(&engine, &id, &table.table_id).await;
                let during = tag_rows(&engine, &table.table_arn).await;
                call.await.expect("task").expect("DeleteTable");
                let after = tag_rows(&engine, &table.table_arn).await;
                assert_eq!(
                    during, 1,
                    "round {round}: the tag row is there once the data table is gone"
                );
                assert_eq!(after, 0, "round {round}: it goes with the table");
            }
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// How many tag rows the catalog holds for `arn`.
async fn tag_rows(engine: &CassandraEngine, arn: &str) -> usize {
    catalog_rows(
        engine,
        &format!(
            "SELECT tag_key FROM {}.tags WHERE resource_arn = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(arn),
    )
    .await
}

/// A `DeleteTable` of a table with two indexes whose first index table drop
/// fails, after the claim. The call answers that error and hands the row back
/// to the sweep. The row's deadline is then no later than the current time.
/// So the sweep's next pass takes the row without waiting five minutes for
/// the lease. The index tables go before their metadata, so the metadata
/// outlives the failed drop, and that pass drops both index tables.
#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_that_fails_after_its_claim_is_finished_by_the_next_sweep() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, id) = (engine.clone(), id.clone());
        async move {
            let name = "FailsAfterTheClaim";
            let table = engine
                .create_table(&id, gsi_input(name, 2))
                .await
                .expect("create the table");
            activate(&engine, &id, name).await;
            let index_tables = index_table_names(&engine, &table.table_id).await;
            assert_eq!(tables_left(&engine, &id, &index_tables).await, 2);
            let keyspace = engine.account_keyspace(&id);
            extenddb_storage_cassandra::test_hooks::fail_next_index_table_drop(&keyspace);
            match engine.delete_table(&id, delete_input(name)).await {
                Err(extenddb_storage::error::StorageError::Internal(message)) => assert_eq!(
                    message,
                    format!("test hook: the drop of an index data table in {keyspace} failed")
                ),
                other => panic!("expected the failed drop's error, got {other:?}"),
            }
            assert_eq!(
                row_status(&engine, &id, name).await.as_deref(),
                Some("DELETING")
            );
            let deadline = row_deadline_ms(&engine, &id, name)
                .await
                .expect("the row keeps a deadline");
            assert!(
                deadline <= chrono::Utc::now().timestamp_millis(),
                "the call hands the row to the sweep, but its deadline is {deadline}"
            );

            engine
                .process_control_plane_transitions()
                .await
                .expect("the sweep");
            // Another test's sweep may take the row first.
            wait_for_row_gone(&engine, &id, name).await;
            assert!(!data_table_exists(&engine, &id, &table.table_id).await);
            assert_eq!(
                tables_left(&engine, &id, &index_tables).await,
                0,
                "the sweep drops both index tables"
            );
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// How many of `names` are tables in the account's keyspace.
#[cfg(feature = "test-hooks")]
async fn tables_left(engine: &CassandraEngine, account_id: &str, names: &[String]) -> usize {
    let mut left = 0;
    for name in names {
        if crate::backup_engine::data_table_exists(engine, account_id, name).await {
            left += 1;
        }
    }
    left
}

/// The names of the index data tables of `table_id`, read from its index
/// metadata.
#[cfg(feature = "test-hooks")]
async fn index_table_names(engine: &CassandraEngine, table_id: &str) -> Vec<String> {
    use cdrs_tokio::types::IntoRustByName as _;
    extenddb_storage_cassandra::cassandra_util::query_rows::<extenddb_storage::error::StorageError>(
        &engine.session_arc(),
        &format!(
            "SELECT index_id FROM {}.indexes WHERE table_id = ?",
            engine.catalog_keyspace()
        ),
        cdrs_tokio::query_values!(table_id),
        "index ids",
    )
    .await
    .expect("read the index metadata")
    .iter()
    .map(|row| {
        let index_id: String = row.get_r_by_name("index_id").expect("index_id");
        extenddb_storage_cassandra::data::ddl::index_table_name(&index_id)
    })
    .collect()
}

/// A `DeleteTable` whose request is dropped once its claim applies, as a
/// request is when its client closes the connection. The claim and the
/// removal run in a task that the request only awaits. So the table is still
/// removed, within seconds and not after the five-minute lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_whose_request_is_dropped_after_its_claim_still_removes_the_table() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, id) = (engine.clone(), id.clone());
        async move {
            let name = "DroppedAfterTheClaim";
            let table = engine
                .create_table(&id, gsi_input(name, 4))
                .await
                .expect("create the table");
            activate(&engine, &id, name).await;
            let call = {
                let (engine, id) = (engine.clone(), id.clone());
                tokio::spawn(async move { engine.delete_table(&id, delete_input(name)).await })
            };
            wait_for_status(&engine, &id, name, "DELETING").await;
            call.abort();
            wait_for_row_gone(&engine, &id, name).await;
            assert!(!data_table_exists(&engine, &id, &table.table_id).await);
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// How long a test waits for a removal to reach a pause it armed.
#[cfg(feature = "test-hooks")]
const PAUSE_LIMIT: Duration = Duration::from_secs(60);

/// When the paused call of
/// `a_delete_whose_row_a_sweep_took_over_stops_at_its_renewal` resumes.
#[cfg(feature = "test-hooks")]
#[derive(Clone, Copy, Debug)]
enum Resume {
    /// Once the sweep has removed the table and the name was created again
    /// and tagged.
    AfterTheSweepFinished,
    /// While the sweep holds the row: it has taken the row, run its drops,
    /// and is paused before its own renewal.
    WhileTheSweepHoldsTheRow,
}

/// A `DeleteTable` paused between its drops and its renewal, and a sweep that
/// takes its row over once the deadline is moved into the past. The call's
/// renewal then does not apply, so the call stops before the tag delete and
/// the row delete, and answers 200 with `DELETING`. Resumed after the sweep
/// finished, it leaves the tag of the table created into the name since.
/// Resumed while the sweep holds the row, it leaves the row and its tag to
/// the sweep, which then finishes the removal.
#[cfg(feature = "test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_whose_row_a_sweep_took_over_stops_at_its_renewal() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, id) = (engine.clone(), id.clone());
        async move {
            let cases = [
                Resume::AfterTheSweepFinished,
                Resume::WhileTheSweepHoldsTheRow,
            ];
            for (round, resume) in cases.into_iter().enumerate() {
                let name = format!("TakenOverAtTheRenewal{round}");
                taken_over_at_the_renewal(&engine, &id, &name, resume).await;
            }
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// One case of `a_delete_whose_row_a_sweep_took_over_stops_at_its_renewal`.
#[cfg(feature = "test-hooks")]
async fn taken_over_at_the_renewal(engine: &CassandraEngine, id: &str, name: &str, resume: Resume) {
    use extenddb_storage_cassandra::test_hooks::pause_before_delete_lease_renewal;

    let first = engine
        .create_table(id, gsi_input(name, 1))
        .await
        .expect("create the table");
    activate(engine, id, name).await;
    engine
        .tag_resource(
            &first.table_arn,
            &[Tag {
                key: "owner".to_owned(),
                value: "first".to_owned(),
            }],
        )
        .await
        .expect("tag the table");
    let call_pause = pause_before_delete_lease_renewal(&first.table_id);
    let call = {
        let (engine, id, name) = (engine.clone(), id.to_owned(), name.to_owned());
        tokio::spawn(async move { engine.delete_table(&id, delete_input(&name)).await })
    };
    tokio::time::timeout(PAUSE_LIMIT, call_pause.reached())
        .await
        .expect("the call reaches its renewal");
    // Armed before the deadline moves, so whichever sweep takes the row
    // pauses there.
    let sweep_pause = matches!(resume, Resume::WhileTheSweepHoldsTheRow)
        .then(|| pause_before_delete_lease_renewal(&first.table_id));
    move_deadline_into_the_past(engine, id, name).await;
    let sweep = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.process_control_plane_transitions().await })
    };

    match sweep_pause {
        None => {
            wait_for_row_gone(engine, id, name).await;
            let second = create_one(engine, id, name).await;
            tag_second(engine, &second.table_arn).await;
            call_pause.release();
            let answer = call.await.expect("task").expect("DeleteTable");
            assert_eq!(
                answer.table_status,
                extenddb_core::types::TableStatus::Deleting,
                "{resume:?}: the call answers DELETING"
            );
            assert_eq!(
                row_table_id(engine, id, name).await.as_deref(),
                Some(second.table_id.as_str()),
                "{resume:?}: the new table keeps its catalog row"
            );
            assert_eq!(
                tags_of(engine, &second.table_arn).await,
                second_tag(),
                "{resume:?}: the new table keeps its tag"
            );
            engine
                .delete_table(id, delete_input(name))
                .await
                .expect("remove the new table");
        }
        Some(sweep_pause) => {
            tokio::time::timeout(PAUSE_LIMIT, sweep_pause.reached())
                .await
                .expect("the sweep reaches its renewal");
            call_pause.release();
            let answer = call.await.expect("task");
            let row_after_the_call = row_table_id(engine, id, name).await;
            let tags_after_the_call = tag_rows(engine, &first.table_arn).await;
            // Released before the checks, so a failing check does not leave
            // another test's sweep held at this pause.
            sweep_pause.release();
            assert_eq!(
                answer.expect("DeleteTable").table_status,
                extenddb_core::types::TableStatus::Deleting,
                "{resume:?}: the call answers DELETING"
            );
            assert_eq!(
                row_after_the_call.as_deref(),
                Some(first.table_id.as_str()),
                "{resume:?}: the call leaves the row to the sweep"
            );
            assert_eq!(
                tags_after_the_call, 1,
                "{resume:?}: the call runs no tag delete"
            );
            wait_for_row_gone(engine, id, name).await;
            assert_eq!(
                tag_rows(engine, &first.table_arn).await,
                0,
                "{resume:?}: the sweep deletes the tags"
            );
        }
    }
    sweep.await.expect("task").expect("the sweep");
    assert!(
        !data_table_exists(engine, id, &first.table_id).await,
        "{resume:?}: the data table is gone"
    );
}
