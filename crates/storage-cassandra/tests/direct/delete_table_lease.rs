// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct tests for who may finish a `DeleteTable` on this backend.
//!
//! A `DeleteTable` claims the catalog row as `DELETING` with a deadline five
//! minutes ahead. The control-plane sweep takes a row whose deadline has
//! passed with a compare-and-set on that deadline, and the actor that holds
//! the row renews it with the same swap before the two deletes keyed on the
//! name (the `continuous_backups` row and the tags), which run last, then
//! deletes the row on its table id. These tests put a second actor beside the
//! one that holds the row, and a table created into the name in between, and
//! check that the newer table keeps its catalog row, its PITR row and its tags.
//!
//! Rows with a deadline in the past are visible to every sweep in this binary,
//! so the tests wait on the state they expect rather than on the sweep they
//! started, and each runs inside `removing_rows_after`, which removes the
//! account's catalog rows whether the test passes or fails.

use std::time::Duration;

use extenddb_core::types::{AttributeValue, Tag};
use extenddb_storage::{
    BackupEngine, DataEngine, MetadataEngine, RestoreRequest, RestoreTableOverrides, TableEngine,
};
use extenddb_storage_cassandra::CassandraEngine;

use crate::delete_table_support::{
    activate, catalog_rows, create_input, create_one, data_table_exists, delete_input, gsi_input,
    move_deadline_into_the_past, pitr_row, removing_rows_after, row_deadline_ms, row_status,
    row_table_id, tags_of, wait_for_data_table_gone, wait_for_row_gone, wait_for_status,
};
use crate::helpers::{TestAccount, setup_engine, test_config};

/// A claim whose call stopped, its deadline passed, and two servers' sweeps
/// started 300 ms apart. The first sweep takes the row by moving its deadline,
/// so the second one's select no longer returns it, and the first holds the
/// row until it has deleted it. A table created into the name once the row is
/// gone keeps its catalog row, its PITR row and its data table.
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
            engine
                .update_continuous_backups(&id, name, true)
                .await
                .expect("enable PITR");
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
            engine
                .update_continuous_backups(&id, name, true)
                .await
                .expect("enable PITR on the new table");
            sweep_a.await.expect("task").expect("first sweep");
            sweep_b.await.expect("task").expect("second sweep");

            assert_eq!(
                row_table_id(&engine, &id, name).await.as_deref(),
                Some(second.table_id.as_str()),
                "the new table keeps its catalog row"
            );
            assert_eq!(
                pitr_row(&engine, &id, name).await,
                Some(true),
                "the new table keeps its PITR row"
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
/// deletes keyed on the name and the row delete, and does not report the
/// table as removed.
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
            engine
                .update_continuous_backups(&id, name, true)
                .await
                .expect("enable PITR");
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
            engine
                .update_continuous_backups(&id, name, true)
                .await
                .expect("enable PITR on the new table");

            let swept = sweep.await.expect("task").expect("the sweep");
            assert_eq!(
                row_table_id(&engine, &id, name).await.as_deref(),
                Some(second.table_id.as_str()),
                "the new table keeps its catalog row"
            );
            assert_eq!(
                pitr_row(&engine, &id, name).await,
                Some(true),
                "the new table keeps its PITR row"
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

/// A restored table keeps its restore registration columns after activation,
/// and its heartbeat stops there. A `DeleteTable` claim on it makes a
/// `DELETING` row with a registration and a stale heartbeat, the row the
/// abandoned-restore sweep removes. The sweep skips it because the claim
/// wrote a deadline, so the claim stays with its call. The heartbeat is moved
/// two minutes back, the state the row reaches two minutes after the restore,
/// and the sweep runs with the production threshold of 60 s, so the test
/// reaches no other test's restore.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_restore_sweep_leaves_a_claimed_restored_table_to_its_delete() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = TestAccount::new(&engine, &test_config().keyspace_prefix).await;
    let id = account.account_id.clone();
    let test = {
        let (engine, id) = (engine.clone(), id.clone());
        async move {
            create_one(&engine, &id, "RestoreSource").await;
            activate(&engine, &id, "RestoreSource").await;
            let key_info = engine
                .table_key_info(&id, "RestoreSource")
                .await
                .expect("key info");
            let item = [("pk".to_owned(), AttributeValue::S("k0".to_owned()))].into();
            engine
                .put_item(&key_info, item, false, None, &Default::default(), None)
                .await
                .expect("put an item");
            let backup = engine
                .create_backup(&id, "RestoreSource", "lease")
                .await
                .expect("create a backup");
            let source = engine
                .backup_restore_source(&id, &backup.backup_arn)
                .await
                .expect("restore source");
            let input = source.restore_input("Restored", &RestoreTableOverrides::default());
            let restored = engine
                .restore_table_from_backup(
                    &id,
                    RestoreRequest {
                        backup_arn: backup.backup_arn.clone(),
                        source_table_arn: source.source_table_arn.clone(),
                        restore_date_time: source.backup_creation_date_time,
                        input,
                    },
                )
                .await
                .expect("restore");
            wait_for_status(&engine, &id, "Restored", "ACTIVE").await;

            let stale = extenddb_storage_cassandra::cassandra_util::now_millis() - 120_000;
            let age_heartbeat = format!(
                "UPDATE {}.tables SET restore_heartbeat = ? WHERE account_id = ? \
                 AND table_name = ? IF table_id = ? AND table_status = 'ACTIVE'",
                engine.catalog_keyspace()
            );
            let result = extenddb_storage_cassandra::cassandra_util::query_lwt(
                engine.session(),
                &age_heartbeat,
                cdrs_tokio::query_values!(
                    stale,
                    id.as_str(),
                    "Restored",
                    restored.table_id.as_str()
                ),
            )
            .await
            .expect("age the heartbeat");
            assert!(
                extenddb_storage_cassandra::cassandra_util::lwt_applied(&result)
                    .expect("read the answer"),
                "the restored table is ACTIVE"
            );
            assert!(
                engine
                    .claim_table_for_delete(&id, "Restored", &restored.table_id)
                    .await
                    .expect("claim"),
                "the claim applies"
            );

            let removed = engine
                .sweep_abandoned_restores(Duration::from_secs(60))
                .await
                .expect("restore sweep");
            assert!(
                !removed.iter().any(|table| table == "Restored"),
                "the restore sweep left the claimed table alone: {removed:?}"
            );
            assert_eq!(
                row_status(&engine, &id, "Restored").await.as_deref(),
                Some("DELETING")
            );
            assert!(data_table_exists(&engine, &id, &restored.table_id).await);
        }
    };
    removing_rows_after(&engine, &id, test).await;
}

/// A `DeleteTable` of a table with eight indexes, and, once its base data
/// table is gone and while it drops the index tables, an
/// `UpdateContinuousBackups` (the storage call the engine makes; nothing
/// refuses it during `DELETING`, as the service serves it) and a tag written
/// at the storage layer, below the engine's validation. The deletes keyed on
/// the name run after the drops, so both rows go with the table, and a table
/// created into the name afterwards has no PITR row and no tags.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pitr_and_tags_written_during_the_drops_go_with_the_table() {
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
                .update_continuous_backups(&id, name, true)
                .await
                .expect("UpdateContinuousBackups is served during DELETING");
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
                pitr_row(&engine, &id, name).await,
                None,
                "the new table has no PITR row"
            );
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

/// The order of a `DeleteTable`: the deletes keyed on the name run after the
/// drops. Eight rounds, each a tagged, PITR-enabled table with one index;
/// while the call runs, the moment its base data table is gone the table's
/// tag rows and its continuous-backup row are still there (the index table is
/// dropped between the two), and once the call returns both are gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_name_keyed_rows_outlive_the_drops_and_go_with_the_row() {
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
                engine
                    .update_continuous_backups(&id, &name, true)
                    .await
                    .expect("enable PITR");
                let call = {
                    let (engine, id, name) = (engine.clone(), id.clone(), name.clone());
                    tokio::spawn(async move { engine.delete_table(&id, delete_input(&name)).await })
                };
                wait_for_data_table_gone(&engine, &id, &table.table_id).await;
                let during = (
                    tag_rows(&engine, &table.table_arn).await,
                    pitr_row(&engine, &id, &name).await,
                );
                call.await.expect("task").expect("DeleteTable");
                let after = (
                    tag_rows(&engine, &table.table_arn).await,
                    pitr_row(&engine, &id, &name).await,
                );
                assert_eq!(
                    during,
                    (1, Some(true)),
                    "round {round}: the tag and PITR rows are there once the data table is gone"
                );
                assert_eq!(after, (0, None), "round {round}: both go with the table");
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
