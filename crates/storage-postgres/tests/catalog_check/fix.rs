// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What the drop drops and keeps, and what it waits for.

use std::time::{Duration, Instant};

use extenddb_storage::diagnostics::{DiagnosticsStore, OrphanDrop};
use extenddb_storage_postgres::PostgresCatalogStore;
use sqlx::Connection;

use crate::support::{ACCOUNT, Deployment, TABLE, base_conn, hold_relation, skip};

/// `--fix` drops the relation no catalog row names and nothing else: not the
/// live table's relations, not a table created after the check, not a table
/// without the backend's prefix. The live data is intact afterwards.
#[tokio::test]
async fn fix_drops_only_the_relation_no_catalog_row_names() {
    let test = "fix_drops_only_the_relation_no_catalog_row_names";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    let key_info = d.create_table_with_indexes(TABLE).await;
    let live = d.storage_relations().await;
    let orphan = d.plant_orphan().await;
    sqlx::query("CREATE TABLE operator_notes (note TEXT)")
        .execute(&d.data)
        .await
        .expect("an unrelated table");

    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check");
    assert_eq!(check.orphaned, vec![orphan.clone()]);
    assert!(check.missing.is_empty(), "missing: {:?}", check.missing);

    // A CreateTable that lands between the check and the drop: its relations
    // were not in the listing, and a stale candidate list naming them must
    // not get them dropped.
    d.create_table_with_indexes("later").await;
    let later: Vec<String> = d
        .storage_relations()
        .await
        .difference(&live)
        .filter(|r| **r != orphan)
        .cloned()
        .collect();
    assert_eq!(later.len(), if d.pgvector().await { 4 } else { 3 });
    let mut candidates = vec![orphan.clone(), "operator_notes".to_owned()];
    candidates.extend(later.iter().cloned());

    let outcomes = d
        .diagnostics
        .drop_orphaned_relations(candidates.clone())
        .await
        .expect("drop");
    let names: Vec<&String> = outcomes.iter().map(|(n, _)| n).collect();
    assert_eq!(names, candidates.iter().collect::<Vec<_>>());
    assert_eq!(outcomes[0].1, OrphanDrop::Dropped, "{outcomes:?}");
    assert_eq!(
        outcomes[1].1,
        OrphanDrop::Kept("not a data relation of this backend".to_owned()),
        "{outcomes:?}"
    );
    for (name, outcome) in &outcomes[2..] {
        match outcome {
            OrphanDrop::Kept(reason) => assert!(
                reason.starts_with("the catalog names it: ")
                    && reason.ends_with(&format!("table {ACCOUNT}/later")),
                "{name}: {reason}"
            ),
            other => panic!("{name}: {other:?}"),
        }
    }

    assert!(!d.relation_exists(&orphan).await, "the orphan was dropped");
    assert!(d.relation_exists("operator_notes").await);
    let after = d.storage_relations().await;
    assert!(live.is_subset(&after), "live relations survived: {after:?}");
    assert!(later.iter().all(|r| after.contains(r)), "{after:?}");
    d.assert_data_intact(&key_info).await;

    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check again");
    assert!(check.orphaned.is_empty(), "orphaned: {:?}", check.orphaned);
    assert!(check.missing.is_empty(), "missing: {:?}", check.missing);
}

/// An `UpdateTable` adding a GSI has inserted the index row and committed
/// the index's relation, and its catalog transaction is still open when the
/// drop is asked to drop that relation. The drop waits for the add; the add's
/// next write, to the table row, does not wait on the drop (the drop locks
/// only `indexes`); another write to `indexes` queues behind the drop's
/// request for one attempt at most; and once the add commits, its relation is
/// kept. Run at both the default isolation and REPEATABLE READ.
async fn an_index_added_during_the_drop_keeps_its_relation(
    store: &PostgresCatalogStore,
    d: &Deployment,
) {
    d.create_table_with_indexes(TABLE).await;
    let mut add = d.begin_index_add("gsi9").await;
    let relation = add.relation.clone();
    let five = Duration::from_secs(5);
    let (outcomes, (table_write, other_write)) = tokio::join!(
        store.drop_orphaned_relations(vec![relation.clone()]),
        async move {
            Deployment::wait_for_lock_request(&d.catalog, "indexes", "ShareLock").await;
            let table_write = tokio::time::timeout(five, add.write_table_row()).await;
            let started = Instant::now();
            let other_write = tokio::time::timeout(
                five,
                sqlx::query(
                    "UPDATE indexes SET index_status = index_status WHERE index_name = 'gsi1'",
                )
                .execute(&d.catalog),
            )
            .await
            .map(|r| r.map(|_| started.elapsed()));
            // Open past several attempts, so the drop has to try again.
            tokio::time::sleep(Duration::from_millis(600)).await;
            add.commit().await;
            (table_write, other_write)
        }
    );

    let table_write = table_write
        .expect("the add's table write did not wait out 5 s")
        .expect("the add's table write succeeded");
    assert!(
        table_write < Duration::from_millis(900),
        "the add's table write waited {table_write:?}, as long as a deadlock check"
    );
    let other_write = other_write
        .expect("the other index write did not wait out 5 s")
        .expect("the other index write succeeded");
    assert!(
        other_write < Duration::from_secs(1),
        "another index write waited {other_write:?} behind the drop"
    );
    assert_eq!(
        outcomes.expect("drop"),
        vec![(
            relation.clone(),
            OrphanDrop::Kept(format!(
                "the catalog names it: index gsi9 of table {ACCOUNT}/{TABLE}"
            ))
        )]
    );
    assert!(d.relation_exists(&relation).await, "{relation} kept");
}

#[tokio::test]
async fn an_index_added_during_the_drop_keeps_its_relation_at_the_default_isolation() {
    let test = "an_index_added_during_the_drop_keeps_its_relation_at_the_default_isolation";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    an_index_added_during_the_drop_keeps_its_relation(&d.diagnostics, &d).await;
}

#[tokio::test]
async fn an_index_added_during_the_drop_keeps_its_relation_under_repeatable_read() {
    let test = "an_index_added_during_the_drop_keeps_its_relation_under_repeatable_read";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    let store = d.diagnostics_at("repeatable read").await;
    an_index_added_during_the_drop_keeps_its_relation(&store, &d).await;
}

/// The same GSI add in flight while the check runs: its relation exists and
/// its row has not committed, so the check waits for the add before it reads
/// the catalog, and the relation is not reported orphaned.
#[tokio::test]
async fn an_index_added_during_the_check_is_not_reported_orphaned() {
    let test = "an_index_added_during_the_check_is_not_reported_orphaned";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let mut add = d.begin_index_add("gsi9").await;
    let d = &d;
    let (check, table_write) = tokio::join!(d.diagnostics.check_storage_relations(), async move {
        Deployment::wait_for_lock_request(&d.catalog, "indexes", "ShareLock").await;
        let table_write = add.write_table_row().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        add.commit().await;
        table_write
    });
    let table_write = table_write.expect("the add's table write succeeded");
    assert!(table_write < Duration::from_millis(900), "{table_write:?}");
    let check = check.expect("check");
    assert!(check.orphaned.is_empty(), "orphaned: {:?}", check.orphaned);
    assert!(check.missing.is_empty(), "missing: {:?}", check.missing);
}

/// A session holds the orphan the drop is asked to drop. The drop gives up
/// after its own lock timeout and reports the relation failed, and a
/// `CreateTable` sent while it waits completes without waiting on it.
#[tokio::test]
async fn a_held_orphan_fails_within_its_lock_timeout_and_catalog_writes_go_on() {
    let test = "a_held_orphan_fails_within_its_lock_timeout_and_catalog_writes_go_on";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let orphan = d.plant_orphan().await;
    let holder = hold_relation(&d, &orphan).await;

    let d = &d;
    let started = Instant::now();
    let ((outcomes, dropped_at), ()) = tokio::join!(
        async {
            let outcomes = d
                .diagnostics
                .drop_orphaned_relations(vec![orphan.clone()])
                .await;
            (outcomes, started.elapsed())
        },
        async {
            Deployment::wait_for_lock_request(&d.data, &orphan, "AccessExclusiveLock").await;
            // Before the change, the drop held SHARE on `tables` and `indexes`
            // until it finished, so this waited for the whole lock budget.
            tokio::time::timeout(
                Duration::from_secs(5),
                d.create_table_with_indexes("meanwhile"),
            )
            .await
            .expect("the CreateTable did not wait out 5 s");
        }
    );
    assert!(
        dropped_at < Duration::from_secs(5),
        "the drop took {dropped_at:?}"
    );
    let outcomes = outcomes.expect("drop");
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    match &outcomes[0] {
        (name, OrphanDrop::Failed(reason)) if *name == orphan => assert!(
            reason.starts_with("another session held a lock on it for more than 1000 ms"),
            "{reason}"
        ),
        other => panic!("{other:?}"),
    }
    assert!(d.relation_exists(&orphan).await, "the held orphan was kept");

    holder.close().await.expect("release the orphan");
    let outcomes = d
        .diagnostics
        .drop_orphaned_relations(vec![orphan.clone()])
        .await
        .expect("drop again");
    assert_eq!(outcomes, vec![(orphan, OrphanDrop::Dropped)]);
}

/// A relation another session dropped after the check reported it, as a
/// server's `DeleteTable` drops one, is reported as already gone, not as
/// dropped by the command.
#[tokio::test]
async fn a_relation_dropped_first_by_another_session_is_already_gone() {
    let test = "a_relation_dropped_first_by_another_session_is_already_gone";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    let orphan = d.plant_orphan().await;
    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check");
    assert_eq!(check.orphaned, vec![orphan.clone()]);
    d.remove_relation(&orphan).await;
    let outcomes = d
        .diagnostics
        .drop_orphaned_relations(check.orphaned)
        .await
        .expect("drop");
    assert_eq!(outcomes, vec![(orphan, OrphanDrop::AlreadyGone)]);
}
