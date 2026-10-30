// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What the check reports.

use std::collections::BTreeSet;

use extenddb_core::types::DeleteTableInput;
use extenddb_storage::TableEngine;
use extenddb_storage::diagnostics::{CatalogRelation, DiagError, DiagnosticsStore};
use extenddb_storage_postgres::{CatalogCheckPoint, arm_catalog_check_pause};

use crate::support::{ACCOUNT, Deployment, TABLE, base_conn, skip};

/// A table with a GSI, an LSI and a vector index has every relation the
/// engine created named by the catalog: nothing orphaned, nothing missing.
#[tokio::test]
async fn live_relations_are_neither_orphaned_nor_missing() {
    let test = "live_relations_are_neither_orphaned_nor_missing";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let relations = d.storage_relations().await;
    let expected = if d.pgvector().await { 4 } else { 3 };
    assert_eq!(relations.len(), expected, "{relations:?}");

    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check");
    assert!(check.orphaned.is_empty(), "orphaned: {:?}", check.orphaned);
    assert!(check.missing.is_empty(), "missing: {:?}", check.missing);
}

/// A relation the catalog names and the data database lacks is reported
/// missing, with the index and table that own it; the relation that took
/// its place is reported orphaned.
#[tokio::test]
async fn a_relation_the_catalog_names_is_reported_missing() {
    let test = "a_relation_the_catalog_names_is_reported_missing";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let gsi = d.index_relation(TABLE, "gsi1").await;
    assert!(d.relation_exists(&gsi).await, "{gsi}");
    let elsewhere = format!("_ddb_{}", uuid::Uuid::new_v4());
    sqlx::query(&format!("ALTER TABLE \"{gsi}\" RENAME TO \"{elsewhere}\""))
        .execute(&d.data)
        .await
        .expect("rename the GSI relation");

    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check");
    assert_eq!(check.orphaned, vec![elsewhere.clone()]);
    assert_eq!(
        check.missing,
        vec![CatalogRelation {
            relation: gsi.clone(),
            owner: format!("index gsi1 of table {ACCOUNT}/{TABLE}"),
        }]
    );

    sqlx::query(&format!("ALTER TABLE \"{elsewhere}\" RENAME TO \"{gsi}\""))
        .execute(&d.data)
        .await
        .expect("rename it back");
    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check again");
    assert!(check.orphaned.is_empty(), "orphaned: {:?}", check.orphaned);
    assert!(check.missing.is_empty(), "missing: {:?}", check.missing);
}

/// Rows in every status name their relations: a table in CREATING, UPDATING
/// or DELETING and an index or vector index in CREATING or DELETING keep
/// their relations out of the orphan list.
#[tokio::test]
async fn rows_in_every_status_count_as_live() {
    let test = "rows_in_every_status_count_as_live";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let table_status = "UPDATE tables SET table_status = $1";
    let index_status = "UPDATE indexes SET index_status = $1";
    let vector_status = "UPDATE vector_indexes SET index_status = $1";

    for status in ["CREATING", "UPDATING", "DELETING"] {
        d.set_status(table_status, status).await;
        let check = d
            .diagnostics
            .check_storage_relations()
            .await
            .expect("check");
        assert!(check.orphaned.is_empty(), "{status}: {:?}", check.orphaned);
        assert!(check.missing.is_empty(), "{status}: {:?}", check.missing);
    }
    d.set_status(table_status, "ACTIVE").await;

    for status in ["CREATING", "DELETING"] {
        d.set_status(index_status, status).await;
        d.set_status(vector_status, status).await;
        let check = d
            .diagnostics
            .check_storage_relations()
            .await
            .expect("check");
        assert!(check.orphaned.is_empty(), "{status}: {:?}", check.orphaned);
        assert!(check.missing.is_empty(), "{status}: {:?}", check.missing);
    }
}

/// A row being created or deleted is not expected to have its relations:
/// a catalog-first path commits the row before it creates them, and a drop
/// may run before the row goes. With the relations gone, a table or index in
/// CREATING or DELETING is not reported missing; back in ACTIVE, the same
/// rows are.
#[tokio::test]
async fn rows_being_created_or_deleted_are_not_reported_missing() {
    let test = "rows_being_created_or_deleted_are_not_reported_missing";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let table_status = "UPDATE tables SET table_status = $1";
    let index_status = "UPDATE indexes SET index_status = $1";
    let vector_status = "UPDATE vector_indexes SET index_status = $1";
    let mut index_relations = vec![
        d.index_relation(TABLE, "gsi1").await,
        d.index_relation(TABLE, "lsi1").await,
    ];
    if d.pgvector().await {
        index_relations.push(d.vector_relation(TABLE, "vec1").await);
    }
    let all: BTreeSet<String> = d.storage_relations().await;

    // The index relations go while their rows are DELETING, the way a drop
    // runs before its row is removed.
    d.set_status(index_status, "DELETING").await;
    d.set_status(vector_status, "DELETING").await;
    for relation in &index_relations {
        d.remove_relation(relation).await;
    }
    for status in ["DELETING", "CREATING"] {
        d.set_status(index_status, status).await;
        d.set_status(vector_status, status).await;
        let check = d
            .diagnostics
            .check_storage_relations()
            .await
            .expect("check");
        assert!(
            check.missing.is_empty(),
            "index {status}: {:?}",
            check.missing
        );
        assert!(
            check.orphaned.is_empty(),
            "index {status}: {:?}",
            check.orphaned
        );
    }

    // Then the base relation, with the table DELETING.
    let base: Vec<String> = all
        .iter()
        .filter(|r| !index_relations.contains(r))
        .cloned()
        .collect();
    assert_eq!(base.len(), 1, "{all:?}");
    d.set_status(table_status, "DELETING").await;
    d.remove_relation(&base[0]).await;
    for status in ["DELETING", "CREATING"] {
        d.set_status(table_status, status).await;
        let check = d
            .diagnostics
            .check_storage_relations()
            .await
            .expect("check");
        assert!(
            check.missing.is_empty(),
            "table {status}: {:?}",
            check.missing
        );
    }

    // Settled again, every relation is missing.
    d.set_status(table_status, "ACTIVE").await;
    d.set_status(index_status, "ACTIVE").await;
    d.set_status(vector_status, "ACTIVE").await;
    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check");
    let missing: BTreeSet<String> = check.missing.into_iter().map(|m| m.relation).collect();
    assert_eq!(missing, all);
}

/// A catalog with no data database setting, or an empty one, is refused
/// with the message the command has always given, by the check and by the
/// drop: the relations could only be looked for in the catalog's own
/// database, a layout `init` does not create.
#[tokio::test]
async fn a_catalog_without_the_data_database_setting_is_refused() {
    let test = "a_catalog_without_the_data_database_setting_is_refused";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let orphan = d.plant_orphan().await;
    let refusal = "No data database connection string in settings. Run `extenddb init`.";

    for change in [
        "UPDATE settings SET value = '' WHERE key = 'data_database_connection_string'",
        "DELETE FROM settings WHERE key = 'data_database_connection_string'",
    ] {
        sqlx::query(change)
            .execute(&d.catalog)
            .await
            .expect("change the setting");
        match d.diagnostics.check_storage_relations().await {
            Err(DiagError::NotConfigured(msg)) => assert_eq!(msg, refusal, "{change}"),
            other => panic!("{change}: check gave {other:?}"),
        }
        match d
            .diagnostics
            .drop_orphaned_relations(vec![orphan.clone()])
            .await
        {
            Err(DiagError::NotConfigured(msg)) => assert_eq!(msg, refusal, "{change}"),
            other => panic!("{change}: drop gave {other:?}"),
        }
        assert!(
            d.relation_exists(&orphan).await,
            "{change}: nothing dropped"
        );
    }
}

/// Changes that land between the check's first catalog read and its second
/// listing are not reported: a table a `DeleteTable` removes (row first,
/// then relations) is not missing, a relation another session drops is not
/// orphaned, and a relation created after the first listing waits for the
/// next check.
#[tokio::test]
async fn changes_during_the_check_are_not_reported() {
    let test = "changes_during_the_check_are_not_reported";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let kept = d.storage_relations().await;
    d.create_table_with_indexes("doomed").await;
    let doomed: Vec<String> = d
        .storage_relations()
        .await
        .difference(&kept)
        .cloned()
        .collect();
    let gone = d.plant_orphan().await;

    let mut pause = arm_catalog_check_pause(CatalogCheckPoint::AfterCatalogRead);
    let reached = pause.take_reached();
    let mut check = d.diagnostics.check_storage_relations();
    tokio::select! {
        r = &mut check => panic!("the check finished while it should be paused: {r:?}"),
        r = reached => r.expect("the check reached the pause"),
    }
    d.engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: "doomed".to_owned(),
            },
        )
        .await
        .expect("delete the table");
    for relation in &doomed {
        assert!(!d.relation_exists(relation).await, "{relation} dropped");
    }
    d.remove_relation(&gone).await;
    let late = d.plant_orphan().await;
    pause.resume();
    let found = check.await.expect("check");
    drop(pause);
    assert!(found.missing.is_empty(), "missing: {:?}", found.missing);
    assert!(found.orphaned.is_empty(), "orphaned: {:?}", found.orphaned);

    let next = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check again");
    assert_eq!(next.orphaned, vec![late]);
    assert!(next.missing.is_empty(), "missing: {:?}", next.missing);
}
