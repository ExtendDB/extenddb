// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the MongoDB migration table that need no database.

use extenddb_storage::migrations::{
    Body, Migration, MigrationId, RELEASED_THROUGH, StoreKind, check_released, refuse_additive,
};

use super::{RELEASED, migration};

#[test]
fn the_released_bodies_match_their_checksums() {
    let lines = check_released("mongodb", migration, RELEASED, RELEASED_THROUGH)
        .unwrap_or_else(|e| panic!("{e}"));
    println!("{lines}");
}

#[test]
fn no_migration_is_declared_additive_before_the_old_binary_job_exists() {
    refuse_additive("mongodb", migration).unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn every_migration_records_a_row_in_the_catalog_and_names_its_stores() {
    for id in MigrationId::ALL {
        let kinds = migration(id).store_kinds();
        assert_eq!(kinds.first(), Some(&StoreKind::Catalog), "{id}");
        assert!(!kinds.contains(&StoreKind::Account), "{id}");
    }
    assert_eq!(
        migration(MigrationId::Baseline).store_kinds(),
        [StoreKind::Catalog, StoreKind::Data]
    );
    assert_eq!(
        migration(MigrationId::ResourceTagConditionKeys).store_kinds(),
        [StoreKind::Catalog]
    );
}

#[test]
fn every_body_is_code_and_three_and_four_do_not_apply() {
    for id in MigrationId::ALL {
        for body in migration(id).bodies() {
            assert!(matches!(body.body, Body::Code { .. }), "{id}");
        }
    }
    for id in [
        MigrationId::SortKeyDefinitions,
        MigrationId::IndexTableBaseKey,
    ] {
        let Migration::NotApplicable(reason) = migration(id) else {
            panic!("{id} applies on MongoDB");
        };
        assert!(!reason.is_empty(), "{id}");
    }
    assert!(migration(MigrationId::Baseline).convergent());
    assert!(migration(MigrationId::ResourceTagConditionKeys).convergent());
}

/// The lock, the registrations and the ledgers are read and written through
/// this handle: primary reads, and write concern majority whatever the
/// connection string says.
#[tokio::test]
async fn the_runner_reads_the_primary_and_writes_with_write_concern_majority() {
    use mongodb::options::{Acknowledgment, ReadPreference, SelectionCriteria};
    // The client connects lazily, so no server is contacted.
    let client = mongodb::Client::with_uri_str("mongodb://127.0.0.1:1/?directConnection=true&w=1")
        .await
        .expect("client");
    let db = super::catalog_handle(&client, "any");
    assert_eq!(
        db.write_concern().and_then(|w| w.w.clone()),
        Some(Acknowledgment::Majority)
    );
    assert!(matches!(
        db.selection_criteria(),
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary))
    ));
}
