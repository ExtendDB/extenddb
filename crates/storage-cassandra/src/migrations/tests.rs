// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the Cassandra migration table that need no database.

use extenddb_storage::migrations::{
    Body, Kind, Migration, MigrationId, RELEASED_THROUGH, StoreKind, check_released,
    refuse_additive,
};

use super::{ACCOUNT_BASELINE, CATALOG_BASELINE, RELEASED, migration};

#[test]
fn the_released_bodies_match_their_checksums() {
    let lines = check_released("cassandra", migration, RELEASED, RELEASED_THROUGH)
        .unwrap_or_else(|e| panic!("{e}"));
    println!("{lines}");
}

#[test]
fn no_migration_is_additive_before_the_old_binary_job() {
    refuse_additive("cassandra", migration).unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn every_migration_records_a_row_in_the_catalog_and_names_its_stores() {
    for id in MigrationId::ALL {
        let kinds = migration(id).store_kinds();
        assert_eq!(kinds.first(), Some(&StoreKind::Catalog), "{id}");
        assert!(!kinds.contains(&StoreKind::Data), "{id}");
    }
    assert_eq!(
        migration(MigrationId::Baseline).store_kinds(),
        [StoreKind::Catalog, StoreKind::Account]
    );
    assert_eq!(
        migration(MigrationId::ResourceTagConditionKeys).store_kinds(),
        [StoreKind::Catalog]
    );
    for id in [
        MigrationId::SortKeyDefinitions,
        MigrationId::IndexTableBaseKey,
    ] {
        assert!(matches!(migration(id), Migration::NotApplicable(_)), "{id}");
    }
    for id in MigrationId::ALL {
        if let Migration::Applies { kind, .. } = migration(id) {
            assert_eq!(kind, Kind::Breaking, "{id}");
        }
    }
}

#[test]
fn the_baselines_write_no_setting_except_deleting_the_marker() {
    for (store, text) in [("catalog", CATALOG_BASELINE), ("account", ACCOUNT_BASELINE)] {
        let statements = super::cql::statements("ks", store, text).expect(store);
        for statement in &statements {
            let upper = statement.text.to_ascii_uppercase();
            if upper.contains("KS.SETTINGS") && !upper.starts_with("CREATE TABLE") {
                assert_eq!(
                    statement.text, "DELETE FROM ks.settings WHERE key = 'resource_tag_migration'",
                    "{store}"
                );
            }
            if upper.starts_with("CREATE") {
                assert!(upper.contains("IF NOT EXISTS"), "{}", statement.text);
            }
            if upper.starts_with("ALTER TABLE") {
                assert!(upper.contains(" ADD IF NOT EXISTS "), "{}", statement.text);
            }
        }
    }
}

#[test]
fn the_baselines_record_every_legacy_file() {
    // A release before 1.0 runs no legacy file once every version it knows is
    // recorded in its ledger.
    for version in 1..=5 {
        assert!(
            CATALOG_BASELINE.contains(&format!(
                "INSERT INTO schema_history (version, description) VALUES ({version}, "
            )),
            "catalog {version}"
        );
    }
    for version in 1..=2 {
        assert!(
            ACCOUNT_BASELINE.contains(&format!(
                "INSERT INTO schema_history (version, description) VALUES ({version}, "
            )),
            "account {version}"
        );
    }
    let baseline = migration(MigrationId::Baseline);
    let Body::File { text, .. } = &baseline.bodies()[0].body else {
        panic!("migration 1's catalog body is a file");
    };
    assert!(text.contains("CREATE TABLE IF NOT EXISTS extenddb_servers"));
}

#[test]
fn a_statement_under_the_lock_is_bounded_inside_the_lease_margin() {
    // A statement the holder sent before its deadline is done before its
    // row can expire, so a second migrator never runs beside it (the
    // module header's rule). CreateAccount holds no lease and gets the
    // longer bound a loaded node needs.
    let times = super::LeaseTimes::DEFAULT;
    assert!(super::backend::ddl_bound(true) < times.margin);
    // It is under the stall too, since the runner records progress when a
    // statement completes, so a statement that takes the whole bound does
    // not stop the renewal.
    assert!(super::backend::ddl_bound(true) < times.stall);
    assert_eq!(
        super::backend::ddl_bound(true),
        super::connect::LOCKED_DDL_TIMEOUT
    );
    // Measured on one node under ten parallel migration tests: a table drop
    // took 14.8 s, which the request timeout cut and this bound covers.
    assert!(super::backend::ddl_bound(true) >= std::time::Duration::from_secs(15));
    assert!(super::backend::ddl_bound(true) > super::connect::REQUEST_TIMEOUT);
    assert_eq!(
        super::backend::ddl_bound(false),
        super::connect::UNLOCKED_DDL_TIMEOUT
    );
    assert!(super::backend::ddl_bound(false) > super::backend::ddl_bound(true));
}

#[test]
fn the_settings_table_is_created_with_the_bound_of_a_schema_statement_outside_the_lock() {
    // init and migrate create the settings table before the lock exists,
    // because the lock is a row of it, so the CREATE TABLE holds no lease.
    // Under the request timeout it failed on a node shared with the direct
    // integration suite: "create the settings table: no answer within 10 s".
    assert_eq!(
        super::SETTINGS_TABLE_BOUND,
        super::connect::UNLOCKED_DDL_TIMEOUT
    );
    assert!(super::SETTINGS_TABLE_BOUND > super::connect::REQUEST_TIMEOUT);
}
