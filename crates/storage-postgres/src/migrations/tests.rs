// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the PostgreSQL migration table that need no database.

use extenddb_storage::migrations::{
    Body, MigrationId, RELEASED_THROUGH, StoreKind, check_released,
};

use super::{RELEASED, RunsIn, migration, runs_in};

#[test]
fn the_released_bodies_match_their_checksums() {
    let lines = check_released("postgres", migration, RELEASED, RELEASED_THROUGH)
        .unwrap_or_else(|e| panic!("{e}"));
    println!("{lines}");
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
        migration(MigrationId::IndexTableBaseKey).store_kinds(),
        [StoreKind::Catalog, StoreKind::Data]
    );
}

#[test]
fn only_the_index_builds_run_outside_a_transaction() {
    for id in MigrationId::ALL {
        let autocommit = runs_in(id) == RunsIn::Autocommit;
        assert_eq!(autocommit, id == MigrationId::IndexTableBaseKey, "{id}");
        if autocommit {
            // A body outside a transaction is code that uses the index helper.
            for body in migration(id).bodies() {
                assert!(matches!(body.body, Body::Code { .. }), "{id}");
            }
        }
    }
}

#[test]
fn the_baseline_files_keep_the_legacy_index_names_and_guards() {
    let baseline = migration(MigrationId::Baseline);
    let Body::File { text, .. } = &baseline.bodies()[0].body else {
        panic!("migration 1's catalog body is a file");
    };
    // A release before 1.0 that replays legacy 001 stops at its first index,
    // before it changes anything, only while the names stay the same.
    for name in [
        "idx_tables_pending_transition",
        "idx_stream_shards_table",
        "idx_stream_records_created",
        "idx_idempotency_tokens_created",
        "idx_metrics_bucket",
        "idx_login_attempts_principal_time",
        "idx_login_attempts_source_ip_time",
        "idx_backups_table",
        "idx_backup_items_arn",
    ] {
        assert!(
            text.contains(&format!("CREATE INDEX IF NOT EXISTS {name}")),
            "{name}"
        );
    }
    let unguarded = text
        .lines()
        .filter(|l| {
            let l = l.trim_start();
            (l.starts_with("CREATE INDEX ") || l.starts_with("CREATE UNIQUE INDEX "))
                && !l.contains("IF NOT EXISTS")
        })
        .count();
    assert_eq!(unguarded, 0);
    for column in [
        "table_class TEXT",
        "sse_specification JSONB",
        "on_demand_throughput JSONB",
    ] {
        assert!(
            text.contains(&format!(
                "ALTER TABLE tables ADD COLUMN IF NOT EXISTS {column}"
            )),
            "{column}"
        );
    }
}
