// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb catalog-check` run as the binary, on the deployment that the
//! PostgreSQL storage tests of the check build (`init`'s layout: a catalog
//! database and a separate data database). What the command prints and its
//! exit code for an orphaned relation and a missing one, with and without
//! `--fix`, and its refusal of a catalog without the data database setting.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, as those tests do; without
//! it the test reports a skip and passes, and `EXTENDDB_TEST_PG_REQUIRED`
//! turns the skip into a failure.

#![cfg(feature = "postgres")]

use std::path::Path;
use std::process::{Command, Output, Stdio};

use sqlx::Connection;

#[path = "../../storage-postgres/tests/catalog_check/support.rs"]
#[allow(dead_code)]
mod support;

use support::{ACCOUNT, Deployment, TABLE, base_conn, skip};

fn catalog_check(config: &Path, fix: bool) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_extenddb"));
    command.arg("catalog-check").arg("--config").arg(config);
    if fix {
        command.arg("--fix");
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    command
}

fn assert_run(run: &str, output: &Output, stdout: &str) {
    let printed = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(printed, stdout, "{run}: stdout (stderr: {stderr})");
    assert_eq!(output.status.code(), Some(1), "{run}: exit status");
}

#[tokio::test]
async fn catalog_check_prints_each_finding_and_exits_with_status_1() {
    let test = "catalog_check_prints_each_finding_and_exits_with_status_1";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let orphan = d.plant_orphan().await;
    let gsi = d.index_relation(TABLE, "gsi1").await;
    d.remove_relation(&gsi).await;

    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("catalog-check-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).expect("a directory for the config");
    let config = dir.join("extenddb.toml");
    std::fs::write(
        &config,
        format!(
            "[server]\nrun_dir = {:?}\n\n[storage.postgres]\nconnection_string = {:?}\n",
            dir.join("run").display().to_string(),
            d.catalog_url()
        ),
    )
    .expect("write the config");

    let head = "=== extenddb catalog-check ===\nConnected to catalog.\n";
    let missing = format!(
        "--- Checking for missing data tables...\n  FOUND: 1 missing data table(s):\n    - \
         {gsi} (index gsi1 of table {ACCOUNT}/{TABLE})\n"
    );
    let catalog_checks = "--- Checking for orphaned index catalog entries...\n  OK: No orphaned \
                          index catalog entries.\n--- Checking for stuck transitions...\n  OK: No \
                          stuck transitions.\n";

    let fixed = catalog_check(&config, true)
        .output()
        .expect("run catalog-check --fix");
    assert_run(
        "--fix",
        &fixed,
        &format!(
            "{head}Compared data relations with the catalog.\n\n--- Checking for orphaned data \
             tables...\n  FOUND: 1 orphaned table(s):\n    - {orphan}\n  Cleaning up orphaned \
             tables...\n    Dropped: {orphan}\n{missing}{catalog_checks}\n=== 2 issue(s) found \
             ===\n"
        ),
    );
    assert!(!d.relation_exists(&orphan).await, "--fix dropped the orphan");

    // The report while a session holds a lock on a catalog table that the
    // check reads, for longer than a statement takes to count as slow.
    let mut holder = sqlx::PgConnection::connect(&d.catalog_url())
        .await
        .expect("a catalog session for the lock");
    sqlx::query("BEGIN")
        .execute(&mut holder)
        .await
        .expect("begin");
    sqlx::query("LOCK TABLE vector_indexes IN ACCESS EXCLUSIVE MODE")
        .execute(&mut holder)
        .await
        .expect("lock vector_indexes");
    let report = catalog_check(&config, false)
        .spawn()
        .expect("run catalog-check");
    Deployment::wait_for_lock_request(&d.catalog, "vector_indexes", "AccessShareLock").await;
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    holder.close().await.expect("release the lock");
    let report = report.wait_with_output().expect("catalog-check ended");
    assert_run(
        "report",
        &report,
        &format!(
            "{head}Compared data relations with the catalog.\n\n--- Checking for orphaned data \
             tables...\n  OK: No orphaned tables.\n{missing}{catalog_checks}\n=== 1 issue(s) \
             found ===\nRun with --fix to clean up orphaned data tables.\n"
        ),
    );

    sqlx::query("DELETE FROM settings WHERE key = 'data_database_connection_string'")
        .execute(&d.catalog)
        .await
        .expect("remove the setting");
    let refused = catalog_check(&config, false)
        .output()
        .expect("run catalog-check");
    assert_run("without the setting", &refused, head);
    assert_eq!(
        String::from_utf8_lossy(&refused.stderr).lines().last(),
        Some("Error: No data database connection string in settings. Run `extenddb init`."),
        "without the setting: stderr"
    );

    std::fs::remove_dir_all(&dir).expect("remove the config");
}
