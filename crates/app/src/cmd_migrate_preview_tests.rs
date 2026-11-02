// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The preview's exit status and that it changes nothing (it used to
//! exit 1 with "Error: --yes is required", which stops a
//! scripted upgrade under `set -e`).
use std::sync::Mutex;

use super::{ResourceTagStep, StepFuture};

use async_trait::async_trait;
use extenddb_storage::bootstrapper::{AdminBootstrapResult, Bootstrapper, KeyDefinitionRepair};
use extenddb_storage::management_store::{OpError, OpResult};

use super::{EXIT_PENDING, MigrateArgs, apply_migrations, op_err, storage_err};

struct Fake {
    version: Mutex<Option<String>>,
    data_pending: Mutex<Vec<String>>,
    catalog_runs: Mutex<u32>,
    data_runs: Mutex<u32>,
}

impl Fake {
    fn new(version: Option<&str>, data_pending: &[&str]) -> Self {
        Self {
            version: Mutex::new(version.map(str::to_owned)),
            data_pending: Mutex::new(data_pending.iter().map(|s| (*s).to_owned()).collect()),
            catalog_runs: Mutex::new(0),
            data_runs: Mutex::new(0),
        }
    }
    fn runs(&self) -> (u32, u32) {
        (
            *self.catalog_runs.lock().unwrap(),
            *self.data_runs.lock().unwrap(),
        )
    }
}

fn unused<T>() -> OpResult<T> {
    Err(OpError::Internal("not used by migrate".to_owned()))
}

#[async_trait]
impl Bootstrapper for Fake {
    async fn ensure_app_user(&self) -> OpResult<()> {
        unused()
    }
    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        unused()
    }
    async fn create_catalog_db(&self) -> OpResult<()> {
        unused()
    }
    async fn create_data_db(&self) -> OpResult<()> {
        unused()
    }
    async fn run_catalog_migrations(&self) -> OpResult<()> {
        *self.catalog_runs.lock().unwrap() += 1;
        *self.version.lock().unwrap() = Some(self.expected_catalog_version());
        Ok(())
    }
    async fn run_data_migrations(&self) -> OpResult<()> {
        *self.data_runs.lock().unwrap() += 1;
        self.data_pending.lock().unwrap().clear();
        Ok(())
    }
    async fn repair_lost_sort_key_definitions(
        &self,
        _apply: bool,
    ) -> OpResult<KeyDefinitionRepair> {
        Ok(KeyDefinitionRepair::default())
    }
    async fn pending_data_migrations(&self) -> OpResult<Vec<String>> {
        Ok(self.data_pending.lock().unwrap().clone())
    }
    async fn record_data_connection(&self) -> OpResult<()> {
        unused()
    }
    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        unused()
    }
    async fn bootstrap_default_account(&self) -> OpResult<()> {
        unused()
    }
    async fn bootstrap_admin_user(
        &self,
        _env_user: Option<&str>,
        _env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        unused()
    }
    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        Ok(true)
    }
    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        unused()
    }
    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        unused()
    }
    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        unused()
    }
    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        Ok(self.version.lock().unwrap().clone())
    }
    fn expected_catalog_version(&self) -> String {
        "0.0.4".to_owned()
    }
    fn catalog_database_name(&self) -> String {
        "fake".to_owned()
    }
    fn endpoint_info(&self) -> String {
        "fake".to_owned()
    }
    fn catalog_connection_url(&self) -> String {
        "fake://".to_owned()
    }
    fn generate_backend_config_section(&self) -> String {
        String::new()
    }
}

/// A catalog whose policies need no resource-tag rewrite and that already
/// records the completion marker.
struct NoResourceTags;

impl ResourceTagStep for NoResourceTags {
    fn detect(
        &self,
    ) -> StepFuture<'_, anyhow::Result<(extenddb_storage::policy_migration::MigrationReport, bool)>>
    {
        Box::pin(async { Ok((Default::default(), true)) })
    }
    fn rewrite(&self) -> StepFuture<'_, extenddb_storage::policy_migration::MigrationReport> {
        Box::pin(async { Default::default() })
    }
    fn record_marker(&self) -> StepFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

fn args(yes: bool) -> MigrateArgs {
    MigrateArgs {
        config: None,
        pg_user: None,
        pg_pass: None,
        data_db: None,
        cassandra_user: None,
        cassandra_pass: None,
        yes,
        list: false,
        reconverge: false,
        forget: None,
        lock_timeout: None,
    }
}

#[tokio::test]
async fn a_preview_with_pending_work_exits_3_and_changes_nothing() {
    for (version, data) in [
        (Some("0.0.3"), &[][..]),
        (Some("0.0.4"), &["rebuild backup_items (2 rows)"][..]),
        (None, &["004_x.sql"][..]),
    ] {
        let fake = Fake::new(version, data);
        let code = apply_migrations(&fake, &args(false), &NoResourceTags)
            .await
            .expect("preview");
        assert_eq!(code, EXIT_PENDING, "{version:?} {data:?}");
        assert_eq!(fake.runs(), (0, 0), "the preview applied something");
        assert_eq!(
            fake.read_catalog_version().await.unwrap().as_deref(),
            version
        );
    }
}

#[tokio::test]
async fn up_to_date_exits_0_with_or_without_yes() {
    for yes in [false, true] {
        let fake = Fake::new(Some("0.0.4"), &[]);
        assert_eq!(
            apply_migrations(&fake, &args(yes), &NoResourceTags)
                .await
                .unwrap(),
            0
        );
        assert_eq!(fake.runs(), (0, 0));
    }
}

#[tokio::test]
async fn yes_applies_and_exits_0_and_a_second_preview_is_up_to_date() {
    let fake = Fake::new(Some("0.0.3"), &["rebuild backup_items (2 rows)"]);
    assert_eq!(
        apply_migrations(&fake, &args(true), &NoResourceTags)
            .await
            .unwrap(),
        0
    );
    assert_eq!(fake.runs(), (1, 1));
    assert_eq!(
        apply_migrations(&fake, &args(false), &NoResourceTags)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn a_newer_catalog_is_an_error_not_a_preview() {
    let fake = Fake::new(Some("0.0.5"), &[]);
    let err = apply_migrations(&fake, &args(false), &NoResourceTags)
        .await
        .expect_err("newer");
    assert!(err.to_string().contains("does not downgrade"), "{err}");
    assert_eq!(fake.runs(), (0, 0));
}

#[test]
fn errors_print_their_message_without_the_variant_name() {
    let shown = format!(
        "{:?}",
        op_err(OpError::Internal("Cannot connect to PostgreSQL".to_owned()))
    );
    assert_eq!(shown, "Cannot connect to PostgreSQL");
    let shown = format!(
        "{:?}",
        storage_err(extenddb_storage::error::StorageError::Internal(
            "Cannot connect to PostgreSQL".to_owned()
        ))
    );
    assert_eq!(shown, "Cannot connect to PostgreSQL");
}
