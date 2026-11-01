// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `init` under the migration lock: a store that is not a catalog is created
//! through its migrations, a catalog whose migrations are applied and that
//! has no admin user is finished with the rows it lacks, and a catalog with
//! an admin user is refused.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use extenddb_storage::bootstrapper::{AdminBootstrapResult, Bootstrapper};
use extenddb_storage::management_store::OpResult;
use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, HeldLock, Kind, LedgerRow, Listed, MigrationError,
    MigrationId, Pending, RunKind, RunReport, StoreKind, Target,
};

use super::init_on_runner;

type Log = Arc<Mutex<Vec<String>>>;

fn record(log: &Log, entry: &str) {
    log.lock().unwrap().push(entry.to_owned());
}

struct FakeMigrations {
    status: CatalogStatus,
    log: Log,
}

#[async_trait]
impl CatalogMigrations for FakeMigrations {
    fn listed(&self, _id: MigrationId) -> Listed {
        Listed::NotApplicable("fake")
    }
    fn backup_advice(&self, target: &Target) -> String {
        format!("back up {}", target.location)
    }
    async fn status(&self, _target: &Target) -> Result<CatalogStatus, MigrationError> {
        record(&self.log, "status");
        Ok(self.status.clone())
    }
    async fn catalog_ledger(&self, _: &Target) -> Result<Vec<LedgerRow>, MigrationError> {
        Ok(Vec::new())
    }
    async fn lock(&self, target: &Target, _: Duration) -> Result<HeldLock, MigrationError> {
        record(&self.log, "lock");
        Ok(HeldLock::new(target.location.as_str(), ()))
    }
    async fn run(
        &self,
        _lock: &HeldLock,
        _release: &str,
        kind: RunKind,
    ) -> Result<RunReport, MigrationError> {
        assert_eq!(kind, RunKind::Create);
        record(&self.log, "run the migrations");
        Ok(RunReport::default())
    }
    async fn forget(&self, _lock: &HeldLock, _id: u32) -> Result<(), MigrationError> {
        unreachable!("init forgets nothing")
    }
    async fn release(&self, _lock: HeldLock) {
        record(&self.log, "release");
    }
}

struct FakeBootstrapper {
    admin: bool,
    log: Log,
}

#[async_trait]
impl Bootstrapper for FakeBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        Ok(())
    }
    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        Ok(())
    }
    async fn create_catalog_db(&self) -> OpResult<()> {
        Ok(())
    }
    async fn create_data_db(&self) -> OpResult<()> {
        Ok(())
    }
    async fn record_data_connection(&self) -> OpResult<()> {
        record(&self.log, "data connection");
        Ok(())
    }
    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        record(&self.log, "encryption key");
        Ok(())
    }
    async fn bootstrap_default_account(&self) -> OpResult<()> {
        record(&self.log, "default account");
        Ok(())
    }
    async fn bootstrap_admin_user(
        &self,
        _env_user: Option<&str>,
        _env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        record(&self.log, "admin user");
        Ok(AdminBootstrapResult {
            username: "admin".to_owned(),
            generated_password: Some("generated".to_owned()),
            already_existed: false,
            from_env: false,
        })
    }
    async fn has_admin_user(&self) -> OpResult<bool> {
        record(&self.log, "has an admin user?");
        Ok(self.admin)
    }
    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        Ok(Vec::new())
    }
    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        Ok(None)
    }
    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        unreachable!("init drops nothing")
    }
    fn catalog_database_name(&self) -> String {
        "fake".to_owned()
    }
    fn endpoint_info(&self) -> String {
        "fake".to_owned()
    }
    fn catalog_connection_url(&self) -> String {
        "fake://catalog".to_owned()
    }
    fn hold(&self, _lock: &HeldLock) {
        record(&self.log, "hold");
    }
    fn generate_backend_config_section(&self) -> String {
        String::new()
    }
}

/// `init_on_runner` on a catalog of `status` whose admin users `admin`
/// says exist: its result and every call it made, in order.
async fn init(status: CatalogStatus, admin: bool) -> (anyhow::Result<String>, Vec<String>) {
    let log = Log::default();
    let migrations = FakeMigrations {
        status,
        log: Arc::clone(&log),
    };
    let bootstrapper = FakeBootstrapper {
        admin,
        log: Arc::clone(&log),
    };
    let result = init_on_runner(&bootstrapper, &migrations, "1.0.0-test")
        .await
        .map(|admin| admin.username);
    let calls = log.lock().unwrap().clone();
    (result, calls)
}

const ROWS: [&str; 5] = [
    "hold",
    "encryption key",
    "default account",
    "admin user",
    "data connection",
];

fn calls(first: &[&str], last: &[&str]) -> Vec<String> {
    first.iter().chain(last).map(|c| (*c).to_owned()).collect()
}

#[tokio::test]
async fn a_store_that_is_not_a_catalog_is_created_then_given_its_rows() {
    let (result, log) = init(CatalogStatus::Uninitialized, false).await;
    assert_eq!(result.expect("init"), "admin");
    let mut expected = calls(&["lock", "status", "run the migrations"], &ROWS);
    expected.push("release".to_owned());
    assert_eq!(log, expected);
}

/// What an `init` stopped after its migrations leaves: every migration is
/// recorded and there is no admin user. `init` runs no migration and writes
/// the rows, each of which the backend writes only when it is missing.
#[tokio::test]
async fn a_current_catalog_without_an_admin_user_is_finished_without_a_migration() {
    let current = CatalogStatus::Current {
        version: MigrationId::LATEST.number(),
    };
    let (result, log) = init(current, false).await;
    assert_eq!(result.expect("init"), "admin");
    let mut expected = calls(&["lock", "status", "has an admin user?"], &ROWS);
    expected.push("release".to_owned());
    assert_eq!(log, expected);
}

/// A catalog with an admin user, and one whose migrations are not all
/// applied, are refused as initialized, with nothing run or written and the
/// lock released.
#[tokio::test]
async fn a_catalog_with_an_admin_user_or_pending_migrations_is_refused() {
    let current = CatalogStatus::Current {
        version: MigrationId::LATEST.number(),
    };
    let (result, log) = init(current, true).await;
    let message = result.expect_err("refused").to_string();
    assert_eq!(
        message,
        format!(
            "the catalog is already initialized (the catalog is at migration {}); use \
             'extenddb migrate' for pending migrations",
            MigrationId::LATEST.number()
        )
    );
    assert_eq!(
        log,
        calls(&["lock", "status", "has an admin user?"], &["release"])
    );

    let behind = CatalogStatus::Behind {
        version: Some(1),
        pending: vec![Pending {
            id: 2,
            name: "resource_tag_condition_keys",
            store: StoreKind::Catalog,
            store_name: "fake".to_owned(),
            kind: Kind::Breaking,
        }],
    };
    let (result, log) = init(behind, false).await;
    let message = result.expect_err("refused").to_string();
    assert!(message.contains("already initialized"), "{message}");
    assert_eq!(log, calls(&["lock", "status"], &["release"]));
}
