// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The startup policy on a backend that records what it is asked: which
//! statuses serve, refuse or migrate, when the lock is taken, and the lines
//! a run prints.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use super::{Prepared, Startup, prepare_to_serve};
use crate::migrations::{
    AppliedStep, CatalogMigrations, CatalogStatus, HeldLock, Kind, LedgerRow, Listed,
    MigrationError, MigrationId, Pending, RunKind, RunReport, StoreKind, Target, exit,
};

struct Fake {
    /// The statuses the next reads return, the last one repeated.
    statuses: Mutex<VecDeque<CatalogStatus>>,
    lock: Result<(), MigrationError>,
    report: RunReport,
    run_takes: Duration,
    in_process: bool,
    calls: Mutex<Vec<&'static str>>,
}

impl Fake {
    fn new(statuses: &[CatalogStatus]) -> Self {
        Self {
            statuses: Mutex::new(statuses.iter().cloned().collect()),
            lock: Ok(()),
            report: applied_report(),
            run_takes: Duration::ZERO,
            in_process: false,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }

    fn call(&self, name: &'static str) {
        self.calls.lock().unwrap().push(name);
    }
}

#[async_trait]
impl CatalogMigrations for Fake {
    fn listed(&self, _id: MigrationId) -> Listed {
        Listed::NotApplicable("fake")
    }
    fn backup_advice(&self, target: &Target) -> String {
        format!("back up {}", target.location)
    }
    fn in_process(&self, _target: &Target) -> bool {
        self.in_process
    }
    async fn status(&self, _target: &Target) -> Result<CatalogStatus, MigrationError> {
        self.call("status");
        let mut statuses = self.statuses.lock().unwrap();
        let status = statuses.front().cloned().expect("a status");
        if statuses.len() > 1 {
            statuses.pop_front();
        }
        Ok(status)
    }
    async fn catalog_ledger(&self, _: &Target) -> Result<Vec<LedgerRow>, MigrationError> {
        Ok(Vec::new())
    }
    async fn lock(&self, target: &Target, _: Duration) -> Result<HeldLock, MigrationError> {
        self.call("lock");
        self.lock
            .clone()
            .map(|()| HeldLock::new(target.location.as_str(), ()))
    }
    async fn run(
        &self,
        _lock: &HeldLock,
        _release: &str,
        kind: RunKind,
    ) -> Result<RunReport, MigrationError> {
        assert_eq!(kind, RunKind::Migrate);
        self.call("run");
        tokio::time::sleep(self.run_takes).await;
        Ok(self.report.clone())
    }
    async fn forget(&self, _lock: &HeldLock, _id: u32) -> Result<(), MigrationError> {
        unreachable!("a server forgets nothing")
    }
    async fn release(&self, lock: HeldLock) {
        self.call("release");
        drop(lock);
    }
}

fn behind() -> CatalogStatus {
    CatalogStatus::Behind {
        version: Some(1),
        pending: MigrationId::ALL
            .iter()
            .skip(1)
            .map(|id| Pending {
                id: id.number(),
                name: id.name(),
                store: StoreKind::Catalog,
                store_name: "db".to_owned(),
                kind: Kind::Breaking,
            })
            .collect(),
    }
}

fn current() -> CatalogStatus {
    CatalogStatus::Current {
        version: MigrationId::LATEST.number(),
    }
}

fn applied_report() -> RunReport {
    RunReport {
        from: Some(1),
        to: Some(MigrationId::LATEST.number()),
        applied: MigrationId::ALL
            .iter()
            .skip(1)
            .map(|id| AppliedStep {
                id: id.number(),
                name: id.name(),
                store_name: "db".to_owned(),
                notes: Vec::new(),
            })
            .collect(),
        notes: vec!["copied the database to db.pre-migration-1-to-4-x".to_owned()],
    }
}

const fn startup(apply: bool) -> Startup<'static> {
    Startup {
        apply,
        creates_uninitialized: false,
        release: "1.0.0-test",
        lock_timeout: Duration::from_secs(1),
        heartbeat: Duration::from_millis(50),
    }
}

/// Run the policy, returning its outcome and the lines it reported.
async fn prepare(
    fake: &Fake,
    startup: &Startup<'_>,
) -> (Result<Prepared, MigrationError>, Vec<String>) {
    let lines = Mutex::new(Vec::new());
    let result = prepare_to_serve(fake, &Target::at("db"), startup, &|line| {
        lines.lock().unwrap().push(line.to_owned());
    })
    .await;
    (result, lines.into_inner().unwrap())
}

#[tokio::test]
async fn refuse_stops_a_catalog_behind_with_exit_3_and_takes_no_lock() {
    let fake = Fake::new(&[behind()]);
    let (result, lines) = prepare(&fake, &startup(false)).await;
    let error = result.expect_err("refused");
    assert_eq!(error, MigrationError::Status(behind()));
    assert_eq!(error.exit_code(), exit::PENDING);
    assert_eq!(fake.calls(), ["status"]);
    assert!(lines.is_empty(), "{lines:?}");
}

/// Under `apply` the run is `migrate --yes`'s: the lock, the status read
/// again under it, the run, and the release.
#[tokio::test]
async fn apply_migrates_a_catalog_behind_under_the_lock_and_releases_it() {
    let fake = Fake::new(&[behind()]);
    let (result, lines) = prepare(&fake, &startup(true)).await;
    let prepared = result.expect("applied");
    assert_eq!(prepared.run, Some(applied_report()));
    assert_eq!(prepared.warning, None);
    assert_eq!(fake.calls(), ["status", "lock", "status", "run", "release"]);
    let latest = MigrationId::LATEST.number();
    assert_eq!(
        lines[0],
        format!(
            "the catalog is at migration 1 and this binary needs migration {latest}; applying \
             the pending migrations before the server starts"
        )
    );
    assert!(
        lines[1].starts_with("Pending (all breaking): 2 "),
        "{lines:?}"
    );
    assert_eq!(
        lines[2],
        "taking the migration lock, waiting up to 1 s for a server or another migration"
    );
    assert_eq!(lines[3], "copied the database to db.pre-migration-1-to-4-x");
    assert_eq!(lines[4], "applied 2 resource_tag_condition_keys");
    assert_eq!(
        lines.last().map(String::as_str),
        Some(format!("the catalog is at migration {latest}").as_str())
    );
}

#[tokio::test]
async fn a_current_catalog_serves_with_no_lock_and_no_run() {
    for apply in [false, true] {
        let fake = Fake::new(&[current()]);
        let (result, lines) = prepare(&fake, &startup(apply)).await;
        assert_eq!(result, Ok(Prepared::default()));
        assert_eq!(fake.calls(), ["status"]);
        assert!(lines.is_empty(), "{lines:?}");
    }
}

#[tokio::test]
async fn a_catalog_ahead_by_additive_migrations_serves_with_a_warning() {
    let ahead = CatalogStatus::AheadCompatible {
        version: MigrationId::LATEST.number() + 1,
        unknown: vec![(MigrationId::LATEST.number() + 1, "later".to_owned())],
    };
    for apply in [false, true] {
        let fake = Fake::new(&[ahead.clone()]);
        let (result, _) = prepare(&fake, &startup(apply)).await;
        let prepared = result.expect("served");
        assert_eq!(prepared.run, None);
        let warning = prepared.warning.expect("a warning");
        assert!(warning.contains("additive"), "{warning}");
        assert_eq!(fake.calls(), ["status"]);
    }
}

/// No setting serves a catalog this binary cannot use: under `apply` every
/// status but `Behind` is refused as under `refuse`, before any lock.
#[tokio::test]
async fn apply_refuses_every_other_status_with_its_code_and_takes_no_lock() {
    let cases = [
        (
            CatalogStatus::NeedsAttention {
                items: vec!["migration 2: a collision".to_owned()],
            },
            exit::ATTENTION,
        ),
        (
            CatalogStatus::Changed {
                id: 1,
                store_name: "db".to_owned(),
                recorded: "a".to_owned(),
                expected: "b".to_owned(),
            },
            exit::ATTENTION,
        ),
        (
            CatalogStatus::WrittenByOlderRelease {
                trace: "catalog_version is 0.0.4".to_owned(),
            },
            exit::ATTENTION,
        ),
        (
            CatalogStatus::AheadIncompatible {
                unknown: vec![(MigrationId::LATEST.number() + 1, "later".to_owned())],
            },
            exit::AHEAD_INCOMPATIBLE,
        ),
        (CatalogStatus::Uninitialized, exit::ERROR),
    ];
    for (status, code) in cases {
        for apply in [false, true] {
            let fake = Fake::new(&[status.clone()]);
            let (result, lines) = prepare(&fake, &startup(apply)).await;
            let error = result.expect_err("refused");
            assert_eq!(error, MigrationError::Status(status.clone()));
            assert_eq!(error.exit_code(), code, "{status:?}");
            assert_eq!(fake.calls(), ["status"], "{status:?}");
            assert!(lines.is_empty(), "{lines:?}");
        }
    }
}

/// A dev build serves a store that is not a catalog yet, which its server
/// creates; a production build refuses it with exit 1.
#[tokio::test]
async fn a_dev_build_leaves_an_uninitialized_store_to_the_server() {
    let fake = Fake::new(&[CatalogStatus::Uninitialized]);
    let dev = Startup {
        creates_uninitialized: true,
        ..startup(false)
    };
    let (result, _) = prepare(&fake, &dev).await;
    assert_eq!(result, Ok(Prepared::default()));
    assert_eq!(fake.calls(), ["status"]);
}

/// An in-memory store is created by the server that opens it: nothing is
/// read before.
#[tokio::test]
async fn an_in_process_store_is_not_read() {
    let mut fake = Fake::new(&[behind()]);
    fake.in_process = true;
    let (result, _) = prepare(&fake, &startup(true)).await;
    assert_eq!(result, Ok(Prepared::default()));
    assert!(fake.calls().is_empty(), "{:?}", fake.calls());
}

/// A lock that stays held past the timeout stops the server with exit 6
/// while the catalog is still behind.
#[tokio::test]
async fn a_lock_held_past_the_timeout_exits_6() {
    let mut fake = Fake::new(&[behind()]);
    fake.lock = Err(MigrationError::Locked(
        "another migration holds it".to_owned(),
    ));
    let (result, _) = prepare(&fake, &startup(true)).await;
    let error = result.expect_err("locked");
    assert_eq!(error.exit_code(), exit::LOCKED);
    assert_eq!(error.to_string(), "another migration holds it");
    // The status is read again after the wait, and the backend ran nothing.
    assert_eq!(fake.calls(), ["status", "lock", "status"]);
}

/// Another process migrated the catalog while this one waited, and holds
/// what the lock waits for because it serves: the catalog needs nothing,
/// and the server starts as `serve` would.
#[tokio::test]
async fn a_catalog_migrated_by_another_process_during_the_wait_serves() {
    let mut fake = Fake::new(&[behind(), current()]);
    fake.lock = Err(MigrationError::Locked("a server serves it".to_owned()));
    let (result, lines) = prepare(&fake, &startup(true)).await;
    assert_eq!(result, Ok(Prepared::default()));
    assert_eq!(fake.calls(), ["status", "lock", "status"]);
    assert_eq!(
        lines.last().map(String::as_str),
        Some(super::MIGRATED_ELSEWHERE)
    );
}

/// The status read under the lock finds the catalog migrated: the run
/// applies nothing, and says so.
#[tokio::test]
async fn a_run_that_finds_the_catalog_migrated_under_the_lock_applies_nothing() {
    let mut fake = Fake::new(&[behind(), current()]);
    fake.report = RunReport {
        from: Some(MigrationId::LATEST.number()),
        to: Some(MigrationId::LATEST.number()),
        ..RunReport::default()
    };
    let (result, lines) = prepare(&fake, &startup(true)).await;
    let prepared = result.expect("served");
    assert!(prepared.run.expect("a run").applied.is_empty());
    assert_eq!(fake.calls(), ["status", "lock", "status", "run", "release"]);
    assert_eq!(
        lines.last().map(String::as_str),
        Some(super::MIGRATED_ELSEWHERE)
    );
}

/// While the run goes on, a line says so every heartbeat, which is what
/// keeps the npm launcher waiting through a long migration.
#[tokio::test]
async fn a_long_run_reports_that_it_is_in_progress_every_heartbeat() {
    let mut fake = Fake::new(&[behind()]);
    fake.run_takes = Duration::from_millis(400);
    let (result, lines) = prepare(&fake, &startup(true)).await;
    result.expect("applied");
    let beats = lines
        .iter()
        .filter(|l| l.starts_with("in progress for "))
        .count();
    // Eight at the 50 ms heartbeat; fewer only when the host is loaded.
    assert!(beats >= 4, "{beats} heartbeats: {lines:?}");
}
