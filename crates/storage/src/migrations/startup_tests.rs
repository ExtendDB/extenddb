// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The startup policy on a backend that records what it is asked: which
//! statuses serve, refuse or migrate, when the lock is taken, and the lines
//! a run prints.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use super::{MIGRATED_ELSEWHERE, Prepared, READING, Startup, prepare_to_serve};
use crate::migrations::{
    AppliedStep, CatalogMigrations, CatalogStatus, HeldLock, Kind, LedgerRow, Listed,
    MigrationError, MigrationId, Pending, RunKind, RunReport, StatusNote, StoreKind, Target, exit,
};

struct Fake {
    /// The statuses the next reads return, the last one repeated.
    statuses: Mutex<VecDeque<CatalogStatus>>,
    lock: Result<(), MigrationError>,
    report: RunReport,
    run_takes: Duration,
    in_process: bool,
    /// What `server_creates_catalog` answers.
    creates: bool,
    notes: Vec<StatusNote>,
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
            creates: true,
            notes: Vec::new(),
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
    async fn server_creates_catalog(&self, _target: &Target) -> Result<bool, MigrationError> {
        self.call("creates");
        Ok(self.creates)
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
    async fn status_notes(&self, _target: &Target) -> Result<Vec<StatusNote>, MigrationError> {
        self.call("notes");
        Ok(self.notes.clone())
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

/// A catalog a newer binary migrated, ahead of this one by an additive
/// migration.
fn ahead() -> CatalogStatus {
    CatalogStatus::AheadCompatible {
        version: MigrationId::LATEST.number() + 1,
        unknown: vec![(MigrationId::LATEST.number() + 1, "later".to_owned())],
    }
}

/// The statuses no server serves under either value of the policy, with
/// their exit codes, apart from `Uninitialized`, which a dev build serves.
fn unservable() -> Vec<(CatalogStatus, u8)> {
    vec![
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
    ]
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
                outcome: crate::migrations::Outcome::Applied,
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

/// A dev build's policy: the server creates a catalog in a store that is
/// not one yet.
const fn dev(apply: bool) -> Startup<'static> {
    Startup {
        creates_uninitialized: true,
        ..startup(apply)
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
    assert_eq!(lines, [READING]);
}

/// Under `apply` the run is `migrate --yes`'s: the status notes, the lock,
/// the status read again under it, the run, and the release. A line comes
/// before the first status read, which runs `detect`.
#[tokio::test]
async fn apply_migrates_a_catalog_behind_under_the_lock_and_releases_it() {
    let fake = Fake::new(&[behind()]);
    let (result, lines) = prepare(&fake, &startup(true)).await;
    let prepared = result.expect("applied");
    assert_eq!(prepared.run, Some(applied_report()));
    assert_eq!(prepared.warning, None);
    assert_eq!(
        fake.calls(),
        ["status", "notes", "lock", "status", "run", "release"]
    );
    let latest = MigrationId::LATEST.number();
    assert_eq!(lines[0], READING);
    assert_eq!(
        lines[1],
        format!(
            "the catalog is at migration 1 and this binary needs migration {latest}; applying \
             the pending migrations before the server starts"
        )
    );
    assert!(
        lines[2].starts_with("Pending (all breaking): 2 "),
        "{lines:?}"
    );
    assert_eq!(
        lines[3],
        "taking the migration lock, waiting up to 1 s for a server or another migration"
    );
    assert_eq!(lines[4], "copied the database to db.pre-migration-1-to-4-x");
    assert_eq!(lines[5], "applied 2 resource_tag_condition_keys");
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
        assert_eq!(lines, [READING]);
    }
}

#[tokio::test]
async fn a_catalog_ahead_by_additive_migrations_serves_with_a_warning() {
    let ahead = ahead();
    for apply in [false, true] {
        let fake = Fake::new(std::slice::from_ref(&ahead));
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
    let mut cases = unservable();
    cases.push((CatalogStatus::Uninitialized, exit::ERROR));
    for (status, code) in cases {
        for apply in [false, true] {
            let fake = Fake::new(std::slice::from_ref(&status));
            let (result, lines) = prepare(&fake, &startup(apply)).await;
            let error = result.expect_err("refused");
            assert_eq!(error, MigrationError::Status(status.clone()));
            assert_eq!(error.exit_code(), code, "{status:?}");
            assert_eq!(fake.calls(), ["status"], "{status:?}");
            assert_eq!(lines, [READING]);
        }
    }
}

/// A dev build serves a store that is not a catalog yet, which its server
/// creates; a production build refuses it with exit 1.
#[tokio::test]
async fn a_dev_build_leaves_an_uninitialized_store_to_the_server() {
    let fake = Fake::new(&[CatalogStatus::Uninitialized]);
    let (result, _) = prepare(&fake, &dev(false)).await;
    assert_eq!(result, Ok(Prepared::default()));
    assert_eq!(fake.calls(), ["status", "creates"]);
}

/// A dev build serves only what any server serves, and a store its server
/// creates: every other status is refused with its code under both values,
/// and `Behind` under `refuse`, before any lock. `Behind` under `apply` is
/// migrated.
#[tokio::test]
async fn a_dev_build_refuses_every_status_its_server_cannot_serve() {
    for (status, code) in unservable() {
        for apply in [false, true] {
            let fake = Fake::new(std::slice::from_ref(&status));
            let (result, _) = prepare(&fake, &dev(apply)).await;
            let error = result.expect_err("refused");
            assert_eq!(error, MigrationError::Status(status.clone()));
            assert_eq!(error.exit_code(), code, "{status:?}");
            assert_eq!(fake.calls(), ["status"], "{status:?}");
        }
    }
    let fake = Fake::new(&[behind()]);
    let (result, _) = prepare(&fake, &dev(false)).await;
    assert_eq!(result.expect_err("refused").exit_code(), exit::PENDING);
    assert_eq!(fake.calls(), ["status"]);

    for status in [current(), ahead(), CatalogStatus::Uninitialized] {
        for apply in [false, true] {
            let fake = Fake::new(std::slice::from_ref(&status));
            let (result, _) = prepare(&fake, &dev(apply)).await;
            assert_eq!(result.expect("served").run, None, "{status:?}");
            assert!(!fake.calls().contains(&"lock"), "{status:?}");
        }
    }
    let fake = Fake::new(&[behind()]);
    let (result, _) = prepare(&fake, &dev(true)).await;
    assert_eq!(result.expect("applied").run, Some(applied_report()));
}

/// A store the backend's server would refuse, such as a SQLite file that
/// holds another program's tables, is refused before the server starts,
/// as `Uninitialized` with exit 1, in a dev build too.
#[tokio::test]
async fn a_dev_build_refuses_a_store_its_server_would_refuse() {
    let mut fake = Fake::new(&[CatalogStatus::Uninitialized]);
    fake.creates = false;
    for apply in [false, true] {
        let (result, _) = prepare(&fake, &dev(apply)).await;
        let error = result.expect_err("refused");
        assert_eq!(error, MigrationError::Status(CatalogStatus::Uninitialized));
        assert_eq!(error.exit_code(), exit::ERROR);
    }
    assert!(!fake.calls().contains(&"lock"), "{:?}", fake.calls());
}

/// An in-memory store is created by the server that opens it: nothing is
/// read before.
#[tokio::test]
async fn an_in_process_store_is_not_read() {
    let mut fake = Fake::new(&[behind()]);
    fake.in_process = true;
    let (result, lines) = prepare(&fake, &startup(true)).await;
    assert_eq!(result, Ok(Prepared::default()));
    assert!(fake.calls().is_empty(), "{:?}", fake.calls());
    assert!(lines.is_empty(), "{lines:?}");
}

/// A lock that stays held past the timeout stops the server with exit 6
/// while the catalog is still behind.
#[tokio::test]
async fn a_lock_held_past_the_timeout_exits_6() {
    let mut fake = Fake::new(&[behind()]);
    fake.lock = Err(MigrationError::Locked(
        "another migration holds it".to_owned(),
    ));
    let (result, lines) = prepare(&fake, &startup(true)).await;
    let error = result.expect_err("locked");
    assert_eq!(error.exit_code(), exit::LOCKED);
    assert_eq!(error.to_string(), "another migration holds it");
    // The status is read again after the wait, after a line saying so, and
    // the backend ran nothing.
    assert_eq!(fake.calls(), ["status", "notes", "lock", "status"]);
    assert_eq!(lines.last().map(String::as_str), Some(super::READING_AGAIN));
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
    assert_eq!(fake.calls(), ["status", "notes", "lock", "status"]);
    assert_eq!(lines.last().map(String::as_str), Some(MIGRATED_ELSEWHERE));
}

/// The same, with a newer binary that left the catalog ahead of this one
/// by additive migrations: it is served with the warning.
#[tokio::test]
async fn the_fallback_after_the_wait_serves_a_catalog_ahead_by_additive_migrations() {
    let mut fake = Fake::new(&[behind(), ahead()]);
    fake.lock = Err(MigrationError::Locked("a server serves it".to_owned()));
    let (result, lines) = prepare(&fake, &startup(true)).await;
    let prepared = result.expect("served");
    assert_eq!(prepared.run, None);
    let warning = prepared.warning.expect("a warning");
    assert!(warning.contains("additive"), "{warning}");
    assert_eq!(fake.calls(), ["status", "notes", "lock", "status"]);
    assert_eq!(lines.last().map(String::as_str), Some(MIGRATED_ELSEWHERE));
}

/// The status read under the lock finds the catalog migrated: the lock is
/// released and the catalog served without the backend's run, which on
/// PostgreSQL would make the pgvector attempt and open the runner's
/// sessions.
#[tokio::test]
async fn a_run_that_finds_the_catalog_migrated_under_the_lock_applies_nothing() {
    let fake = Fake::new(&[behind(), current()]);
    let (result, lines) = prepare(&fake, &startup(true)).await;
    let prepared = result.expect("served");
    let latest = MigrationId::LATEST.number();
    assert_eq!(
        prepared.run,
        Some(RunReport {
            from: Some(latest),
            to: Some(latest),
            ..RunReport::default()
        })
    );
    assert_eq!(prepared.warning, None);
    // The status read after the run looks for a catalog ahead by additive
    // migrations.
    assert_eq!(
        fake.calls(),
        ["status", "notes", "lock", "status", "release", "status"]
    );
    assert_eq!(lines.last().map(String::as_str), Some(MIGRATED_ELSEWHERE));
}

/// A newer binary migrated the catalog while this one waited, ahead of it
/// by an additive migration: the run under the lock applies nothing, and
/// the status read after it gives the warning `migrate --yes` prints.
#[tokio::test]
async fn a_run_that_applied_nothing_warns_of_a_catalog_ahead_by_additive_migrations() {
    let fake = Fake::new(&[behind(), ahead()]);
    let (result, _) = prepare(&fake, &startup(true)).await;
    let prepared = result.expect("served");
    assert!(prepared.run.expect("a run").applied.is_empty());
    let warning = prepared.warning.expect("a warning");
    assert!(warning.contains("additive"), "{warning}");
    assert_eq!(
        fake.calls(),
        ["status", "notes", "lock", "status", "release", "status"]
    );
}

/// Under `apply` the server checks the status notes `migrate --yes` checks
/// before it takes the lock: a note that refuses runs stops it with exit 1
/// and the same notes, naming the server's command, before any lock. A
/// note that does not refuse is printed as a progress line.
#[tokio::test]
async fn a_note_that_refuses_runs_stops_apply_before_the_lock() {
    let note = StatusNote {
        text: "the role does not own the tables".to_owned(),
        refuses_runs: true,
    };
    let mut fake = Fake::new(&[behind()]);
    fake.notes = vec![note.clone()];
    let (result, lines) = prepare(&fake, &startup(true)).await;
    let error = result.expect_err("refused");
    assert_eq!(error.exit_code(), exit::ERROR);
    assert_eq!(
        error.to_string().lines().collect::<Vec<_>>(),
        [
            "serve with --migrate or catalog_upgrade = \"apply\" refuses to apply migrations \
             while these notes hold:",
            "Note: the role does not own the tables",
            "Nothing was changed and no lock was taken.",
        ]
    );
    assert_eq!(fake.calls(), ["status", "notes", "status"]);
    assert!(
        !lines
            .iter()
            .any(|l| l.starts_with("taking the migration lock")),
        "{lines:?}"
    );

    let mut fake = Fake::new(&[behind()]);
    fake.notes = vec![StatusNote {
        refuses_runs: false,
        ..note
    }];
    let (result, lines) = prepare(&fake, &startup(true)).await;
    result.expect("applied");
    assert!(
        lines
            .iter()
            .any(|l| l == "note: the role does not own the tables"),
        "{lines:?}"
    );
    assert!(fake.calls().contains(&"run"), "{:?}", fake.calls());
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
