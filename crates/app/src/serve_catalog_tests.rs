// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog check `serve` makes before it binds its port: `--migrate`
//! over `[server] catalog_upgrade`, the prefix on every line it prints, the
//! exit code of a refusal, the lock timeout's bound, and a runtime that
//! does not stop in time before a fork.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use extenddb_config::{CatalogUpgrade, ServerConfig};
use extenddb_storage::migrations::{
    AppliedStep, CatalogMigrations, CatalogStatus, HeldLock, Kind, LedgerRow, Listed,
    MigrationError, MigrationId, Pending, RunKind, RunReport, StoreKind, Target, exit,
};

use super::{PROGRESS_PREFIX, ServeStart, check, lock_timeout};

/// The cap the tests give the check's runtime to stop.
const CAP: Duration = Duration::from_secs(10);

struct Fake {
    /// What the next status reads return, the last one repeated.
    statuses: Mutex<VecDeque<Result<CatalogStatus, MigrationError>>>,
    lock: Result<(), MigrationError>,
    report: RunReport,
    /// A blocking thread the run starts and does not wait for sleeps this
    /// long.
    leaves_thread: Option<Duration>,
    calls: Mutex<Vec<&'static str>>,
}

impl Fake {
    fn new(statuses: Vec<Result<CatalogStatus, MigrationError>>) -> Self {
        Self {
            statuses: Mutex::new(statuses.into()),
            lock: Ok(()),
            report: applied_report(),
            leaves_thread: None,
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
    async fn status(&self, _target: &Target) -> Result<CatalogStatus, MigrationError> {
        self.call("status");
        let mut statuses = self.statuses.lock().unwrap();
        let status = statuses.front().cloned().expect("a status");
        if statuses.len() > 1 {
            statuses.pop_front();
        }
        status
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
        _kind: RunKind,
    ) -> Result<RunReport, MigrationError> {
        self.call("run");
        if let Some(sleep) = self.leaves_thread {
            drop(tokio::task::spawn_blocking(move || {
                std::thread::sleep(sleep)
            }));
        }
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

/// A run's report whose copy note spans two lines.
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
                notes: vec!["a note\nthat spans two lines".to_owned()],
            })
            .collect(),
        notes: vec!["copied the database to db.pre-migration-1-to-4-x".to_owned()],
    }
}

fn server(upgrade: CatalogUpgrade) -> ServerConfig {
    ServerConfig {
        catalog_upgrade: upgrade,
        catalog_upgrade_lock_timeout: 1,
        ..ServerConfig::default()
    }
}

fn start(server: &ServerConfig, migrate: bool, daemonizes: bool) -> ServeStart<'_> {
    ServeStart {
        server,
        location: "db",
        migrate,
        release: "1.0.0-test",
        config_file: Some("/etc/extenddb/extenddb.toml"),
        daemonizes,
    }
}

/// Run the check, returning its exit code and every line it printed, one
/// entry per line.
fn run_check(fake: &Fake, start: &ServeStart<'_>, cap: Duration) -> (u8, Vec<String>) {
    let printed = Mutex::new(Vec::new());
    let code = check(fake, start, cap, &|text| {
        printed.lock().unwrap().push(text.to_owned());
    })
    .expect("the check runs");
    let lines = printed
        .into_inner()
        .unwrap()
        .iter()
        .flat_map(|text| text.lines().map(str::to_owned).collect::<Vec<_>>())
        .collect();
    (code, lines)
}

/// The launcher matches this string at the start of a line and restarts
/// its startup timeout at each line that has it: `PROGRESS_PREFIX` in
/// packaging/npm/startup.js, and the copy in its test server, must change
/// with this one.
#[test]
fn the_prefix_is_the_one_the_npm_launcher_matches() {
    assert_eq!(PROGRESS_PREFIX, "catalog migration: ");
}

/// `--migrate` applies a catalog behind this binary for one run under
/// `catalog_upgrade = "refuse"`, which without it exits 3 and takes no
/// lock; `catalog_upgrade = "apply"` applies it without the flag.
#[test]
fn migrate_applies_a_catalog_behind_for_one_run() {
    let refuse = server(CatalogUpgrade::Refuse);
    let fake = Fake::new(vec![Ok(behind())]);
    let (code, _) = run_check(&fake, &start(&refuse, false, false), CAP);
    assert_eq!(code, exit::PENDING);
    assert_eq!(fake.calls(), ["status"]);

    let fake = Fake::new(vec![Ok(behind())]);
    let (code, lines) = run_check(&fake, &start(&refuse, true, false), CAP);
    assert_eq!(code, exit::OK, "{lines:?}");
    assert_eq!(fake.calls(), ["status", "lock", "status", "run", "release"]);

    let apply = server(CatalogUpgrade::Apply);
    let fake = Fake::new(vec![Ok(behind())]);
    let (code, lines) = run_check(&fake, &start(&apply, false, false), CAP);
    assert_eq!(code, exit::OK, "{lines:?}");
    assert!(fake.calls().contains(&"run"), "{:?}", fake.calls());
}

/// Every line a run prints starts with the prefix, the lines of a note that
/// spans several included, so the launcher counts each of them.
#[test]
fn every_progress_line_starts_with_the_prefix() {
    let refuse = server(CatalogUpgrade::Refuse);
    let fake = Fake::new(vec![Ok(behind())]);
    let (code, lines) = run_check(&fake, &start(&refuse, true, false), CAP);
    assert_eq!(code, exit::OK, "{lines:?}");
    assert_eq!(lines[0], "catalog migration: reading the catalog status");
    assert!(
        lines.contains(&"catalog migration: that spans two lines".to_owned()),
        "{lines:?}"
    );
    for line in &lines {
        assert!(line.starts_with(PROGRESS_PREFIX), "{line:?} in {lines:?}");
    }
}

/// A refusal prints its message on stderr and exits with its status's
/// code, and never with 0: an error whose code would be 0 exits 1.
#[test]
fn a_refusal_exits_with_its_code_and_never_with_0() {
    let refuse = server(CatalogUpgrade::Refuse);
    let cases = [
        (Ok(behind()), exit::PENDING),
        (
            Ok(CatalogStatus::NeedsAttention {
                items: vec!["migration 2: a collision".to_owned()],
            }),
            exit::ATTENTION,
        ),
        (
            Ok(CatalogStatus::AheadIncompatible {
                unknown: vec![(MigrationId::LATEST.number() + 1, "later".to_owned())],
            }),
            exit::AHEAD_INCOMPATIBLE,
        ),
        (
            Ok(CatalogStatus::Uninitialized),
            if cfg!(feature = "dev-mode") {
                exit::OK
            } else {
                exit::ERROR
            },
        ),
        (
            Err(MigrationError::Status(CatalogStatus::Current {
                version: MigrationId::LATEST.number(),
            })),
            exit::ERROR,
        ),
    ];
    for (status, code) in cases {
        let fake = Fake::new(vec![status.clone()]);
        let (got, lines) = run_check(&fake, &start(&refuse, false, false), CAP);
        assert_eq!(got, code, "{status:?}: {lines:?}");
        if code != exit::OK {
            assert!(
                lines.iter().any(|l| l.starts_with("Error: ")),
                "{status:?}: {lines:?}"
            );
        }
    }

    // A lock that stays held: exit 6 once the wait reaches the timeout.
    let mut fake = Fake::new(vec![Ok(behind())]);
    fake.lock = Err(MigrationError::Locked("another migration holds it".into()));
    let (code, lines) = run_check(&fake, &start(&refuse, true, false), CAP);
    assert_eq!(code, exit::LOCKED, "{lines:?}");
}

/// A blocking thread of the check still running when the runtime's
/// shutdown reaches its cap keeps a daemon from forking: the check prints
/// the cause and exits 1. A foreground server does not fork, and starts.
#[test]
fn a_thread_still_running_after_the_check_keeps_a_daemon_from_forking() {
    let refuse = server(CatalogUpgrade::Refuse);
    let mut fake = Fake::new(vec![Ok(behind())]);
    fake.leaves_thread = Some(Duration::from_millis(500));
    let cap = Duration::from_millis(50);
    let (code, lines) = run_check(&fake, &start(&refuse, true, true), cap);
    assert_eq!(code, exit::ERROR, "{lines:?}");
    let last = lines.last().expect("a line");
    assert!(
        last.starts_with("Error: a blocking thread of the catalog check")
            && last.contains("still running"),
        "{lines:?}"
    );

    let (code, lines) = run_check(&fake, &start(&refuse, true, false), cap);
    assert_eq!(code, exit::OK, "{lines:?}");

    // A check whose threads end in time forks.
    let fake = Fake::new(vec![Ok(behind())]);
    let (code, lines) = run_check(&fake, &start(&refuse, true, true), CAP);
    assert_eq!(code, exit::OK, "{lines:?}");
}

/// `serve` refuses a lock timeout above a day, as `migrate --lock-timeout`
/// does, before anything reads the catalog.
#[test]
fn a_lock_timeout_above_a_day_is_refused() {
    let over = ServerConfig {
        catalog_upgrade_lock_timeout: 86_401,
        ..ServerConfig::default()
    };
    let err = lock_timeout(&over).expect_err("above a day");
    assert!(err.to_string().contains("at most 86400 s"), "{err}");
    let day = ServerConfig {
        catalog_upgrade_lock_timeout: 86_400,
        ..ServerConfig::default()
    };
    assert_eq!(lock_timeout(&day).ok(), Some(Duration::from_secs(86_400)));

    let fake = Fake::new(vec![Ok(behind())]);
    let refused = check(&fake, &start(&over, true, false), CAP, &|_| {});
    assert!(refused.is_err(), "{refused:?}");
    assert!(fake.calls().is_empty(), "{:?}", fake.calls());
}
