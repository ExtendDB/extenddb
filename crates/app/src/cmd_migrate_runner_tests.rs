// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The preview takes no lock and runs nothing; `--yes` and `--forget`
//! take the lock first; each ends with the status's or the error's code.
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use extenddb_storage::migrations::advice::Advice;
use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, HeldLock, Kind, LedgerRow, Listed, MigrationError,
    MigrationId, Pending, RunKind, RunReport, StatusNote, StoreKind, Target, exit,
};

use super::{apply, forget, preview};

struct Fake {
    status: CatalogStatus,
    lock: Result<(), MigrationError>,
    notes: Vec<StatusNote>,
    calls: Mutex<Vec<&'static str>>,
}

impl Fake {
    fn new(status: CatalogStatus) -> Self {
        Self {
            status,
            lock: Ok(()),
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
    async fn status(&self, _target: &Target) -> Result<CatalogStatus, MigrationError> {
        self.call("status");
        Ok(self.status.clone())
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
        self.call(match kind {
            RunKind::Create => "create",
            RunKind::Migrate => "run",
            RunKind::Reconverge => "reconverge",
        });
        Ok(RunReport::default())
    }
    async fn forget(&self, _lock: &HeldLock, _id: u32) -> Result<(), MigrationError> {
        self.call("forget");
        Ok(())
    }
    async fn status_notes(&self, _: &Target) -> Result<Vec<StatusNote>, MigrationError> {
        Ok(self.notes.clone())
    }
}

const ADVICE: Advice<'static> = Advice {
    release: "1.0.0-test",
    location: "db",
    backup: "back up db",
    config: Some("extenddb.toml"),
    assignment: None,
};

fn behind() -> CatalogStatus {
    CatalogStatus::Behind {
        version: None,
        pending: MigrationId::ALL
            .iter()
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

#[tokio::test]
async fn the_preview_reads_the_status_and_takes_no_lock() {
    let cases = [
        (behind(), exit::PENDING),
        (
            CatalogStatus::Current {
                version: MigrationId::LATEST.number(),
            },
            exit::OK,
        ),
        (
            CatalogStatus::NeedsAttention {
                items: vec!["collision".to_owned()],
            },
            exit::ATTENTION,
        ),
        (CatalogStatus::Uninitialized, exit::ERROR),
        (
            CatalogStatus::AheadIncompatible {
                unknown: vec![(MigrationId::LATEST.number() + 1, "later".to_owned())],
            },
            exit::AHEAD_INCOMPATIBLE,
        ),
    ];
    for (status, code) in cases {
        let fake = Fake::new(status.clone());
        assert_eq!(
            preview(&fake, &Target::at("db"), &ADVICE).await,
            Ok(code),
            "{status:?}"
        );
        assert_eq!(fake.calls(), ["status"], "{status:?}");
    }
}

#[tokio::test]
async fn yes_takes_the_lock_before_it_runs() {
    let fake = Fake::new(behind());
    let code = apply(
        &fake,
        &Target::at("db"),
        "1.0.0-test",
        RunKind::Migrate,
        Duration::ZERO,
    )
    .await;
    assert_eq!(code, Ok(exit::OK));
    // The status is read under the lock before the run; a run that
    // applied nothing reads it again, to warn about a catalog ahead by
    // additive migrations.
    assert_eq!(fake.calls(), ["lock", "status", "run", "status"]);

    let fake = Fake::new(behind());
    let code = apply(
        &fake,
        &Target::at("db"),
        "1.0.0-test",
        RunKind::Reconverge,
        Duration::ZERO,
    )
    .await;
    assert_eq!(code, Ok(exit::OK));
    assert_eq!(fake.calls(), ["lock", "status", "reconverge"]);
}

/// A store that is not a catalog is refused under the lock, with the
/// status's exit code 1 and the message naming `extenddb init`, and the
/// backend runs nothing.
#[tokio::test]
async fn yes_refuses_an_uninitialized_store() {
    for kind in [RunKind::Migrate, RunKind::Reconverge] {
        let fake = Fake::new(CatalogStatus::Uninitialized);
        let err = apply(&fake, &Target::at("db"), "1.0.0-test", kind, Duration::ZERO)
            .await
            .expect_err("uninitialized");
        assert_eq!(err.exit_code(), exit::ERROR);
        assert_eq!(err, MigrationError::Status(CatalogStatus::Uninitialized));
        assert!(
            super::advice::error_message(&err, &ADVICE).contains("extenddb init"),
            "{err}"
        );
        assert_eq!(fake.calls(), ["lock", "status"], "{kind:?}");
    }
}

/// `--forget` on a store that is not a catalog reads the status under the
/// lock and refuses, naming `extenddb init`, without calling the backend's
/// forget, which would open the store to write.
#[tokio::test]
async fn forget_refuses_an_uninitialized_store() {
    let fake = Fake::new(CatalogStatus::Uninitialized);
    let err = forget(&fake, &Target::at("db"), 4, Duration::ZERO)
        .await
        .expect_err("uninitialized");
    assert_eq!(err.exit_code(), exit::ERROR);
    assert!(
        super::advice::error_message(&err, &ADVICE).contains("extenddb init"),
        "{err}"
    );
    assert_eq!(fake.calls(), ["lock", "status"]);
}

/// A note that refuses runs stops `--yes` before the lock on a catalog
/// with something to apply, and every `--reconverge`. On a current
/// catalog `--yes` applies nothing and goes on, as it does past a note
/// that does not refuse; the preview prints the note and keeps the
/// status's code.
#[tokio::test]
async fn a_refusing_note_stops_yes_before_the_lock() {
    let current = CatalogStatus::Current {
        version: MigrationId::LATEST.number(),
    };
    let note = StatusNote {
        text: "the role does not own the tables".to_owned(),
        refuses_runs: true,
    };
    let with = |status: CatalogStatus, note: &StatusNote| {
        let mut fake = Fake::new(status);
        fake.notes = vec![note.clone()];
        fake
    };
    let target = Target::at("db");
    for (status, kind) in [
        (behind(), RunKind::Migrate),
        (current.clone(), RunKind::Reconverge),
    ] {
        let fake = with(status, &note);
        let err = apply(&fake, &target, "1.0.0-test", kind, Duration::ZERO)
            .await
            .expect_err("refused");
        assert_eq!(err.exit_code(), exit::ERROR, "{kind:?}");
        assert!(err.to_string().contains("does not own the tables"), "{err}");
        assert!(
            !fake.calls().contains(&"lock"),
            "{kind:?}: {:?}",
            fake.calls()
        );
    }
    // Each refusing note is a line of its own, as the preview prints it, so
    // the statements a note ends with can be copied alone.
    let mut fake = with(behind(), &note);
    fake.notes.push(StatusNote {
        text: "a second note, ending with GRANT x TO y;".to_owned(),
        refuses_runs: true,
    });
    let err = apply(
        &fake,
        &target,
        "1.0.0-test",
        RunKind::Migrate,
        Duration::ZERO,
    )
    .await
    .expect_err("refused");
    assert_eq!(
        err.to_string().lines().collect::<Vec<_>>(),
        [
            "migrate --yes refuses to apply migrations while these notes hold:",
            "Note: the role does not own the tables",
            "Note: a second note, ending with GRANT x TO y;",
            "Nothing was changed and no lock was taken.",
        ]
    );
    let fake = with(current, &note);
    let code = apply(
        &fake,
        &target,
        "1.0.0-test",
        RunKind::Migrate,
        Duration::ZERO,
    )
    .await;
    assert_eq!(code, Ok(exit::OK));
    assert!(fake.calls().contains(&"lock"), "{:?}", fake.calls());
    let informational = StatusNote {
        refuses_runs: false,
        ..note.clone()
    };
    let fake = with(behind(), &informational);
    let code = apply(
        &fake,
        &target,
        "1.0.0-test",
        RunKind::Migrate,
        Duration::ZERO,
    )
    .await;
    assert_eq!(code, Ok(exit::OK));
    assert!(fake.calls().contains(&"run"), "{:?}", fake.calls());
    let fake = with(behind(), &note);
    assert_eq!(preview(&fake, &target, &ADVICE).await, Ok(exit::PENDING));
    assert_eq!(fake.calls(), ["status"]);
}

#[tokio::test]
async fn a_held_lock_runs_nothing_and_exits_6() {
    let mut fake = Fake::new(behind());
    fake.lock = Err(MigrationError::Locked("held".to_owned()));
    let err = apply(
        &fake,
        &Target::at("db"),
        "1.0.0-test",
        RunKind::Migrate,
        Duration::ZERO,
    )
    .await
    .expect_err("locked");
    assert_eq!(err.exit_code(), exit::LOCKED);
    assert_eq!(fake.calls(), ["lock"]);
    let err = forget(
        &fake,
        &Target::at("db"),
        MigrationId::LATEST.number(),
        Duration::ZERO,
    )
    .await
    .expect_err("locked");
    assert_eq!(err.exit_code(), exit::LOCKED);
    assert_eq!(fake.calls(), ["lock", "lock"]);
}

#[tokio::test]
async fn forget_takes_the_lock() {
    let fake = Fake::new(behind());
    let code = forget(
        &fake,
        &Target::at("db"),
        MigrationId::LATEST.number(),
        Duration::ZERO,
    )
    .await;
    assert_eq!(code, Ok(exit::OK));
    assert_eq!(fake.calls(), ["lock", "status", "forget"]);
}

fn parse(argv: &[&str]) -> Result<crate::cmd_migrate::MigrateArgs, clap::Error> {
    use clap::{Args as _, FromArgMatches as _};
    let cmd = crate::cmd_migrate::MigrateArgs::augment_args(clap::Command::new("migrate"));
    let matches =
        cmd.try_get_matches_from(std::iter::once("migrate").chain(argv.iter().copied()))?;
    crate::cmd_migrate::MigrateArgs::from_arg_matches(&matches)
}

/// A lock timeout above a day is refused before anything runs, so the
/// wait never overflows `Instant`.
#[test]
fn a_lock_timeout_above_a_day_is_refused() {
    assert!(parse(&["--yes", "--lock-timeout", "18446744073709551615"]).is_err());
    assert!(parse(&["--yes", "--lock-timeout", "86401"]).is_err());
    let args = parse(&["--yes", "--lock-timeout", "86400"]).expect("a day");
    assert_eq!(args.lock_timeout, Some(86_400));
}

/// The preview takes no lock, so a lock timeout given to it is refused
/// instead of ignored.
#[tokio::test]
async fn the_preview_refuses_a_lock_timeout() {
    let args = parse(&["--lock-timeout", "5"]).expect("parse");
    let fake = Fake::new(behind());
    let err = super::run(&args, &fake, "1.0.0-test")
        .await
        .expect_err("refused");
    assert!(
        err.to_string()
            .contains("--lock-timeout applies to --yes and --forget"),
        "{err}"
    );
    assert!(fake.calls().is_empty());
}

fn role(user: &str, password: Option<&str>) -> Option<extenddb_storage::migrations::AdminRole> {
    Some(extenddb_storage::migrations::AdminRole {
        user: user.to_owned(),
        password: password.map(str::to_owned),
    })
}

/// The admin role is the backend's own: a Cassandra binary reads
/// `--cassandra-user` with `--cassandra-pass` or
/// `EXTENDDB_CASSANDRA_PASSWORD` and ignores `--pg-user`; every other
/// binary reads `--pg-user` with `--pg-pass` or `EXTENDDB_PG_PASSWORD` and
/// ignores `--cassandra-user`.
#[test]
fn the_admin_role_is_chosen_by_the_backend() {
    let both = parse(&[
        "--yes",
        "--pg-user",
        "pg_admin",
        "--pg-pass",
        "pg_secret",
        "--cassandra-user",
        "cass_admin",
        "--cassandra-pass",
        "cass_secret",
    ])
    .expect("parse");
    let pick = |backend: Option<&str>, args| {
        super::admin_role_for(backend, args, None, None).expect("admin role")
    };
    assert_eq!(
        pick(Some("cassandra"), &both),
        role("cass_admin", Some("cass_secret"))
    );
    assert_eq!(
        pick(Some("postgres"), &both),
        role("pg_admin", Some("pg_secret"))
    );
    assert_eq!(
        pick(Some("sqlite"), &both),
        role("pg_admin", Some("pg_secret"))
    );
    assert_eq!(
        pick(Some("mongodb"), &both),
        role("pg_admin", Some("pg_secret"))
    );
    assert_eq!(pick(None, &both), role("pg_admin", Some("pg_secret")));

    let cassandra_only = parse(&["--yes", "--cassandra-user", "cass_admin"]).expect("parse");
    assert_eq!(pick(Some("postgres"), &cassandra_only), None);
    assert_eq!(
        pick(Some("cassandra"), &cassandra_only),
        role("cass_admin", None)
    );

    let pg_only = parse(&["--yes", "--pg-user", "pg_admin"]).expect("parse");
    assert_eq!(pick(Some("cassandra"), &pg_only), None);
    assert_eq!(pick(Some("postgres"), &pg_only), role("pg_admin", None));
}

/// Each backend's password variable fills in for its password flag, and the
/// flag wins over the variable; the other backend's variable is ignored.
#[test]
fn the_password_variable_is_the_backends_own() {
    let env = |value: &str| Some(std::ffi::OsString::from(value));
    let cassandra = parse(&["--yes", "--cassandra-user", "cass_admin"]).expect("parse");
    assert_eq!(
        super::admin_role_for(
            Some("cassandra"),
            &cassandra,
            env("pg_env"),
            env("cass_env")
        )
        .expect("admin role"),
        role("cass_admin", Some("cass_env"))
    );
    assert_eq!(
        super::admin_role_for(Some("cassandra"), &cassandra, env("pg_env"), None)
            .expect("admin role"),
        role("cass_admin", None)
    );
    let with_flag = parse(&[
        "--yes",
        "--cassandra-user",
        "cass_admin",
        "--cassandra-pass",
        "flag",
    ])
    .expect("parse");
    assert_eq!(
        super::admin_role_for(Some("cassandra"), &with_flag, None, env("cass_env"))
            .expect("admin role"),
        role("cass_admin", Some("flag"))
    );

    let postgres = parse(&["--yes", "--pg-user", "pg_admin"]).expect("parse");
    assert_eq!(
        super::admin_role_for(Some("postgres"), &postgres, env("pg_env"), env("cass_env"))
            .expect("admin role"),
        role("pg_admin", Some("pg_env"))
    );
    assert_eq!(
        super::admin_role_for(Some("postgres"), &postgres, None, env("cass_env"))
            .expect("admin role"),
        role("pg_admin", None)
    );
}
