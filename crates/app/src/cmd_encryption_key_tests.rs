// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of `extenddb encryption-key`: the command line, the standard input
//! rules, the lock, and what the commands print.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use extenddb_storage::key_commands::{NO_REVOCATION_NOTICE, Outcome, StatusReport, Summary};
use extenddb_storage::key_pass::{DataConnection, Opener, SecretListing, Survey, TableCounts};
use extenddb_storage::key_ring::{KekRef, KekSource, KeyId, KeyRing, WrappedDek};
use extenddb_storage::migrations::{
    HeldLock, LedgerRow, Listed, MigrationId, Pending, RunKind, RunReport, StoreKind,
};
use extenddb_storage::secret_keys::SecretTable;

use super::*;

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    args: EncryptionKeyArgs,
}

/// Each command's options: the lock options only on the commands that take
/// the lock, `--yes` on every command that writes, and the flags that the
/// RFC gives a command only on that command.
#[test]
fn each_command_takes_its_own_options() {
    let cases: &[(&[&str], bool)] = &[
        (&["status"], true),
        (&["status", "--config", "/etc/extenddb/extenddb.toml"], true),
        (&["status", "--yes"], false),
        (&["switch", "--key-file", "/run/kek"], true),
        (
            &[
                "switch",
                "--key-file",
                "/run/kek",
                "--yes",
                "--lock-timeout",
                "86400",
                "--accept-catalog-key",
                "--keep-unopenable",
            ],
            true,
        ),
        (&["switch"], false),
        (
            &[
                "switch",
                "--key-file",
                "/run/kek",
                "--lock-timeout",
                "86401",
            ],
            false,
        ),
        (&["rewrap", "--key-file", "/run/new", "--yes"], true),
        (
            &["rewrap", "--key-file", "/run/new", "--lock-timeout", "5"],
            false,
        ),
        (
            &["rewrap", "--key-file", "/run/new", "--keep-unopenable"],
            false,
        ),
        (
            &[
                "rotate",
                "--yes",
                "--accept-catalog-key",
                "--keep-unopenable",
            ],
            true,
        ),
        (&["rotate", "--key-file", "/run/kek"], false),
        (
            &[
                "reset",
                "--key-file",
                "/run/new",
                "--discard-secrets",
                "--yes",
            ],
            true,
        ),
        (
            &["reset", "--key-file", "/run/new", "--keep-unopenable"],
            false,
        ),
        (&["set-data-connection", "--yes"], true),
        (&["set-data-connection", "--lock-timeout", "5"], false),
    ];
    for (argv, parses) in cases {
        let line = std::iter::once("encryption-key").chain(argv.iter().copied());
        let result = Cli::try_parse_from(line);
        assert_eq!(result.is_ok(), *parses, "{argv:?}: {:?}", result.err());
    }
}

/// Standard input as `reset` and `set-data-connection` read it: one
/// trailing newline stripped, a terminal, empty input and more than 8 KiB
/// refused. No refusal repeats the input.
#[test]
fn standard_input_follows_the_rules_of_section_8_7() {
    const CONN: &str = "postgresql://extenddb:apppassword@db.internal:5432/extenddb";
    let long = "x".repeat(8193);
    /// What the case is, the input, whether it is a terminal, and the text
    /// or a part of the refusal.
    type Case<'a> = (&'a str, &'a [u8], bool, Result<&'a str, &'a str>);
    let cases: Vec<Case<'_>> = vec![
        (
            "one newline",
            b"postgresql://extenddb:apppassword@db.internal:5432/extenddb\n",
            false,
            Ok(CONN),
        ),
        ("no newline", CONN.as_bytes(), false, Ok(CONN)),
        ("two newlines", b"a\n\n", false, Ok("a\n")),
        (
            "a terminal",
            CONN.as_bytes(),
            true,
            Err("standard input is a terminal"),
        ),
        ("empty", b"", false, Err("standard input is empty")),
        (
            "a newline only",
            b"\n",
            false,
            Err("standard input is empty"),
        ),
        (
            "more than 8 KiB",
            long.as_bytes(),
            false,
            Err("more than 8192 bytes"),
        ),
        (
            "not UTF-8",
            b"\xff\xfeapppassword",
            false,
            Err("not UTF-8 text"),
        ),
    ];
    for (what, input, terminal, expected) in cases {
        match (read_secret_input(input, terminal), expected) {
            (Ok(text), Ok(want)) => assert_eq!(text.as_str(), want, "{what}"),
            (Err(failure), Err(part)) => {
                assert!(
                    failure.message.contains(part),
                    "{what}: {}",
                    failure.message
                );
                assert!(!failure.message.contains("apppassword"), "{what}");
                assert_eq!(failure.code, 1, "{what}");
            }
            (got, _) => panic!("{what}: {:?}", got.map(|t| t.len())),
        }
    }
}

/// A fake catalog whose lock and status the test chooses, recording the
/// calls in order.
struct FakeMigrations {
    lock: Result<(), MigrationError>,
    status: CatalogStatus,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl FakeMigrations {
    fn call(&self, name: &'static str) {
        self.calls.lock().unwrap().push(name);
    }
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
    async fn run(&self, _: &HeldLock, _: &str, _: RunKind) -> Result<RunReport, MigrationError> {
        unreachable!("the key commands run no migration")
    }
    async fn forget(&self, _lock: &HeldLock, _id: u32) -> Result<(), MigrationError> {
        unreachable!("the key commands forget no migration")
    }
    async fn release(&self, lock: HeldLock) {
        self.call("release");
        drop(lock);
    }
}

/// The lock is taken and freed as `migrate --yes` does: a lock held past
/// `--lock-timeout` exits 6 with nothing run; a catalog whose migrations are
/// not current exits with `migrate`'s code for it, and the lock is freed.
#[tokio::test]
async fn the_lock_exits_6_when_held_and_a_catalog_behind_is_refused() {
    let behind = CatalogStatus::Behind {
        version: Some(8),
        pending: vec![Pending {
            id: 9,
            name: "warm_throughput",
            store: StoreKind::Catalog,
            store_name: "extenddb.sqlite".to_owned(),
            kind: extenddb_storage::migrations::Kind::Additive,
        }],
    };
    let cases = [
        (
            Err(MigrationError::Locked("held by pid 7".to_owned())),
            CatalogStatus::Current { version: 9 },
            6,
            vec!["lock"],
        ),
        (Ok(()), behind, 3, vec!["lock", "status", "release"]),
    ];
    for (lock, status, code, calls) in cases {
        let fake = FakeMigrations {
            lock,
            status,
            calls: Arc::default(),
        };
        let target = Target::at("/var/lib/extenddb/extenddb.sqlite");
        let failure = locked(&fake, &target, Some(1), None, async |_stores| {
            Ok::<(), KeyCommandError>(())
        })
        .await
        .expect_err("refused");
        assert_eq!(failure.code, code, "{}", failure.message);
        assert_eq!(*fake.calls.lock().unwrap(), calls, "{}", failure.message);
    }
}

fn key_id(hex: &str) -> KeyId {
    KeyId::parse(hex).expect("a key id")
}

fn listing(access_key_id: &str, opener: Opener) -> SecretListing {
    SecretListing {
        table: SecretTable::AccessKeys,
        access_key_id: access_key_id.to_owned(),
        account_id: "123456789012".to_owned(),
        principal_name: "alice".to_owned(),
        created_at: Some(time::OffsetDateTime::from_unix_timestamp(1_794_823_200).expect("time")),
        opener,
    }
}

/// The summary prints the KEK id, the DEK id, the counts per table under the
/// command's key names, the stored secrets that no key opens, the `key_file`
/// line and the notice, which for `reset` says that every access key and
/// session was deleted.
#[test]
fn the_summary_names_the_keys_and_ends_with_the_notice() {
    let counts = TableCounts {
        target: 1,
        old: vec![0],
        unopenable: 1,
        rewritten: 2,
    };
    let summary = Summary {
        kek_id: key_id("3f9a0c51e2d47b86"),
        dek_id: 1,
        tables: [
            (SecretTable::AccessKeys, counts),
            (SecretTable::IamSessions, TableCounts::default()),
        ],
        data_connection_rewritten: None,
        unopenable: vec![listing("AKIAEXAMPLE000000001", Opener::None)],
        deleted_access_key_ids: Vec::new(),
        deleted_catalog_key: true,
        key_file: "/etc/extenddb/kek".to_owned(),
        key_names: vec!["DEK 1".to_owned(), "the catalog key".to_owned()],
    };
    let text = outcome_text("switch", &Outcome::Done(summary.clone()));
    for line in [
        "=== extenddb encryption-key switch complete ===",
        "KEK id: 3f9a0c51e2d47b86",
        "DEK id: 1",
        "access_keys: 1 under DEK 1, 0 under the catalog key, 1 that no key opens, 2 \
         re-encrypted",
        "iam_sessions: 0 under DEK 1, 0 that no key opens",
        "The encryption_key row was deleted.",
        "access_keys AKIAEXAMPLE000000001: account 123456789012, owner alice, created \
         2026-11-16T10:00:00Z, under no key",
        "key_file = \"/etc/extenddb/kek\"",
        NO_REVOCATION_NOTICE,
    ] {
        assert!(text.contains(line), "{line}\n{text}");
    }
    assert!(!text.contains("data_database_connection_string"), "{text}");

    let text = outcome_text("reset", &Outcome::Done(summary.clone()));
    assert!(
        text.ends_with(&format!("{}\n", key_commands::reset_notice())),
        "{text}"
    );
    assert!(text.contains("The KEK of a catalog copy is lost"), "{text}");
    assert!(!text.contains(NO_REVOCATION_NOTICE), "{text}");

    // A daemon `serve` runs from `/`, so a relative path is printed absolute.
    let relative = Summary {
        key_file: "kek".to_owned(),
        ..summary
    };
    let absolute = std::env::current_dir().expect("cwd").join("kek");
    assert_eq!(
        relative.key_file_line(),
        format!("key_file = {:?}", absolute.display().to_string())
    );

    let text = outcome_text(
        "rotate",
        &Outcome::NothingToDo {
            kek_id: key_id("3f9a0c51e2d47b86"),
            dek_id: 2,
        },
    );
    assert_eq!(
        text,
        "Nothing to do: the key ring is already in the target state (KEK id \
         3f9a0c51e2d47b86, DEK id 2).\n"
    );
}

/// `status` prints the ring's fields, and says whether they are
/// authenticated: no KEK given, another KEK, a DEK that fails to unwrap, or
/// the configured KEK.
#[test]
fn status_says_whether_the_ring_is_authenticated() {
    let ring = KeyRing {
        version: 1,
        kek: KekRef {
            source: KekSource::File,
            id: key_id("3f9a0c51e2d47b86"),
        },
        keys: vec![
            WrappedDek {
                id: 2,
                wrapped: "c2VhbGVk".to_owned(),
            },
            WrappedDek {
                id: 1,
                wrapped: "c2VhbGVk".to_owned(),
            },
        ],
        switch_from_catalog_key: false,
        retiring_catalog_key_id: None,
    };
    let cases = [
        (None, vec![], "Not authenticated: no KEK file was given"),
        (
            Some(key_id("00000000000000aa")),
            vec![],
            "Not authenticated: the configured KEK has id 00000000000000aa, and the ring names \
             KEK id 3f9a0c51e2d47b86.",
        ),
        (
            Some(key_id("3f9a0c51e2d47b86")),
            vec![1],
            "Not authenticated: DEK ids 1 fail to unwrap under the configured KEK",
        ),
        (
            Some(key_id("3f9a0c51e2d47b86")),
            vec![],
            "Authenticated under the configured KEK.",
        ),
    ];
    for (given_kek_id, failed_dek_ids, expected) in cases {
        let report = StatusReport {
            catalog_key_present: false,
            ring: Some(key_commands::RingStatus {
                ring: ring.clone(),
                given_kek_id,
                failed_dek_ids,
            }),
            survey: None,
        };
        let text = status_text(&report);
        assert!(
            text.contains(
                "Key ring (encryption_key_ring row): KEK source file, KEK id 3f9a0c51e2d47b86, \
                 DEK ids 1, 2, switch_from_catalog_key false, retiring_catalog_key_id none"
            ),
            "{text}"
        );
        assert!(text.contains(expected), "{expected}\n{text}");
        assert!(
            text.contains("Catalog key (encryption_key row): none"),
            "{text}"
        );
    }

    let survey = Survey {
        tables: [
            (
                SecretTable::AccessKeys,
                TableCounts {
                    target: 2,
                    old: vec![],
                    unopenable: 1,
                    rewritten: 0,
                },
            ),
            (SecretTable::IamSessions, TableCounts::default()),
        ],
        not_target: vec![listing("AKIAEXAMPLE000000002", Opener::None)],
        data_connection: DataConnection::Absent,
        key_names: vec!["the catalog key".to_owned()],
    };
    let text = status_text(&StatusReport {
        catalog_key_present: true,
        ring: None,
        survey: Some(survey),
    });
    for line in [
        "Catalog key (encryption_key row): present",
        "Key ring (encryption_key_ring row): none",
        "access_keys: 2 under the catalog key, 1 that no key opens",
        "Stored secrets under a key other than the catalog key:",
        "access_keys AKIAEXAMPLE000000002: account 123456789012, owner alice",
    ] {
        assert!(text.contains(line), "{line}\n{text}");
    }
}
