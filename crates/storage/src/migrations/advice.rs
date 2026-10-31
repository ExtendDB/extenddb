// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The messages `serve`, `migrate` and `verify` print when they stop on the
//! catalog: what the status is, and the command that resolves it, written
//! without a password in it.

use super::catalog::{CatalogMigrations, Target};
use super::registry::{MigrationId, RELEASED_THROUGH};
use super::status::{CatalogStatus, MigrationError, Pending};
use super::table::Kind;

/// What a message needs beyond the status.
#[derive(Debug, Clone, Copy)]
pub struct Advice<'a> {
    /// The ExtendDB release of this binary.
    pub release: &'a str,
    /// The deployment's location, for display (a SQLite path, a redacted URL).
    pub location: &'a str,
    /// The backend's [`backup_advice`](super::CatalogMigrations::backup_advice).
    pub backup: &'a str,
    /// The config file the commands were given, as an absolute path; `None`
    /// when the command ran without one (a dev-mode server configured by
    /// `EXTENDDB__` variables), so the advised command names none either.
    pub config: Option<&'a str>,
    /// The backend's
    /// [`location_assignment`](super::CatalogMigrations::location_assignment)
    /// when there is no config file, or when the environment named the
    /// store over the config file's location: the advised command starts
    /// with it, so it acts on this deployment's store wherever it runs and
    /// whatever `extenddb.toml` the directory holds.
    pub assignment: Option<&'a str>,
}

impl Advice<'_> {
    fn command(&self, flags: &str) -> String {
        let assignment = self
            .assignment
            .map_or_else(String::new, |a| format!("{a} "));
        match self.config {
            Some(config) => format!("  {assignment}extenddb migrate --config {config} {flags}"),
            None => format!("  {assignment}extenddb migrate {flags}"),
        }
    }
}

/// The values an [`Advice`] borrows, built once by each command that prints
/// one (`serve`, `migrate`, `verify`).
#[derive(Debug, Clone)]
pub struct AdviceFor {
    release: String,
    location: String,
    backup: String,
    config: Option<String>,
    assignment: Option<String>,
}

impl AdviceFor {
    /// The advice for the deployment at `target`, shown as `location` (the
    /// caller redacts any password), with the backup clause of `migrations`.
    #[must_use]
    pub fn new(
        release: &str,
        location: String,
        migrations: Option<&dyn CatalogMigrations>,
        target: &Target,
        config: Option<&str>,
    ) -> Self {
        Self {
            release: release.to_owned(),
            location,
            backup: migrations.map_or_else(
                || "back up the catalog".to_owned(),
                |m| m.backup_advice(target),
            ),
            config: config.map(|c| {
                std::path::absolute(c).map_or_else(|_| c.to_owned(), |p| p.display().to_string())
            }),
            assignment: migrations
                .filter(|m| config.is_none() || m.location_from_environment())
                .and_then(|m| m.location_assignment(target)),
        }
    }

    /// The advice, borrowing these values.
    #[must_use]
    pub fn advice(&self) -> Advice<'_> {
        Advice {
            release: &self.release,
            location: &self.location,
            backup: &self.backup,
            config: self.config.as_deref(),
            assignment: self.assignment.as_deref(),
        }
    }
}

/// The message for a status a command does not proceed on. `Current` and
/// `AheadCompatible` get the status line alone.
#[must_use]
pub fn status_message(status: &CatalogStatus, advice: &Advice<'_>) -> String {
    let latest = MigrationId::LATEST.number();
    match status {
        CatalogStatus::Behind { version, pending } => {
            let (at, stop) = match version {
                Some(v) => (
                    format!("the catalog is at migration {v}"),
                    "Stop the server.",
                ),
                None => (
                    "the catalog has no migration ledger (a release before 1.0 created it)"
                        .to_owned(),
                    "Stop every server that uses this catalog: a server of a release before 1.0 \
                     is not detected while it runs.",
                ),
            };
            format!(
                "{at}, and ExtendDB {release} needs migration {latest}.\n{}\n{stop}\n{backup}, \
                 then run:\n{}",
                pending_line(pending),
                advice.command("--yes"),
                release = advice.release,
                backup = capitalized(advice.backup),
            )
        }
        // `init --config <file>` refuses a config file that exists, and this
        // catalog was found through one, so the command is named alone.
        CatalogStatus::Uninitialized => format!(
            "the catalog at {} is not initialized; create it with 'extenddb init'",
            advice.location
        ),
        CatalogStatus::NeedsAttention { items } => format!(
            "a pending migration needs a person before it can run:\n{}\nFix each item, then run:\n{}",
            items
                .iter()
                .map(|item| format!("  {item}"))
                .collect::<Vec<_>>()
                .join("\n"),
            advice.command("--yes")
        ),
        CatalogStatus::Changed { id, .. } if *id > RELEASED_THROUGH => format!(
            "{status}.\nMigration {id} is unreleased: clear its rows, then apply it again:\n{}\n{}",
            advice.command(&format!("--forget {id}")),
            advice.command("--yes")
        ),
        CatalogStatus::Changed { .. } => format!(
            "{status}.\nA released migration never changes; run the ExtendDB release that \
             recorded it, or restore the backup taken before it ran."
        ),
        CatalogStatus::WrittenByOlderRelease { trace } => format!(
            "a release before 1.0 wrote to this catalog after it was migrated ({trace}).\nStop \
             every server of a release before 1.0, then run:\n{}",
            advice.command("--yes --reconverge")
        ),
        CatalogStatus::AheadIncompatible { .. } => format!(
            "{status}.\nExtendDB {} cannot use it: run the release that applied those \
             migrations, or restore the backup taken before they ran.",
            advice.release
        ),
        CatalogStatus::Current { .. } | CatalogStatus::AheadCompatible { .. } => status.to_string(),
    }
}

/// The message for an error a command stopped on.
#[must_use]
pub fn error_message(error: &MigrationError, advice: &Advice<'_>) -> String {
    match error {
        MigrationError::Status(status) => status_message(status, advice),
        MigrationError::Locked(m) => format!("{m}. Retry once it has finished."),
        MigrationError::Store(m)
        | MigrationError::Database { message: m, .. }
        | MigrationError::Refused(m)
        | MigrationError::Blocked(m) => m.clone(),
        MigrationError::Stopped { error, report } => {
            let mut message = error_message(error, advice);
            if !report.applied.is_empty() {
                let steps: Vec<String> = report
                    .applied
                    .iter()
                    .map(|s| format!("{} {} in {}", s.id, s.name, s.store_name))
                    .collect();
                message.push_str(&format!(
                    "\nBefore it stopped, this run committed {}; the next run goes on from there.",
                    steps.join(", ")
                ));
            }
            for note in &report.notes {
                message.push('\n');
                message.push_str(note);
            }
            message
        }
    }
}

fn capitalized(text: &str) -> String {
    let mut chars = text.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_uppercase().chain(chars).collect()
    })
}

/// `Pending (all breaking): 2 name (catalog), ...`, or each kind beside its
/// migration when the kinds differ.
fn pending_line(pending: &[Pending]) -> String {
    let kinds: Vec<Kind> = pending.iter().map(|p| p.kind).collect();
    let same = kinds.windows(2).all(|w| w[0] == w[1]);
    let items: Vec<String> = pending
        .iter()
        .map(|p| {
            if same {
                format!("{} {} ({})", p.id, p.name, p.store.as_str())
            } else {
                format!(
                    "{} {} ({}, {})",
                    p.id,
                    p.name,
                    p.store.as_str(),
                    p.kind.as_str()
                )
            }
        })
        .collect();
    match kinds.first() {
        Some(kind) if same => format!("Pending (all {}): {}.", kind.as_str(), items.join(", ")),
        _ => format!("Pending: {}.", items.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::{Advice, error_message, status_message};
    use crate::migrations::{
        AppliedStep, CatalogStatus, Kind, MigrationError, MigrationId, Pending, RunReport,
        StoreKind,
    };

    const ADVICE: Advice<'static> = Advice {
        release: "1.0.1",
        location: "/var/lib/extenddb/extenddb.sqlite",
        backup: "back up the SQLite database file /var/lib/extenddb/extenddb.sqlite",
        config: Some("/etc/extenddb/extenddb.toml"),
        assignment: None,
    };

    fn pending(kind: Kind) -> Vec<Pending> {
        MigrationId::ALL
            .iter()
            .skip(1)
            .map(|id| Pending {
                id: id.number(),
                name: id.name(),
                store: StoreKind::Catalog,
                store_name: "db".to_owned(),
                kind,
            })
            .collect()
    }

    #[test]
    fn a_catalog_behind_names_each_migration_the_backup_and_the_command() {
        let message = status_message(
            &CatalogStatus::Behind {
                version: Some(1),
                pending: pending(Kind::Breaking),
            },
            &ADVICE,
        );
        let latest = MigrationId::LATEST.number();
        let first = message.lines().next().unwrap_or_default();
        assert_eq!(
            first,
            format!("the catalog is at migration 1, and ExtendDB 1.0.1 needs migration {latest}.")
        );
        assert!(message.contains("Pending (all breaking): 2 "), "{message}");
        assert!(
            message.contains(
                "Stop the server.\nBack up the SQLite database file \
                 /var/lib/extenddb/extenddb.sqlite, then run:"
            ),
            "{message}"
        );
        assert!(
            message.ends_with("  extenddb migrate --config /etc/extenddb/extenddb.toml --yes"),
            "{message}"
        );
    }

    #[test]
    fn a_catalog_with_no_ledger_says_to_stop_every_server() {
        let message = status_message(
            &CatalogStatus::Behind {
                version: None,
                pending: pending(Kind::Breaking),
            },
            &ADVICE,
        );
        assert!(message.starts_with("the catalog has no migration ledger"));
        assert!(message.contains("Stop every server that uses this catalog"));
    }

    #[test]
    fn mixed_kinds_are_named_per_migration() {
        let mut list = pending(Kind::Breaking);
        list[0].kind = Kind::Additive;
        let message = status_message(
            &CatalogStatus::Behind {
                version: Some(1),
                pending: list,
            },
            &ADVICE,
        );
        assert!(message.contains("Pending: 2 "), "{message}");
        assert!(message.contains("(catalog, additive)"), "{message}");
        assert!(message.contains("(catalog, breaking)"), "{message}");
    }

    #[test]
    fn each_refusal_names_its_command() {
        let cases = [
            (
                CatalogStatus::Uninitialized,
                "create it with 'extenddb init'",
            ),
            (
                CatalogStatus::WrittenByOlderRelease {
                    trace: "catalog_version is 0.0.4".to_owned(),
                },
                "--yes --reconverge",
            ),
            (
                CatalogStatus::NeedsAttention {
                    items: vec!["migration 2 resource_tag_condition_keys: a collision".to_owned()],
                },
                "  migration 2 resource_tag_condition_keys: a collision",
            ),
            (
                CatalogStatus::Changed {
                    id: 4,
                    store_name: "db".to_owned(),
                    recorded: "a".to_owned(),
                    expected: "b".to_owned(),
                },
                "--forget 4",
            ),
            (
                CatalogStatus::AheadIncompatible {
                    unknown: vec![(9, "later".to_owned())],
                },
                "ExtendDB 1.0.1 cannot use it",
            ),
        ];
        for (status, expected) in cases {
            let message = status_message(&status, &ADVICE);
            assert!(message.contains(expected), "{status:?}: {message}");
            assert!(
                !message.contains("--pg-pass") && !message.contains("password"),
                "{message}"
            );
        }
    }

    #[test]
    fn a_held_lock_says_to_retry() {
        let message = error_message(&MigrationError::Locked("held".to_owned()), &ADVICE);
        assert_eq!(message, "held. Retry once it has finished.");
    }

    /// Sessions that never finish by themselves carry their own remedy.
    #[test]
    fn a_blocked_run_prints_its_own_remedy_alone() {
        let error = MigrationError::Blocked("end them, then run again".to_owned());
        assert_eq!(error_message(&error, &ADVICE), "end them, then run again");
        assert_eq!(error.exit_code(), crate::migrations::exit::LOCKED);
    }

    /// A dev server configured by `EXTENDDB__` variables has no config file,
    /// and `migrate` finds the same database from the same variables.
    #[test]
    fn without_a_config_file_the_command_names_none() {
        let advice = Advice {
            config: None,
            ..ADVICE
        };
        let message = status_message(
            &CatalogStatus::WrittenByOlderRelease {
                trace: "catalog_version is 0.0.4".to_owned(),
            },
            &advice,
        );
        assert!(
            message.ends_with("\n  extenddb migrate --yes --reconverge"),
            "{message}"
        );
        let advice = Advice {
            config: None,
            assignment: Some("EXTENDDB__STORAGE__SQLITE__PATH=/srv/x.sqlite"),
            ..ADVICE
        };
        let message = status_message(
            &CatalogStatus::WrittenByOlderRelease {
                trace: "catalog_version is 0.0.4".to_owned(),
            },
            &advice,
        );
        assert!(
            message.ends_with(
                "\n  EXTENDDB__STORAGE__SQLITE__PATH=/srv/x.sqlite extenddb migrate --yes \
                 --reconverge"
            ),
            "{message}"
        );
        // A store the environment named over the config file's location: the
        // command carries both, as the server found its store.
        let advice = Advice {
            assignment: Some("EXTENDDB__STORAGE__SQLITE__PATH=/srv/x.sqlite"),
            ..ADVICE
        };
        let message = status_message(
            &CatalogStatus::WrittenByOlderRelease {
                trace: "catalog_version is 0.0.4".to_owned(),
            },
            &advice,
        );
        assert!(
            message.ends_with(
                "\n  EXTENDDB__STORAGE__SQLITE__PATH=/srv/x.sqlite extenddb migrate --config \
                 /etc/extenddb/extenddb.toml --yes --reconverge"
            ),
            "{message}"
        );
    }

    #[test]
    fn a_stopped_run_names_what_it_committed_and_its_copy() {
        let error = MigrationError::Stopped {
            error: Box::new(MigrationError::Store("migration 3 failed".to_owned())),
            report: RunReport {
                from: None,
                to: Some(2),
                applied: vec![AppliedStep {
                    id: 1,
                    name: "baseline",
                    store_name: "db".to_owned(),
                    notes: Vec::new(),
                }],
                notes: vec!["copied the database to db.pre-migration-legacy-to-4-x".to_owned()],
            },
        };
        assert_eq!(error.exit_code(), crate::migrations::exit::ERROR);
        let message = error_message(&error, &ADVICE);
        assert_eq!(
            message,
            "migration 3 failed\nBefore it stopped, this run committed 1 baseline in db; the \
             next run goes on from there.\ncopied the database to db.pre-migration-legacy-to-4-x"
        );
    }
}
