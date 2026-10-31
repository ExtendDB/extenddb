// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The advised command runs in a POSIX shell as printed, from any
//! directory: every path in it is quoted, and a store that the config file
//! names by a relative location is named by its absolute location.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;

use super::{Advice, AdviceFor, shell_word, status_message};
use crate::migrations::{
    CatalogMigrations, CatalogStatus, HeldLock, LedgerRow, Listed, MigrationError, MigrationId,
    RunKind, RunReport, Target,
};

/// A backend whose location is a file, named by an assignment the way the
/// SQLite backend names it. The advice reads nothing else of it.
struct FileBackend;

#[async_trait]
impl CatalogMigrations for FileBackend {
    fn listed(&self, _id: MigrationId) -> Listed {
        Listed::NotApplicable("test")
    }
    fn backup_advice(&self, target: &Target) -> String {
        format!("back up {}", target.location)
    }
    fn location_assignment(&self, target: &Target) -> Option<String> {
        Some(format!(
            "EXTENDDB__STORAGE__SQLITE__PATH={}",
            shell_word(&target.location)
        ))
    }
    async fn status(&self, _: &Target) -> Result<CatalogStatus, MigrationError> {
        Err(MigrationError::Store("not read".to_owned()))
    }
    async fn catalog_ledger(&self, _: &Target) -> Result<Vec<LedgerRow>, MigrationError> {
        Err(MigrationError::Store("not read".to_owned()))
    }
    async fn lock(&self, _: &Target, _: Duration) -> Result<HeldLock, MigrationError> {
        Err(MigrationError::Store("not taken".to_owned()))
    }
    async fn run(&self, _: &HeldLock, _: &str, _: RunKind) -> Result<RunReport, MigrationError> {
        Err(MigrationError::Store("not run".to_owned()))
    }
    async fn forget(&self, _: &HeldLock, _: u32) -> Result<(), MigrationError> {
        Err(MigrationError::Store("not run".to_owned()))
    }
}

/// A directory of its own under the temporary directory, removed when the
/// value is dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("extenddb-advice-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the scratch directory");
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A config file in a directory whose name holds a space and a single
/// quote, with `section` as its `[storage.sqlite]` body.
fn config_file(scratch: &Scratch, section: &str) -> PathBuf {
    let dir = scratch.0.join("sp ace's dir");
    std::fs::create_dir_all(&dir).expect("create the config directory");
    let path = dir.join("extenddb.toml");
    std::fs::write(&path, format!("[storage.sqlite]\n{section}")).expect("write the config");
    path
}

/// The command `status` advises, as one line without its indent.
fn advised(status: &CatalogStatus, advice: &Advice<'_>) -> String {
    let message = status_message(status, advice);
    message
        .lines()
        .last()
        .map(str::trim_start)
        .unwrap_or_default()
        .to_owned()
}

fn written_by_older_release() -> CatalogStatus {
    CatalogStatus::WrittenByOlderRelease {
        trace: "catalog_version is 0.0.4".to_owned(),
    }
}

#[test]
fn a_word_with_shell_characters_is_single_quoted() {
    assert_eq!(shell_word("/srv/x.sqlite"), "/srv/x.sqlite");
    assert_eq!(shell_word("/a b/it's"), "'/a b/it'\\''s'");
    assert_eq!(shell_word("/a/$HOME/`x`"), "'/a/$HOME/`x`'");
    assert_eq!(shell_word(""), "''");
}

#[test]
fn the_config_path_is_quoted_for_a_posix_shell() {
    let advice = Advice {
        release: "1.0.1",
        location: "/srv/x.sqlite",
        backup: "back up /srv/x.sqlite",
        config: Some("/srv/sp ace's dir/extenddb.toml"),
        assignment: None,
        serving: false,
    };
    assert_eq!(
        advised(&written_by_older_release(), &advice),
        "extenddb migrate --config '/srv/sp ace'\\''s dir/extenddb.toml' --yes --reconverge"
    );
}

/// A config file that names the store by a relative location, or not at
/// all, gets the assignment with the absolute location; one that names it
/// by that absolute location does not.
#[test]
fn a_store_the_config_file_names_relatively_is_named_by_its_absolute_path() {
    let scratch = Scratch::new("relative");
    let store = scratch.0.join("served").join("extenddb.sqlite");
    let location = store.to_string_lossy().into_owned();
    let assignment = |section: &str| {
        let config = config_file(&scratch, section);
        AdviceFor::new(
            "1.0.1",
            location.clone(),
            Some(&FileBackend),
            &Target::at(location.as_str()),
            Some(&config.to_string_lossy()),
        )
        .assignment
    };
    let pinned = Some(format!(
        "EXTENDDB__STORAGE__SQLITE__PATH={}",
        shell_word(&location)
    ));
    assert_eq!(assignment("path = \"extenddb.sqlite\"\n"), pinned);
    assert_eq!(assignment("pool_size = 10\n"), pinned);
    assert_eq!(
        assignment(&format!(
            "path = {}\n",
            toml::Value::from(location.as_str())
        )),
        None
    );
    assert_eq!(assignment("not toml at all ]["), pinned);
}

/// The advised command, pasted into `sh` in another directory, passes the
/// config path as one argument and the store's absolute path, whatever the
/// directory holds. A stand-in `extenddb` prints what it received.
#[cfg(unix)]
#[test]
fn the_advised_command_runs_as_printed_from_another_directory() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new("shell");
    let config = config_file(&scratch, "path = \"extenddb.sqlite\"\n");
    // Where the command that printed the advice resolved the relative path.
    let location = config
        .parent()
        .map(|dir| dir.join("extenddb.sqlite"))
        .expect("the config directory");
    let location = location.to_string_lossy().into_owned();
    let bin = scratch.0.join("bin");
    std::fs::create_dir_all(&bin).expect("create the bin directory");
    let fake = bin.join("extenddb");
    std::fs::write(
        &fake,
        "#!/bin/sh\nprintf '%s\\n' \"${EXTENDDB__STORAGE__SQLITE__PATH-unset}\" \"$(pwd -P)\" \
         \"$@\"\n",
    )
    .expect("write the stand-in");
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755))
        .expect("make the stand-in executable");
    let elsewhere = scratch.0.join("elsewhere");
    std::fs::create_dir_all(&elsewhere).expect("create another directory");

    let config_shown = config.to_string_lossy().into_owned();
    let values = AdviceFor::new(
        "1.0.1",
        location.clone(),
        Some(&FileBackend),
        &Target::at(location.as_str()),
        Some(&config_shown),
    );
    let command = advised(&written_by_older_release(), &values.advice());
    let path = std::env::var("PATH").unwrap_or_default();
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(&command)
        .current_dir(&elsewhere)
        .env("PATH", format!("{}:{path}", bin.display()))
        .env_remove("EXTENDDB__STORAGE__SQLITE__PATH")
        .output()
        .expect("run sh");
    assert!(output.status.success(), "{command}: {output:?}");
    let printed = String::from_utf8_lossy(&output.stdout);
    let elsewhere = std::fs::canonicalize(&elsewhere).expect("canonicalize");
    let elsewhere = elsewhere.to_string_lossy();
    assert_eq!(
        printed.lines().collect::<Vec<_>>(),
        [
            location.as_str(),
            &*elsewhere,
            "migrate",
            "--config",
            config_shown.as_str(),
            "--yes",
            "--reconverge",
        ],
        "{command}"
    );
    assert!(std::path::Path::new(&location).is_absolute(), "{location}");
}
