// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The encryption-key commands end to end on a SQLite file catalog: the
//! migration lock, the stores the commands open, the storage crate's steps,
//! and what a server started afterwards opens.
//!
//! The KEK files here are read without the file rule, which the key file
//! module tests on its own: the rule refuses every path under a directory
//! that another user owns, and a test host's temp directory can sit under
//! one. Each test skips with a printed reason while the SQLite backend lacks
//! the stores of a held lock or the pass methods (`read_secret_page` and the
//! rest).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use extenddb_storage::key_commands::{self, KekFile, KeyStores, Outcome, RunFlags};
use extenddb_storage::key_ring::{kek_id, parse_kek_file};
use extenddb_storage::management_store::OpError;
use extenddb_storage::migrations::MigrationError;
use extenddb_storage::secret_keys::SecretTable;
use extenddb_storage::server_components::{BackendError, KeySource, ServerComponentsOptions};
use zeroize::Zeroizing;

use super::{Failure, Stores, locked, outcome_text, status_text};

const ACCOUNT: &str = "111122223333";
const USER: &str = "alice";
const ACCESS_KEY_ID: &str = "AKIAEXTENDDBKEYTEST1";
const SECRET: &str = "extenddbKeyCommandsEndToEndSecret0000001";

/// A KEK file read without the file rule. `fail_after` makes every read
/// after that many fail, as a file that went away during a run.
struct TestKek {
    path: String,
    reads: AtomicUsize,
    fail_after: usize,
}

impl TestKek {
    fn new(path: &Path) -> Self {
        Self::failing_after(path, usize::MAX)
    }

    fn failing_after(path: &Path, fail_after: usize) -> Self {
        Self {
            path: path.to_str().expect("utf-8 path").to_owned(),
            reads: AtomicUsize::new(0),
            fail_after,
        }
    }

    fn key(&self) -> Zeroizing<[u8; 32]> {
        Zeroizing::new(**parse_kek_file(&std::fs::read(&self.path).expect("read")).expect("a KEK"))
    }
}

impl KekFile for TestKek {
    fn path(&self) -> &str {
        &self.path
    }

    fn read(&self) -> Result<Zeroizing<[u8; 32]>, String> {
        if self.reads.fetch_add(1, Ordering::SeqCst) >= self.fail_after {
            return Err(format!("KEK file {} cannot be resolved: gone", self.path));
        }
        Ok(self.key())
    }
}

/// One SQLite catalog in a temp directory, made by `init`'s steps, with one
/// access key under the catalog key, and three KEK files.
struct Deployment {
    _dir: tempfile::TempDir,
    config: extenddb_config::AppConfig,
    keks: [PathBuf; 3],
}

impl Deployment {
    /// `None` when the SQLite store has no pass methods yet.
    async fn new() -> Option<Self> {
        let _ = extenddb_storage::set_backend(extenddb_storage_sqlite::backend());
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("extenddb.toml");
        std::fs::write(
            &config_path,
            format!(
                "[storage.sqlite]\npath = {:?}\n\n[auth.encryption]\nrequire_locked_memory = \
                 false\n",
                dir.path().join("catalog.sqlite").display().to_string()
            ),
        )
        .expect("config");
        let config_path = config_path.to_str().expect("utf-8").to_owned();
        let config = extenddb_config::load(&config_path).expect("load");

        let bootstrapper = extenddb_storage::bootstrapper::create_bootstrapper(&config_path, &[])
            .await
            .expect("bootstrapper");
        bootstrapper.create_catalog_db().await.expect("catalog db");
        bootstrapper.create_data_db().await.expect("data db");
        let migrations = extenddb_storage::migrations::installed().expect("installed");
        crate::cmd_init::init_on_runner(bootstrapper.as_ref(), migrations, "test", &mut |_| {})
            .await
            .expect("init");

        let keks = [1u8, 2, 3].map(|n| {
            let path = dir.path().join(format!("kek-{n}"));
            std::fs::write(&path, format!("{}\n", base64_of([n; 32]))).expect("kek");
            path
        });
        let deployment = Self {
            _dir: dir,
            config,
            keks,
        };
        let components = deployment
            .open(KeySource::CatalogKey)
            .await
            .expect("catalog key");
        let store = components.catalog_store.clone();
        store
            .create_account(ACCOUNT, "test")
            .await
            .expect("account");
        store.create_user(ACCOUNT, USER, None).await.expect("user");
        store
            .import_access_key(ACCOUNT, USER, ACCESS_KEY_ID, SECRET)
            .await
            .expect("access key");
        close(components).await;

        // The commands need the stores of a held lock and the pass methods.
        let migrations = extenddb_storage::migrations::installed().expect("installed");
        let lock = migrations
            .lock(&deployment.target(), std::time::Duration::from_secs(5))
            .await
            .expect("lock");
        let missing = match migrations.locked_stores(&lock, None).await {
            Err(MigrationError::Refused(m)) => Some(m),
            Err(e) => panic!("locked_stores: {e:?}"),
            Ok(stores) => match stores
                .management()
                .read_secret_page(SecretTable::AccessKeys, None, 1)
                .await
            {
                Err(OpError::Internal(m))
                    if m.contains("is not supported by this storage backend") =>
                {
                    Some(m)
                }
                Err(e) => panic!("read_secret_page: {e:?}"),
                Ok(_) => None,
            },
        };
        migrations.release(lock).await;
        match missing {
            Some(m) => {
                eprintln!("skipped: the SQLite backend of this build lacks a method: {m}");
                None
            }
            None => Some(deployment),
        }
    }

    fn target(&self) -> extenddb_storage::migrations::Target {
        extenddb_storage::migrations::Target::at(self.config.storage.connection_config())
    }

    fn kek(&self, n: usize) -> TestKek {
        TestKek::new(&self.keks[n])
    }

    /// The server's components with `key_source`, as `serve` builds them.
    async fn open(
        &self,
        key_source: KeySource,
    ) -> Result<extenddb_storage::server_components::ServerComponents, BackendError> {
        let mut options = ServerComponentsOptions::default();
        options.release = "test";
        options.key_source = key_source;
        extenddb_storage::create_server_components(
            self.config.storage.as_trait(),
            "us-east-1",
            options,
        )
        .await
    }

    /// The secret a server started with `key_source` reads for the access
    /// key, or the refusal of its start.
    async fn served_secret(&self, key_source: KeySource) -> Result<Option<String>, String> {
        let components = self.open(key_source).await.map_err(|e| e.to_string())?;
        let found = components
            .credential_store
            .lookup_credential(ACCESS_KEY_ID)
            .await
            .map_err(|e| format!("{e:?}"));
        close(components).await;
        Ok(found?.map(|c| c.secret_key.clone()))
    }

    /// Run a locked command, as `switch`, `rotate` and `reset` do.
    async fn locked(
        &self,
        f: impl AsyncFnOnce(KeyStores<'_>) -> Result<Outcome, key_commands::KeyCommandError>,
    ) -> Result<Outcome, Failure> {
        let migrations = extenddb_storage::migrations::installed().expect("installed");
        locked(migrations, &self.target(), Some(5), None, f).await
    }
}

fn base64_of(bytes: [u8; 32]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

async fn close(components: extenddb_storage::server_components::ServerComponents) {
    if let Some(registration) = components
        .runtime_hooks
        .as_ref()
        .and_then(|hooks| hooks.serving_registrar())
    {
        registration.release().await;
    }
}

fn flags(yes: bool) -> RunFlags {
    RunFlags {
        yes,
        ..RunFlags::default()
    }
}

fn kek_source(kek: &TestKek) -> KeySource {
    KeySource::Kek(kek.key().into())
}

/// `switch`: the preview writes nothing, `--yes` moves the secret under a DEK
/// that the KEK wraps and deletes the catalog key, the server then opens the
/// secret with the KEK only, and a second run has nothing to do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_moves_every_secret_to_a_dek_under_the_kek() {
    let Some(d) = Deployment::new().await else {
        return;
    };
    let kek = d.kek(0);
    let preview = d
        .locked(async |s| key_commands::switch(s, &kek, flags(false)).await)
        .await
        .expect("preview");
    assert!(matches!(preview, Outcome::Preview(_)), "{preview:?}");
    assert_eq!(
        d.served_secret(KeySource::CatalogKey).await,
        Ok(Some(SECRET.to_owned()))
    );

    let done = d
        .locked(async |s| key_commands::switch(s, &kek, flags(true)).await)
        .await
        .expect("switch");
    let text = outcome_text("switch", &done);
    assert!(
        text.contains(&format!("KEK id: {}", kek_id(&kek.key()))),
        "{text}"
    );
    assert!(
        text.contains("The encryption_key row was deleted."),
        "{text}"
    );
    assert!(!text.contains(SECRET), "{text}");

    assert_eq!(
        d.served_secret(kek_source(&kek)).await,
        Ok(Some(SECRET.to_owned()))
    );
    let refused = d.served_secret(KeySource::CatalogKey).await;
    assert!(
        refused.is_err(),
        "a server without key_file starts after the switch"
    );

    let again = d
        .locked(async |s| key_commands::switch(s, &kek, flags(true)).await)
        .await
        .expect("again");
    assert!(
        matches!(again, Outcome::NothingToDo { dek_id: 1, .. }),
        "{again:?}"
    );
}

/// A switch that stops when the KEK file goes away before the catalog key
/// is deleted leaves a ring that the server refuses; the same command run
/// again resumes and finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_switch_resumes() {
    let Some(d) = Deployment::new().await else {
        return;
    };
    let stops = TestKek::failing_after(&d.keks[0], 1);
    let stopped = d
        .locked(async |s| key_commands::switch(s, &stops, flags(true)).await)
        .await;
    assert!(stopped.is_err(), "{stopped:?}");
    let kek = d.kek(0);
    let refused = d.served_secret(kek_source(&kek)).await;
    assert!(
        refused.is_err(),
        "a server with key_file starts on the ring of a stopped switch"
    );

    let done = d
        .locked(async |s| key_commands::switch(s, &kek, flags(true)).await)
        .await
        .expect("resume");
    assert!(matches!(done, Outcome::Done(_)), "{done:?}");
    assert_eq!(
        d.served_secret(kek_source(&kek)).await,
        Ok(Some(SECRET.to_owned()))
    );
}

/// `rewrap` changes the KEK and no stored secret; `rotate` replaces the DEK;
/// `status` reports the ring under the configured KEK; `reset` with a new
/// KEK deletes the access key and writes a ring that the new KEK opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rewrap_rotate_status_and_reset() {
    let Some(d) = Deployment::new().await else {
        return;
    };
    let (first, second, third) = (d.kek(0), d.kek(1), d.kek(2));
    d.locked(async |s| key_commands::switch(s, &first, flags(true)).await)
        .await
        .expect("switch");

    let stores = Stores::open(&d.target(), true).await.expect("stores");
    let rewrapped = key_commands::rewrap(stores.view(), &first, &second, true)
        .await
        .expect("rewrap");
    assert!(matches!(rewrapped, Outcome::Done(_)), "{rewrapped:?}");
    let report = key_commands::status(stores.view(), Some(&second as &dyn KekFile))
        .await
        .expect("status");
    let text = status_text(&report);
    assert!(
        text.contains("Authenticated under the configured KEK."),
        "{text}"
    );
    assert!(
        text.contains("Catalog key (encryption_key row): none"),
        "{text}"
    );
    drop(stores);
    let mismatch = d.served_secret(kek_source(&first)).await;
    assert!(
        mismatch.is_err(),
        "a server with the first KEK starts after the rewrap"
    );
    assert_eq!(
        d.served_secret(kek_source(&second)).await,
        Ok(Some(SECRET.to_owned()))
    );

    let rotated = d
        .locked(async |s| key_commands::rotate(s, &second, flags(true)).await)
        .await
        .expect("rotate");
    assert!(
        matches!(&rotated, Outcome::Done(s) if s.dek_id == 2),
        "{rotated:?}"
    );
    assert_eq!(
        d.served_secret(kek_source(&second)).await,
        Ok(Some(SECRET.to_owned()))
    );

    let reset = d
        .locked(async |s| key_commands::reset(s, &third, flags(true), true, None).await)
        .await
        .expect("reset");
    let text = outcome_text("reset", &reset);
    assert!(text.contains(ACCESS_KEY_ID), "{text}");
    assert!(text.contains("DEK id: 3"), "{text}");
    assert_eq!(d.served_secret(kek_source(&third)).await, Ok(None));
}

/// `set-data-connection` is refused on SQLite before it reads anything.
#[tokio::test]
async fn set_data_connection_is_refused_on_sqlite() {
    let _ = extenddb_storage::set_backend(extenddb_storage_sqlite::backend());
    let write = super::Write {
        common: super::Common { config: None },
        yes: true,
    };
    let failure = super::set_data_connection(&write)
        .await
        .expect_err("refused");
    assert!(
        failure
            .message
            .contains("this binary serves sqlite, whose catalog has no such row"),
        "{}",
        failure.message
    );
    assert_eq!(failure.code, 1);
}

/// `status` on a configuration whose SQLite path holds no file refuses with
/// the text of a catalog whose migrations are not current, and creates no
/// file there.
#[tokio::test]
async fn status_on_a_path_with_no_catalog_refuses_and_creates_no_file() {
    let _ = extenddb_storage::set_backend(extenddb_storage_sqlite::backend());
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("missing.sqlite");
    let config = dir.path().join("extenddb.toml");
    std::fs::write(
        &config,
        format!(
            "[storage.sqlite]\npath = {:?}\n\n[auth.encryption]\nrequire_locked_memory = false\n",
            path.display().to_string()
        ),
    )
    .expect("config");
    let common = super::Common {
        config: Some(config.to_str().expect("utf-8").to_owned()),
    };
    let failure = super::command(super::Command::Status(common))
        .await
        .expect_err("refused");
    assert_eq!(
        failure.message,
        "the catalog is not initialized. The encryption-key commands run on a catalog whose \
         migrations are current: run `extenddb migrate` first"
    );
    assert_eq!(failure.code, 1);
    let left: Vec<_> = std::fs::read_dir(dir.path())
        .expect("dir")
        .map(|entry| entry.expect("entry").file_name())
        .collect();
    assert_eq!(left, vec![std::ffi::OsString::from("extenddb.toml")]);
}
