// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Owner-only modes for a SQLite database file and the files SQLite keeps
//! beside it.
//!
//! The database stores the AES key next to the secrets it encrypts
//! (access-key secrets, password hashes), so neither the file nor its `-wal`,
//! `-shm` and `-journal` files may be readable by other local users. SQLite's
//! unix VFS creates a database file with mode 0644 less the umask, and creates
//! each sidecar with the mode of the database file. So every pool opener on a
//! file database calls [`prepare`] before it connects, which creates the
//! database file with mode 0600 (or sets an existing one to 0600) before the
//! connection can create a sidecar from it, and [`restrict`] after it
//! connects, which sets the file and every sidecar present to 0600 whoever
//! created them.
//!
//! As with the lock file in `serve_lock`, a mode that cannot be set is an
//! error. The directory's mode is left alone. On platforms other than Unix
//! both functions do nothing.

#[cfg(unix)]
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::str::FromStr;

#[cfg(unix)]
use crate::serve_lock::{database_file, resolve_dangling_symlinks};
#[cfg(unix)]
use crate::sqlite_util::sqlite_url;

/// Before a pool connects to `location`: create the database file with mode
/// 0600 if the connection would create it, then set the file and any sidecar
/// left by an earlier run to 0600, so the sidecars this connection creates
/// take mode 0600 from the database file.
///
/// A location that names no file on this machine (in-memory, unparseable, a
/// remote `file:` authority) is left to the connection, which reports it in
/// its own terms.
///
/// # Errors
///
/// A file's mode cannot be set.
#[cfg(unix)]
pub(crate) fn prepare(location: &str) -> Result<(), String> {
    let Ok(Some(path)) = database_file(location) else {
        return Ok(());
    };
    if open_creates_file(location) {
        create_owner_only(&resolve_dangling_symlinks(&path))?;
    }
    restrict_files(&path)
}

#[cfg(not(unix))]
pub(crate) fn prepare(_location: &str) -> Result<(), String> {
    Ok(())
}

/// After a pool connects to `location`: set the database file and every
/// `-wal`, `-shm` and `-journal` file present to mode 0600.
///
/// # Errors
///
/// A file's mode cannot be set.
#[cfg(unix)]
pub(crate) fn restrict(location: &str) -> Result<(), String> {
    match database_file(location) {
        Ok(Some(path)) => restrict_files(&path),
        _ => Ok(()),
    }
}

#[cfg(not(unix))]
pub(crate) fn restrict(_location: &str) -> Result<(), String> {
    Ok(())
}

/// Whether connecting to `location` creates the database file when it is
/// missing. sqlx reads the pairs after the first `?` as form-urlencoded and
/// passes `SQLITE_OPEN_CREATE` for `mode=rwc` unless `mode=ro` is also given;
/// a plain path always gets `mode=rwc` from `sqlite_url`. Two forms are left
/// to SQLite because they can keep it from creating a file on disk: a `file:`
/// URI with its own query (its `mode` can narrow the open) and a VFS outside
/// the unix family.
#[cfg(unix)]
fn open_creates_file(location: &str) -> bool {
    let url = sqlite_url(location);
    let Some((_, query)) = url.split_once('?') else {
        return false;
    };
    let (mut rwc, mut ro, mut unix_vfs) = (false, false, true);
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match &*key {
            "mode" => {
                rwc |= value == "rwc";
                ro |= value == "ro";
            }
            "vfs" => unix_vfs &= value.starts_with("unix"),
            _ => {}
        }
    }
    if !rwc || ro || !unix_vfs {
        return false;
    }
    sqlx::sqlite::SqliteConnectOptions::from_str(&url).is_ok_and(|options| {
        let name = options.get_filename().to_string_lossy();
        !(name.starts_with("file:") && name.contains('?'))
    })
}

/// Create `path` with mode 0600 if nothing is there yet.
#[cfg(unix)]
fn create_owner_only(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        // The umask applies to `mode` and can remove owner bits as well.
        Ok(file) => file
            .set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|e| {
                format!(
                    "cannot set the mode of SQLite database {}: {e}",
                    path.display()
                )
            }),
        // The file exists, or cannot be created here. SQLite's own open
        // decides what that means and reports it in its usual terms.
        Err(_) => Ok(()),
    }
}

/// Set the database file and each sidecar that exists to mode 0600.
#[cfg(unix)]
fn restrict_files(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    // SQLite follows symlinks before it names the sidecars, so they sit next
    // to the target, not next to the link.
    let base = std::fs::canonicalize(path).unwrap_or_else(|_| resolve_dangling_symlinks(path));
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut name = base.clone().into_os_string();
        name.push(suffix);
        let file = PathBuf::from(name);
        match std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(format!(
                    "cannot set the mode of SQLite file {}: {e}",
                    file.display()
                ));
            }
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    use super::{prepare, restrict};

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-file-mode-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("dir");
        dir
    }

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path)
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
            .permissions()
            .mode()
            & 0o777
    }

    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }

    fn sidecar(db: &Path, suffix: &str) -> PathBuf {
        let mut name = db.as_os_str().to_owned();
        name.push(suffix);
        PathBuf::from(name)
    }

    /// A pool opened the way the crate's openers open theirs (WAL journal),
    /// without `prepare` or `restrict`, holding its connection so the
    /// sidecars stay on disk.
    async fn raw_wal_pool(path: &Path) -> sqlx::SqlitePool {
        use sqlx::Executor;
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    conn.execute("PRAGMA journal_mode = WAL").await?;
                    Ok(())
                })
            })
            .connect(&crate::sqlite_util::sqlite_url(&path.to_string_lossy()))
            .await
            .expect("connect");
        sqlx::query("CREATE TABLE IF NOT EXISTS t (k INTEGER PRIMARY KEY)")
            .execute(&pool)
            .await
            .expect("write");
        pool
    }

    #[test]
    fn prepare_creates_a_missing_database_with_mode_0600() {
        let dir = temp_dir();
        let db = dir.join("db.sqlite");
        prepare(&db.to_string_lossy()).expect("prepare");
        assert_eq!(mode(&db), 0o600);
        assert_eq!(std::fs::metadata(&db).expect("metadata").len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepare_and_restrict_set_an_existing_database_and_its_sidecars_to_0600() {
        let dir = temp_dir();
        let db = dir.join("db.sqlite");
        let files: Vec<PathBuf> = ["", "-wal", "-shm", "-journal"]
            .iter()
            .map(|suffix| sidecar(&db, suffix))
            .collect();
        for file in &files {
            std::fs::write(file, b"").expect("create");
            set_mode(file, 0o644);
        }
        prepare(&db.to_string_lossy()).expect("prepare");
        for file in &files {
            assert_eq!(mode(file), 0o600, "{}", file.display());
        }
        for file in &files {
            set_mode(file, 0o644);
        }
        restrict(&db.to_string_lossy()).expect("restrict");
        for file in &files {
            assert_eq!(mode(file), 0o600, "{}", file.display());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepare_creates_nothing_when_the_connection_would_not() {
        let dir = temp_dir();
        let plain = dir.join("plain.sqlite");
        let read_only = dir.join("ro.sqlite");
        // A sqlite: URL is passed to sqlx as given: without mode=rwc sqlx does
        // not pass SQLITE_OPEN_CREATE, and mode=ro wins over mode=rwc.
        prepare(&format!("sqlite://{}", plain.display())).expect("prepare");
        prepare(&format!("sqlite://{}?mode=ro", read_only.display())).expect("prepare");
        prepare(&format!(
            "sqlite://{}?mode=rwc&mode=ro",
            read_only.display()
        ))
        .expect("prepare");
        prepare(":memory:").expect("prepare");
        prepare("sqlite::memory:").expect("prepare");
        restrict(&plain.to_string_lossy()).expect("restrict");
        assert_eq!(std::fs::read_dir(&dir).expect("read_dir").count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The rule the fix relies on: SQLite gives a new `-wal` or `-shm` file
    /// the mode of the database file, so a database created at 0600 gets
    /// 0600 sidecars without any chmod after the fact.
    #[tokio::test]
    async fn sidecars_created_after_prepare_take_mode_0600() {
        let dir = temp_dir();
        let db = dir.join("db.sqlite");
        prepare(&db.to_string_lossy()).expect("prepare");
        let pool = raw_wal_pool(&db).await;
        for suffix in ["", "-wal", "-shm"] {
            let file = sidecar(&db, suffix);
            assert!(file.exists(), "{} missing", file.display());
            assert_eq!(mode(&file), 0o600, "{}", file.display());
        }
        pool.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn prepare_creates_the_target_of_a_dangling_symlink() {
        let dir = temp_dir();
        let target = dir.join("real.sqlite");
        let link = dir.join("link.sqlite");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");
        let location = link.to_string_lossy().into_owned();
        prepare(&location).expect("prepare");
        assert!(target.exists(), "target not created");
        assert_eq!(mode(&target), 0o600);
        // SQLite names the sidecars after the target, and they take its mode.
        let pool = raw_wal_pool(&link).await;
        for suffix in ["-wal", "-shm"] {
            let file = sidecar(&target, suffix);
            assert!(file.exists(), "{} missing", file.display());
            assert_eq!(mode(&file), 0o600, "{}", file.display());
        }
        set_mode(&sidecar(&target, "-wal"), 0o644);
        restrict(&location).expect("restrict");
        assert_eq!(mode(&sidecar(&target, "-wal")), 0o600);
        pool.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
