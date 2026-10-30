// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for `file_mode`.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::{connect, prepare, restrict};

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

/// A configured `sqlite:` URL names the same files as the path in it.
#[test]
fn prepare_and_restrict_accept_a_sqlite_url() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    let location = format!("sqlite://{}?mode=rwc", db.display());
    prepare(&location).expect("prepare");
    assert_eq!(mode(&db), 0o600);
    let wal = sidecar(&db, "-wal");
    std::fs::write(&wal, b"").expect("create");
    set_mode(&db, 0o644);
    set_mode(&wal, 0o644);
    restrict(&location).expect("restrict");
    assert_eq!(mode(&db), 0o600);
    assert_eq!(mode(&wal), 0o600);
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
    // A `file:` URI with its own query (here `?mode=ro`, percent-encoded
    // for sqlx), and a VFS outside the unix family, are left to SQLite.
    prepare(&format!(
        "sqlite:file:{}%3Fmode=ro?mode=rwc",
        dir.join("uri.sqlite").display()
    ))
    .expect("prepare");
    prepare(&format!(
        "sqlite://{}?mode=rwc&vfs=memdb",
        dir.join("vfs.sqlite").display()
    ))
    .expect("prepare");
    prepare(":memory:").expect("prepare");
    prepare("sqlite::memory:").expect("prepare");
    restrict(&plain.to_string_lossy()).expect("restrict");
    assert_eq!(std::fs::read_dir(&dir).expect("read_dir").count(), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Open a pool through `connect` that switches to WAL and writes in its
/// connection setup, and return it with the modes of the database file
/// and its `-wal` and `-shm` files as that setup saw them, which is before
/// `connect` calls `restrict`. With `set_in_setup`, the setup then sets
/// those files to that mode.
async fn connect_and_record(
    db: &Path,
    set_in_setup: Option<u32>,
) -> (sqlx::SqlitePool, Vec<(&'static str, u32)>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let options = {
        let (seen, db) = (Arc::clone(&seen), db.to_path_buf());
        sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(move |conn, _| {
                let (seen, db) = (Arc::clone(&seen), db.clone());
                Box::pin(async move {
                    use sqlx::Executor;
                    conn.execute("PRAGMA journal_mode = WAL").await?;
                    conn.execute("CREATE TABLE IF NOT EXISTS t (k INTEGER PRIMARY KEY)")
                        .await?;
                    let files = ["", "-wal", "-shm"].map(|suffix| (suffix, sidecar(&db, suffix)));
                    let modes = files.clone().map(|(suffix, file)| (suffix, mode(&file)));
                    seen.lock().expect("lock").extend(modes);
                    if let Some(setup_mode) = set_in_setup {
                        for (_, file) in &files {
                            set_mode(file, setup_mode);
                        }
                    }
                    Ok(())
                })
            })
    };
    let pool = connect(&db.to_string_lossy(), options)
        .await
        .expect("connect");
    let seen = seen.lock().expect("lock").clone();
    (pool, seen)
}

/// The database file `connect` creates, and the sidecars its connection
/// creates, are 0600 from the moment they exist.
#[tokio::test]
async fn connect_creates_a_missing_database_and_its_sidecars_at_0600() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    let (pool, seen) = connect_and_record(&db, None).await;
    assert_eq!(seen, [("", 0o600), ("-wal", 0o600), ("-shm", 0o600)]);
    pool.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The rule the fix relies on, whatever the umask: SQLite gives a new
/// `-wal` or `-shm` file the mode of the database file. A database left
/// at 0644 with no sidecars is set to 0600 by `connect` before the
/// connection opens it, so the sidecars the connection creates are 0600.
#[tokio::test]
async fn connect_restricts_an_existing_database_before_sqlite_creates_its_sidecars() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    raw_wal_pool(&db).await.close().await;
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(sidecar(&db, suffix));
    }
    set_mode(&db, 0o644);
    let (pool, seen) = connect_and_record(&db, None).await;
    assert_eq!(seen, [("", 0o600), ("-wal", 0o600), ("-shm", 0o600)]);
    pool.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Whatever the connection leaves behind is set to 0600 once the pool
/// has connected. Here the connection setup itself sets the database file
/// and its sidecars to 0644.
#[tokio::test]
async fn connect_restricts_the_files_after_the_pool_connects() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    let (pool, _) = connect_and_record(&db, Some(0o644)).await;
    for suffix in ["", "-wal", "-shm"] {
        assert_eq!(mode(&sidecar(&db, suffix)), 0o600, "{suffix}");
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

/// Mode calls that record each existing file they are asked to change and
/// fail the change, as `chmod` does on a read-only mount. Nothing outside the
/// test's directory is touched, whoever runs the test.
fn read_only_calls<'a>(existing: &'a dyn Fn(&Path) -> std::io::Result<()>) -> super::ModeCalls<'a> {
    super::ModeCalls {
        created: &super::set_created_owner_only,
        existing,
    }
}

/// Run `f` on another thread and fail the test if it does not return within
/// ten seconds, so a call that blocks shows up as a failure, not a hang.
fn within_ten_seconds(
    f: impl FnOnce() -> Result<(), String> + Send + 'static,
) -> Result<(), String> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(std::time::Duration::from_secs(10))
        .expect("blocked for ten seconds")
}

/// A database path that names a directory (a config with the file name left
/// off) is left to SQLite, which refuses it; the directory keeps its mode.
#[test]
fn a_directory_at_the_database_name_keeps_its_mode() {
    let dir = temp_dir();
    let db_dir = dir.join("dbdir");
    std::fs::create_dir(&db_dir).expect("dbdir");
    set_mode(&db_dir, 0o755);
    for location in [
        db_dir.to_string_lossy().into_owned(),
        format!("{}/", db_dir.display()),
    ] {
        prepare(&location).expect("prepare");
        restrict(&location).expect("restrict");
        assert_eq!(mode(&db_dir), 0o755, "{location}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Only a regular file is changed. A symlink at a sidecar name is not
/// followed (its target keeps its mode), and a FIFO or a socket there keeps
/// its mode; the FIFO, which no writer ever opens, does not make the call
/// wait. The database file itself is narrowed.
#[test]
fn only_regular_files_are_changed_and_nothing_waits() {
    use std::os::unix::ffi::OsStrExt;
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    std::fs::write(&db, b"").expect("db");
    set_mode(&db, 0o644);
    let decoy = dir.join("decoy.txt");
    std::fs::write(&decoy, b"").expect("decoy");
    set_mode(&decoy, 0o644);
    let journal = sidecar(&db, "-journal");
    std::os::unix::fs::symlink(&decoy, &journal).expect("symlink");
    let fifo = sidecar(&db, "-wal");
    let name = std::ffi::CString::new(fifo.as_os_str().as_bytes()).expect("name");
    // SAFETY: `mkfifo` reads the NUL-terminated name, which `name` owns for
    // the duration of the call.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o644) }, 0, "mkfifo");
    set_mode(&fifo, 0o644);
    // A socket node, made with mknod because the test directory's path is
    // longer than a socket address allows; Linux lets any user make one.
    let socket = sidecar(&db, "-shm");
    #[cfg(target_os = "linux")]
    {
        let name = std::ffi::CString::new(socket.as_os_str().as_bytes()).expect("name");
        // SAFETY: `mknod` reads the NUL-terminated name, which `name` owns for
        // the duration of the call.
        let rc = unsafe { libc::mknod(name.as_ptr(), libc::S_IFSOCK | 0o644, 0) };
        assert_eq!(rc, 0, "mknod");
        set_mode(&socket, 0o644);
    }

    let location = db.to_string_lossy().into_owned();
    let (for_prepare, for_restrict) = (location.clone(), location);
    within_ten_seconds(move || prepare(&for_prepare)).expect("prepare");
    within_ten_seconds(move || restrict(&for_restrict)).expect("restrict");

    assert_eq!(mode(&db), 0o600);
    assert_eq!(mode(&decoy), 0o644, "the symlink at -journal was followed");
    let link = std::fs::symlink_metadata(&journal).expect("lstat -journal");
    assert!(link.file_type().is_symlink(), "-journal replaced");
    assert_eq!(mode(&fifo), 0o644, "the FIFO at -wal was changed");
    #[cfg(target_os = "linux")]
    assert_eq!(mode(&socket), 0o644, "the socket at -shm was changed");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A regular file wider than 0600 whose mode cannot be changed (here the
/// change fails with EROFS) is an error naming the file, and stays as it was.
#[test]
fn a_file_that_cannot_be_narrowed_is_an_error() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    std::fs::write(&db, b"").expect("db");
    set_mode(&db, 0o644);
    let fail = |_: &Path| Err(std::io::Error::from_raw_os_error(libc::EROFS));
    let calls = read_only_calls(&fail);
    let location = db.to_string_lossy().into_owned();
    for result in [
        super::prepare_with(&location, &calls),
        super::restrict_with(&location, &calls),
    ] {
        let err = result.expect_err("a mode that cannot be set is accepted");
        assert!(
            err.starts_with("cannot set the mode of SQLite file ")
                && err.contains(&*db.to_string_lossy())
                && err.contains("Read-only file system"),
            "{err}"
        );
    }
    assert_eq!(mode(&db), 0o644);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Files already 0600, or narrower, are not changed at all, so a read-only
/// mount whose files are already owner-only still opens.
#[test]
fn owner_only_files_are_left_alone() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    for (suffix, file_mode) in [("", 0o600), ("-wal", 0o600), ("-shm", 0o400)] {
        std::fs::write(sidecar(&db, suffix), b"").expect("create");
        set_mode(&sidecar(&db, suffix), file_mode);
    }
    let asked = Mutex::new(Vec::new());
    let fail = |file: &Path| {
        asked.lock().expect("lock").push(file.to_path_buf());
        Err(std::io::Error::from_raw_os_error(libc::EROFS))
    };
    let calls = read_only_calls(&fail);
    let location = db.to_string_lossy().into_owned();
    super::prepare_with(&location, &calls).expect("prepare");
    super::restrict_with(&location, &calls).expect("restrict");
    assert_eq!(*asked.lock().expect("lock"), Vec::<PathBuf>::new());
    assert_eq!(mode(&db), 0o600);
    assert_eq!(mode(&sidecar(&db, "-shm")), 0o400);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Whether a write lock on `len` bytes at `start` of `file` would conflict
/// with a lock already held there. The question is asked with an open file
/// description lock, which conflicts with this process's own POSIX locks as
/// well as other processes'. Closing the descriptor afterwards releases this
/// process's POSIX locks on the file, so ask once per file, last.
#[cfg(target_os = "linux")]
fn write_lock_conflicts(file: &Path, start: i64, len: i64) -> bool {
    use std::os::fd::AsRawFd;
    let probe = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file)
        .expect("open");
    // SAFETY: `flock` is plain data, for which all zero bytes is a value.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as libc::c_short;
    lock.l_whence = libc::SEEK_SET as libc::c_short;
    lock.l_start = start;
    lock.l_len = len;
    // SAFETY: F_OFD_GETLK reads and writes the `flock` that `lock` owns; the
    // descriptor belongs to `probe`, which lives until the call returns.
    let rc = unsafe { libc::fcntl(probe.as_raw_fd(), libc::F_OFD_GETLK, &mut lock) };
    assert_eq!(rc, 0, "F_OFD_GETLK: {}", std::io::Error::last_os_error());
    lock.l_type != libc::F_UNLCK as libc::c_short
}

/// Narrowing the files of a database this process has open keeps the
/// connection's POSIX locks: the read lock a WAL connection holds on the
/// database file's shared range and the one on the `-shm` file's DMS byte.
/// A closed read descriptor on either file would release them, and another
/// process could then reset the `-shm` file or delete the WAL under the open
/// connection.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn narrowing_keeps_the_locks_of_an_open_connection() {
    let dir = temp_dir();
    let db = dir.join("db.sqlite");
    let pool = raw_wal_pool(&db).await;
    for suffix in ["", "-wal", "-shm"] {
        set_mode(&sidecar(&db, suffix), 0o644);
    }
    restrict(&db.to_string_lossy()).expect("restrict");
    for suffix in ["", "-wal", "-shm"] {
        assert_eq!(mode(&sidecar(&db, suffix)), 0o600, "{suffix}");
    }
    assert!(
        write_lock_conflicts(&db, 0x4000_0002, 510),
        "the connection's lock on the database file was released"
    );
    assert!(
        write_lock_conflicts(&sidecar(&db, "-shm"), 128, 1),
        "the connection's lock on the -shm file was released"
    );
    pool.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}
