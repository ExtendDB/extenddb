// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The PID file that `extenddb status` and `extenddb stop` read.
//!
//! A running server holds an exclusive `flock` on its PID file: `daemonize`
//! takes it in daemon mode, and [`hold`] takes it in foreground mode. A reader
//! that finds the lock free knows that no server owns the file, so the PID in
//! it is stale even when a process with that PID is alive, for example after
//! the server crashed and the system gave its PID to another process.
//!
//! `flock` locks belong to open file descriptions, not to processes, so two
//! descriptors of the same file conflict even inside one process. [`read`]
//! takes the PID and the lock state from one descriptor, and [`hold`] writes a
//! new file and renames it into place, so a reader sees either an old file no
//! process holds or the server's file, complete and held. In a run directory
//! the server cannot write, [`hold`] rewrites an existing file in place.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

/// What [`hold`] did with the PID file.
#[derive(Debug)]
pub enum Hold {
    /// This process wrote the file and holds its lock for as long as the
    /// `File` stays open.
    Taken(File),
    /// The file already names this process and is locked: in daemon mode
    /// `daemonize` wrote it and keeps the lock on its own descriptor.
    Daemonized,
    /// Another running process holds the file, for example a server on the
    /// same port bound to another address. The file is left as it is; the
    /// value is the PID it names.
    HeldByOther(String),
}

/// A PID file as one open of it saw it.
#[derive(Debug)]
pub struct Contents {
    /// The file's text, normally a PID and a newline.
    pub text: String,
    /// Whether a process held the file's lock, that is, whether a running
    /// server owns it.
    pub held: bool,
    dev: u64,
    ino: u64,
}

/// Write this process's PID to `path` and hold an exclusive lock on the file.
///
/// The PID goes to a new file that is renamed into place. When the run
/// directory cannot be written but the PID file can, as when an init system
/// created the file for the server, the file is rewritten in place, as
/// earlier releases did.
///
/// # Errors
///
/// Returns the I/O error when the existing file cannot be read or the new
/// file cannot be created, locked, written, or renamed into place.
pub fn hold(path: &Path) -> io::Result<Hold> {
    let pid = std::process::id().to_string();
    match read(path) {
        Ok(existing) if existing.held => {
            let named = existing.text.trim();
            return Ok(if named == pid {
                Hold::Daemonized
            } else {
                Hold::HeldByOther(named.to_owned())
            });
        }
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let tmp = path.with_extension(format!("pid.{pid}.tmp"));
    let write = || -> io::Result<File> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        try_lock(&file, libc::LOCK_EX)?;
        file.write_all(format!("{pid}\n").as_bytes())?;
        std::fs::rename(&tmp, path)?;
        Ok(file)
    };
    match write() {
        Ok(file) => Ok(Hold::Taken(file)),
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            if e.kind() == io::ErrorKind::PermissionDenied && path.exists() {
                return rewrite(path, &pid).map(Hold::Taken);
            }
            Err(e)
        }
    }
}

/// Lock the existing file at `path` and replace its text with `pid`. A reader
/// that read the old text can then remove the file this server holds, which
/// the rename avoids, but only a process that can write the directory can
/// remove a file in it.
fn rewrite(path: &Path, pid: &str) -> io::Result<File> {
    let mut file = OpenOptions::new().write(true).open(path)?;
    try_lock(&file, libc::LOCK_EX)?;
    file.set_len(0)?;
    file.write_all(format!("{pid}\n").as_bytes())?;
    Ok(file)
}

/// Read the PID file at `path` and probe its lock through the same open.
///
/// # Errors
///
/// Returns the I/O error when the file cannot be opened, read, or probed; a
/// missing file is `NotFound`.
pub fn read(path: &Path) -> io::Result<Contents> {
    let mut file = File::open(path)?;
    let held = match try_lock(&file, libc::LOCK_SH) {
        Ok(()) => {
            // Free, so no server owns it. Release the shared lock at once. A
            // foreground server locks its new file before renaming it into
            // place, so it never meets this lock. `daemonize` opens the file
            // at its path and then locks it without waiting, so a daemon that
            // starts in the two calls this lock lasts fails to start.
            unlock(&file)?;
            false
        }
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => true,
        Err(e) => return Err(e),
    };
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    let meta = file.metadata()?;
    Ok(Contents {
        text,
        held,
        dev: meta.dev(),
        ino: meta.ino(),
    })
}

/// Remove the stale PID file at `path`, unless the path now names another
/// file than the one `contents` was read from, as it does once a new server
/// has renamed its own file into place.
///
/// # Errors
///
/// Returns the I/O error of the removal; a file already gone is not an error.
pub fn remove_stale(path: &Path, contents: &Contents) -> io::Result<()> {
    match std::fs::metadata(path) {
        Ok(meta) if meta.dev() == contents.dev && meta.ino() == contents.ino => {
            match std::fs::remove_file(path) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
                _ => Ok(()),
            }
        }
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn try_lock(file: &File, operation: libc::c_int) -> io::Result<()> {
    // SAFETY: `flock` reads only the descriptor, which `file` keeps open for
    // the duration of the call.
    if unsafe { libc::flock(file.as_raw_fd(), operation | libc::LOCK_NB) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn unlock(file: &File) -> io::Result<()> {
    // SAFETY: as in `try_lock`.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;

    use super::{Hold, hold, read, remove_stale};

    fn scratch(name: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-pid-file-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir(&dir).expect("scratch dir");
        let path = dir.join("extenddb-18443.pid");
        (dir, path)
    }

    fn own_pid_line() -> String {
        format!("{}\n", std::process::id())
    }

    /// A file locked on its own descriptor, as `daemonize` or another server
    /// leaves it.
    fn locked_file(path: &PathBuf, text: &str) -> File {
        let file = File::create(path).unwrap();
        std::fs::write(path, text).unwrap();
        // SAFETY: `file` keeps the descriptor open for the call.
        assert_eq!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        file
    }

    fn taken(result: Hold) -> File {
        match result {
            Hold::Taken(file) => file,
            other => panic!("expected Taken, got {other:?}"),
        }
    }

    /// The file names this process while it is held, and a reader sees the
    /// lock until the holder is dropped.
    #[test]
    fn a_held_pid_file_is_seen_as_held_until_it_is_dropped() {
        let (dir, path) = scratch("held");
        let held = taken(hold(&path).expect("hold"));
        let seen = read(&path).unwrap();
        assert_eq!(seen.text, own_pid_line());
        assert!(seen.held);
        drop(held);
        assert!(!read(&path).unwrap().held);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A PID file left by a server that is gone holds no lock, whatever PID it
    /// names, and a new server replaces it with a new file.
    #[test]
    fn a_stale_pid_file_is_free_and_is_replaced() {
        let (dir, path) = scratch("stale");
        std::fs::write(&path, "1\n").unwrap();
        let old_ino = std::fs::metadata(&path).unwrap().ino();
        assert!(!read(&path).unwrap().held);
        let held = taken(hold(&path).expect("hold"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), own_pid_line());
        assert_ne!(std::fs::metadata(&path).unwrap().ino(), old_ino);
        assert!(read(&path).unwrap().held);
        drop(held);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A PID file the server can write, in a run directory it cannot, is
    /// rewritten in place and held.
    #[test]
    fn a_writable_file_in_a_read_only_directory_is_rewritten_in_place() {
        use std::os::unix::fs::PermissionsExt;

        // Root writes any directory, so the rename path would run instead.
        // SAFETY: `geteuid` takes no arguments and cannot fail.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let (dir, path) = scratch("read-only-dir");
        std::fs::write(&path, "1\n").unwrap();
        let ino = std::fs::metadata(&path).unwrap().ino();
        let mode = |m| std::fs::Permissions::from_mode(m);
        std::fs::set_permissions(&dir, mode(0o555)).unwrap();
        let result = hold(&path);
        std::fs::set_permissions(&dir, mode(0o755)).unwrap();
        let held = taken(result.expect("hold"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), own_pid_line());
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), ino);
        assert!(read(&path).unwrap().held);
        drop(held);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A free file that names this process by coincidence, after the system
    /// reused the PID of an earlier server, is not mistaken for daemonize's.
    #[test]
    fn a_free_file_naming_this_process_is_taken_over() {
        let (dir, path) = scratch("own-stale");
        std::fs::write(&path, own_pid_line()).unwrap();
        let held = taken(hold(&path).expect("hold"));
        assert!(read(&path).unwrap().held);
        drop(held);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// In daemon mode `daemonize` has already written this PID and keeps the
    /// lock on its own descriptor; `hold` leaves both alone.
    #[test]
    fn a_file_daemonize_holds_for_this_process_is_left_to_it() {
        let (dir, path) = scratch("daemon");
        let daemon = locked_file(&path, &own_pid_line());
        assert!(matches!(hold(&path).expect("hold"), Hold::Daemonized));
        assert!(read(&path).unwrap().held);
        drop(daemon);
        assert!(!read(&path).unwrap().held);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A file another running process holds is not touched.
    #[test]
    fn a_file_another_process_holds_is_left_alone() {
        let (dir, path) = scratch("other");
        let other = locked_file(&path, "424242\n");
        match hold(&path).expect("hold") {
            Hold::HeldByOther(pid) => assert_eq!(pid, "424242"),
            other => panic!("expected HeldByOther, got {other:?}"),
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "424242\n");
        drop(other);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A stale file is removed, but not a new file that a server renamed into
    /// its place after the stale one was read.
    #[test]
    fn remove_stale_removes_only_the_file_that_was_read() {
        let (dir, path) = scratch("remove");
        std::fs::write(&path, "1\n").unwrap();
        let stale = read(&path).unwrap();
        let held = taken(hold(&path).expect("hold"));
        remove_stale(&path, &stale).unwrap();
        assert!(path.exists(), "the new server's file must stay");
        drop(held);
        let stale = read(&path).unwrap();
        remove_stale(&path, &stale).unwrap();
        assert!(!path.exists());
        remove_stale(&path, &stale).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_missing_pid_file_is_not_found() {
        let (dir, path) = scratch("missing");
        assert_eq!(
            read(&path).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
