// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The PID file's removal when the server process exits by
//! `std::process::exit`, which runs no destructors and skips the graceful
//! shutdown path that otherwise removes the file. A backend ends the process
//! that way when it can no longer hold the catalog for serving (PostgreSQL,
//! when its serving registration is lost). `exit` runs the C library's
//! `atexit` handlers, so the file is removed from one. `atexit` is declared
//! here rather than taken from `libc`, which this crate depends on only on
//! Unix; every target's C runtime provides it, the Windows CRT included.

use std::ffi::c_int;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

unsafe extern "C" {
    /// The C standard library's `atexit`.
    fn atexit(callback: extern "C" fn()) -> c_int;
}

/// The PID file this process wrote; set once.
static PID_FILE: OnceLock<PathBuf> = OnceLock::new();

/// Remove `path` when the process exits, unless it no longer holds this
/// process's id (another server wrote it since). Registers once per process;
/// a second call is ignored.
pub(crate) fn remove_at_exit(path: &Path) {
    if PID_FILE.set(path.to_path_buf()).is_ok() {
        // SAFETY: `remove_pid_file` is a plain `extern "C"` function that
        // takes no arguments, as `atexit` requires; it only reads a static
        // set before registration and removes a file.
        let _ = unsafe { atexit(remove_pid_file) };
    }
}

extern "C" fn remove_pid_file() {
    if let Some(path) = PID_FILE.get() {
        remove_if_ours(path, std::process::id());
    }
}

/// Remove `path` when it holds `pid`.
fn remove_if_ours(path: &Path, pid: u32) {
    let ours = std::fs::read_to_string(path).is_ok_and(|text| text.trim() == pid.to_string());
    if ours {
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::remove_if_ours;

    #[test]
    fn only_a_pid_file_with_this_process_id_is_removed() {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-pid-exit-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("extenddb.pid");
        std::fs::write(&path, "4242\n").expect("write");
        remove_if_ours(&path, 4243);
        assert!(path.exists(), "another server's PID file was removed");
        remove_if_ours(&path, 4242);
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
