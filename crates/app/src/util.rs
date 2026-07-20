// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared utility functions for the extenddb binary crate.

/// Check whether a process is alive using `kill(pid, 0)` (POSIX signal 0).
/// Works on both Linux and macOS (no `/proc` dependency).
pub fn is_process_alive(pid: i32) -> bool {
    // SAFETY: kill with signal 0 performs error checking without sending a
    // signal. Returns 0 if the process exists and we have permission.
    unsafe { libc::kill(pid, 0) == 0 }
}
