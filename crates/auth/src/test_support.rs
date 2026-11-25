// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helpers for the tests of this crate.

use std::process::Output;

/// Run the test at `test_path` (its full module path) in a child process,
/// with `env` set to `1`, and return its output. The locked key store is set
/// up once per process, so a test that sets it up runs in a child of its own:
/// the child test function returns at once when `env` is unset, and does its
/// work when it is set.
pub(crate) fn run_test_in_child(test_path: &str, env: &str) -> Output {
    std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", test_path, "--nocapture", "--test-threads=1"])
        .env(env, "1")
        .output()
        .expect("child")
}

/// Whether this is the child process of [`run_test_in_child`] for `env`.
pub(crate) fn is_child(env: &str) -> bool {
    std::env::var_os(env).is_some()
}
