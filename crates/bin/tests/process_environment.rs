// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `load_builtin_defaults` and `load` over the environment of this process.
//! The test sets variables of the process, which is `unsafe` while another
//! thread can read the environment, so it is the only test in this binary.

#![cfg(all(unix, any(feature = "postgres", feature = "sqlite")))]

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

/// A variable without the `EXTENDDB__` prefix may hold bytes that are not
/// UTF-8, in its name or in its value: both loaders leave it out and apply
/// the `EXTENDDB__` variables.
#[test]
fn loaders_skip_other_variables_that_are_not_utf8() {
    #[cfg(all(feature = "postgres", not(feature = "sqlite")))]
    let backend = extenddb_storage_postgres::backend();
    #[cfg(feature = "sqlite")]
    let backend = extenddb_storage_sqlite::backend();
    let section = format!("[storage.{}]\n", backend.name);
    extenddb_storage::set_backend(backend).expect("the only backend this binary installs");
    // SAFETY: this is the only test in the binary, and no other thread of the
    // process reads or writes the environment while it runs.
    unsafe {
        std::env::set_var("OTHER_VALUE_NOT_UTF8", OsStr::from_bytes(b"\xff"));
        std::env::set_var(OsStr::from_bytes(b"OTHER_NAME_NOT_UTF8_\xff"), "1");
        std::env::set_var("EXTENDDB__SERVER__PORT", "18977");
    }

    let defaults = extenddb_config::load_builtin_defaults().expect("load_builtin_defaults");
    assert_eq!(defaults.server.port, 18977, "load_builtin_defaults");

    let file = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("process_environment.toml");
    std::fs::write(&file, section).expect("write the config file");
    let loaded = extenddb_config::load(file.to_str().expect("a UTF-8 path"));
    std::fs::remove_file(&file).expect("remove the config file");
    assert_eq!(loaded.expect("load").server.port, 18977, "load");
}
