// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pins the `[storage.sqlite]` section of `extenddb.sample.toml` to
//! [`SqliteConfig`](crate::config::SqliteConfig). The other sections are pinned by
//! `extenddb-config`; `docs/configuration.md` is generated from the sample.

use crate::config::SqliteConfig;
use crate::sample_toml_support as support;

#[test]
fn storage_sqlite_section_matches_the_struct() {
    let root = support::sample();
    // The sample documents the file default; the `memory` feature changes the
    // compiled-in default to `:memory:`.
    let mut expected = SqliteConfig::default();
    if cfg!(feature = "memory") {
        assert_eq!(expected.path, ":memory:");
        expected.path = "extenddb.sqlite".to_owned();
    }
    support::pin(&root, "storage.sqlite", &[], &expected);
}
