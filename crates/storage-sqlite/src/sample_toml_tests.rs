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
    // The sample documents the file build's default path. The `memory`
    // feature changes the compiled-in default to `:memory:`, as the sample
    // says, so that build checks the path's type only.
    if cfg!(feature = "memory") {
        let missing: SqliteConfig =
            toml::from_str("").expect("empty [storage.sqlite] deserializes");
        assert_eq!(missing.path, ":memory:");
        support::pin::<SqliteConfig>(&root, "storage.sqlite", &["path"]);
    } else {
        support::pin::<SqliteConfig>(&root, "storage.sqlite", &[]);
    }
}
