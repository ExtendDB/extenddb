// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pins the `[storage.postgres]` section of `extenddb.sample.toml` to
//! [`PostgresStorageConfig`](crate::config::PostgresStorageConfig). The other sections are pinned by
//! `extenddb-config`; `docs/configuration.md` is generated from the sample.

use crate::config::PostgresStorageConfig;
use crate::sample_toml_support as support;

#[test]
fn storage_postgres_section_matches_the_struct() {
    let root = support::sample();
    // `catalog_pool_size` is `Option<u32>` that falls back to `pool_size`; the
    // sample shows the fallback as the example, written as an integer.
    support::pin(
        &root,
        "storage.postgres",
        &["catalog_pool_size"],
        &PostgresStorageConfig::default(),
    );
    support::assert_integers(&root, "storage.postgres", &["catalog_pool_size"]);
}
