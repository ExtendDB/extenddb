// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pins the `[storage.postgres]` section of `extenddb.sample.toml` to
//! [`PostgresStorageConfig`](crate::config::PostgresStorageConfig). The other sections are pinned by
//! `extenddb-config`; `docs/configuration.md` is generated from the sample.

use extenddb_storage::config::StorageConfig as _;

use crate::config::PostgresStorageConfig;
use crate::sample_toml_support as support;

#[test]
fn storage_postgres_section_matches_the_struct() {
    let root = support::sample();
    // `catalog_pool_size` is `Option<u32>` that falls back to `pool_size`; the
    // sample shows the fallback as the example, written as an integer, and
    // the example is pinned to what the getter gives when the key is unset.
    support::pin::<PostgresStorageConfig>(&root, "storage.postgres", &["catalog_pool_size"]);
    let unset: PostgresStorageConfig =
        toml::from_str("").expect("empty [storage.postgres] deserializes");
    assert_eq!(
        support::table_at(&root, "storage.postgres")
            .get("catalog_pool_size")
            .and_then(toml::Value::as_integer),
        Some(i64::from(unset.max_catalog_connections())),
        "[storage.postgres] catalog_pool_size example must equal the fallback for an unset key"
    );
}
