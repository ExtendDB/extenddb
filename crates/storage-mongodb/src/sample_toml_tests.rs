// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pins the `[storage.mongodb]` section of `extenddb.sample.toml` to
//! [`MongoStorageConfig`](crate::config::MongoStorageConfig). The other sections are pinned by
//! `extenddb-config`; `docs/configuration.md` is generated from the sample.

use crate::config::MongoStorageConfig;
use crate::sample_toml_support as support;

#[test]
fn storage_mongodb_section_matches_the_struct() {
    let root = support::sample();
    // The struct has no `Default`: `connection_string` is required, so the
    // expected value is the minimal document with the sample's example string.
    // `max_connections` and `max_catalog_connections` are `Option<u32>` whose
    // fallbacks (50 and 20) the sample shows as examples.
    let expected: MongoStorageConfig =
        toml::from_str(r#"connection_string = "mongodb://localhost:27017/?replicaSet=rs0""#)
            .expect("minimal MongoDB config deserializes");
    support::pin(
        &root,
        "storage.mongodb",
        &["max_connections", "max_catalog_connections"],
        &expected,
    );
}
