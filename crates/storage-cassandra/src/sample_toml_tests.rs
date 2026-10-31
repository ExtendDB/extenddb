// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pins the `[storage.cassandra]` section of `extenddb.sample.toml` to
//! [`CassandraStorageConfig`](crate::config::CassandraStorageConfig). The other sections are pinned by
//! `extenddb-config`; `docs/configuration.md` is generated from the sample.

use crate::config::CassandraStorageConfig;
use crate::sample_toml_support as support;

#[test]
fn storage_cassandra_section_matches_the_struct() {
    let root = support::sample();
    // The struct has no `Default`: `contact_points` is required, so the
    // expected value is `new` with the sample's example contact point.
    // `username` and `password` are `Option<String>` shown as examples.
    let expected = CassandraStorageConfig::new(vec!["localhost:9042".to_owned()]);
    support::pin(
        &root,
        "storage.cassandra",
        &["username", "password"],
        &expected,
    );
}
