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
    // The struct has no `Default`: `contact_points` is required and the
    // sample shows an example, so its value is not compared. `username` and
    // `password` are `Option<String>` shown as examples.
    let expected = CassandraStorageConfig::new(vec!["example.invalid:9042".to_owned()]);
    support::pin(
        &root,
        "storage.cassandra",
        &["contact_points", "username", "password"],
        &expected,
    );
    // The struct does not deny unknown fields, so a stray sub-table would
    // pass deserialization; the section must have none.
    let section = support::table_at(&root, "storage.cassandra");
    assert!(
        support::table_keys(section).is_empty(),
        "[storage.cassandra] has sub-tables the struct does not: {:?}",
        support::table_keys(section)
    );
}
