// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Tags without a released fixture ship the same PostgreSQL schema files,
//! blob for blob, as the tag of a fixture that stands for them
//! (`tests/fixtures/released/README.md`). Needs `git` and the release tags
//! in the repository; without them it reports a skip and passes.

use std::process::Command;

/// `(tag without a fixture, tag whose fixture stands for it)`.
const SAME_FILES: &[(&str, &str)] = &[
    ("v0.1.1", "v0.1.0"),
    ("v0.1.3", "v0.1.5"),
    ("v0.1.4", "v0.1.5"),
    ("v0.1.7", "v0.1.8"),
    ("v0.1.9", "v0.1.8"),
];

/// The names and blob ids of the tag's schema files, or `None` when git or
/// the tag is not available.
fn schema_files(tag: &str) -> Option<String> {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let out = Command::new("git")
        .args([
            "-C",
            root,
            "ls-tree",
            tag,
            "--",
            "crates/storage-postgres/migrations/",
            "crates/storage-postgres/data_migrations/",
        ])
        .output()
        .ok()?;
    let text = String::from_utf8(out.stdout).ok()?;
    (out.status.success() && !text.trim().is_empty()).then_some(text)
}

#[test]
fn tags_without_a_fixture_ship_the_schema_files_of_one_with_a_fixture() {
    for (tag, stands_for) in SAME_FILES {
        let (Some(files), Some(expected)) = (schema_files(tag), schema_files(stands_for)) else {
            eprintln!("SKIP: git or the release tags {tag} and {stands_for} are not available");
            return;
        };
        assert_eq!(
            files, expected,
            "{tag} and {stands_for} ship different schema files"
        );
    }
}
