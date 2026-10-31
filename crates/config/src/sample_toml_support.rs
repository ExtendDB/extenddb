// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Test support for pinning `extenddb.sample.toml` to the configuration
//! structs. Included as a unit-test module by `extenddb-config` and, through
//! `#[path]`, by each storage backend crate, so there is one copy of the
//! sample's comment convention in the test code.
//!
//! Convention of the sample: every key is one commented line `# key = value`,
//! every section header is `[section]` (or `# [section]`), and every other
//! `#` line is prose. Uncommenting the key and header lines yields a TOML
//! document whose tables hold every key with its default.

// Each including crate uses a subset of these helpers.
#![allow(dead_code)]

use std::collections::BTreeMap;

use toml::{Table, Value};

/// Workspace-root path of the sample, relative to any `crates/<name>` crate.
const SAMPLE_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../extenddb.sample.toml");

/// `# ident = value` with the comment marker removed, or `None`.
fn key_line(line: &str) -> Option<String> {
    let rest = line.strip_prefix("# ")?;
    let (ident, _) = rest.split_once(" = ")?;
    let is_ident = !ident.is_empty()
        && ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !ident.starts_with(|c: char| c.is_ascii_digit());
    is_ident.then(|| rest.to_owned())
}

/// `# [section]` with the comment marker removed, or `None`.
fn header_line(line: &str) -> Option<String> {
    let rest = line.strip_prefix("# ")?;
    (rest.starts_with('[') && rest.ends_with(']')).then(|| rest.to_owned())
}

/// The sample with every key and header line uncommented, parsed as TOML.
pub fn sample() -> Table {
    let text =
        std::fs::read_to_string(SAMPLE_PATH).unwrap_or_else(|e| panic!("read {SAMPLE_PATH}: {e}"));
    let uncommented: Vec<String> = text
        .lines()
        .filter_map(|line| {
            if line.starts_with('[') {
                Some(line.to_owned())
            } else {
                key_line(line).or_else(|| header_line(line))
            }
        })
        .collect();
    uncommented
        .join("\n")
        .parse::<Table>()
        .unwrap_or_else(|e| panic!("uncommented sample is not valid TOML: {e}"))
}

/// The table at a dotted path such as `storage.postgres`, sub-tables included.
pub fn table_at<'a>(root: &'a Table, path: &str) -> &'a Table {
    let mut table = root;
    for part in path.split('.') {
        table = table
            .get(part)
            .and_then(Value::as_table)
            .unwrap_or_else(|| panic!("sample has no [{path}] section"));
    }
    table
}

/// The table at a dotted path such as `storage.postgres`, without its
/// sub-tables, with every `~/` string expanded to `$HOME` (the server does
/// the same at startup) and with the named keys removed. `remove` lists the
/// `Option` fields whose sample line is an example rather than a default.
pub fn section(root: &Table, path: &str, remove: &[&str]) -> Table {
    let table = table_at(root, path);
    let home = std::env::var("HOME").expect("HOME is set");
    let mut out = Table::new();
    for (key, value) in table {
        if value.is_table() || remove.contains(&key.as_str()) {
            continue;
        }
        let value = match value.as_str().and_then(|s| s.strip_prefix("~/")) {
            Some(rest) => Value::String(format!("{home}/{rest}")),
            None => value.clone(),
        };
        out.insert(key.clone(), value);
    }
    out
}

/// A struct's defaults as a TOML table without sub-tables. `None` fields are
/// absent, as the TOML serializer leaves them out.
pub fn defaults<T: serde::Serialize>(value: &T) -> Table {
    let mut table = Value::try_from(value)
        .expect("struct serializes to TOML")
        .as_table()
        .cloned()
        .expect("struct serializes to a table");
    table.retain(|_, v| !v.is_table());
    table
}

/// Assert the sample section and the struct defaults hold the same keys with
/// the same values, naming every key that differs.
pub fn assert_pinned(what: &str, sample: &Table, expected: &Table) {
    let sample: BTreeMap<_, _> = sample.iter().collect();
    let expected: BTreeMap<_, _> = expected.iter().collect();
    let mut problems = Vec::new();
    for (key, value) in &expected {
        match sample.get(key) {
            None => problems.push(format!("{key}: missing from the sample (default {value})")),
            Some(actual) if *actual != *value => {
                problems.push(format!(
                    "{key}: sample has {actual}, struct default is {value}"
                ));
            }
            Some(_) => {}
        }
    }
    for key in sample.keys() {
        if !expected.contains_key(key) {
            problems.push(format!(
                "{key}: in the sample but not a field of the struct"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "[{what}] in extenddb.sample.toml does not match the struct:\n  {}",
        problems.join("\n  ")
    );
}

/// Deserialize the sample section into `T` (so a key the struct lacks fails
/// here when `T` denies unknown fields), then assert its scalar keys equal
/// the defaults of `expected`. `remove` lists the `Option` fields whose
/// sample line is an example rather than a default.
pub fn pin<T>(root: &Table, path: &str, remove: &[&str], expected: &T)
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let full = table_at(root, path).clone();
    let _: T = Value::Table(full)
        .try_into()
        .unwrap_or_else(|e| panic!("[{path}] of extenddb.sample.toml does not deserialize: {e}"));
    assert_pinned(path, &section(root, path, remove), &defaults(expected));
}

/// Names of the non-table keys of a table, sorted.
pub fn scalar_keys(table: &Table) -> Vec<&str> {
    let mut keys: Vec<&str> = table
        .iter()
        .filter(|(_, v)| !v.is_table())
        .map(|(k, _)| k.as_str())
        .collect();
    keys.sort_unstable();
    keys
}
