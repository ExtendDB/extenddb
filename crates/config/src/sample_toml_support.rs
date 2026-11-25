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
//!
//! [`pin`] checks a section three ways: the table deserializes into the
//! struct; the set of keys equals the struct's field names, read from its
//! derived `Deserialize` so that `Option` fields and struct-valued fields
//! count too; and every key's value equals the value serde gives the field
//! when the key is missing, except the named example keys, whose value is
//! only type checked. A field added to a struct without a sample line fails
//! here, `Option`, struct or not.

// Each including crate uses a subset of these helpers.
#![allow(dead_code)]

use std::collections::BTreeSet;

use serde::de::{DeserializeOwned, Error as _, Visitor};
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
/// the same at startup) and with the named keys removed.
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

/// A struct's field values as a TOML table without sub-tables. `None` fields
/// are absent, as the TOML serializer leaves them out.
pub fn defaults<T: serde::Serialize>(value: &T) -> Table {
    let mut table = Value::try_from(value)
        .expect("struct serializes to TOML")
        .as_table()
        .cloned()
        .expect("struct serializes to a table");
    table.retain(|_, v| !v.is_table());
    table
}

/// A `Deserializer` that records the field list a derived `Deserialize`
/// passes to `deserialize_struct`, then stops. It needs no value of the
/// struct, so it works for `AppConfig`, whose `storage` field cannot be
/// built in this crate.
struct FieldRecorder<'a>(&'a mut Option<&'static [&'static str]>);

impl<'de> serde::Deserializer<'de> for FieldRecorder<'_> {
    type Error = serde::de::value::Error;

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        *self.0 = Some(fields);
        Err(Self::Error::custom("field list recorded"))
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(Self::Error::custom(
            "not a struct with a derived Deserialize",
        ))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map enum identifier ignored_any
    }
}

/// The field names of `T` as its derived `Deserialize` declares them: the
/// serde name of a renamed field, `Option` and struct-valued fields included,
/// `#[serde(skip)]` fields left out. The derive's list also holds every
/// `#[serde(alias)]` name, so `pin` cannot pass for a struct with an alias.
/// Without a sample line for the alias, `pin` reports the alias as missing.
/// With one, the derived `Deserialize` refuses the duplicate field. A struct
/// with a `#[serde(flatten)]` field passes no list, and this panics.
pub fn struct_fields<T: DeserializeOwned>() -> BTreeSet<String> {
    let mut fields = None;
    let _ = T::deserialize(FieldRecorder(&mut fields));
    fields
        .expect("T has a derived Deserialize")
        .iter()
        .map(|field| (*field).to_owned())
        .collect()
}

/// Assert the sample section holds exactly the struct's fields, and that the
/// value of every field not named in `examples` equals the value a missing
/// key gives.
pub fn assert_pinned(
    what: &str,
    sample: &Table,
    fields: &BTreeSet<String>,
    defaults: &Table,
    examples: &[&str],
) {
    let mut problems = Vec::new();
    for field in fields {
        if !sample.contains_key(field) {
            problems.push(format!(
                "{field}: field of the struct, missing from the sample"
            ));
        }
    }
    for (key, actual) in sample {
        if !fields.contains(key) {
            problems.push(format!(
                "{key}: in the sample but not a field of the struct"
            ));
        } else if !examples.contains(&key.as_str()) {
            match defaults.get(key) {
                Some(value) if value == actual => {}
                Some(value) => problems.push(format!(
                    "{key}: sample has {actual}, a missing key gives {value}"
                )),
                None => problems.push(format!(
                    "{key}: a missing key gives None; list it as an example key"
                )),
            }
        }
    }
    for example in examples {
        assert!(
            fields.contains(*example),
            "[{what}] example key {example} is not a field of the struct"
        );
    }
    assert!(
        problems.is_empty(),
        "[{what}] in extenddb.sample.toml does not match the struct:\n  {}",
        problems.join("\n  ")
    );
}

/// Pin the sample section at `path` to `T`: the table deserializes into `T`
/// (a key the struct lacks fails here when `T` denies unknown fields), its
/// keys are exactly the field names of `T`'s derived `Deserialize`, and
/// every value equals the value serde fills in for a missing key, except for
/// the `examples`, whose sample line is an example rather than a default and
/// whose value is only type checked by the deserialization. The expected
/// values come from deserializing a table that holds only the `examples`
/// (the required keys are among them), so a `#[serde(default = ...)]`
/// function that disagrees with the sample fails here. A field the sample
/// writes as a sub-table (an `Option` of a struct, `None` when missing) is
/// left to the pin of that sub-table. Returns the field set it compared.
pub fn pin<T>(root: &Table, path: &str, examples: &[&str]) -> BTreeSet<String>
where
    T: DeserializeOwned + serde::Serialize,
{
    let full = table_at(root, path).clone();
    let mut fields = struct_fields::<T>();
    fields.retain(|field| !full.get(field).is_some_and(Value::is_table));
    let _: T = Value::Table(full.clone())
        .try_into()
        .unwrap_or_else(|e| panic!("[{path}] of extenddb.sample.toml does not deserialize: {e}"));
    let seed: Table = full
        .iter()
        .filter(|(k, _)| examples.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let expected: T = Value::Table(seed).try_into().unwrap_or_else(|e| {
        panic!("[{path}] of extenddb.sample.toml: the example keys alone do not deserialize: {e}")
    });
    assert_pinned(
        path,
        &section(root, path, &[]),
        &fields,
        &defaults(&expected),
        examples,
    );
    fields
}

/// [`pin`], then the same comparison with `T::default()`: the value of every
/// field when the file leaves the whole section out, which the parent
/// field's `#[serde(default)]` fills in.
pub fn pin_with_default<T>(root: &Table, path: &str, examples: &[&str])
where
    T: Default + DeserializeOwned + serde::Serialize,
{
    let fields = pin::<T>(root, path, examples);
    assert_pinned(
        &format!("{path} (section left out)"),
        &section(root, path, &[]),
        &fields,
        &defaults(&T::default()),
        examples,
    );
}

/// Assert the named keys of the sample section carry integer values (the
/// example value of a numeric key must not be a string).
pub fn assert_integers(root: &Table, path: &str, keys: &[&str]) {
    let table = table_at(root, path);
    for key in keys {
        let value = table
            .get(*key)
            .unwrap_or_else(|| panic!("[{path}] has no key {key}"));
        assert!(
            value.is_integer(),
            "[{path}] {key} must be an integer in the sample, found {value}"
        );
    }
}

/// Dotted paths of the keys whose description (the `#` lines directly above
/// the key line) carries the "no default" marker the generator reads, sorted.
/// Follows the sample's comment convention the same way the generator does.
pub fn no_default_keys() -> Vec<String> {
    let text =
        std::fs::read_to_string(SAMPLE_PATH).unwrap_or_else(|e| panic!("read {SAMPLE_PATH}: {e}"));
    let mut section = String::new();
    let mut description: Vec<&str> = Vec::new();
    let mut found = Vec::new();
    for line in text.lines() {
        if let Some(header) =
            header_line(line).or_else(|| line.starts_with('[').then(|| line.to_owned()))
        {
            section = header.trim_matches(['[', ']']).to_owned();
            description.clear();
        } else if let Some(key) = key_line(line) {
            let name = key.split_once(" = ").map_or(key.as_str(), |(k, _)| k);
            let marker = description.join(" ").to_ascii_lowercase();
            if marker.contains("no default") {
                found.push(if section.is_empty() {
                    name.to_owned()
                } else {
                    format!("{section}.{name}")
                });
            }
            description.clear();
        } else if line.trim().is_empty() {
            description.clear();
        } else if let Some(rest) = line.strip_prefix('#') {
            description.push(rest.trim());
        }
    }
    found.sort_unstable();
    found
}

/// Names of the sub-tables of a table, sorted.
pub fn table_keys(table: &Table) -> Vec<&str> {
    let mut keys: Vec<&str> = table
        .iter()
        .filter(|(_, v)| v.is_table())
        .map(|(k, _)| k.as_str())
        .collect();
    keys.sort_unstable();
    keys
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
