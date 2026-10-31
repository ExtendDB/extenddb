// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The post-check of migration 1: a keyspace's definitions in
//! `system_schema` compared with the ones migration 1 creates in an empty
//! keyspace, embedded from `0001_baseline.expected.json` beside each file.
//!
//! A definition is a column's kind (partition key, clustering, regular,
//! static), position, clustering order and type, and a secondary index's
//! table, kind and target; column order in the table is ignored. Every
//! object the expected file names is checked, and objects it does not name
//! are ignored, because account keyspaces also hold the per-table tables
//! that CreateTable makes. The definitions are read from `system_schema`,
//! never from the data directory: two coordinators creating the same table
//! at once can leave an orphan table directory on disk while `system_schema`
//! holds one table id per name. A difference stops migration 1 and names the
//! object, before its ledger row is written, so the next run applies it
//! again.

use std::collections::{BTreeMap, HashMap};

use cdrs_tokio::consistency::Consistency;
use extenddb_storage::migrations::MigrationError;
use serde::{Deserialize, Serialize};

use super::connect::{column, rows, text_map};
use crate::cassandra_util::CassandraSession;

/// One column's definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub kind: String,
    pub position: i32,
    pub clustering_order: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

/// One secondary index's definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    pub table: String,
    pub kind: String,
    pub target: String,
}

/// A keyspace's definitions: columns keyed `table.column`, indexes by name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Definitions {
    pub columns: BTreeMap<String, Column>,
    pub indexes: BTreeMap<String, Index>,
}

/// The runner's ledger, which no migration body creates.
const LEDGER: &str = "extenddb_migrations";

/// The definitions of `keyspace` in `system_schema`, the runner's ledger
/// left out.
///
/// # Errors
///
/// A read of `system_schema` fails.
pub async fn describe(
    session: &CassandraSession,
    keyspace: &str,
) -> Result<Definitions, MigrationError> {
    let mut definitions = Definitions::default();
    let found = rows(
        session,
        "SELECT table_name, column_name, kind, position, clustering_order, type FROM \
         system_schema.columns WHERE keyspace_name = ?",
        cdrs_tokio::query_values!(keyspace),
        Consistency::One,
        &format!("read the columns of {keyspace} from system_schema"),
    )
    .await?;
    for row in &found {
        let context = "read system_schema.columns";
        let table: String = column(row, "table_name", context)?;
        if table == LEDGER {
            continue;
        }
        let name: String = column(row, "column_name", context)?;
        definitions.columns.insert(
            format!("{table}.{name}"),
            Column {
                kind: column(row, "kind", context)?,
                position: column(row, "position", context)?,
                clustering_order: column(row, "clustering_order", context)?,
                data_type: column(row, "type", context)?,
            },
        );
    }
    let found = rows(
        session,
        "SELECT table_name, index_name, kind, options FROM system_schema.indexes WHERE \
         keyspace_name = ?",
        cdrs_tokio::query_values!(keyspace),
        Consistency::One,
        &format!("read the indexes of {keyspace} from system_schema"),
    )
    .await?;
    for row in &found {
        let context = "read system_schema.indexes";
        let options: HashMap<String, String> = text_map(row, "options", context)?;
        definitions.indexes.insert(
            column(row, "index_name", context)?,
            Index {
                table: column(row, "table_name", context)?,
                kind: column(row, "kind", context)?,
                target: options.get("target").cloned().unwrap_or_default(),
            },
        );
    }
    Ok(definitions)
}

/// The differences between `expected` and `actual`, one line per object:
/// a column or index of `expected` missing or defined otherwise, and a
/// column `actual` has in a table `expected` names that `expected` lacks.
#[must_use]
pub fn compare(expected: &Definitions, actual: &Definitions) -> Vec<String> {
    let mut differences = Vec::new();
    for (key, want) in &expected.columns {
        match actual.columns.get(key) {
            None => differences.push(format!("column {key} is missing")),
            Some(found) if found == want => {}
            Some(found) => {
                differences.push(format!("column {key} is {found:?}, expected {want:?}"));
            }
        }
    }
    let named = |key: &str| {
        let table = key.split_once('.').map_or(key, |(t, _)| t);
        expected
            .columns
            .keys()
            .any(|k| k.split_once('.').is_some_and(|(t, _)| t == table))
    };
    for key in actual.columns.keys() {
        if named(key) && !expected.columns.contains_key(key) {
            differences.push(format!("column {key} is not part of the v1.0 schema"));
        }
    }
    for (name, want) in &expected.indexes {
        match actual.indexes.get(name) {
            None => differences.push(format!("index {name} is missing")),
            Some(found) if found == want => {}
            Some(found) => {
                differences.push(format!("index {name} is {found:?}, expected {want:?}"));
            }
        }
    }
    differences
}

/// Compare `keyspace` with `expected_json`; stop naming every difference.
///
/// # Errors
///
/// The expected file cannot be read, `system_schema` cannot be read, or the
/// keyspace differs.
pub(crate) async fn check(
    session: &CassandraSession,
    expected_json: &str,
    keyspace: &str,
) -> Result<(), MigrationError> {
    let expected: Definitions = serde_json::from_str(expected_json).map_err(|e| {
        MigrationError::Store(format!(
            "the expected definitions embedded in this binary cannot be read: {e}"
        ))
    })?;
    let actual = describe(session, keyspace).await?;
    let differences = compare(&expected, &actual);
    if differences.is_empty() {
        return Ok(());
    }
    Err(MigrationError::Store(format!(
        "migration 1 left {keyspace} different from the v1.0 schema, so it was not recorded: {}",
        differences.join("; ")
    )))
}

#[cfg(test)]
mod tests {
    use super::{Column, Definitions, Index, compare};

    fn col(kind: &str, data_type: &str) -> Column {
        Column {
            kind: kind.to_owned(),
            position: -1,
            clustering_order: "none".to_owned(),
            data_type: data_type.to_owned(),
        }
    }

    fn expected() -> Definitions {
        let mut d = Definitions::default();
        d.columns.insert("t.k".into(), col("partition_key", "text"));
        d.columns.insert("t.v".into(), col("regular", "text"));
        d.indexes.insert(
            "i".into(),
            Index {
                table: "t".into(),
                kind: "COMPOSITES".into(),
                target: "v".into(),
            },
        );
        d
    }

    #[test]
    fn the_check_names_every_difference_and_ignores_unnamed_tables() {
        let want = expected();
        let mut same = want.clone();
        // A table the expected file does not name (a per-table table) is
        // ignored.
        same.columns
            .insert("items_x.pk".into(), col("partition_key", "blob"));
        assert!(compare(&want, &same).is_empty());

        let mut other = want.clone();
        other.columns.remove("t.v");
        other
            .columns
            .insert("t.k".into(), col("partition_key", "int"));
        other
            .columns
            .insert("t.extra".into(), col("regular", "text"));
        other.indexes.clear();
        let differences = compare(&want, &other);
        assert_eq!(differences.len(), 4, "{differences:?}");
        for needle in [
            "t.v is missing",
            "t.k is",
            "t.extra is not part",
            "index i is missing",
        ] {
            assert!(
                differences.iter().any(|d| d.contains(needle)),
                "{needle}: {differences:?}"
            );
        }
    }

    #[test]
    fn the_embedded_expected_files_can_be_read() {
        for text in [
            super::super::CATALOG_EXPECTED,
            super::super::ACCOUNT_EXPECTED,
        ] {
            let definitions: Definitions = serde_json::from_str(text).expect("expected json");
            assert!(!definitions.columns.is_empty());
            assert!(
                !definitions
                    .columns
                    .keys()
                    .any(|k| k.starts_with("extenddb_migrations."))
            );
        }
    }
}
