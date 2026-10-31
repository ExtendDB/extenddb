// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The post-check of migration 1: the database's definitions compared with
//! the ones migration 1 creates in an empty database, embedded from
//! `0001_baseline.expected.json` beside each file.
//!
//! A definition is a column's type, nullability, default and collation, an
//! index's `pg_get_indexdef` and `indisvalid`, a constraint's
//! `pg_get_constraintdef`, and a sequence's existence; column order is
//! ignored. Every object the expected file names is checked, and objects it
//! does not name are ignored, because the same databases hold per-table
//! objects no migration creates. A difference stops migration 1 and names the
//! object; the check runs inside migration 1's transaction, so nothing is
//! committed. One pre-release shape is accepted with a note:
//! `table_restores.source_table_arn` nullable, which a build from before the
//! release of `table_restores` left.

use std::collections::{BTreeMap, BTreeSet};

use extenddb_storage::migrations::MigrationError;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// One column's definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    #[serde(rename = "type")]
    pub data_type: String,
    pub not_null: bool,
    pub default: Option<String>,
    /// `None` for the type's default collation.
    pub collation: Option<String>,
}

/// One index's definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    pub table: String,
    pub definition: String,
    pub valid: bool,
}

/// A database's definitions, keyed for comparison: columns as
/// `table.column`, constraints as `table.constraint`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Definitions {
    pub columns: BTreeMap<String, Column>,
    pub indexes: BTreeMap<String, Index>,
    pub constraints: BTreeMap<String, String>,
    pub sequences: BTreeSet<String>,
}

/// table, column, type, not null, default, collation.
type ColumnRow = (String, String, String, bool, Option<String>, Option<String>);

/// The runner's own ledger, which no migration creates.
const LEDGER: &str = "extenddb_migrations";

fn read_error(e: sqlx::Error) -> MigrationError {
    MigrationError::store("read the schema's definitions", e)
}

/// The definitions of every object in the public schema, the runner's ledger
/// excluded.
///
/// # Errors
///
/// The catalog tables of the database cannot be read.
pub async fn describe(pool: &PgPool) -> Result<Definitions, MigrationError> {
    let mut defs = Definitions::default();
    let columns: Vec<ColumnRow> = sqlx::query_as(
        "SELECT c.relname::text, a.attname::text, format_type(a.atttypid, a.atttypmod), \
                a.attnotnull, pg_get_expr(d.adbin, d.adrelid), \
                CASE WHEN a.attcollation <> 0 AND a.attcollation <> t.typcollation \
                     THEN co.collname::text END \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         JOIN pg_type t ON t.oid = a.atttypid \
         LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
         LEFT JOIN pg_collation co ON co.oid = a.attcollation \
         WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') \
           AND a.attnum > 0 AND NOT a.attisdropped",
    )
    .fetch_all(pool)
    .await
    .map_err(read_error)?;
    for (table, column, data_type, not_null, default, collation) in columns {
        if table != LEDGER {
            defs.columns.insert(
                format!("{table}.{column}"),
                Column {
                    data_type,
                    not_null,
                    default,
                    collation,
                },
            );
        }
    }
    let indexes: Vec<(String, String, String, bool)> = sqlx::query_as(
        "SELECT ic.relname::text, c.relname::text, pg_get_indexdef(i.indexrelid), i.indisvalid \
         FROM pg_index i \
         JOIN pg_class ic ON ic.oid = i.indexrelid \
         JOIN pg_class c ON c.oid = i.indrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'public'",
    )
    .fetch_all(pool)
    .await
    .map_err(read_error)?;
    for (name, table, definition, valid) in indexes {
        if table != LEDGER {
            defs.indexes.insert(
                name,
                Index {
                    table,
                    definition,
                    valid,
                },
            );
        }
    }
    // PostgreSQL 18 records each NOT NULL as a constraint (`contype = 'n'`)
    // whose name embeds the table; the column's `not_null` already compares
    // it, and earlier servers have no such rows.
    let constraints: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT c.relname::text, con.conname::text, pg_get_constraintdef(con.oid) \
         FROM pg_constraint con \
         JOIN pg_class c ON c.oid = con.conrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'public' AND con.contype <> 'n'",
    )
    .fetch_all(pool)
    .await
    .map_err(read_error)?;
    for (table, name, definition) in constraints {
        if table != LEDGER {
            defs.constraints
                .insert(format!("{table}.{name}"), definition);
        }
    }
    let sequences: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'public' AND c.relkind = 'S'",
    )
    .fetch_all(pool)
    .await
    .map_err(read_error)?;
    defs.sequences = sequences.into_iter().collect();
    Ok(defs)
}

/// Whether a difference in `key` from `expected` to `actual` is the accepted
/// pre-release shape.
fn accepted(key: &str, expected: &Column, actual: &Column) -> bool {
    key == "table_restores.source_table_arn"
        && expected.not_null
        && !actual.not_null
        && Column {
            not_null: true,
            ..actual.clone()
        } == *expected
}

/// The differences between `expected` and `actual`, and the notes for the
/// accepted shape.
#[must_use]
pub fn compare(expected: &Definitions, actual: &Definitions) -> (Vec<String>, Vec<String>) {
    let mut differences = Vec::new();
    let mut notes = Vec::new();
    let named_tables: BTreeSet<&str> = expected
        .columns
        .keys()
        .filter_map(|k| k.split_once('.').map(|(t, _)| t))
        .collect();
    for (key, column) in &expected.columns {
        match actual.columns.get(key) {
            None => differences.push(format!("column {key} is missing")),
            Some(found) if found == column => {}
            Some(found) if accepted(key, column, found) => notes.push(format!(
                "{key} is nullable, as a build from before the release of table_restores left it; \
                 its rows may hold NULL"
            )),
            Some(found) => {
                differences.push(format!("column {key} is {found:?}, expected {column:?}"))
            }
        }
    }
    for key in actual.columns.keys() {
        let table = key.split_once('.').map_or(key.as_str(), |(t, _)| t);
        if named_tables.contains(table) && !expected.columns.contains_key(key) {
            differences.push(format!("column {key} is not part of the v1.0 schema"));
        }
    }
    for (name, index) in &expected.indexes {
        match actual.indexes.get(name) {
            None => differences.push(format!("index {name} is missing")),
            Some(found) if found == index => {}
            Some(found) => {
                differences.push(format!("index {name} is {found:?}, expected {index:?}"))
            }
        }
    }
    for (key, definition) in &expected.constraints {
        match actual.constraints.get(key) {
            None => differences.push(format!("constraint {key} is missing")),
            Some(found) if found == definition => {}
            Some(found) => differences.push(format!(
                "constraint {key} is {found}, expected {definition}"
            )),
        }
    }
    for sequence in &expected.sequences {
        if !actual.sequences.contains(sequence) {
            differences.push(format!("sequence {sequence} is missing"));
        }
    }
    (differences, notes)
}

/// Compare the database with `expected_json`; returns the notes, or stops
/// naming every difference.
pub(crate) async fn check(
    pool: &PgPool,
    expected_json: &str,
    database: &str,
) -> Result<Vec<String>, MigrationError> {
    let expected: Definitions = serde_json::from_str(expected_json).map_err(|e| {
        MigrationError::Store(format!(
            "migration 1's expected definitions do not parse: {e}"
        ))
    })?;
    let actual = describe(pool).await?;
    let (differences, notes) = compare(&expected, &actual);
    if differences.is_empty() {
        Ok(notes)
    } else {
        Err(MigrationError::Store(format!(
            "after migration 1 the database {database} differs from the v1.0 schema, so nothing \
             was committed: {}",
            differences.join("; ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{Column, Definitions, Index, compare};

    fn definitions() -> Definitions {
        let mut defs = Definitions::default();
        defs.columns.insert(
            "t.id".to_owned(),
            Column {
                data_type: "integer".to_owned(),
                not_null: true,
                default: None,
                collation: None,
            },
        );
        defs.indexes.insert(
            "t_pkey".to_owned(),
            Index {
                table: "t".to_owned(),
                definition: "CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id)".to_owned(),
                valid: true,
            },
        );
        defs.constraints
            .insert("t.t_pkey".to_owned(), "PRIMARY KEY (id)".to_owned());
        defs.sequences.insert("t_seq".to_owned());
        defs
    }

    #[test]
    fn identical_definitions_have_no_difference() {
        let (differences, notes) = compare(&definitions(), &definitions());
        assert!(
            differences.is_empty() && notes.is_empty(),
            "{differences:?}"
        );
    }

    #[test]
    fn an_index_on_other_columns_is_a_difference() {
        let mut actual = definitions();
        actual.indexes.get_mut("t_pkey").unwrap().definition =
            "CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id, other)".to_owned();
        let (differences, _) = compare(&definitions(), &actual);
        assert_eq!(differences.len(), 1, "{differences:?}");
        assert!(
            differences[0].starts_with("index t_pkey is "),
            "{differences:?}"
        );
        let mut invalid = definitions();
        invalid.indexes.get_mut("t_pkey").unwrap().valid = false;
        assert_eq!(compare(&definitions(), &invalid).0.len(), 1);
    }

    #[test]
    fn a_constraint_with_another_definition_is_a_difference() {
        let mut actual = definitions();
        actual
            .constraints
            .insert("t.t_pkey".to_owned(), "PRIMARY KEY (id, other)".to_owned());
        let (differences, _) = compare(&definitions(), &actual);
        assert_eq!(
            differences,
            ["constraint t.t_pkey is PRIMARY KEY (id, other), expected PRIMARY KEY (id)"]
        );
    }

    #[test]
    fn a_missing_sequence_index_or_constraint_is_a_difference() {
        let mut actual = definitions();
        actual.sequences.clear();
        actual.indexes.clear();
        actual.constraints.clear();
        let (differences, _) = compare(&definitions(), &actual);
        assert_eq!(
            differences,
            [
                "index t_pkey is missing",
                "constraint t.t_pkey is missing",
                "sequence t_seq is missing"
            ]
        );
    }
}
