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
//! objects no migration creates. A difference stops migration 1, and the
//! refusal names each object with what it is, what v1.0 has and what to do;
//! the check runs inside migration 1's transaction, so nothing is
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

/// A column's definition as `ALTER TABLE ... ADD COLUMN` writes it after the
/// name: `bigint NOT NULL DEFAULT 0`.
fn column_sql(column: &Column) -> String {
    let mut sql = column.data_type.clone();
    if let Some(collation) = &column.collation {
        sql.push_str(&format!(" COLLATE \"{collation}\""));
    }
    if column.not_null {
        sql.push_str(" NOT NULL");
    }
    if let Some(default) = &column.default {
        sql.push_str(&format!(" DEFAULT {default}"));
    }
    sql
}

/// How `found` differs from `expected`, one clause per attribute, such as
/// `has type integer where v1.0 has bigint`.
fn column_differences(found: &Column, expected: &Column) -> Vec<String> {
    let collation = |c: &Option<String>| {
        c.as_ref()
            .map_or_else(|| "the type's default".to_owned(), |c| format!("\"{c}\""))
    };
    let mut clauses = Vec::new();
    if found.data_type != expected.data_type {
        clauses.push(format!(
            "has type {} where v1.0 has {}",
            found.data_type, expected.data_type
        ));
    }
    if found.not_null != expected.not_null {
        clauses.push(if found.not_null {
            "is NOT NULL where v1.0 allows NULL".to_owned()
        } else {
            "allows NULL where v1.0 has NOT NULL".to_owned()
        });
    }
    match (&found.default, &expected.default) {
        (Some(f), Some(e)) if f != e => {
            clauses.push(format!("has default {f} where v1.0 has {e}"));
        }
        (Some(f), None) => clauses.push(format!("has default {f} where v1.0 has none")),
        (None, Some(e)) => clauses.push(format!("has no default where v1.0 has {e}")),
        _ => {}
    }
    if found.collation != expected.collation {
        clauses.push(format!(
            "has collation {} where v1.0 has {}",
            collation(&found.collation),
            collation(&expected.collation)
        ));
    }
    clauses
}

/// `items` joined as a sentence lists them: `a`, `a and b`, `a, b and c`.
fn listed(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [one] => one.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

/// A `table.name` key split into its table and its name.
fn split(key: &str) -> (&str, &str) {
    key.split_once('.').unwrap_or(("", key))
}

/// The differences between `expected` and `actual`, each naming the object,
/// what it is and what v1.0 has, and what to do about it; and the notes for
/// the accepted shape.
#[must_use]
pub fn compare(expected: &Definitions, actual: &Definitions) -> (Vec<String>, Vec<String>) {
    let mut differences = Vec::new();
    let mut notes = Vec::new();
    let named_tables: BTreeSet<&str> = expected.columns.keys().map(|k| split(k).0).collect();
    for (key, column) in &expected.columns {
        match actual.columns.get(key) {
            None => differences.push(format!(
                "column {key} is missing; add it as {}",
                column_sql(column)
            )),
            Some(found) if found == column => {}
            Some(found) if accepted(key, column, found) => notes.push(format!(
                "{key} is nullable, as a build from before the release of table_restores left it; \
                 its rows may hold NULL"
            )),
            Some(found) => differences.push(format!(
                "column {key} {}; change it to {}",
                listed(&column_differences(found, column)),
                column_sql(column)
            )),
        }
    }
    for key in actual.columns.keys() {
        if named_tables.contains(split(key).0) && !expected.columns.contains_key(key) {
            differences.push(format!(
                "column {key} is not part of the v1.0 schema; drop it, after saving any data in \
                 it that you need"
            ));
        }
    }
    for (name, index) in &expected.indexes {
        let on = format!("index {name} on table {}", index.table);
        match actual.indexes.get(name) {
            None => differences.push(format!(
                "{on} is missing; create it with {}",
                index.definition
            )),
            Some(found) if found == index => {}
            Some(found) if found.definition != index.definition => differences.push(format!(
                "{on} is defined as {} where v1.0 defines it as {}; drop it and create it with \
                 the v1.0 definition",
                found.definition, index.definition
            )),
            Some(_) => differences.push(format!(
                "{on} is not valid, as an index build that did not finish leaves it; rebuild it \
                 with REINDEX INDEX {name}"
            )),
        }
    }
    for (key, definition) in &expected.constraints {
        let (table, constraint) = split(key);
        let on = format!("constraint {constraint} on table {table}");
        match actual.constraints.get(key) {
            None => differences.push(format!("{on} is missing; add it as {definition}")),
            Some(found) if found == definition => {}
            Some(found) => differences.push(format!(
                "{on} is {found} where v1.0 has {definition}; drop it and add it as {definition}"
            )),
        }
    }
    for sequence in &expected.sequences {
        if !actual.sequences.contains(sequence) {
            differences.push(format!("sequence {sequence} is missing; create it"));
        }
    }
    (differences, notes)
}

/// The refusal for `differences` found in `database`: one line per
/// difference, then what to do.
fn refusal(database: &str, differences: &[String]) -> String {
    let lines: Vec<String> = differences.iter().map(|d| format!("  {d}")).collect();
    format!(
        "after migration 1 the database {database} differs from the v1.0 schema in {} place(s), \
         so nothing was committed:\n{}\nMake each change in the database {database}, then run \
         migrate --yes again; migration 1 checks the schema again before it commits.",
        differences.len(),
        lines.join("\n")
    )
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
        Err(MigrationError::Store(refusal(database, &differences)))
    }
}

#[cfg(test)]
mod tests {
    use super::{Column, Definitions, Index, compare, refusal};

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
        assert_eq!(
            differences,
            [
                "index t_pkey on table t is defined as CREATE UNIQUE INDEX t_pkey ON public.t \
                 USING btree (id, other) where v1.0 defines it as CREATE UNIQUE INDEX t_pkey ON \
                 public.t USING btree (id); drop it and create it with the v1.0 definition"
            ]
        );
        let mut invalid = definitions();
        invalid.indexes.get_mut("t_pkey").unwrap().valid = false;
        assert_eq!(
            compare(&definitions(), &invalid).0,
            [
                "index t_pkey on table t is not valid, as an index build that did not finish \
                 leaves it; rebuild it with REINDEX INDEX t_pkey"
            ]
        );
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
            [
                "constraint t_pkey on table t is PRIMARY KEY (id, other) where v1.0 has PRIMARY \
                 KEY (id); drop it and add it as PRIMARY KEY (id)"
            ]
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
                "index t_pkey on table t is missing; create it with CREATE UNIQUE INDEX t_pkey \
                 ON public.t USING btree (id)",
                "constraint t_pkey on table t is missing; add it as PRIMARY KEY (id)",
                "sequence t_seq is missing; create it"
            ]
        );
    }

    /// Each attribute of a column that differs is named in words with what
    /// v1.0 has, followed by the definition to change it to; a missing
    /// column gives the definition to add, and a column v1.0 does not have
    /// is to be dropped.
    #[test]
    fn a_column_difference_names_each_attribute_and_the_v1_definition() {
        let mut actual = definitions();
        actual.columns.insert(
            "t.id".to_owned(),
            Column {
                data_type: "double precision".to_owned(),
                not_null: false,
                default: Some("1".to_owned()),
                collation: Some("C".to_owned()),
            },
        );
        let (differences, _) = compare(&definitions(), &actual);
        assert_eq!(
            differences,
            [
                "column t.id has type double precision where v1.0 has integer, allows NULL where \
                 v1.0 has NOT NULL, has default 1 where v1.0 has none and has collation \"C\" \
                 where v1.0 has the type's default; change it to integer NOT NULL"
            ]
        );
        let mut actual = definitions();
        actual.columns.get_mut("t.id").unwrap().default = None;
        let mut expected = definitions();
        expected.columns.get_mut("t.id").unwrap().default = Some("0".to_owned());
        assert_eq!(
            compare(&expected, &actual).0,
            [
                "column t.id has no default where v1.0 has 0; change it to integer NOT NULL \
                 DEFAULT 0"
            ]
        );
        let mut actual = definitions();
        actual.columns.clear();
        actual.columns.insert(
            "t.extra".to_owned(),
            Column {
                data_type: "text".to_owned(),
                not_null: false,
                default: None,
                collation: None,
            },
        );
        assert_eq!(
            compare(&definitions(), &actual).0,
            [
                "column t.id is missing; add it as integer NOT NULL",
                "column t.extra is not part of the v1.0 schema; drop it, after saving any data \
                 in it that you need"
            ]
        );
    }

    /// The refusal prints no Rust structure: one line per difference, then
    /// what to do.
    #[test]
    fn the_refusal_lists_each_difference_and_what_to_do() {
        let mut actual = definitions();
        actual.columns.get_mut("t.id").unwrap().data_type = "bigint".to_owned();
        actual.sequences.clear();
        let (differences, _) = compare(&definitions(), &actual);
        let message = refusal("acme_catalog", &differences);
        assert_eq!(
            message,
            "after migration 1 the database acme_catalog differs from the v1.0 schema in 2 \
             place(s), so nothing was committed:\n  column t.id has type bigint where v1.0 has \
             integer; change it to integer NOT NULL\n  sequence t_seq is missing; create it\n\
             Make each change in the database acme_catalog, then run migrate --yes again; \
             migration 1 checks the schema again before it commits."
        );
        assert!(
            !message.contains("Column {") && !message.contains("Some("),
            "{message}"
        );
    }
}
