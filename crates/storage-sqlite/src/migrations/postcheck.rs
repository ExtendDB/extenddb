// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The check after migration 1: the database's normalised definitions are
//! compared with `migrations/0001_baseline.expected.json`, which is generated
//! by running migration 1 on an empty database.
//!
//! A table's definition is the set of its column definitions and constraints,
//! each normalised (comments removed, whitespace collapsed, plain identifiers
//! unquoted), so column order does not count. An index's is its normalised
//! `CREATE INDEX` statement. Only objects the expected file names are
//! checked: per-table objects that no migration creates (`_ddb_<id>` data and
//! index tables and their indexes) are not in it.

use std::collections::BTreeMap;

use extenddb_storage::migrations::{MigrationError, split_comments};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

/// One object in the expected file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Expected {
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub table: String,
    pub definition: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ExpectedFile {
    objects: Vec<Expected>,
}

/// A difference that a pre-release build left and that is accepted:
/// `(table, expected part, accepted part, note recorded in the ledger)`.
const ACCEPTED: &[(&str, &str, &str, &str)] = &[(
    "table_restores",
    "restore_date_time TEXT NOT NULL",
    "restore_date_time TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
    "table_restores.restore_date_time keeps the default a pre-release build gave it; every row \
     this build writes supplies the value",
)];

/// Normalise one `sqlite_schema` statement: comments removed, whitespace
/// collapsed to single spaces and dropped around `(`, `)` and `,`, and
/// double quotes removed from identifiers that need none. String literals
/// are kept as written.
pub(crate) fn normalize(sql: &str) -> String {
    let (code, _) = split_comments(sql, false);
    let mut out = String::with_capacity(code.len());
    let mut space = false;
    let mut chars = code.chars().peekable();
    let push = |out: &mut String, piece: &str, space: &mut bool| {
        let punct = matches!(piece, "(" | ")" | ",");
        let after_punct = out.ends_with(['(', ')', ',']);
        if *space && !punct && !after_punct && !out.is_empty() {
            out.push(' ');
        }
        *space = false;
        out.push_str(piece);
    };
    while let Some(c) = chars.next() {
        match c {
            c if c.is_whitespace() => space = true,
            '\'' => {
                let mut literal = String::from('\'');
                while let Some(d) = chars.next() {
                    literal.push(d);
                    if d == '\'' {
                        if chars.peek() == Some(&'\'') {
                            literal.push('\'');
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
                push(&mut out, &literal, &mut space);
            }
            '"' => {
                let mut ident = String::new();
                while let Some(d) = chars.next() {
                    if d == '"' {
                        if chars.peek() == Some(&'"') {
                            ident.push('"');
                            chars.next();
                        } else {
                            break;
                        }
                    } else {
                        ident.push(d);
                    }
                }
                let plain = !ident.is_empty()
                    && ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
                if plain {
                    push(&mut out, &ident, &mut space);
                } else {
                    push(
                        &mut out,
                        &format!("\"{}\"", ident.replace('"', "\"\"")),
                        &mut space,
                    );
                }
            }
            _ => push(&mut out, &c.to_string(), &mut space),
        }
    }
    out
}

/// A normalised table statement's column definitions and constraints, in
/// sorted order.
pub(crate) fn table_parts(normalized: &str) -> Vec<String> {
    let (Some(open), Some(close)) = (normalized.find('('), normalized.rfind(')')) else {
        return vec![normalized.to_owned()];
    };
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut quoted = false;
    let mut current = String::new();
    for c in normalized[open + 1..close].chars() {
        match c {
            '\'' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => depth = depth.saturating_sub(1),
            ',' if !quoted && depth == 0 => {
                parts.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    parts.push(current);
    let tail = normalized[close + 1..].trim();
    if !tail.is_empty() {
        parts.push(tail.to_owned());
    }
    parts.sort_unstable();
    parts
}

/// Every object of the database, except SQLite's own and the ledger, by
/// type and name.
pub(crate) async fn definitions(
    pool: &SqlitePool,
) -> Result<BTreeMap<(String, String), Expected>, MigrationError> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT type, name, tbl_name, sql FROM sqlite_master \
         WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' AND tbl_name <> 'extenddb_migrations'",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| MigrationError::store("read the schema", e))?;
    Ok(rows
        .into_iter()
        .map(|(kind, name, table, sql)| {
            let normalized = normalize(&sql);
            let definition = if kind == "table" {
                table_parts(&normalized)
            } else {
                vec![normalized]
            };
            (
                (kind.clone(), name.clone()),
                Expected {
                    kind,
                    name,
                    table,
                    definition,
                },
            )
        })
        .collect())
}

/// The expected file's contents for the database as it is, for the test
/// that regenerates it from an empty database.
#[cfg(test)]
pub(crate) async fn expected_file(pool: &SqlitePool) -> Result<String, MigrationError> {
    let objects = definitions(pool).await?.into_values().collect();
    let mut text = serde_json::to_string_pretty(&ExpectedFile { objects })
        .map_err(|e| MigrationError::store("serialize the expected definitions", e))?;
    text.push('\n');
    Ok(text)
}

/// Compare the database with the expected definitions. Returns the notes for
/// accepted differences.
///
/// # Errors
///
/// An object the expected file names is missing or differs; the error names
/// it and the parts that differ.
pub(crate) async fn check(
    pool: &SqlitePool,
    expected: &str,
) -> Result<Vec<String>, MigrationError> {
    let expected: ExpectedFile = serde_json::from_str(expected)
        .map_err(|e| MigrationError::store("parse 0001_baseline.expected.json", e))?;
    let actual = definitions(pool).await?;
    let mut notes = Vec::new();
    let mut problems = Vec::new();
    for object in expected.objects {
        let key = (object.kind.clone(), object.name.clone());
        let Some(found) = actual.get(&key) else {
            problems.push(format!("{} {} is missing", object.kind, object.name));
            continue;
        };
        let mut missing: Vec<&String> = object
            .definition
            .iter()
            .filter(|p| !found.definition.contains(p))
            .collect();
        let mut extra: Vec<&String> = found
            .definition
            .iter()
            .filter(|p| !object.definition.contains(p))
            .collect();
        for (table, expected_part, accepted_part, note) in ACCEPTED {
            if object.name == *table
                && missing.iter().any(|p| p.as_str() == *expected_part)
                && extra.iter().any(|p| p.as_str() == *accepted_part)
            {
                missing.retain(|p| p.as_str() != *expected_part);
                extra.retain(|p| p.as_str() != *accepted_part);
                notes.push((*note).to_owned());
            }
        }
        if !missing.is_empty() || !extra.is_empty() {
            problems.push(format!(
                "{} {} differs: expected [{}], found [{}]",
                object.kind,
                object.name,
                missing
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join("; "),
                extra
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        }
    }
    if problems.is_empty() {
        Ok(notes)
    } else {
        Err(MigrationError::Store(format!(
            "after migration 1 the database does not have the expected schema, so nothing was \
             committed: {}",
            problems.join("; ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize, table_parts};

    #[test]
    fn normalisation_ignores_layout_comments_and_plain_quotes() {
        let a = "CREATE TABLE \"t\" (\n    a TEXT NOT NULL, -- note, with a comma\n    b INTEGER DEFAULT (strftime('%s', 'now')),\n    PRIMARY KEY (a)\n)";
        let b = "CREATE TABLE t(b INTEGER DEFAULT(strftime('%s','now')),a TEXT NOT NULL,PRIMARY KEY(a))";
        assert_eq!(table_parts(&normalize(a)), table_parts(&normalize(b)));
        // A literal keeps its spaces, and a quoted name that needs quotes keeps them.
        assert_eq!(normalize("SELECT 'a  b'"), "SELECT 'a  b'");
        assert_eq!(
            normalize("CREATE INDEX \"i-1\" ON \"_ddb_x-y\" (c)"),
            "CREATE INDEX \"i-1\" ON \"_ddb_x-y\"(c)"
        );
    }
}
