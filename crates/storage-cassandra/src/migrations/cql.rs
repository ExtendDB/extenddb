// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A CQL body's statements, each with the keyspace written in front of the
//! table it names (#632).
//!
//! The runner never selects a keyspace with `USE`. cdrs-tokio runs a `USE` on
//! one pooled connection and copies it to the others from a background task,
//! so a statement that relied on it could run in the keyspace the connection
//! it landed on had selected before. A body names its tables without a
//! keyspace, and [`statements`] writes the keyspace of the store the runner
//! migrates in front of each one.

use extenddb_storage::migrations::{MigrationError, split_comments};

/// One statement of a body, qualified with the store's keyspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Statement {
    /// The statement, without comments, with `keyspace.table` in it.
    pub(crate) text: String,
    /// A `CREATE` or `ALTER`: the runner waits for every live node to agree
    /// on the schema after it.
    pub(crate) ddl: bool,
}

/// The statements of body `file` with comments removed, split on the
/// semicolons outside quotes, each with its table written as
/// `keyspace.table`. A statement whose table cannot be qualified fails the
/// whole file before any statement runs.
///
/// # Errors
///
/// A statement is not one of the forms [`qualify`] recognises.
pub(crate) fn statements(
    keyspace: &str,
    file: &str,
    text: &str,
) -> Result<Vec<Statement>, MigrationError> {
    let (code, _) = split_comments(text, true);
    split(&code)
        .into_iter()
        .map(|statement| qualify(keyspace, file, statement))
        .collect()
}

/// `code` split on the semicolons outside single and double quotes, each
/// piece trimmed and the empty ones dropped.
fn split(code: &str) -> Vec<&str> {
    let mut pieces = Vec::new();
    let mut quote: Option<char> = None;
    let mut start = 0;
    for (i, c) in code.char_indices() {
        match (quote, c) {
            (None, '\'' | '"') => quote = Some(c),
            (Some(q), c) if c == q => quote = None,
            (None, ';') => {
                pieces.push(&code[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    pieces.push(&code[start..]);
    pieces
        .into_iter()
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect()
}

/// `statement` with its table written as `keyspace.table`. Recognises
/// `CREATE TABLE [IF NOT EXISTS] t`, `CREATE INDEX [IF NOT EXISTS] [name]
/// ON t`, `ALTER TABLE [IF EXISTS] t`, `INSERT INTO t` and `DELETE FROM t`,
/// each naming an unquoted table without a keyspace.
fn qualify(keyspace: &str, file: &str, statement: &str) -> Result<Statement, MigrationError> {
    let first_line = statement.lines().next().unwrap_or_default();
    let refuse = || {
        MigrationError::Store(format!(
            "migration file {file} has a statement that names no table the runner can qualify \
             with a keyspace: {first_line}"
        ))
    };
    let (at, ddl) = table_offset(statement).ok_or_else(refuse)?;
    let table: String = statement[at..]
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    let next = statement[at + table.len()..].chars().next();
    let starts_well = table
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if !starts_well || matches!(next, Some('.' | '"')) {
        return Err(refuse());
    }
    Ok(Statement {
        text: format!("{}{keyspace}.{}", &statement[..at], &statement[at..]),
        ddl,
    })
}

/// The byte offset of the table name in `statement`, and whether the
/// statement changes the schema.
fn table_offset(statement: &str) -> Option<(usize, bool)> {
    let words: Vec<(usize, &str)> = words(statement).collect();
    let is = |i: usize, keyword: &str| {
        words
            .get(i)
            .is_some_and(|(_, w)| w.eq_ignore_ascii_case(keyword))
    };
    let at = |i: usize| words.get(i).map(|(offset, _)| *offset);
    if is(0, "CREATE") && is(1, "TABLE") {
        let i = if is(2, "IF") { 5 } else { 2 };
        if i == 5 && !(is(3, "NOT") && is(4, "EXISTS")) {
            return None;
        }
        return at(i).map(|o| (o, true));
    }
    if is(0, "CREATE") && is(1, "INDEX") {
        let i = if is(2, "IF") {
            if !(is(3, "NOT") && is(4, "EXISTS")) {
                return None;
            }
            5
        } else {
            2
        };
        // The index name is optional: `ON` is at i or i + 1.
        let on = if is(i, "ON") { i } else { i + 1 };
        if !is(on, "ON") {
            return None;
        }
        return at(on + 1).map(|o| (o, true));
    }
    if is(0, "ALTER") && is(1, "TABLE") {
        // `IF EXISTS` comes before the table. `IF NOT EXISTS` is not a form
        // of `ALTER TABLE`, and `IF` cannot name a table unquoted.
        let i = if is(2, "IF") {
            if !is(3, "EXISTS") {
                return None;
            }
            4
        } else {
            2
        };
        return at(i).map(|o| (o, true));
    }
    if (is(0, "INSERT") && is(1, "INTO")) || (is(0, "DELETE") && is(1, "FROM")) {
        return at(2).map(|o| (o, false));
    }
    None
}

/// The words of `statement` with their byte offsets. A word ends at
/// whitespace, `(` or `,`.
fn words(statement: &str) -> impl Iterator<Item = (usize, &str)> {
    let mut rest = statement;
    let mut offset = 0;
    std::iter::from_fn(move || {
        let skip = rest.len()
            - rest
                .trim_start_matches(|c: char| c.is_whitespace() || c == '(' || c == ',')
                .len();
        offset += skip;
        rest = &rest[skip..];
        if rest.is_empty() {
            return None;
        }
        let len = rest
            .find(|c: char| c.is_whitespace() || c == '(' || c == ',')
            .unwrap_or(rest.len());
        let word = (offset, &rest[..len]);
        offset += len;
        rest = &rest[len..];
        Some(word)
    })
}

#[cfg(test)]
mod tests {
    use super::{Statement, statements};

    fn one(sql: &str) -> Result<Statement, String> {
        let mut all = statements("ks", "f.cql", sql).map_err(|e| e.to_string())?;
        assert_eq!(all.len(), 1, "{sql}");
        Ok(all.remove(0))
    }

    #[test]
    fn every_form_is_qualified_once_in_front_of_its_table() {
        let cases = [
            (
                "CREATE TABLE IF NOT EXISTS settings (key text PRIMARY KEY)",
                "CREATE TABLE IF NOT EXISTS ks.settings (key text PRIMARY KEY)",
                true,
            ),
            (
                "create table t(a int primary key)",
                "create table ks.t(a int primary key)",
                true,
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx ON tables (status)",
                "CREATE INDEX IF NOT EXISTS idx ON ks.tables (status)",
                true,
            ),
            ("CREATE INDEX ON t (c)", "CREATE INDEX ON ks.t (c)", true),
            (
                "CREATE INDEX idx ON tables (table_id)",
                "CREATE INDEX idx ON ks.tables (table_id)",
                true,
            ),
            (
                "ALTER TABLE tables ADD IF NOT EXISTS c text",
                "ALTER TABLE ks.tables ADD IF NOT EXISTS c text",
                true,
            ),
            (
                "ALTER TABLE IF EXISTS tables ADD IF NOT EXISTS c text",
                "ALTER TABLE IF EXISTS ks.tables ADD IF NOT EXISTS c text",
                true,
            ),
            (
                "alter table if exists tables with comment = 'x'",
                "alter table if exists ks.tables with comment = 'x'",
                true,
            ),
            (
                "INSERT INTO schema_history (version) VALUES (1)",
                "INSERT INTO ks.schema_history (version) VALUES (1)",
                false,
            ),
            (
                "DELETE FROM settings WHERE key = 'x'",
                "DELETE FROM ks.settings WHERE key = 'x'",
                false,
            ),
        ];
        for (input, want, ddl) in cases {
            assert_eq!(
                one(input),
                Ok(Statement {
                    text: want.to_owned(),
                    ddl
                }),
                "{input}"
            );
        }
    }

    #[test]
    fn a_statement_the_runner_cannot_qualify_fails_the_file() {
        for sql in [
            "UPDATE settings SET value = 'x' WHERE key = 'y'",
            "CREATE TABLE other.t (a int PRIMARY KEY)",
            "CREATE TABLE \"T\" (a int PRIMARY KEY)",
            "CREATE TABLE IF EXISTS t (a int PRIMARY KEY)",
            "CREATE TABLE 9t (a int PRIMARY KEY)",
            "CREATE INDEX IF NOT EXISTS i t (c)",
            "CREATE INDEX IF EXISTS idx ON tables (table_id)",
            "ALTER TABLE IF NOT EXISTS tables ADD c text",
            "ALTER TABLE IF tables ADD c text",
            "USE ks",
            "DROP TABLE t",
        ] {
            let error = one(sql).expect_err(sql);
            assert!(
                error.contains("f.cql") && error.contains("qualify"),
                "{error}"
            );
        }
        // One bad statement refuses the whole file, before anything runs.
        assert!(statements("ks", "f.cql", "CREATE TABLE a (x int PRIMARY KEY); USE ks").is_err());
    }

    #[test]
    fn comments_and_quoted_semicolons_do_not_split_a_statement() {
        let all = statements(
            "ks",
            "f.cql",
            "-- a comment\nCREATE TABLE a (x text PRIMARY KEY); // another\n\
             INSERT INTO a (x) VALUES ('p;q');\n/* block */",
        )
        .expect("statements");
        let texts: Vec<&str> = all.iter().map(|s| s.text.as_str()).collect();
        assert_eq!(
            texts,
            [
                "CREATE TABLE ks.a (x text PRIMARY KEY)",
                "INSERT INTO ks.a (x) VALUES ('p;q')"
            ]
        );
    }

    /// Every statement of both baselines, as the runner sends it, is the
    /// statement as written with the keyspace written in once, as a word of
    /// its own in front of the table: after `ON` in a `CREATE INDEX`, after
    /// `INTO` in an `INSERT INTO`, after `FROM` in a `DELETE FROM`, and after
    /// `TABLE` or `EXISTS` otherwise, never in front of the `IF` that opens
    /// an `IF EXISTS` or `IF NOT EXISTS` clause.
    #[test]
    fn every_statement_of_both_baselines_is_qualified() {
        for (file, text) in [
            ("catalog", super::super::CATALOG_BASELINE),
            ("account", super::super::ACCOUNT_BASELINE),
        ] {
            let all = statements("ks", file, text).expect(file);
            let (code, _) = super::split_comments(text, true);
            let written = super::split(&code);
            assert!(!all.is_empty(), "{file}");
            assert_eq!(all.len(), written.len(), "{file}");
            for (statement, written) in all.iter().zip(written) {
                let sent = statement.text.as_str();
                assert_eq!(sent.matches("ks.").count(), 1, "{sent}");
                assert_eq!(sent.replacen("ks.", "", 1), written, "{file}");
                assert!(!sent.contains("--"), "{sent}");
                let (before, after) = sent.split_once("ks.").unwrap_or_default();
                let keyword = if before.ends_with(char::is_whitespace) {
                    before.split_whitespace().next_back().unwrap_or_default()
                } else {
                    ""
                };
                let table = after
                    .split(|c: char| c.is_whitespace() || c == '(')
                    .next()
                    .unwrap_or_default();
                let mut words = written.split_whitespace().map(str::to_ascii_uppercase);
                let expected: &[&str] = match (words.next().as_deref(), words.next().as_deref()) {
                    (Some("CREATE"), Some("INDEX")) => &["ON"],
                    (Some("INSERT"), Some("INTO")) => &["INTO"],
                    (Some("DELETE"), Some("FROM")) => &["FROM"],
                    _ => &["TABLE", "EXISTS"],
                };
                assert!(
                    expected.iter().any(|k| keyword.eq_ignore_ascii_case(k))
                        && !table.is_empty()
                        && !table.eq_ignore_ascii_case("IF"),
                    "{file}: keyspace not in front of the table: {sent}"
                );
            }
        }
    }
}
