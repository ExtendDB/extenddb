// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra schema migrations.
//!
//! ## Migration Versioning
//!
//! This module uses Flyway-style versioning for migration files:
//!
//! - Format: `V###__description.cql` (e.g., `V001__initial_schema.cql`)
//! - Version: Integer extracted from `V###__` prefix (e.g., 1 from V001)
//! - Description: Text after double underscore (e.g., `initial_schema`)
//!
//! Flyway is a popular database migration tool that uses this naming convention.
//! See: <https://flywaydb.org/documentation/concepts/migrations#naming>
//!
//! ## Migration Tracking
//!
//! Applied migrations are tracked in the `schema_history` table:
//! - `version` (int): Extracted from filename (V001 → 1)
//! - `description` (text): Extracted from filename (`V001__initial_schema.cql` → `initial_schema`)
//! - `applied_at` (timestamp): When the migration was applied
//!
//! Migrations are applied in order by version number. Already-applied migrations
//! are skipped based on the version number in the tracking table.
//!
//! ## Adding New Migrations
//!
//! 1. Create a new file: `migrations/catalog/V###__description.cql`
//! 2. Use the next sequential version number (e.g., V003 after V002)
//! 3. Add the migration to the `CATALOG_MIGRATIONS` array below
//! 4. Migrations are embedded at compile time via `include_str!()`
//! 5. Name tables without a keyspace. The runner writes the keyspace it
//!    migrates into each `CREATE TABLE`, `CREATE INDEX`, `ALTER TABLE` and
//!    `INSERT INTO` statement, and rejects any other statement form.

use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use extenddb_storage::management_store::{OpError, OpResult};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Serializes migration runs across the whole process (issue #408).
///
/// Migrations set the keyspace with `USE <keyspace>` on the shared cdrs-tokio
/// `Session` and then run UNQUALIFIED DDL (`CREATE TABLE IF NOT EXISTS <name>`).
/// cdrs-tokio keeps one current keyspace per `Session`, and the whole process
/// shares one `Session`, so two concurrent migration runs (e.g. two accounts
/// created at once) interleave their `USE` and one account's DDL lands in the
/// other's keyspace — while its own `schema_history` still records the
/// migration as applied, so it never retries. Holding this lock for the whole
/// `USE`-then-DDL sequence makes each run atomic with respect to the shared
/// keyspace state. Both catalog and data migrations take the SAME lock, because
/// they race on the same `Session`.
///
/// The lock is held only during a migration run (rare: account/catalog
/// creation), never on the request hot path.
static MIGRATION_LOCK: Mutex<()> = Mutex::const_new(());

type CassandraSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

/// Embedded catalog migration files, applied in order.
pub(crate) const CATALOG_MIGRATIONS: &[(&str, &str)] = &[
    (
        "V001__initial_schema.cql",
        include_str!("../migrations/catalog/V001__initial_schema.cql"),
    ),
    (
        "V002__backup_restore.cql",
        include_str!("../migrations/catalog/V002__backup_restore.cql"),
    ),
    (
        "V003__account_scoped_idempotency.cql",
        include_str!("../migrations/catalog/V003__account_scoped_idempotency.cql"),
    ),
    (
        "V004__ttl_backfill_cursor.cql",
        include_str!("../migrations/catalog/V004__ttl_backfill_cursor.cql"),
    ),
    (
        "V005__backup_definitions_and_restore_registration.cql",
        include_str!("../migrations/catalog/V005__backup_definitions_and_restore_registration.cql"),
    ),
];

/// Embedded data migration files, applied in order.
pub(crate) const DATA_MIGRATIONS: &[(&str, &str)] = &[
    (
        "V001__initial_data_schema.cql",
        include_str!("../migrations/data/V001__initial_data_schema.cql"),
    ),
    (
        "V002__backup_items.cql",
        include_str!("../migrations/data/V002__backup_items.cql"),
    ),
];

/// Run catalog migrations, skipping already-applied ones.
pub async fn run_catalog_migrations(
    session: &Arc<CassandraSession>,
    keyspace: &str,
) -> OpResult<()> {
    // Hold the process-wide migration lock for the whole USE-then-DDL sequence
    // so a concurrent run cannot move the Session's current keyspace (issue
    // #408). Held until this function returns.
    let _guard = MIGRATION_LOCK.lock().await;

    println!("--- Running catalog migrations...");

    for (filename, sql) in CATALOG_MIGRATIONS {
        if is_migration_applied(session, keyspace, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        execute_migration(session, keyspace, filename, sql).await?;
        record_migration(session, keyspace, filename).await?;
    }
    println!("    Migrations applied.");
    Ok(())
}

/// Run data database migrations.
pub async fn run_data_migrations(session: &Arc<CassandraSession>, keyspace: &str) -> OpResult<()> {
    // Hold the process-wide migration lock for the whole USE-then-DDL sequence
    // so a concurrent run cannot move the Session's current keyspace out from
    // under this account's unqualified DDL (issue #408). Held until return.
    let _guard = MIGRATION_LOCK.lock().await;

    println!("--- Running data migrations...");

    for (filename, sql) in DATA_MIGRATIONS {
        if is_migration_applied(session, keyspace, filename).await? {
            println!("    {filename} — already applied, skipping.");
            continue;
        }
        println!("    Applying {filename}...");
        execute_migration(session, keyspace, filename, sql).await?;
        record_migration(session, keyspace, filename).await?;
    }
    println!("    Data migrations applied.");
    Ok(())
}

/// Check if a table exists in the given keyspace.
pub(crate) async fn table_exists(
    session: &Arc<CassandraSession>,
    keyspace: &str,
    table_name: &str,
) -> OpResult<bool> {
    let query =
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?";
    let exists = session
        .query_with_values(query, cdrs_tokio::query_values!(keyspace, table_name))
        .await
        .ok()
        .and_then(|frame| frame.response_body().ok())
        .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
        .is_some_and(|rows| !rows.is_empty());
    Ok(exists)
}

/// Return the union of data migrations pending in any account keyspace.
///
/// A migration is pending if at least one existing account has not recorded it.
/// This keeps a partially completed multi-account upgrade retryable.
pub(crate) async fn pending_data_migrations(
    session: &Arc<CassandraSession>,
    catalog_keyspace: &str,
    account_keyspace_fn: impl Fn(&str) -> String,
) -> OpResult<Vec<String>> {
    let query = format!("SELECT account_id FROM {catalog_keyspace}.accounts");
    let rows = crate::cassandra_util::query_rows::<OpError>(
        &Arc::clone(session),
        &query,
        cdrs_tokio::query_values!(),
        "pending_data_migrations",
    )
    .await?;

    let mut pending = std::collections::BTreeSet::new();
    for row in rows {
        let account_id: String =
            crate::cassandra_util::get_column(&row, "account_id", "pending_data_migrations")?;
        let keyspace = account_keyspace_fn(&account_id);
        for (filename, _sql) in DATA_MIGRATIONS {
            if !is_migration_applied(session, &keyspace, filename).await? {
                pending.insert((*filename).to_owned());
            }
        }
    }
    Ok(pending.into_iter().collect())
}

/// Check if a migration has already been applied.
async fn is_migration_applied(
    session: &Arc<CassandraSession>,
    keyspace: &str,
    filename: &str,
) -> OpResult<bool> {
    let version = migration_version(filename)?;

    let check_cql = format!("SELECT version FROM {keyspace}.schema_history WHERE version = ?");

    let applied = session
        .query_with_values(check_cql, cdrs_tokio::query_values!(version))
        .await
        .ok()
        .and_then(|frame| frame.response_body().ok())
        .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
        .is_some_and(|rows| !rows.is_empty());

    Ok(applied)
}

/// Execute a migration by splitting on semicolons and running each statement
/// with the table it names written as `keyspace.table`.
///
/// The runners never select a keyspace with `USE` (#632). cdrs-tokio runs a
/// `USE` on one pooled connection and copies it to the others from a
/// background task, so a statement relying on it can run in the keyspace the
/// connection it lands on had selected before.
async fn execute_migration(
    session: &Arc<CassandraSession>,
    keyspace: &str,
    filename: &str,
    sql: &str,
) -> OpResult<()> {
    for statement in migration_statements(keyspace, filename, sql)? {
        session
            .query(statement)
            .await
            .map_err(|e| OpError::Internal(format!("Migration failed: {e}")))?;
    }
    Ok(())
}

/// The statements of migration file `filename`, split on semicolons, each
/// qualified with `keyspace` by [`qualify_statement`]. If one cannot be
/// qualified, the file fails before any of its statements runs.
#[doc(hidden)]
pub fn migration_statements(keyspace: &str, filename: &str, sql: &str) -> OpResult<Vec<String>> {
    sql.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(|statement| qualify_statement(keyspace, filename, statement))
        .collect()
}

/// `statement` with the table it names written as `keyspace.table`.
///
/// Recognises `CREATE TABLE [IF NOT EXISTS]`,
/// `CREATE INDEX [IF NOT EXISTS] [name] ON`, `ALTER TABLE [IF EXISTS]` and
/// `INSERT INTO`, each naming an unquoted table without a keyspace. Any other
/// statement is an error naming `filename`, so no statement runs in whatever
/// keyspace the session has selected.
fn qualify_statement(keyspace: &str, filename: &str, statement: &str) -> OpResult<String> {
    let body = skip_comments(statement);
    if body.is_empty() {
        return Err(OpError::Internal(format!(
            "Migration file {filename} has a fragment between semicolons that holds only a \
             comment (a semicolon inside a comment, or a comment after the last statement): {}",
            statement.lines().next().unwrap_or_default()
        )));
    }
    let at = table_name_offset(body).ok_or_else(|| {
        OpError::Internal(format!(
            "Migration file {filename} has a statement that names no table the runner can \
             qualify with a keyspace: {}",
            body.lines().next().unwrap_or_default()
        ))
    })?;
    let at = statement.len() - body.len() + at;
    Ok(format!(
        "{}{keyspace}.{}",
        &statement[..at],
        &statement[at..]
    ))
}

/// `statement` from its first character outside whitespace and `--` or `//`
/// line comments.
fn skip_comments(mut statement: &str) -> &str {
    loop {
        statement = statement.trim_start();
        if !statement.starts_with("--") && !statement.starts_with("//") {
            return statement;
        }
        statement = statement.split_once('\n').map_or("", |(_, rest)| rest);
    }
}

/// The byte offset in `body` of the table name, for the statement forms
/// [`qualify_statement`] recognises.
fn table_name_offset(body: &str) -> Option<usize> {
    fn is(word: Option<(usize, &str)>, keyword: &str) -> bool {
        word.is_some_and(|(_, word)| word.eq_ignore_ascii_case(keyword))
    }
    let mut words = words(body).peekable();
    let (first, second) = (words.next(), words.next());
    if is(first, "CREATE") && (is(second, "TABLE") || is(second, "INDEX")) {
        if words
            .next_if(|&(_, w)| w.eq_ignore_ascii_case("IF"))
            .is_some()
            && !(is(words.next(), "NOT") && is(words.next(), "EXISTS"))
        {
            return None;
        }
        // CREATE INDEX [name] ON table: the index name is optional.
        if is(second, "INDEX") && !is(words.next(), "ON") && !is(words.next(), "ON") {
            return None;
        }
    } else if is(first, "ALTER") && is(second, "TABLE") {
        if words
            .next_if(|&(_, w)| w.eq_ignore_ascii_case("IF"))
            .is_some()
            && !is(words.next(), "EXISTS")
        {
            return None;
        }
    } else if !(is(first, "INSERT") && is(second, "INTO")) {
        return None;
    }
    let (at, table) = words.next()?;
    let mut chars = table.chars();
    let unquoted = chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    unquoted.then_some(at)
}

/// The words of `body` with their byte offsets, a word being a run of
/// characters ended by whitespace or an opening parenthesis.
fn words(body: &str) -> impl Iterator<Item = (usize, &str)> {
    let mut at = 0;
    std::iter::from_fn(move || {
        let rest = &body[at..];
        let start = at + (rest.len() - rest.trim_start().len());
        let word = &body[start..];
        let end = word
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(word.len());
        at = start + end;
        (end > 0).then(|| (start, &word[..end]))
    })
}

/// Record a migration in the `schema_history` table.
async fn record_migration(
    session: &Arc<CassandraSession>,
    keyspace: &str,
    filename: &str,
) -> OpResult<()> {
    // Extract version and description from filename
    let version = migration_version(filename)?;

    let description = filename
        .split("__")
        .nth(1)
        .and_then(|s| s.strip_suffix(".cql"))
        .unwrap_or("unknown");

    let record_cql = format!(
        "INSERT INTO {keyspace}.schema_history (version, description, applied_at) VALUES (?, ?, toTimestamp(now()))"
    );

    session
        .query_with_values(record_cql, cdrs_tokio::query_values!(version, description))
        .await
        .map_err(|e| OpError::Internal(format!("Record migration: {e}")))?;

    Ok(())
}

/// The version a migration file records in `schema_history`: the integer of
/// its `V###__` prefix (`V001__initial_schema.cql` -> 1).
fn migration_version(filename: &str) -> OpResult<i32> {
    filename
        .strip_prefix("V")
        .and_then(|s| s.split("__").next())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| OpError::Internal(format!("Invalid migration filename: {filename}")))
}

/// The catalog migration files this binary carries that the catalog's
/// `schema_history` does not record, in order. Unlike the runner's per-file
/// check, a failed read is an error, not "pending".
pub(crate) async fn pending_catalog_migrations(
    session: &Arc<CassandraSession>,
    catalog_keyspace: &str,
) -> Result<Vec<&'static str>, extenddb_storage::error::StorageError> {
    use extenddb_storage::error::StorageError;
    let rows = crate::cassandra_util::query_rows::<StorageError>(
        session,
        &format!("SELECT version FROM {catalog_keyspace}.schema_history"),
        cdrs_tokio::query_values!(),
        "pending_catalog_migrations",
    )
    .await?;
    let mut recorded = std::collections::BTreeSet::new();
    for row in &rows {
        let version: i32 =
            crate::cassandra_util::get_column(row, "version", "pending_catalog_migrations")?;
        recorded.insert(version);
    }
    let mut pending = Vec::new();
    for (filename, _sql) in CATALOG_MIGRATIONS {
        let version = migration_version(filename).map_err(|e| match e {
            OpError::Validation(m)
            | OpError::AlreadyExists(m)
            | OpError::NotFound(m)
            | OpError::HasDependents(m)
            | OpError::Internal(m)
            | OpError::Database { message: m, .. } => StorageError::Internal(m),
        })?;
        if !recorded.contains(&version) {
            pending.push(*filename);
        }
    }
    Ok(pending)
}

#[cfg(test)]
mod tests {
    #[test]
    fn every_migration_file_has_a_distinct_version() {
        let mut seen = std::collections::BTreeSet::new();
        for (filename, _) in super::CATALOG_MIGRATIONS {
            let v = super::migration_version(filename).expect(filename);
            assert!(seen.insert(v), "{filename}: version {v} repeated");
        }
        assert_eq!(
            super::migration_version("V005__backup_definitions_and_restore_registration.cql")
                .expect("V005"),
            5
        );
        assert!(super::migration_version("005_x.cql").is_err());
    }

    #[test]
    fn qualifies_each_statement_form_with_the_keyspace() {
        for (statement, qualified) in [
            (
                "CREATE TABLE IF NOT EXISTS settings (\n    key text PRIMARY KEY\n)",
                "CREATE TABLE IF NOT EXISTS ks_a.settings (\n    key text PRIMARY KEY\n)",
            ),
            (
                "create table accounts(account_id text PRIMARY KEY)",
                "create table ks_a.accounts(account_id text PRIMARY KEY)",
            ),
            (
                "CREATE INDEX IF NOT EXISTS ON tables (table_id)",
                "CREATE INDEX IF NOT EXISTS ON ks_a.tables (table_id)",
            ),
            (
                "CREATE INDEX ON tables (table_id)",
                "CREATE INDEX ON ks_a.tables (table_id)",
            ),
            (
                "CREATE INDEX idx ON tables (table_id)",
                "CREATE INDEX idx ON ks_a.tables (table_id)",
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_txn_started_at ON transaction_ledger (started_at)",
                "CREATE INDEX IF NOT EXISTS idx_txn_started_at ON ks_a.transaction_ledger (started_at)",
            ),
            (
                "ALTER TABLE tables ADD IF NOT EXISTS ttl_backfill_cursor text",
                "ALTER TABLE ks_a.tables ADD IF NOT EXISTS ttl_backfill_cursor text",
            ),
            (
                "ALTER TABLE IF EXISTS tables ADD IF NOT EXISTS c text",
                "ALTER TABLE IF EXISTS ks_a.tables ADD IF NOT EXISTS c text",
            ),
            (
                "alter table if exists tables with comment = 'x'",
                "alter table if exists ks_a.tables with comment = 'x'",
            ),
            (
                "-- Seed settings\n// two comments\nINSERT INTO settings (key) VALUES ('a')",
                "-- Seed settings\n// two comments\nINSERT INTO ks_a.settings (key) VALUES ('a')",
            ),
        ] {
            let got =
                super::qualify_statement("ks_a", "V999__test.cql", statement).expect(statement);
            assert_eq!(got, qualified);
        }
    }

    #[test]
    fn rejects_statements_it_cannot_qualify() {
        for statement in [
            "UPDATE settings SET value = 'x' WHERE key = 'y'",
            "DROP TABLE settings",
            "CREATE TABLE other.settings (key text PRIMARY KEY)",
            "CREATE TABLE \"Settings\" (key text PRIMARY KEY)",
            "CREATE TABLE IF EXISTS settings (key text PRIMARY KEY)",
            "CREATE INDEX IF EXISTS idx ON tables (table_id)",
            "ALTER TABLE IF NOT EXISTS tables ADD c text",
            "CREATE INDEX idx settings (table_id)",
            "-- a comment and no statement",
        ] {
            assert!(
                super::qualify_statement("ks_a", "V999__test.cql", statement).is_err(),
                "{statement}"
            );
        }
    }

    /// A semicolon inside a comment, or a comment after a file's last
    /// statement, leaves a fragment that holds only a comment.
    #[test]
    fn a_fragment_of_only_a_comment_fails_naming_the_file() {
        let sql = "CREATE TABLE t (k int PRIMARY KEY);\n-- a note after the last statement\n";
        let Err(super::OpError::Internal(message)) =
            super::migration_statements("ks_a", "V999__note.cql", sql)
        else {
            panic!("a fragment that holds only a comment was accepted");
        };
        assert!(
            message.contains("V999__note.cql")
                && message.contains("holds only a comment")
                && message.ends_with("-- a note after the last statement"),
            "{message}"
        );
    }

    /// Every statement of every embedded migration file, as the runner sends
    /// it, is the file's statement with the keyspace written in once, in front
    /// of its table: after the statement's leading comments, as a word of its
    /// own after `ON` in a `CREATE INDEX`, after `INTO` in an `INSERT INTO`,
    /// and after `TABLE` or `EXISTS` otherwise, and not in front of the `IF`
    /// that opens an `IF EXISTS` or `IF NOT EXISTS` clause.
    #[test]
    fn every_migration_statement_names_the_keyspace() {
        let files = super::CATALOG_MIGRATIONS
            .iter()
            .chain(super::DATA_MIGRATIONS);
        for (filename, sql) in files {
            let sent = super::migration_statements("ks_a", filename, sql).expect(filename);
            let written: Vec<&str> = sql
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();
            assert_eq!(sent.len(), written.len(), "{filename}");
            for (sent, written) in sent.iter().zip(written) {
                assert_eq!(sent.matches("ks_a.").count(), 1, "{filename}: {sent}");
                assert_eq!(sent.replacen("ks_a.", "", 1), written, "{filename}");
                let (before, after) = super::skip_comments(sent)
                    .split_once("ks_a.")
                    .unwrap_or_else(|| panic!("{filename}: keyspace in a leading comment: {sent}"));
                let keyword = if before.ends_with(char::is_whitespace) {
                    before.split_whitespace().next_back().unwrap_or_default()
                } else {
                    ""
                };
                let table = after
                    .split(|c: char| c.is_whitespace() || c == '(')
                    .next()
                    .unwrap_or_default();
                let mut words = super::skip_comments(written)
                    .split_whitespace()
                    .map(str::to_ascii_uppercase);
                let expected: &[&str] = match (words.next().as_deref(), words.next().as_deref()) {
                    (Some("CREATE"), Some("INDEX")) => &["ON"],
                    (Some("INSERT"), Some("INTO")) => &["INTO"],
                    _ => &["TABLE", "EXISTS"],
                };
                assert!(
                    expected.iter().any(|k| keyword.eq_ignore_ascii_case(k))
                        && !table.is_empty()
                        && !table.eq_ignore_ascii_case("IF"),
                    "{filename}: keyspace not in front of the table: {sent}"
                );
            }
        }
    }
}
