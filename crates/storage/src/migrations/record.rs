// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How a backend records a body: the ledger row and the version row, written
//! in the same order on every backend, and the transaction they commit in.
//!
//! A backend implements [`LedgerWrites`] for a store with its own statements
//! and driver calls, and calls [`record_rows`] after the body. A store with
//! transactions that are statements on one connection (the SQL stores) runs
//! the body and the rows through [`in_transaction`]; a store without them
//! (Cassandra), or a body that runs outside one (PostgreSQL migration 4),
//! records the rows after the body.

use std::future::Future;

use async_trait::async_trait;

use super::ledger::LedgerRow;

/// The writes that record a body in one store, in the store's own statements.
#[async_trait]
pub trait LedgerWrites: Sync {
    /// The error the store's driver calls return.
    type Error: Send;

    /// Create the ledger if it does not exist. The runner creates it with
    /// the first row it writes, so no migration body can change it.
    async fn create_ledger(&self) -> Result<(), Self::Error>;

    /// Insert `row`, whose notes are set.
    async fn insert_row(&self, row: &LedgerRow) -> Result<(), Self::Error>;

    /// Set the catalog's `catalog_version` settings row to `version`.
    async fn write_version(&self, version: u32) -> Result<(), Self::Error>;
}

/// Record a body that ran: `row` with `notes` one per line, after creating
/// the ledger, then the version, each when given.
///
/// # Errors
///
/// The first write that fails.
pub async fn record_rows<W: LedgerWrites>(
    store: &W,
    row: Option<LedgerRow>,
    notes: &[String],
    catalog_version: Option<u32>,
) -> Result<(), W::Error> {
    if let Some(mut row) = row {
        row.notes = notes.join("\n");
        store.create_ledger().await?;
        store.insert_row(&row).await?;
    }
    if let Some(version) = catalog_version {
        store.write_version(version).await?;
    }
    Ok(())
}

/// The statement that creates the ledger in a SQL store. `applied_at` is
/// that column's type and default, the one part SQL dialects write
/// differently.
#[must_use]
pub fn sql_ledger_ddl(applied_at: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS extenddb_migrations (id INTEGER PRIMARY KEY, name TEXT NOT \
         NULL, kind TEXT NOT NULL, checksum TEXT NOT NULL, outcome TEXT NOT NULL, notes TEXT NOT \
         NULL DEFAULT '', applied_at {applied_at}, applied_by TEXT NOT NULL)"
    )
}

/// Run `work` in a transaction: `begin`, `work`, then `commit`. When `work`
/// or `commit` fails, `rollback` runs and the first error is returned. When
/// `begin` fails, its error is returned and nothing else runs. Each future
/// runs only when awaited here, so the caller builds all four up front.
///
/// # Errors
///
/// The error of `begin`, `work` or `commit`.
pub async fn in_transaction<T, E>(
    begin: impl Future<Output = Result<(), E>>,
    work: impl Future<Output = Result<T, E>>,
    commit: impl Future<Output = Result<(), E>>,
    rollback: impl Future<Output = ()>,
) -> Result<T, E> {
    begin.await?;
    let result = match work.await {
        Ok(value) => commit.await.map(|()| value),
        Err(e) => Err(e),
    };
    if result.is_err() {
        rollback.await;
    }
    result
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// The calls a fake store received, in order.
    #[derive(Default)]
    struct Calls(Mutex<Vec<String>>);

    impl Calls {
        fn push(&self, call: impl Into<String>) {
            self.0.lock().unwrap().push(call.into());
        }

        fn take(&self) -> Vec<String> {
            std::mem::take(&mut self.0.lock().unwrap())
        }
    }

    #[async_trait]
    impl LedgerWrites for Calls {
        type Error = String;

        async fn create_ledger(&self) -> Result<(), String> {
            self.push("create");
            Ok(())
        }

        async fn insert_row(&self, row: &LedgerRow) -> Result<(), String> {
            self.push(format!("insert {} {:?}", row.id, row.notes));
            Ok(())
        }

        async fn write_version(&self, version: u32) -> Result<(), String> {
            self.push(format!("version {version}"));
            Ok(())
        }
    }

    fn row() -> LedgerRow {
        LedgerRow {
            id: 2,
            name: "resource_tag_condition_keys".to_owned(),
            kind: super::super::Kind::Breaking,
            checksum: "c".to_owned(),
            outcome: super::super::Outcome::Applied,
            notes: String::new(),
            applied_at: String::new(),
            applied_by: "test".to_owned(),
        }
    }

    /// SQLite stores the statement's text in `sqlite_master`, and the schema
    /// files record it, so the text is pinned.
    #[test]
    fn the_sql_ledger_statement_is_the_one_the_stores_record() {
        assert_eq!(
            sql_ledger_ddl("TIMESTAMPTZ NOT NULL DEFAULT now()"),
            "CREATE TABLE IF NOT EXISTS extenddb_migrations (id INTEGER PRIMARY KEY, name TEXT \
             NOT NULL, kind TEXT NOT NULL, checksum TEXT NOT NULL, outcome TEXT NOT NULL, notes \
             TEXT NOT NULL DEFAULT '', applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), applied_by \
             TEXT NOT NULL)"
        );
    }

    #[tokio::test]
    async fn the_rows_are_written_in_order_with_the_notes() {
        let calls = Calls::default();
        let notes = ["one".to_owned(), "two".to_owned()];
        record_rows(&calls, Some(row()), &notes, Some(2))
            .await
            .unwrap();
        assert_eq!(
            calls.take(),
            ["create", "insert 2 \"one\\ntwo\"", "version 2"]
        );
        record_rows(&calls, None, &notes, Some(3)).await.unwrap();
        assert_eq!(calls.take(), ["version 3"]);
        record_rows(&calls, Some(row()), &[], None).await.unwrap();
        assert_eq!(calls.take(), ["create", "insert 2 \"\""]);
    }

    #[tokio::test]
    async fn a_failed_body_or_commit_rolls_back_and_a_failed_begin_runs_nothing() {
        let calls = Calls::default();
        let step = |name: &'static str, ok: bool| {
            let calls = &calls;
            async move {
                calls.push(name);
                if ok { Ok(()) } else { Err(name.to_owned()) }
            }
        };
        let rollback = || async { calls.push("rollback") };
        let done = in_transaction(
            step("begin", true),
            step("work", true),
            step("commit", true),
            rollback(),
        )
        .await;
        assert_eq!(done, Ok(()));
        assert_eq!(calls.take(), ["begin", "work", "commit"]);
        let failed = in_transaction(
            step("begin", true),
            step("work", false),
            step("commit", true),
            rollback(),
        )
        .await;
        assert_eq!(failed, Err("work".to_owned()));
        assert_eq!(calls.take(), ["begin", "work", "rollback"]);
        let failed = in_transaction(
            step("begin", true),
            step("work", true),
            step("commit", false),
            rollback(),
        )
        .await;
        assert_eq!(failed, Err("commit".to_owned()));
        assert_eq!(calls.take(), ["begin", "work", "commit", "rollback"]);
        let failed = in_transaction(
            step("begin", false),
            step("work", true),
            step("commit", true),
            rollback(),
        )
        .await;
        assert_eq!(failed, Err("begin".to_owned()));
        assert_eq!(calls.take(), ["begin"]);
    }
}
