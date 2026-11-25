// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A bounded, synchronous reimplementation of the slice of the `sqlx` API that
//! the SQLite storage backend uses, driven over `sqlite-wasm-rs`'s raw
//! `libsqlite3` C ABI.
//!
//! # Why this exists
//!
//! `sqlx`'s SQLite driver (`sqlx-sqlite`) spawns a dedicated OS thread per
//! connection and depends on the C `libsqlite3-sys`, neither of which a
//! browser `wasm32-unknown-unknown` module can provide (no threads, no C
//! sysroot). The WebAssembly build of the SQLite backend therefore drops
//! `sqlx` + `libsqlite3-sys` and resolves its `sqlx::` paths to this module
//! instead (`extern crate self as sqlx;` at the crate root, with this module
//! re-exported there crate-privately), running the backend's SQL against a
//! real SQLite compiled for wasm (`sqlite-wasm-rs`, in-memory VFS). On every
//! other target the backend keeps real `sqlx` + C SQLite; this module is
//! excluded there (see the `#[cfg(target_arch = "wasm32")]` on its
//! declaration).
//!
//! # What follows `sqlx-sqlite` 0.8.6
//!
//! The rules below are taken from `sqlx-sqlite` 0.8.6 and `sqlx-core` 0.8.6.
//! The crate's `tests/wasm_shim.rs` holds a row per rule and runs on both
//! targets: natively against `sqlx` over the C SQLite, on wasm32 against this
//! module. A row expects the same outcome from both, except its rows named
//! `difference: ...`, which expect each side's own outcome.
//!
//! * **Errors.** [`Error`] has the variants of `sqlx::Error` this module can
//!   produce, with `sqlx`'s `Display` text. A database error carries the
//!   `sqlite3_errmsg` text and the SQLite extended result code and displays as
//!   `(code: <code>) <message>`. The backend reads the message to recognise
//!   UNIQUE / foreign-key violations and the code to recognise the transient
//!   BUSY / LOCKED codes, and some error texts reach its callers.
//! * **Decoding.** A non-NULL value must be of a storage class compatible with
//!   the Rust type (`String` and `serde_json::Value`: TEXT; `i64`, `i32` and
//!   `bool`: INTEGER; `f64`: REAL; `Vec<u8>`: BLOB or TEXT), or the read fails
//!   with [`Error::ColumnDecode`] and `sqlx`'s "mismatched types" text. A NULL
//!   skips that check and reads as the type's empty value (`""`, `0`, `0.0`,
//!   `false`, no bytes), because `sqlx` reads it through the plain C
//!   accessors; for `serde_json::Value` that is a parse of the empty string,
//!   which fails. An `i64` that does not fit an `i32`, invalid UTF-8 read as a
//!   string, and invalid JSON are [`Error::ColumnDecode`] too. A missing column
//!   name is [`Error::ColumnNotFound`], an index past the end
//!   [`Error::ColumnIndexOutOfBounds`]; a repeated column name resolves to the
//!   last column of that name.
//! * **Binding.** Each statement binds its own placeholders,
//!   `1..=sqlite3_bind_parameter_count`. `?NNN` and `$NNN` take argument NNN; a
//!   bare `?` takes the next argument, counting on from the arguments earlier
//!   statements of the same SQL used; `:name`, `@name` and a non-numeric
//!   `$name` are [`Error::Protocol`]. Binding stops at the first placeholder
//!   with no argument, so it and the ones after it are NULL; arguments with no
//!   placeholder are not bound. A call that carries no arguments, `raw_sql`
//!   and `Executor::execute` on a `&str`, binds nothing, so every placeholder
//!   in its SQL, `:name`, `@name`, `$name` and `$0` included, is NULL; a
//!   query built with `query`, `query_as` or `query_scalar` carries arguments
//!   even when none is bound, and binds as above.
//! * **Running SQL.** The SQL is trimmed and every statement in it runs in
//!   turn. `rows_affected` adds up `sqlite3_changes` after each statement (so a
//!   `SELECT` counts the changes of the last write before it, as SQLite reports
//!   them); SQL with no statement reports 0. `fetch_optional` and `fetch_one`
//!   stop at the first row: later rows and statements do not run and their
//!   errors are not seen. `fetch` yields each row in order, a row that fails to
//!   decode as an error in its place, and stops after a database error.
//! * **The pool.** The pool holds one connection. While a [`Transaction`] or a
//!   [`PoolConnection`] holds it, a statement on the pool, `acquire`, `begin`
//!   and `begin_with` fail with [`Error::PoolTimedOut`]; after
//!   [`SqlitePool::close`] they fail with [`Error::PoolClosed`]. A `begin_with`
//!   statement that leaves the connection in autocommit mode fails with
//!   [`Error::BeginFailed`].
//! * **Connection settings.** A connection opens with a 5 s busy timeout and
//!   with foreign keys enforced (`PRAGMA foreign_keys = ON`), as
//!   `sqlx-sqlite`'s default connect options set them on every connection it
//!   opens.
//!
//! # Documented differences from `sqlx-sqlite` 0.8.6
//!
//! The table in `tests/wasm_shim.rs` has a `difference: ...` row for each of
//! **One location**, **One connection** and **After a failed `begin_with`**,
//! and one per terminal (`fetch_one`, `fetch_optional`, `execute`) for
//! **`$0`**; its `DIFFERENCE_ROWS` counts them. The other entries have no
//! `difference: ...` row.
//!
//! * **One location.** The only location that parses is `sqlite::memory:`,
//!   the URL the backend builds for the in-memory database
//!   `SqliteEngine::new_in_memory` opens. Every other location, a file path or
//!   another spelling of an in-memory database such as `:memory:`, is
//!   [`Error::Configuration`] saying that the WebAssembly build opens only an
//!   in-memory database; `sqlx` parses each of them. The connection opens a
//!   private `:memory:` database, not the shared-cache one `sqlx` names.
//! * **The in-memory file name.** [`SqliteConnectOptions::get_filename`]
//!   returns `:memory:`, the name the connection opens; `sqlx` returns
//!   `file:sqlx-in-memory-<n>`, the name of its shared-cache database. The
//!   backend reads the name only in `serve_lock::database_file`, which takes
//!   either name for an in-memory location; a row of the table checks its
//!   answer for the location `:memory:` (the URL `sqlite::memory:`), no
//!   database file, on both targets.
//!
//! * **No acquire wait.** `sqlx` waits up to the pool's acquire timeout (30 s
//!   by default) for the connection before it returns
//!   [`Error::PoolTimedOut`]; this module returns it at once. The transport is
//!   synchronous on a single thread, so nothing could hand the connection back
//!   during a wait. The two rows of the table that find the connection
//!   checked out give their native pools a 50 ms acquire timeout, so they
//!   expect `PoolTimedOut` from both sides.
//! * **One connection.** `max_connections`, `min_connections`, `idle_timeout`
//!   and `max_lifetime` are accepted and not used: the pool never opens a
//!   second connection and never recycles the one it has. Under `sqlx` an
//!   in-memory pool with `max_connections(2)` runs a statement while a
//!   transaction holds the first connection; here it fails with
//!   [`Error::PoolTimedOut`].
//! * **After a failed `begin_with`.** `sqlx` counts the transaction a
//!   `begin_with` statement was meant to open even when it fails with
//!   [`Error::BeginFailed`], so its next `begin_with` on that connection fails
//!   with `InvalidSavePointStatement`; here the connection is left as it was
//!   and the next `begin_with` runs.
//! * **`$0`.** A `$0` placeholder in a query built with `query`, `query_as`
//!   or `query_scalar` is [`Error::Protocol`] from every terminal.
//!   Under `sqlx` the argument lookup for it (`arguments.rs:116`, the index
//!   `n - 1` with `n = 0`) panics the connection's worker thread while it
//!   binds, before the statement runs, and the call returns as if the
//!   statement had produced no row: `execute` returns `Ok` with 0 rows
//!   affected, `fetch_optional` returns `Ok(None)` and `fetch_one` returns
//!   [`Error::RowNotFound`].
//! * **`fetch` runs ahead.** `fetch` runs the whole query before it yields the
//!   first row, so dropping the stream early does not stop later statements
//!   from running.
//! * **SQLite build.** The bundled SQLite (3.54.1, built with
//!   `sqlite-wasm-rs`'s compile flags) is a different build from the one
//!   `libsqlite3-sys` links on other targets (3.46.0), so the query planner may
//!   order rows that are not constrained by an `ORDER BY` differently. Callers
//!   that compare unordered result sets sort them first.
//!
//! # Concurrency
//!
//! A browser module is single-threaded and the backend uses one connection
//! serially, so the connection handle is shared with `Arc` and asserted
//! `Send + Sync` only where the target has no threads (see [`RawConn`]); the
//! query terminals are `async fn`s that complete synchronously inside the
//! future, so the backend's `.await`s on them complete at once.

use std::borrow::Cow;
use std::ffi::{CStr, CString};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::os::raw::c_char;
use std::path::Path;
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlite_wasm_rs as ffi;

// ─────────────────────────────── errors ────────────────────────────────

/// The boxed error source of [`Error::Configuration`] and
/// [`Error::ColumnDecode`], as in `sqlx`.
pub type BoxDynError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A database-level error, mirroring the slice of `sqlx::error::DatabaseError`
/// the backend reads: the driver message and the vendor result code.
///
/// `Send + Sync` so [`Error`] (which boxes this) is `Send + Sync` like
/// `sqlx::Error`; the storage trait futures above the shim are `Send` and hold
/// an error across their `.await` points.
pub trait DatabaseError: fmt::Debug + fmt::Display + Send + Sync {
    /// The driver message (`sqlite3_errmsg`).
    fn message(&self) -> &str;
    /// The SQLite extended result code as a string, so the backend's
    /// `code.parse::<i64>()` classification reads it.
    fn code(&self) -> Option<Cow<'_, str>>;
}

/// Concrete [`DatabaseError`] carrying the SQLite result code and message.
#[derive(Debug)]
pub struct SqliteDbError {
    code: i32,
    message: String,
}

impl fmt::Display for SqliteDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `sqlx-sqlite` shows the code too: some messages are ambiguous
        // without it ("database is locked" is BUSY, "database table is
        // locked" is LOCKED).
        write!(f, "(code: {}) {}", self.code, self.message)
    }
}

impl DatabaseError for SqliteDbError {
    fn message(&self) -> &str {
        &self.message
    }
    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Owned(self.code.to_string()))
    }
}

/// The variants of `sqlx::Error` this module produces or the backend's error
/// handling names, with `sqlx`'s `Display` text.
#[derive(Debug)]
pub enum Error {
    /// A connection string that does not parse.
    Configuration(BoxDynError),
    /// A SQLite database error (constraint violation, busy, etc.).
    Database(Box<dyn DatabaseError>),
    /// An I/O error (never produced by the in-memory transport; the backend's
    /// error classification names it).
    Io(std::io::Error),
    /// Unexpected or invalid data, such as a placeholder form `sqlx` does not
    /// bind.
    Protocol(String),
    /// A `fetch_one` found no row.
    RowNotFound,
    /// A column index past the end of the row.
    ColumnIndexOutOfBounds {
        /// The index asked for.
        index: usize,
        /// The number of columns in the row.
        len: usize,
    },
    /// No column of the given name.
    ColumnNotFound(String),
    /// A column value that does not decode into the Rust type asked for.
    ColumnDecode {
        /// The column, as `sqlx` prints it: `0` for an index, `"name"` for a
        /// name.
        index: String,
        /// Why the value does not decode.
        source: BoxDynError,
    },
    /// The pool's connection is checked out.
    PoolTimedOut,
    /// The pool was closed.
    PoolClosed,
    /// The driver worker crashed (no worker here; the backend's error
    /// classification names it).
    WorkerCrashed,
    /// A `begin_with` statement did not open a transaction.
    BeginFailed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Configuration(e) => write!(f, "error with configuration: {e}"),
            Error::Database(e) => write!(f, "error returned from database: {e}"),
            Error::Io(e) => write!(f, "error communicating with database: {e}"),
            Error::Protocol(m) => write!(f, "encountered unexpected or invalid data: {m}"),
            Error::RowNotFound => {
                f.write_str("no rows returned by a query that expected to return at least one row")
            }
            Error::ColumnIndexOutOfBounds { index, len } => write!(
                f,
                "column index out of bounds: the len is {len}, but the index is {index}"
            ),
            Error::ColumnNotFound(name) => write!(f, "no column found for name: {name}"),
            Error::ColumnDecode { index, source } => {
                write!(f, "error occurred while decoding column {index}: {source}")
            }
            Error::PoolTimedOut => {
                f.write_str("pool timed out while waiting for an open connection")
            }
            Error::PoolClosed => f.write_str("attempted to acquire a connection on a closed pool"),
            Error::WorkerCrashed => {
                f.write_str("attempted to communicate with a crashed background worker")
            }
            Error::BeginFailed => f.write_str(
                "got unexpected connection status after attempting to begin transaction",
            ),
        }
    }
}

// ───────────────────────── raw connection ──────────────────────────────

/// Owns the `*mut sqlite3` and closes it when the last handle drops, plus the
/// flag that marks the connection checked out of its pool.
///
/// `pub` only to satisfy the private-in-public lint on [`Executor::handle`].
#[derive(Debug)]
pub struct RawConn {
    db: *mut ffi::sqlite3,
    /// Set while a [`Transaction`] or a [`PoolConnection`] from the pool holds
    /// this connection (see [`Checkout`]).
    checked_out: AtomicBool,
}

impl Drop for RawConn {
    fn drop(&mut self) {
        // SAFETY: `db` was produced by `sqlite3_open_v2` and is closed exactly
        // once, here, when the last `Arc<RawConn>` drops. No statement is live:
        // every prepared statement is finalized at the end of the call that
        // created it (see `Statement::drop`).
        unsafe {
            ffi::sqlite3_close(self.db);
        }
    }
}

/// Shared handle to the single serial connection.
pub type Handle = Arc<RawConn>;

// On a target with no threads (`wasm32-unknown-unknown`, the browser build),
// the `*mut sqlite3` inside `RawConn` is only ever touched from the one and
// only thread, so asserting `Send + Sync` — which the storage traits above the
// shim require, and which the raw pointer does not derive — is sound. A
// threaded wasm target (the `atomics` feature) can move values between workers,
// where the assertion would be unsound, so it is gated off there.
#[cfg(not(target_feature = "atomics"))]
// SAFETY: see the comment above; sound only because this target is
// single-threaded, hence the `not(target_feature = "atomics")` gate.
unsafe impl Send for RawConn {}
#[cfg(not(target_feature = "atomics"))]
// SAFETY: as for the `Send` impl above.
unsafe impl Sync for RawConn {}

/// Build an [`Error::Database`] from the connection's current error state.
fn db_error(db: *mut ffi::sqlite3) -> Error {
    // SAFETY: `db` is a live connection; `sqlite3_extended_errcode` and
    // `sqlite3_errmsg` read its last-error state; the errmsg pointer is valid
    // until the next call on this connection and is copied out here.
    let (code, message) = unsafe {
        let code = ffi::sqlite3_extended_errcode(db);
        let raw = ffi::sqlite3_errmsg(db);
        let message = if raw.is_null() {
            String::new()
        } else {
            CStr::from_ptr(raw).to_string_lossy().into_owned()
        };
        (code, message)
    };
    Error::Database(Box::new(SqliteDbError { code, message }))
}

/// Whether the connection is in autocommit mode, that is, outside any
/// transaction.
fn autocommit(handle: &Handle) -> bool {
    // SAFETY: `db` is live; `sqlite3_get_autocommit` is a plain read.
    unsafe { ffi::sqlite3_get_autocommit(handle.db) != 0 }
}

/// Open a private in-memory database, the only kind of database a location
/// that parses names, and return the shared handle, with a 5 s busy timeout
/// and foreign keys enforced.
fn open() -> Result<Handle, Error> {
    let mut db: *mut ffi::sqlite3 = ptr::null_mut();
    // SAFETY: a valid NUL-terminated name and a valid out-pointer are passed;
    // the returned handle is stored in `RawConn`, which closes it on drop. On a
    // non-OK return SQLite still allocates a handle that must be closed, so the
    // handle is wrapped before the result is checked.
    let rc = unsafe {
        ffi::sqlite3_open_v2(
            c":memory:".as_ptr(),
            &raw mut db,
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
            ptr::null(),
        )
    };
    let handle = Arc::new(RawConn {
        db,
        checked_out: AtomicBool::new(false),
    });
    if rc != ffi::SQLITE_OK {
        return Err(db_error(db));
    }
    // Unless its options say otherwise, `sqlx-sqlite` gives every connection
    // it opens a 5 s busy timeout, set through the C API, and
    // `PRAGMA foreign_keys = ON`, one of its default pragmas. SQLite leaves
    // foreign keys off unless it is built with `SQLITE_DEFAULT_FOREIGN_KEYS`,
    // which `sqlite-wasm-rs` does not set.
    // SAFETY: `db` is the live handle opened above.
    let rc = unsafe { ffi::sqlite3_busy_timeout(db, 5_000) };
    if rc != ffi::SQLITE_OK {
        return Err(db_error(db));
    }
    run_exec(&handle, "PRAGMA foreign_keys = ON")?;
    Ok(handle)
}

// ──────────────────────── database marker / executor ───────────────────

/// The database marker, mirroring `sqlx::Sqlite`. Ties a query builder's
/// inferred `DB` type to the executor, so both the turbofish `::<_, O>` form
/// and the inference-only form resolve.
#[derive(Debug, Clone, Copy)]
pub struct Sqlite;

/// A target a query can run against: the pool, a connection, or a transaction.
/// All expose the one serial connection handle.
pub trait Executor {
    /// The database the executor belongs to (always [`Sqlite`] here).
    type Database;
    /// The shared connection handle this executor runs statements on.
    fn handle(&self) -> Handle;

    /// Fail if a statement cannot run on this executor now. The default
    /// allows it; the pool fails while its connection is checked out or after
    /// it is closed.
    fn ensure_available(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Run a statement for its side effect, discarding any rows.
    ///
    /// Mirrors `sqlx::Executor::execute`, which the pragma setter calls as
    /// `conn.execute("PRAGMA ...")`.
    fn execute<'a>(
        self,
        sql: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, Error>> + 'a>>
    where
        Self: Sized + 'a,
    {
        Box::pin(async move {
            self.ensure_available()?;
            run_exec(&self.handle(), sql)
        })
    }
}

impl Executor for &SqlitePool {
    type Database = Sqlite;
    fn handle(&self) -> Handle {
        Arc::clone(&self.handle)
    }
    fn ensure_available(&self) -> Result<(), Error> {
        // `sqlx` checks a connection out for the statement and back in after
        // it; the statement runs synchronously, so taking and releasing the
        // checkout here has the same outcome.
        Checkout::take(self).map(drop)
    }
}

impl Executor for &mut SqliteConnection {
    type Database = Sqlite;
    fn handle(&self) -> Handle {
        Arc::clone(&self.handle)
    }
}

impl<DB> Executor for &mut Transaction<'_, DB> {
    type Database = DB;
    fn handle(&self) -> Handle {
        Arc::clone(&self.conn.handle)
    }
}

/// Mirrors `sqlx::SqliteExecutor<'c>` — an [`Executor`] whose database is
/// [`Sqlite`]. Used as a generic bound by the backend's executor-polymorphic
/// helpers. The lifetime parameter is phantom here (the executor borrows
/// nothing).
pub trait SqliteExecutor<'c>: Executor {}

impl<E: Executor> SqliteExecutor<'_> for E {}

// ─────────────────────────── pool / connection ─────────────────────────

/// A connection pool: a clone-able handle to the single serial connection.
#[derive(Clone, Debug)]
pub struct SqlitePool {
    handle: Handle,
    max_connections: u32,
    /// Shared by every clone of the pool, like `sqlx`'s pool state.
    closed: Arc<AtomicBool>,
}

/// The pool's one connection, checked out until this drops.
///
/// `sqlx` hands a statement, `acquire`, `begin` or `begin_with` a connection
/// of the pool, waiting for one while all are in use. Here the pool has one
/// connection; while a [`Transaction`] or [`PoolConnection`] holds it, the
/// next checkout fails with [`Error::PoolTimedOut`] at once (see the module
/// documentation for why there is no wait).
#[derive(Debug)]
struct Checkout {
    handle: Handle,
}

impl Checkout {
    fn take(pool: &SqlitePool) -> Result<Self, Error> {
        if pool.closed.load(Ordering::SeqCst) {
            return Err(Error::PoolClosed);
        }
        if pool.handle.checked_out.swap(true, Ordering::SeqCst) {
            return Err(Error::PoolTimedOut);
        }
        Ok(Checkout {
            handle: Arc::clone(&pool.handle),
        })
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        self.handle.checked_out.store(false, Ordering::SeqCst);
    }
}

impl SqlitePool {
    /// Begin a deferred transaction (`BEGIN`).
    pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>, Error> {
        let PoolConnection { conn, checkout, .. } = self.acquire().await?;
        Transaction::start(conn, checkout, None)
    }

    /// Begin a transaction with an explicit statement, e.g. `BEGIN IMMEDIATE`.
    pub async fn begin_with(&self, stmt: &str) -> Result<Transaction<'static, Sqlite>, Error> {
        let PoolConnection { conn, checkout, .. } = self.acquire().await?;
        Transaction::start(conn, checkout, Some(stmt))
    }

    /// Check the connection out of the pool until the returned value drops.
    pub async fn acquire(&self) -> Result<PoolConnection<Sqlite>, Error> {
        let checkout = Checkout::take(self)?;
        Ok(PoolConnection {
            conn: SqliteConnection {
                handle: Arc::clone(&self.handle),
            },
            checkout,
            _db: PhantomData,
        })
    }

    /// The options the pool was built with (only `get_max_connections` is read).
    pub fn options(&self) -> PoolOptionsView {
        PoolOptionsView {
            max_connections: self.max_connections,
        }
    }

    /// Close the pool: every later statement, `acquire`, `begin` and
    /// `begin_with` fails with [`Error::PoolClosed`]. The connection closes
    /// when the last handle drops.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

/// The read-only view of a pool's options the backend inspects.
#[derive(Clone, Copy, Debug)]
pub struct PoolOptionsView {
    max_connections: u32,
}

impl PoolOptionsView {
    /// The configured maximum connection count.
    pub fn get_max_connections(&self) -> u32 {
        self.max_connections
    }
}

/// A single SQLite connection handle.
#[derive(Clone, Debug)]
pub struct SqliteConnection {
    handle: Handle,
}

/// A connection checked out of a [`SqlitePool`], mirroring
/// `sqlx::pool::PoolConnection`. Dereferences to the connection, so
/// `&mut *conn` is an [`Executor`]; the pool gets the connection back when
/// this drops.
#[derive(Debug)]
pub struct PoolConnection<DB = Sqlite> {
    conn: SqliteConnection,
    checkout: Checkout,
    _db: PhantomData<DB>,
}

impl<DB> std::ops::Deref for PoolConnection<DB> {
    type Target = SqliteConnection;
    fn deref(&self) -> &SqliteConnection {
        &self.conn
    }
}

impl<DB> std::ops::DerefMut for PoolConnection<DB> {
    fn deref_mut(&mut self) -> &mut SqliteConnection {
        &mut self.conn
    }
}

/// An in-flight transaction. Rolls back on drop unless committed, like
/// `sqlx`'s.
///
/// Carries a lifetime and database-marker parameter so the backend's
/// `sqlx::Transaction<'_, sqlx::Sqlite>` spellings resolve; both are phantom,
/// since a transaction is begun only on the pool.
pub struct Transaction<'c, DB = Sqlite> {
    conn: SqliteConnection,
    open: bool,
    /// The pool's connection, checked out until the transaction drops;
    /// released after the rollback in `Drop`, since fields drop after
    /// `Drop::drop` runs.
    _checkout: Checkout,
    _marker: PhantomData<(&'c (), DB)>,
}

impl<DB> Transaction<'_, DB> {
    /// Run `statement` (`BEGIN` when `None`). A custom statement that leaves
    /// the connection in autocommit mode fails with [`Error::BeginFailed`], as
    /// in `sqlx-sqlite`.
    fn start(
        conn: SqliteConnection,
        checkout: Checkout,
        statement: Option<&str>,
    ) -> Result<Self, Error> {
        run_exec(&conn.handle, statement.unwrap_or("BEGIN"))?;
        if statement.is_some() && autocommit(&conn.handle) {
            return Err(Error::BeginFailed);
        }
        Ok(Transaction {
            conn,
            open: true,
            _checkout: checkout,
            _marker: PhantomData,
        })
    }

    /// Commit the transaction (`COMMIT`).
    pub async fn commit(mut self) -> Result<(), Error> {
        run_exec(&self.conn.handle, "COMMIT")?;
        self.open = false;
        Ok(())
    }

    /// Roll the transaction back (`ROLLBACK`).
    pub async fn rollback(mut self) -> Result<(), Error> {
        run_exec(&self.conn.handle, "ROLLBACK")?;
        self.open = false;
        Ok(())
    }
}

impl<DB> std::ops::Deref for Transaction<'_, DB> {
    type Target = SqliteConnection;
    fn deref(&self) -> &SqliteConnection {
        &self.conn
    }
}

impl<DB> std::ops::DerefMut for Transaction<'_, DB> {
    fn deref_mut(&mut self) -> &mut SqliteConnection {
        &mut self.conn
    }
}

impl<DB> Drop for Transaction<'_, DB> {
    fn drop(&mut self) {
        if self.open {
            // Best-effort rollback; the connection is still valid.
            let _ = run_exec(&self.conn.handle, "ROLLBACK");
        }
    }
}

// ─────────────────────────── connect options ───────────────────────────

/// The one location that parses: the URL the backend builds for the in-memory
/// database `SqliteEngine::new_in_memory` opens (`sqlite_url(":memory:")`).
const IN_MEMORY_URL: &str = "sqlite::memory:";

/// Connection options, mirroring `sqlx::sqlite::SqliteConnectOptions` for the
/// one location the WebAssembly build opens, an in-memory database. They hold
/// nothing: that location is the only one that parses, and the pool's
/// connection opens a private `:memory:` database whatever the options.
#[derive(Clone, Debug)]
pub struct SqliteConnectOptions;

impl SqliteConnectOptions {
    /// The database file name: `:memory:`, the name the connection passes to
    /// `sqlite3_open_v2`.
    pub fn get_filename(&self) -> &Path {
        Path::new(":memory:")
    }

    /// The driver's row look-ahead buffer. There is no driver worker here, so
    /// the value is not used; the method exists for the backend's call.
    pub fn row_buffer_size(self, _rows: usize) -> Self {
        self
    }
}

impl std::str::FromStr for SqliteConnectOptions {
    type Err = Error;

    /// Parse `sqlite::memory:`, the location the backend opens on wasm32, and
    /// refuse every other location with [`Error::Configuration`]: the
    /// WebAssembly build opens only an in-memory database.
    fn from_str(url: &str) -> Result<Self, Error> {
        if url != IN_MEMORY_URL {
            return Err(Error::Configuration(
                extenddb_core::error::webassembly_unsupported_message(
                    &format!("The SQLite location {url:?}"),
                    "it opens only an in-memory database (sqlite::memory:)",
                )
                .into(),
            ));
        }
        Ok(SqliteConnectOptions)
    }
}

/// Metadata handed to an `after_connect` hook. Empty; the pool opens one
/// connection, so there is nothing to report.
#[derive(Clone, Copy, Debug, Default)]
pub struct PoolConnectionMetadata;

type AfterConnect = Box<
    dyn for<'a> Fn(
        &'a mut SqliteConnection,
        PoolConnectionMetadata,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>>,
>;

/// Pool builder, mirroring `sqlx::sqlite::SqlitePoolOptions`.
pub struct SqlitePoolOptions {
    max_connections: u32,
    after_connect: Option<AfterConnect>,
}

impl SqlitePoolOptions {
    /// A fresh pool builder.
    ///
    /// Crate-private, where `sqlx`'s is public, so that clippy's
    /// `new_without_default` does not ask for a `Default` impl that nothing
    /// would use. The backend calls it; `tests/wasm_shim.rs` starts from
    /// `wasm_sqlx_for_tests::pool_options`.
    pub(crate) fn new() -> Self {
        SqlitePoolOptions {
            max_connections: 1,
            after_connect: None,
        }
    }

    /// Set the maximum connection count the pool reports (it still opens one).
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = n;
        self
    }

    /// Accepted and not used: the pool opens one connection.
    pub fn min_connections(self, _n: u32) -> Self {
        self
    }

    /// Accepted and not used: the one connection is never closed for idling.
    pub fn idle_timeout(self, _timeout: Option<Duration>) -> Self {
        self
    }

    /// Accepted and not used: the one connection is never recycled.
    pub fn max_lifetime(self, _lifetime: Option<Duration>) -> Self {
        self
    }

    /// Register a hook run once on the single connection after it opens.
    pub fn after_connect<F>(mut self, callback: F) -> Self
    where
        F: for<'a> Fn(
                &'a mut SqliteConnection,
                PoolConnectionMetadata,
            ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>>
            + 'static,
    {
        self.after_connect = Some(Box::new(callback));
        self
    }

    /// Open the pool with the given connect options. Every location that
    /// parses names an in-memory database, so the options carry nothing the
    /// connection needs: it opens a private in-memory database.
    pub async fn connect_with(self, _options: SqliteConnectOptions) -> Result<SqlitePool, Error> {
        let handle = open()?;
        self.finish(handle).await
    }

    async fn finish(self, handle: Handle) -> Result<SqlitePool, Error> {
        if let Some(cb) = &self.after_connect {
            let mut conn = SqliteConnection {
                handle: Arc::clone(&handle),
            };
            cb(&mut conn, PoolConnectionMetadata).await?;
        }
        Ok(SqlitePool {
            handle,
            max_connections: self.max_connections,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }
}

// ──────────────────────────────── params ───────────────────────────────

/// An owned bound parameter value.
#[derive(Clone, Debug)]
pub enum Param {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// A value that can be bound to a positional placeholder.
pub trait Bind {
    /// Convert into the owned parameter representation.
    fn into_param(self) -> Param;
}

impl Bind for String {
    fn into_param(self) -> Param {
        Param::Text(self)
    }
}
impl Bind for &String {
    fn into_param(self) -> Param {
        Param::Text(self.clone())
    }
}
impl Bind for &str {
    fn into_param(self) -> Param {
        Param::Text(self.to_owned())
    }
}
impl Bind for i64 {
    fn into_param(self) -> Param {
        Param::Int(self)
    }
}
impl Bind for bool {
    fn into_param(self) -> Param {
        Param::Int(i64::from(self))
    }
}
impl Bind for f64 {
    fn into_param(self) -> Param {
        Param::Real(self)
    }
}
impl Bind for Vec<u8> {
    fn into_param(self) -> Param {
        Param::Blob(self)
    }
}
impl Bind for &[u8] {
    fn into_param(self) -> Param {
        Param::Blob(self.to_vec())
    }
}
impl Bind for &Vec<u8> {
    fn into_param(self) -> Param {
        Param::Blob(self.clone())
    }
}
impl<T: Bind> Bind for Option<T> {
    fn into_param(self) -> Param {
        match self {
            Some(v) => v.into_param(),
            None => Param::Null,
        }
    }
}
impl<T: Bind + Clone> Bind for &Option<T> {
    fn into_param(self) -> Param {
        self.clone().into_param()
    }
}

// ──────────────────────────────── rows ─────────────────────────────────

/// One column value, copied out of the statement so a [`Row`] outlives it.
/// Text is kept as raw bytes so that invalid UTF-8 is an error only when the
/// value is read as a string, as in `sqlx`.
#[derive(Clone, Debug)]
enum ColVal {
    Null,
    Int(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl ColVal {
    /// The SQL type name `sqlx-sqlite` gives a value of this storage class.
    fn kind(&self) -> &'static str {
        match self {
            ColVal::Null => "NULL",
            ColVal::Int(_) => "INTEGER",
            ColVal::Real(_) => "REAL",
            ColVal::Text(_) => "TEXT",
            ColVal::Blob(_) => "BLOB",
        }
    }
}

/// A borrowed column value, handed to [`Decode`].
#[derive(Clone, Copy, Debug)]
pub struct SqliteValueRef<'r>(&'r ColVal);

/// A fully materialized result row (owned values + column names).
#[derive(Clone, Debug)]
pub struct Row {
    names: Arc<Vec<String>>,
    vals: Vec<ColVal>,
}

/// A column selector: a positional index or a column name. `Debug` prints the
/// selector the way [`Error::ColumnDecode`] names the column.
pub trait ColIndex: fmt::Debug {
    /// Resolve to a 0-based column index within `row`.
    fn index(&self, row: &Row) -> Result<usize, Error>;
}

impl ColIndex for usize {
    fn index(&self, row: &Row) -> Result<usize, Error> {
        let len = row.vals.len();
        if *self < len {
            Ok(*self)
        } else {
            Err(Error::ColumnIndexOutOfBounds { index: *self, len })
        }
    }
}

impl ColIndex for &str {
    fn index(&self, row: &Row) -> Result<usize, Error> {
        // `sqlx-sqlite` maps each name to its position with later columns
        // overwriting earlier ones, so a repeated name finds the last column.
        row.names
            .iter()
            .rposition(|n| n == self)
            .ok_or_else(|| Error::ColumnNotFound((*self).to_owned()))
    }
}

impl Row {
    /// Decode a column, as `sqlx::Row::try_get` does: a non-NULL value whose
    /// storage class the type does not accept is a "mismatched types" error,
    /// and any decode error names the column.
    pub fn try_get<T: Decode, I: ColIndex>(&self, index: I) -> Result<T, Error> {
        let idx = index.index(self)?;
        let value = self.vals.get(idx).ok_or(Error::ColumnIndexOutOfBounds {
            index: idx,
            len: self.vals.len(),
        })?;
        let value = SqliteValueRef(value);
        if !matches!(value.0, ColVal::Null) && !T::compatible(value) {
            return Err(Error::ColumnDecode {
                index: format!("{index:?}"),
                source: format!(
                    "mismatched types; Rust type `{}` (as SQL type `{}`) is not compatible with \
                     SQL type `{}`",
                    std::any::type_name::<T>(),
                    T::sql_type(),
                    value.0.kind()
                )
                .into(),
            });
        }
        T::decode(value).map_err(|source| Error::ColumnDecode {
            index: format!("{index:?}"),
            source,
        })
    }
}

/// A value decodable from a SQLite column, mirroring `sqlx`'s `Type` and
/// `Decode` for SQLite.
///
/// [`Row::try_get`] calls [`Decode::decode`] only for a NULL or for a value
/// [`Decode::compatible`] accepts. A NULL decodes the way `sqlx-sqlite`'s
/// accessors read it: as `0`, `0.0` or no bytes.
pub trait Decode: Sized {
    /// The SQL type `sqlx` names for the Rust type in a mismatch message.
    fn sql_type() -> &'static str;
    /// Whether a non-NULL value of this storage class decodes into the type.
    fn compatible(value: SqliteValueRef<'_>) -> bool;
    /// Decode the value.
    fn decode(value: SqliteValueRef<'_>) -> Result<Self, BoxDynError>;
}

/// The error for a value `compatible` would have refused; [`Row::try_get`]
/// never passes one.
fn incompatible(value: SqliteValueRef<'_>) -> BoxDynError {
    format!("unexpected {} value", value.0.kind()).into()
}

/// The bytes of a TEXT or BLOB value; none for NULL, as SQLite reads it.
fn value_bytes(value: SqliteValueRef<'_>) -> Result<&[u8], BoxDynError> {
    match value.0 {
        ColVal::Null => Ok(&[]),
        ColVal::Text(b) | ColVal::Blob(b) => Ok(b),
        _ => Err(incompatible(value)),
    }
}

impl Decode for String {
    fn sql_type() -> &'static str {
        "TEXT"
    }
    fn compatible(value: SqliteValueRef<'_>) -> bool {
        matches!(value.0, ColVal::Text(_))
    }
    fn decode(value: SqliteValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(std::str::from_utf8(value_bytes(value)?)?.to_owned())
    }
}
impl Decode for i64 {
    fn sql_type() -> &'static str {
        "INTEGER"
    }
    fn compatible(value: SqliteValueRef<'_>) -> bool {
        matches!(value.0, ColVal::Int(_))
    }
    fn decode(value: SqliteValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.0 {
            ColVal::Null => Ok(0),
            ColVal::Int(i) => Ok(*i),
            _ => Err(incompatible(value)),
        }
    }
}
// The backend decodes no `i32` on wasm32. This impl serves the shim table's
// `i32` rows, a REAL, an `i64` too large for `i32` and a NULL read as `i32`,
// which compare this module's integer decoding with `sqlx`'s.
impl Decode for i32 {
    fn sql_type() -> &'static str {
        "INTEGER"
    }
    fn compatible(value: SqliteValueRef<'_>) -> bool {
        matches!(value.0, ColVal::Int(_))
    }
    fn decode(value: SqliteValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(i32::try_from(i64::decode(value)?)?)
    }
}
impl Decode for f64 {
    fn sql_type() -> &'static str {
        "REAL"
    }
    fn compatible(value: SqliteValueRef<'_>) -> bool {
        matches!(value.0, ColVal::Real(_))
    }
    fn decode(value: SqliteValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.0 {
            ColVal::Null => Ok(0.0),
            ColVal::Real(r) => Ok(*r),
            _ => Err(incompatible(value)),
        }
    }
}
impl Decode for bool {
    fn sql_type() -> &'static str {
        "BOOLEAN"
    }
    fn compatible(value: SqliteValueRef<'_>) -> bool {
        matches!(value.0, ColVal::Int(_))
    }
    fn decode(value: SqliteValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(i64::decode(value)? != 0)
    }
}
impl Decode for Vec<u8> {
    fn sql_type() -> &'static str {
        "BLOB"
    }
    fn compatible(value: SqliteValueRef<'_>) -> bool {
        matches!(value.0, ColVal::Blob(_) | ColVal::Text(_))
    }
    fn decode(value: SqliteValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(value_bytes(value)?.to_vec())
    }
}
impl Decode for serde_json::Value {
    fn sql_type() -> &'static str {
        "TEXT"
    }
    fn compatible(value: SqliteValueRef<'_>) -> bool {
        matches!(value.0, ColVal::Text(_))
    }
    fn decode(value: SqliteValueRef<'_>) -> Result<Self, BoxDynError> {
        // Read as a string, then parse: a NULL is the empty string, which is
        // not JSON.
        Ok(serde_json::from_str(std::str::from_utf8(value_bytes(
            value,
        )?)?)?)
    }
}
impl<T: Decode> Decode for Option<T> {
    fn sql_type() -> &'static str {
        T::sql_type()
    }
    fn compatible(value: SqliteValueRef<'_>) -> bool {
        T::compatible(value)
    }
    fn decode(value: SqliteValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.0 {
            ColVal::Null => Ok(None),
            _ => Ok(Some(T::decode(value)?)),
        }
    }
}

// ─────────────────────────────── FromRow ───────────────────────────────

/// A type decodable from a whole result row, mirroring `sqlx::FromRow`.
///
/// The backend's catalog row structs implement it with a by-name column decode
/// (`row.try_get("<field>")`), the default column mapping of `sqlx`'s own
/// `FromRow` derive, and it is implemented for tuples (by position) below.
pub trait FromRow: Sized {
    /// Decode `self` from `row`.
    fn from_row(row: &Row) -> Result<Self, Error>;
}

macro_rules! tuple_from_row {
    ($($idx:tt : $ty:ident),+) => {
        impl<$($ty: Decode),+> FromRow for ($($ty,)+) {
            fn from_row(row: &Row) -> Result<Self, Error> {
                Ok(($(row.try_get::<$ty, usize>($idx)?,)+))
            }
        }
    };
}
// One line per tuple arity the backend's queries decode; a query that
// decodes a tuple of another arity needs its line here.
tuple_from_row!(0: A);
tuple_from_row!(0: A, 1: B);
tuple_from_row!(0: A, 1: B, 2: C);
tuple_from_row!(0: A, 1: B, 2: C, 3: D);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P);

// ───────────────────────── statement execution ─────────────────────────

/// RAII wrapper that finalizes a prepared statement on drop.
struct Statement {
    stmt: *mut ffi::sqlite3_stmt,
}

impl Drop for Statement {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            // SAFETY: `stmt` came from `sqlite3_prepare_v2` and is finalized
            // exactly once, here.
            unsafe {
                ffi::sqlite3_finalize(self.stmt);
            }
        }
    }
}

/// The number at the start of `digits`, or `None` if it does not start with a
/// digit or the number overflows, like the `atoi` crate `sqlx-sqlite` uses.
fn leading_number(digits: &str) -> Option<usize> {
    let end = digits
        .bytes()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(digits.len());
    digits.get(..end).filter(|d| !d.is_empty())?.parse().ok()
}

/// Bind `params` to the placeholders of `stmt`, starting after the `offset`
/// arguments earlier statements of the same SQL used, and return how many
/// arguments this statement used.
///
/// This follows `SqliteArguments::bind` of `sqlx-sqlite` 0.8.6: placeholders
/// `1..=sqlite3_bind_parameter_count` are visited in order; `?NNN` and `$NNN`
/// take argument NNN and a nameless `?` the next one; any other name is
/// refused; binding stops at the first placeholder with no argument, which
/// SQLite then reads as NULL. Arguments past the placeholders are not bound.
/// The `Protocol` texts carry the `sqlx-sqlite` source location its
/// `err_protocol!` macro appends, so the message is the one `sqlx` returns.
fn bind_all(
    db: *mut ffi::sqlite3,
    stmt: *mut ffi::sqlite3_stmt,
    params: &[Param],
    offset: usize,
) -> Result<usize, Error> {
    // SAFETY: `stmt` is a live prepared statement; reading its placeholder
    // count is a simple accessor.
    let count = unsafe { ffi::sqlite3_bind_parameter_count(stmt) };
    let mut arg_i = offset;
    for pos in 1..=count {
        // SAFETY: `stmt` is live and `pos` is in `1..=count`; the returned
        // name (or NULL for a nameless `?`) is valid until the statement is
        // finalized and is copied out before then.
        let name = unsafe {
            let raw = ffi::sqlite3_bind_parameter_name(stmt, pos);
            (!raw.is_null()).then(|| CStr::from_ptr(raw).to_string_lossy().into_owned())
        };
        let n = match &name {
            None => {
                arg_i += 1;
                arg_i
            }
            Some(name) => {
                if let Some(digits) = name.strip_prefix('?') {
                    // SQLite accepts only digits after `?`.
                    leading_number(digits).ok_or_else(|| {
                        Error::Protocol(format!("parameter of the form ?NNN: {name}"))
                    })?
                } else if let Some(digits) = name.strip_prefix('$') {
                    leading_number(digits).ok_or_else(|| {
                        Error::Protocol(format!(
                            "parameters with non-integer names are not currently supported: \
                             {digits} (sqlx_sqlite::arguments:93)"
                        ))
                    })?
                } else {
                    return Err(Error::Protocol(format!(
                        "unsupported SQL parameter format: {name} (sqlx_sqlite::arguments:99)"
                    )));
                }
            }
        };
        if n > params.len() {
            break;
        }
        let param = n
            .checked_sub(1)
            .and_then(|i| params.get(i))
            .ok_or_else(|| {
                Error::Protocol(format!(
                    "parameter {} has no argument position",
                    name.as_deref().unwrap_or("?")
                ))
            })?;
        bind_param(db, stmt, pos, param)?;
    }
    Ok(arg_i - offset)
}

/// Bind one parameter to a prepared statement.
fn bind_param(
    db: *mut ffi::sqlite3,
    stmt: *mut ffi::sqlite3_stmt,
    pos: i32,
    param: &Param,
) -> Result<(), Error> {
    // SAFETY: `stmt` is a live prepared statement; `pos` is a valid 1-based
    // index; text/blob buffers are passed with `SQLITE_TRANSIENT` so SQLite
    // copies them immediately and the Rust-owned buffers need not outlive the
    // call.
    let rc = unsafe {
        match param {
            Param::Null => ffi::sqlite3_bind_null(stmt, pos),
            Param::Int(v) => ffi::sqlite3_bind_int64(stmt, pos, *v),
            Param::Real(v) => ffi::sqlite3_bind_double(stmt, pos, *v),
            Param::Text(s) => {
                let bytes = s.as_bytes();
                ffi::sqlite3_bind_text(
                    stmt,
                    pos,
                    bytes.as_ptr().cast::<c_char>(),
                    i32::try_from(bytes.len())
                        .map_err(|e| Error::Protocol(format!("text too large to bind: {e}")))?,
                    ffi::SQLITE_TRANSIENT(),
                )
            }
            Param::Blob(b) => ffi::sqlite3_bind_blob(
                stmt,
                pos,
                b.as_ptr().cast::<std::ffi::c_void>(),
                i32::try_from(b.len())
                    .map_err(|e| Error::Protocol(format!("blob too large to bind: {e}")))?,
                ffi::SQLITE_TRANSIENT(),
            ),
        }
    };
    if rc != ffi::SQLITE_OK {
        return Err(db_error(db));
    }
    Ok(())
}

/// Read the current result row's column names (once per statement).
fn column_names(stmt: *mut ffi::sqlite3_stmt) -> Vec<String> {
    // SAFETY: `stmt` is a live statement positioned on a row; the count and
    // each name pointer are valid until the next step/finalize and are copied.
    unsafe {
        let n = ffi::sqlite3_column_count(stmt);
        let mut names = Vec::with_capacity(usize::try_from(n.max(0)).unwrap_or(0));
        for i in 0..n {
            let raw = ffi::sqlite3_column_name(stmt, i);
            let name = if raw.is_null() {
                String::new()
            } else {
                CStr::from_ptr(raw).to_string_lossy().into_owned()
            };
            names.push(name);
        }
        names
    }
}

/// Read all columns of the current result row into owned values. TEXT is kept
/// as raw bytes (UTF-8 is checked when the value is read as a string, as in
/// `sqlx`).
fn read_row(stmt: *mut ffi::sqlite3_stmt) -> Vec<ColVal> {
    // SAFETY: `stmt` is positioned on a row (the caller stepped to SQLITE_ROW);
    // the column accessors are valid for `0..column_count`; blob/text pointers
    // are read with their byte length and copied before the next step.
    unsafe {
        let n = ffi::sqlite3_column_count(stmt);
        let mut vals = Vec::with_capacity(usize::try_from(n.max(0)).unwrap_or(0));
        for i in 0..n {
            let ty = ffi::sqlite3_column_type(stmt, i);
            let val = match ty {
                ffi::SQLITE_INTEGER => ColVal::Int(ffi::sqlite3_column_int64(stmt, i)),
                ffi::SQLITE_FLOAT => ColVal::Real(ffi::sqlite3_column_double(stmt, i)),
                ffi::SQLITE_NULL => ColVal::Null,
                ffi::SQLITE_BLOB => ColVal::Blob(column_bytes(stmt, i, false)),
                // SQLITE_TEXT (3) and anything else: read the raw text bytes.
                _ => ColVal::Text(column_bytes(stmt, i, true)),
            };
            vals.push(val);
        }
        vals
    }
}

/// Copy a column's bytes out as an owned `Vec<u8>`. `text` selects the text vs
/// blob accessor (both return the same underlying bytes).
///
/// # Safety
///
/// `stmt` must be a live statement positioned on a result row and `i` a valid
/// column index.
unsafe fn column_bytes(stmt: *mut ffi::sqlite3_stmt, i: i32, text: bool) -> Vec<u8> {
    unsafe {
        let len = ffi::sqlite3_column_bytes(stmt, i);
        let ptr = if text {
            ffi::sqlite3_column_text(stmt, i).cast::<u8>()
        } else {
            ffi::sqlite3_column_blob(stmt, i).cast::<u8>()
        };
        if ptr.is_null() || len <= 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(ptr, usize::try_from(len).unwrap_or(0)).to_vec()
        }
    }
}

/// Rows affected by a run, mirroring `SqliteQueryResult`.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryResult {
    rows_affected: u64,
}

impl QueryResult {
    /// `sqlite3_changes` added up over the statements that ran.
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}

/// Which rows a run keeps.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rows {
    /// None (an `execute`); every statement still runs to completion.
    Discard,
    /// Every row of every statement (`fetch_all`, `fetch`).
    All,
    /// The first row, after which nothing more runs (`fetch_optional`,
    /// `fetch_one`), like `sqlx-sqlite`'s worker with a row limit of 1.
    First,
}

/// What a run produced: the rows kept, the counters, and the database error
/// that stopped it, if one did. The rows before the error are kept so `fetch`
/// can yield them first.
struct Run {
    rows: Vec<Row>,
    result: QueryResult,
    error: Option<Error>,
}

impl Run {
    fn into_result(self) -> Result<(Vec<Row>, QueryResult), Error> {
        match self.error {
            Some(e) => Err(e),
            None => Ok((self.rows, self.result)),
        }
    }
}

/// Prepare, bind and step each statement of `sql` in turn, the way
/// `sqlx-sqlite` 0.8.6 runs a query: the SQL is trimmed, each statement binds
/// its own placeholders with the argument offset carried over (see
/// [`bind_all`]), `sqlite3_changes` is added up after each statement, and the
/// first error stops the run. SQL with no statement runs nothing and reports
/// zero.
///
/// `params` is `None` for a call that carries no arguments, `raw_sql` and
/// `Executor::execute` on a `&str`; `sqlx-sqlite` binds nothing for such a
/// call (`bind` in its `connection/execute.rs` runs `SqliteArguments::bind`
/// only when there are arguments), so every placeholder in its SQL is NULL.
/// The query builders always carry arguments, an empty list when nothing was
/// bound, as `sqlx::query` does.
fn run_query(handle: &Handle, sql: &str, params: Option<&[Param]>, keep: Rows) -> Run {
    let mut run = Run {
        rows: Vec::new(),
        result: QueryResult::default(),
        error: None,
    };
    if let Err(e) = run_statements(handle, sql.trim(), params, keep, &mut run) {
        run.error = Some(e);
    }
    run
}

fn run_statements(
    handle: &Handle,
    sql: &str,
    params: Option<&[Param]>,
    keep: Rows,
    run: &mut Run,
) -> Result<(), Error> {
    let db = handle.db;
    let c_sql = CString::new(sql).map_err(|e| Error::Protocol(e.to_string()))?;
    let end = c_sql.as_bytes().len();
    let start: *const c_char = c_sql.as_ptr();
    let mut cursor = start;
    let mut args_used = 0;

    // `cursor` stays inside `c_sql`, whose bytes end at offset `end`.
    while cursor.addr() - start.addr() < end {
        let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
        let mut tail: *const c_char = ptr::null();
        // SAFETY: `db` is live; `cursor` points into the NUL-terminated
        // `c_sql` buffer, held for the whole loop; `-1` lets SQLite read to the
        // NUL; the out-pointers are valid.
        let rc = unsafe { ffi::sqlite3_prepare_v2(db, cursor, -1, &raw mut stmt, &raw mut tail) };
        if rc != ffi::SQLITE_OK {
            return Err(db_error(db));
        }
        let statement = Statement { stmt };
        if tail.is_null() || tail.addr() <= cursor.addr() {
            // No progress: nothing compilable is left.
            break;
        }
        cursor = tail;
        if stmt.is_null() {
            // Only whitespace, a comment or an empty statement was consumed.
            continue;
        }
        if let Some(params) = params {
            args_used += bind_all(db, stmt, params, args_used)?;
        }

        let mut names: Option<Arc<Vec<String>>> = None;
        loop {
            // SAFETY: `stmt` is a live prepared statement.
            let step = unsafe { ffi::sqlite3_step(stmt) };
            match step {
                ffi::SQLITE_ROW => {
                    if keep != Rows::Discard {
                        let names = names.get_or_insert_with(|| Arc::new(column_names(stmt)));
                        run.rows.push(Row {
                            names: Arc::clone(names),
                            vals: read_row(stmt),
                        });
                        if keep == Rows::First {
                            return Ok(());
                        }
                    }
                }
                ffi::SQLITE_DONE => break,
                _ => return Err(db_error(db)),
            }
        }
        // SAFETY: `db` is live; the counter is a simple read.
        let changes = unsafe { ffi::sqlite3_changes(db) };
        run.result.rows_affected += u64::try_from(changes).unwrap_or(0);
        drop(statement);
    }
    Ok(())
}

/// A statement with no arguments (pragmas and transaction control), run as
/// `Executor::execute` on a `&str` runs it.
fn run_exec(handle: &Handle, sql: &str) -> Result<QueryResult, Error> {
    run_query(handle, sql, None, Rows::Discard)
        .into_result()
        .map(|(_, result)| result)
}

/// Run a query for its first row, if any.
fn first_row<E: Executor>(ex: &E, sql: &str, params: &[Param]) -> Result<Option<Row>, Error> {
    ex.ensure_available()?;
    let (rows, _) = run_query(&ex.handle(), sql, Some(params), Rows::First).into_result()?;
    Ok(rows.into_iter().next())
}

/// Run a query for all its rows.
fn all_rows<E: Executor>(ex: &E, sql: &str, params: &[Param]) -> Result<Vec<Row>, Error> {
    ex.ensure_available()?;
    let (rows, _) = run_query(&ex.handle(), sql, Some(params), Rows::All).into_result()?;
    Ok(rows)
}

/// Run a query for its side effect; `params` as [`run_query`] takes it.
fn execute<E: Executor>(ex: &E, sql: &str, params: Option<&[Param]>) -> Result<QueryResult, Error> {
    ex.ensure_available()?;
    let (_, result) = run_query(&ex.handle(), sql, params, Rows::Discard).into_result()?;
    Ok(result)
}

// ─────────────────────────── query builders ────────────────────────────

/// A positional-parameter SQL query, mirroring `sqlx::query`.
pub struct Query<DB = Sqlite> {
    sql: String,
    params: Vec<Param>,
    _db: PhantomData<DB>,
}

/// Build a query from SQL text.
pub fn query<DB>(sql: &str) -> Query<DB> {
    Query {
        sql: sql.to_owned(),
        params: Vec::new(),
        _db: PhantomData,
    }
}

impl<DB> Query<DB> {
    /// Bind the next positional parameter.
    pub fn bind<T: Bind>(mut self, value: T) -> Self {
        self.params.push(value.into_param());
        self
    }

    /// Execute for side effect.
    pub async fn execute<E: Executor<Database = DB>>(self, ex: E) -> Result<QueryResult, Error> {
        execute(&ex, &self.sql, Some(&self.params))
    }
}

/// A query decoding each row into `O: FromRow`, mirroring `sqlx::query_as`.
pub struct QueryAs<O, DB = Sqlite> {
    sql: String,
    params: Vec<Param>,
    _db: PhantomData<DB>,
    _o: PhantomData<O>,
}

/// Build a typed query. The first type parameter is the database marker (so
/// `query_as::<_, O>(...)` resolves), the second is the row type.
pub fn query_as<DB, O: FromRow>(sql: &str) -> QueryAs<O, DB> {
    QueryAs {
        sql: sql.to_owned(),
        params: Vec::new(),
        _db: PhantomData,
        _o: PhantomData,
    }
}

impl<O: FromRow, DB> QueryAs<O, DB> {
    /// Bind the next positional parameter.
    pub fn bind<T: Bind>(mut self, value: T) -> Self {
        self.params.push(value.into_param());
        self
    }

    /// Fetch at most one typed row; nothing runs after the first row.
    pub async fn fetch_optional<E: Executor<Database = DB>>(
        self,
        ex: E,
    ) -> Result<Option<O>, Error> {
        first_row(&ex, &self.sql, &self.params)?
            .as_ref()
            .map(O::from_row)
            .transpose()
    }

    /// Fetch exactly one typed row.
    pub async fn fetch_one<E: Executor<Database = DB>>(self, ex: E) -> Result<O, Error> {
        self.fetch_optional(ex).await?.ok_or(Error::RowNotFound)
    }

    /// Fetch all typed rows.
    pub async fn fetch_all<E: Executor<Database = DB>>(self, ex: E) -> Result<Vec<O>, Error> {
        all_rows(&ex, &self.sql, &self.params)?
            .iter()
            .map(O::from_row)
            .collect()
    }

    /// Stream typed rows, mirroring `sqlx::query_as(...).fetch(ex)`: each row
    /// in order, a row that fails to decode as an error in its place, and a
    /// database error last, after the rows before it.
    ///
    /// The transport is synchronous, so the query runs to completion first and
    /// the stream yields what it produced. The stream owns its items and
    /// borrows nothing from `ex`.
    pub fn fetch<'e, E: Executor<Database = DB>>(
        self,
        ex: E,
    ) -> futures::stream::BoxStream<'e, Result<O, Error>>
    where
        O: Send + 'e,
    {
        let items: Vec<Result<O, Error>> = match ex.ensure_available() {
            Err(e) => vec![Err(e)],
            Ok(()) => {
                let run = run_query(&ex.handle(), &self.sql, Some(&self.params), Rows::All);
                run.rows
                    .iter()
                    .map(O::from_row)
                    .chain(run.error.map(Err))
                    .collect()
            }
        };
        Box::pin(futures::stream::iter(items))
    }
}

/// A query decoding column 0 of each row, mirroring `sqlx::query_scalar`.
pub struct QueryScalar<O, DB = Sqlite> {
    sql: String,
    params: Vec<Param>,
    _db: PhantomData<DB>,
    _o: PhantomData<O>,
}

/// Build a scalar query. The first type parameter is the database marker (so
/// `query_scalar::<_, O>(...)` resolves), the second is the scalar type.
pub fn query_scalar<DB, O: Decode>(sql: &str) -> QueryScalar<O, DB> {
    QueryScalar {
        sql: sql.to_owned(),
        params: Vec::new(),
        _db: PhantomData,
        _o: PhantomData,
    }
}

impl<O: Decode, DB> QueryScalar<O, DB> {
    /// Bind the next positional parameter.
    pub fn bind<T: Bind>(mut self, value: T) -> Self {
        self.params.push(value.into_param());
        self
    }

    /// Fetch at most one scalar; nothing runs after the first row.
    pub async fn fetch_optional<E: Executor<Database = DB>>(
        self,
        ex: E,
    ) -> Result<Option<O>, Error> {
        first_row(&ex, &self.sql, &self.params)?
            .map(|row| row.try_get::<O, usize>(0))
            .transpose()
    }

    /// Fetch exactly one scalar.
    pub async fn fetch_one<E: Executor<Database = DB>>(self, ex: E) -> Result<O, Error> {
        self.fetch_optional(ex).await?.ok_or(Error::RowNotFound)
    }

    /// Fetch all scalars (column 0 of every row).
    pub async fn fetch_all<E: Executor<Database = DB>>(self, ex: E) -> Result<Vec<O>, Error> {
        all_rows(&ex, &self.sql, &self.params)?
            .iter()
            .map(|row| row.try_get::<O, usize>(0))
            .collect()
    }
}

// ──────────────────────────────── raw_sql ──────────────────────────────

/// A multi-statement SQL script, mirroring `sqlx::raw_sql`.
pub struct RawSql {
    sql: String,
}

/// Build a raw multi-statement script (used for migrations DDL).
pub fn raw_sql(sql: &str) -> RawSql {
    RawSql {
        sql: sql.to_owned(),
    }
}

impl RawSql {
    /// Execute the whole script, running every statement in the string.
    pub async fn execute<E: Executor>(self, ex: E) -> Result<QueryResult, Error> {
        execute(&ex, &self.sql, None)
    }
}

// ───────────────────────── sqlx::sqlite::* paths ───────────────────────

/// Mirrors the `sqlx::sqlite` module the backend imports from.
pub mod sqlite {
    pub use super::{SqliteConnectOptions, SqlitePoolOptions};
}
