// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// This module mirrors the shape of the `sqlx` API, so it carries many small
// public items whose contract is the same as `sqlx`'s; documenting `# Errors`
// and `# Panics` on each, or marking every builder `#[must_use]`, would only
// restate `sqlx`. The error type is uniform ([`Error`]). These pedantic lints
// are therefore allowed module-wide; the behaviour that matters (the fidelity
// contract below) is covered by the module docs and the tests.
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::module_name_repetitions,
    clippy::return_self_not_must_use,
    clippy::doc_markdown,
    // The query terminals and connection methods are `async fn`s even though
    // the in-memory transport completes synchronously: they mirror `sqlx`,
    // whose call sites `.await` them, so the signatures must stay async.
    clippy::unused_async,
    clippy::unused_async_trait_impl
)]

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
//! re-exported), running the backend's unchanged SQL against a real SQLite
//! compiled for wasm (`sqlite-wasm-rs`, in-memory VFS). On every other target
//! the backend keeps real `sqlx` + C SQLite; this module is excluded there
//! (see the `#[cfg(target_arch = "wasm32")]` on its declaration).
//!
//! # Fidelity contract
//!
//! The backend classifies constraint failures by inspecting [`Error::Database`]:
//! it reads [`DatabaseError::message`] to recognise UNIQUE / foreign-key
//! violations and [`DatabaseError::code`] to recognise the transient BUSY /
//! LOCKED codes. Those drive `ConditionalCheckFailedException` and the
//! NotFound / retry paths, so [`Error`] reproduces `sqlx::Error`'s shape and
//! the `Database` variant carries the verbatim `sqlite3_errmsg` text and the
//! numeric SQLite extended result code. A mis-mapped error here is a silent
//! behaviour divergence.
//!
//! Column decoding mirrors `sqlx-sqlite` 0.8.6 exactly: a value's runtime
//! storage class must match the Rust type (a `String` reads only a TEXT value,
//! an `i64`/`i32` only an INTEGER value, an `f64` only a REAL value, a
//! `Vec<u8>` a BLOB or TEXT value; anything else is a decode error), except
//! that a SQL `NULL` decoded into a non-`Option` type yields that type's
//! default (empty string, `0`, `0.0`, `false`, empty bytes) rather than an
//! error — the same values `sqlx` produces, because it reads a NULL through
//! the plain C accessors. Reading a column whose stored value is `NaN` back as
//! `f64` therefore yields `0.0`, because SQLite stores a bound `NaN` as NULL.
//! Invalid UTF-8 in a TEXT value is a decode error when read as a string (it
//! reads fine as `Vec<u8>`). When a result set repeats a column name, lookup by
//! name returns the last such column.
//!
//! # Documented differences from `sqlx-sqlite` 0.8.6
//!
//! These differ and are left as-is because no code path the backend takes on
//! this target reaches them, or because matching is not possible:
//!
//! * Binding more arguments than a statement has placeholders returns the
//!   SQLite range error from the offending bind, as it does under `sqlx`; the
//!   message text differs.
//! * A nested `begin()` issues a second `BEGIN`, which SQLite rejects, rather
//!   than opening a savepoint the way `sqlx` does. The backend never nests
//!   transactions on one connection.
//! * `fetch`/`fetch_optional`/`fetch_one` materialise every result row before
//!   returning instead of streaming; on an in-memory database this changes only
//!   transient memory use, not the rows observed.
//! * The bundled SQLite (3.54.1, built with `sqlite-wasm-rs`'s compile flags)
//!   is a different build from the one `libsqlite3-sys` links on other targets
//!   (3.46.0), so the query planner may order rows that are not constrained by
//!   an `ORDER BY` differently. Callers that compare unordered result sets sort
//!   them first.
//!
//! # Concurrency
//!
//! A browser module is single-threaded and the backend uses one connection
//! serially, so the connection handle is shared with `Arc` and asserted
//! `Send + Sync` only where the target has no threads (see [`RawConn`]); the
//! query terminals are `async fn`s that complete synchronously inside the
//! future, so the backend's existing `.await`s are unchanged.

use std::borrow::Cow;
use std::ffi::{CStr, CString};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::os::raw::c_char;
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlite_wasm_rs as ffi;

// ─────────────────────────────── errors ────────────────────────────────

/// A database-level error, mirroring the slice of `sqlx::error::DatabaseError`
/// the backend reads: the driver message and the vendor result code.
///
/// `Send + Sync` so [`Error`] (which boxes this) is `Send + Sync` like
/// `sqlx::Error`; the storage trait futures above the shim are `Send` and hold
/// an error across their `.await` points.
pub trait DatabaseError: fmt::Debug + fmt::Display + Send + Sync {
    /// The verbatim driver message (`sqlite3_errmsg`).
    fn message(&self) -> &str;
    /// The SQLite extended result code as a string, so the backend's
    /// `code.parse::<i64>()` classification works unchanged.
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
        f.write_str(&self.message)
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

/// The subset of `sqlx::Error` variants this crate constructs or the backend
/// matches.
///
/// The backend matches `Database`, `Io`, `PoolTimedOut`, `WorkerCrashed`,
/// `RowNotFound`, and `Protocol`; the others never arise on the synchronous
/// in-memory transport but are kept so exhaustive matches stay shaped like
/// `sqlx::Error`.
#[derive(Debug)]
pub enum Error {
    /// A SQLite database error (constraint violation, busy, etc.).
    Database(Box<dyn DatabaseError>),
    /// An I/O error (never produced by the in-memory transport).
    Io(std::io::Error),
    /// The connection pool timed out (not reachable with one serial connection).
    PoolTimedOut,
    /// The driver worker crashed (no worker here; kept for shape parity).
    WorkerCrashed,
    /// A `fetch_one` found no row.
    RowNotFound,
    /// A protocol/decoding error (bad column type, bad UTF-8, etc.).
    Protocol(String),
}

impl Error {
    fn protocol(msg: impl Into<String>) -> Self {
        Error::Protocol(msg.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Database(e) => write!(f, "error returned from database: {e}"),
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::PoolTimedOut => {
                f.write_str("pool timed out while waiting for an open connection")
            }
            Error::WorkerCrashed => f.write_str("background worker crashed"),
            Error::RowNotFound => {
                f.write_str("no rows returned by a query that expected to return at least one row")
            }
            Error::Protocol(m) => write!(f, "encountered unexpected or invalid data: {m}"),
        }
    }
}

impl std::error::Error for Error {}

// ───────────────────────── raw connection ──────────────────────────────

/// Owns the `*mut sqlite3` and closes it when the last handle drops, plus the
/// shared open-transaction flag.
///
/// `pub` only to satisfy the private-in-public lint on [`Executor::handle`].
#[derive(Debug)]
pub struct RawConn {
    db: *mut ffi::sqlite3,
    /// Set while a [`Transaction`] is open on this connection. A statement run
    /// through the pool while this is set is rejected (see
    /// [`Executor::ensure_available`]), because `sqlx` would run it on a
    /// separate pooled connection and this crate has only one.
    in_transaction: AtomicBool,
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

/// Open an in-memory SQLite database and return the shared handle.
fn open(url: &str) -> Result<Handle, Error> {
    let filename = sqlite_filename(url);
    let c_name = CString::new(filename).map_err(|e| Error::protocol(e.to_string()))?;
    let mut db: *mut ffi::sqlite3 = ptr::null_mut();
    // SAFETY: a valid NUL-terminated name and a valid out-pointer are passed;
    // the returned handle is stored in `RawConn`, which closes it on drop. On a
    // non-OK return SQLite still allocates a handle that must be closed, so the
    // handle is wrapped before the result is checked.
    let rc = unsafe {
        ffi::sqlite3_open_v2(
            c_name.as_ptr(),
            &raw mut db,
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
            ptr::null(),
        )
    };
    let handle = Arc::new(RawConn {
        db,
        in_transaction: AtomicBool::new(false),
    });
    if rc != ffi::SQLITE_OK {
        return Err(db_error(db));
    }
    Ok(handle)
}

/// Map a sqlx-style SQLite URL to a `sqlite3_open_v2` filename. This build is
/// in-memory only, so `:memory:` / `sqlite::memory:` map to the private
/// in-memory database; any other name keeps its bare form (the in-memory VFS
/// stores it under that name — there is no real filesystem).
fn sqlite_filename(url: &str) -> String {
    if url == ":memory:" || url == "sqlite::memory:" || url == "sqlite://:memory:" {
        return ":memory:".to_owned();
    }
    let stripped = url.strip_prefix("sqlite://").unwrap_or(url);
    let stripped = stripped.strip_prefix("sqlite:").unwrap_or(stripped);
    // Drop any `?mode=...` query string.
    stripped.split('?').next().unwrap_or(stripped).to_owned()
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

    /// Reject a statement that cannot run on this executor right now. The
    /// default allows it; the pool overrides this to fail while a transaction
    /// is open on the shared connection.
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
            let handle = self.handle();
            run_query(&handle, sql, &[], false).map(|(_, result)| result)
        })
    }
}

impl Executor for &SqlitePool {
    type Database = Sqlite;
    fn handle(&self) -> Handle {
        Arc::clone(&self.handle)
    }
    fn ensure_available(&self) -> Result<(), Error> {
        if self.handle.in_transaction.load(Ordering::SeqCst) {
            // `sqlx` would hand this statement a separate pooled connection;
            // this crate has one, so run it loudly rather than silently inside
            // the open transaction.
            return Err(Error::protocol(
                "a pool statement was issued while a transaction is open on the single connection",
            ));
        }
        Ok(())
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

/// A connection pool. Here it is a thin clone-able handle to the single serial
/// in-memory connection.
#[derive(Clone, Debug)]
pub struct SqlitePool {
    handle: Handle,
    max_connections: u32,
}

impl SqlitePool {
    /// Begin a deferred transaction (`BEGIN`).
    pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>, Error> {
        Transaction::start(Arc::clone(&self.handle), "BEGIN")
    }

    /// Begin a transaction with an explicit statement, e.g. `BEGIN IMMEDIATE`.
    pub async fn begin_with(&self, stmt: &str) -> Result<Transaction<'static, Sqlite>, Error> {
        Transaction::start(Arc::clone(&self.handle), stmt)
    }

    /// Acquire a pooled connection. Serial single connection, so this clones
    /// the shared handle; the newtype mirrors `sqlx::pool::PoolConnection`.
    pub async fn acquire(&self) -> Result<PoolConnection<Sqlite>, Error> {
        Ok(PoolConnection {
            conn: SqliteConnection {
                handle: Arc::clone(&self.handle),
            },
            _db: PhantomData,
        })
    }

    /// The options the pool was built with (only `get_max_connections` is read).
    pub fn options(&self) -> PoolOptionsView {
        PoolOptionsView {
            max_connections: self.max_connections,
        }
    }

    /// Close the pool. The connection closes when the last handle drops.
    pub async fn close(&self) {}
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

impl SqliteConnection {
    /// Close the connection. Closes when the last handle drops.
    pub async fn close(self) -> Result<(), Error> {
        Ok(())
    }
}

/// A connection checked out of a [`SqlitePool`], mirroring
/// `sqlx::pool::PoolConnection`. Dereferences to the connection, so
/// `&mut *conn` is an [`Executor`].
#[derive(Debug)]
pub struct PoolConnection<DB = Sqlite> {
    conn: SqliteConnection,
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

/// An in-flight transaction. Rolls back on drop unless committed, matching
/// sqlx's RAII semantics.
///
/// Carries a lifetime and database-marker parameter so the backend's
/// `sqlx::Transaction<'_, sqlx::Sqlite>` spellings resolve; both are phantom
/// here (the transaction owns a clone of the single serial handle and borrows
/// nothing).
pub struct Transaction<'c, DB = Sqlite> {
    conn: SqliteConnection,
    open: bool,
    _marker: PhantomData<(&'c (), DB)>,
}

impl<DB> Transaction<'_, DB> {
    fn start(handle: Handle, stmt: &str) -> Result<Self, Error> {
        // Run the opening statement first; only mark the connection as
        // in-transaction once it succeeds (a nested `BEGIN` fails here).
        run_exec(&handle, stmt)?;
        handle.in_transaction.store(true, Ordering::SeqCst);
        Ok(Transaction {
            conn: SqliteConnection { handle },
            open: true,
            _marker: PhantomData,
        })
    }

    /// Commit the transaction (`COMMIT`).
    pub async fn commit(mut self) -> Result<(), Error> {
        run_exec(&self.conn.handle, "COMMIT")?;
        self.open = false;
        self.conn
            .handle
            .in_transaction
            .store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Roll the transaction back (`ROLLBACK`).
    pub async fn rollback(mut self) -> Result<(), Error> {
        run_exec(&self.conn.handle, "ROLLBACK")?;
        self.open = false;
        self.conn
            .handle
            .in_transaction
            .store(false, Ordering::SeqCst);
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
            self.conn
                .handle
                .in_transaction
                .store(false, Ordering::SeqCst);
        }
    }
}

// ─────────────────────────── connect options ───────────────────────────

/// Connection options, mirroring `sqlx::sqlite::SqliteConnectOptions`.
#[derive(Clone, Debug)]
pub struct SqliteConnectOptions {
    url: String,
    #[allow(dead_code)]
    row_buffer_size: usize,
}

impl SqliteConnectOptions {
    /// A fresh options set for an in-memory database.
    pub fn new() -> Self {
        SqliteConnectOptions {
            url: ":memory:".to_owned(),
            row_buffer_size: 50,
        }
    }

    /// Parse a connection string. Infallible here; `Result` mirrors sqlx.
    // Named `from_str` to match the inherent method the backend calls
    // (`SqliteConnectOptions::from_str(url)`), which mirrors sqlx's own
    // inherent method; the `FromStr` trait is also implemented, below.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(url: &str) -> Result<Self, Error> {
        Ok(SqliteConnectOptions {
            url: url.to_owned(),
            row_buffer_size: 50,
        })
    }

    /// The driver row look-ahead buffer. A no-op on the synchronous transport
    /// (no worker), kept for call-site compatibility.
    pub fn row_buffer_size(mut self, rows: usize) -> Self {
        self.row_buffer_size = rows;
        self
    }
}

impl Default for SqliteConnectOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl std::str::FromStr for SqliteConnectOptions {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        SqliteConnectOptions::from_str(s)
    }
}

/// Mirrors `sqlx::ConnectOptions::connect`.
pub trait ConnectOptions {
    /// Open a single connection with these options.
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<SqliteConnection, Error>> + '_>>;
}

impl ConnectOptions for SqliteConnectOptions {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<SqliteConnection, Error>> + '_>> {
        let url = self.url.clone();
        Box::pin(async move {
            let handle = open(&url)?;
            Ok(SqliteConnection { handle })
        })
    }
}

/// Mirrors the slice of `sqlx::Connection` the backend uses on a bare
/// connection.
pub trait Connection {
    /// Begin a deferred transaction on this connection.
    fn begin(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Transaction<'static, Sqlite>, Error>> + '_>>;
}

impl Connection for SqliteConnection {
    fn begin(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Transaction<'static, Sqlite>, Error>> + '_>> {
        let handle = Arc::clone(&self.handle);
        Box::pin(async move { Transaction::start(handle, "BEGIN") })
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
    min_connections: u32,
    after_connect: Option<AfterConnect>,
}

impl SqlitePoolOptions {
    /// A fresh pool builder.
    pub fn new() -> Self {
        SqlitePoolOptions {
            max_connections: 1,
            min_connections: 0,
            after_connect: None,
        }
    }

    /// Set the maximum connection count (the pool still opens one).
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = n;
        self
    }

    /// Set the minimum connection count (no-op here).
    pub fn min_connections(mut self, n: u32) -> Self {
        self.min_connections = n;
        self
    }

    /// Set the idle timeout (no-op; one persistent connection).
    pub fn idle_timeout(self, _timeout: Option<Duration>) -> Self {
        self
    }

    /// Set the max connection lifetime (no-op).
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

    /// Open the pool with the given connect options.
    pub async fn connect_with(self, options: SqliteConnectOptions) -> Result<SqlitePool, Error> {
        let handle = open(&options.url)?;
        self.finish(handle).await
    }

    /// Open the pool from a URL string.
    pub async fn connect(self, url: &str) -> Result<SqlitePool, Error> {
        let handle = open(url)?;
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
        })
    }
}

impl Default for SqlitePoolOptions {
    fn default() -> Self {
        Self::new()
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

impl Bind for Param {
    fn into_param(self) -> Param {
        self
    }
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
impl Bind for i32 {
    fn into_param(self) -> Param {
        Param::Int(i64::from(self))
    }
}
impl Bind for u32 {
    fn into_param(self) -> Param {
        Param::Int(i64::from(self))
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
impl Bind for serde_json::Value {
    fn into_param(self) -> Param {
        // sqlx's `json` encoding stores a `Value` as its JSON text.
        Param::Text(self.to_string())
    }
}
impl Bind for &serde_json::Value {
    fn into_param(self) -> Param {
        Param::Text(self.to_string())
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

/// One decoded column value, materialized so a [`Row`] outlives its statement.
/// Text is kept as raw bytes so that invalid UTF-8 is an error only when the
/// value is read as a string, matching `sqlx`.
#[derive(Clone, Debug)]
enum ColVal {
    Null,
    Int(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl ColVal {
    /// The storage-class name, for decode mismatch messages.
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

/// A fully materialized result row (owned values + column names).
#[derive(Clone, Debug)]
pub struct Row {
    names: Arc<Vec<String>>,
    vals: Vec<ColVal>,
}

/// A column selector: a positional index or a column name.
pub trait ColIndex {
    /// Resolve to a 0-based column index within `row`.
    fn index(self, row: &Row) -> Result<usize, Error>;
}

impl ColIndex for usize {
    fn index(self, row: &Row) -> Result<usize, Error> {
        if self < row.vals.len() {
            Ok(self)
        } else {
            Err(Error::protocol(format!("column index {self} out of range")))
        }
    }
}

impl ColIndex for &str {
    fn index(self, row: &Row) -> Result<usize, Error> {
        // A repeated column name resolves to the last occurrence, matching
        // sqlx (`rposition`, not `position`).
        row.names
            .iter()
            .rposition(|n| n == self)
            .ok_or_else(|| Error::protocol(format!("no column named {self}")))
    }
}

impl Row {
    /// Decode a column, panicking on error (mirrors `sqlx::Row::get`).
    pub fn get<T: Decode, I: ColIndex>(&self, index: I) -> T {
        self.try_get(index).expect("column decode")
    }

    /// Decode a column, returning an error on type mismatch or bad index.
    pub fn try_get<T: Decode, I: ColIndex>(&self, index: I) -> Result<T, Error> {
        let idx = index.index(self)?;
        T::decode(self, idx)
    }

    /// Borrow a column value by index (same-module access for `Decode` impls).
    fn col(&self, idx: usize) -> Result<&ColVal, Error> {
        self.vals
            .get(idx)
            .ok_or_else(|| Error::protocol(format!("column index {idx} out of range")))
    }
}

/// Produce a "mismatched types" decode error, as `sqlx` does when the stored
/// storage class is not compatible with the Rust type.
fn mismatch(expected: &str, found: &ColVal) -> Error {
    Error::protocol(format!(
        "mismatched types; a {expected} column expected, found {}",
        found.kind()
    ))
}

/// A value decodable from a SQLite column of a [`Row`].
///
/// Each impl mirrors `sqlx-sqlite` 0.8.6: a NULL yields the type's default, and
/// a non-NULL value must be of the one compatible storage class, or the decode
/// is a type-mismatch error.
pub trait Decode: Sized {
    /// Decode column `idx` of `row`.
    fn decode(row: &Row, idx: usize) -> Result<Self, Error>;
}

impl Decode for String {
    fn decode(row: &Row, idx: usize) -> Result<Self, Error> {
        match row.col(idx)? {
            ColVal::Null => Ok(String::new()),
            ColVal::Text(b) => String::from_utf8(b.clone())
                .map_err(|e| Error::protocol(format!("invalid UTF-8 in text column: {e}"))),
            other => Err(mismatch("TEXT", other)),
        }
    }
}
impl Decode for i64 {
    fn decode(row: &Row, idx: usize) -> Result<Self, Error> {
        match row.col(idx)? {
            ColVal::Null => Ok(0),
            ColVal::Int(i) => Ok(*i),
            other => Err(mismatch("INTEGER", other)),
        }
    }
}
impl Decode for i32 {
    fn decode(row: &Row, idx: usize) -> Result<Self, Error> {
        match row.col(idx)? {
            ColVal::Null => Ok(0),
            ColVal::Int(i) => {
                i32::try_from(*i).map_err(|e| Error::protocol(format!("i32 out of range: {e}")))
            }
            other => Err(mismatch("INTEGER", other)),
        }
    }
}
impl Decode for f64 {
    fn decode(row: &Row, idx: usize) -> Result<Self, Error> {
        match row.col(idx)? {
            ColVal::Null => Ok(0.0),
            ColVal::Real(r) => Ok(*r),
            other => Err(mismatch("REAL", other)),
        }
    }
}
impl Decode for bool {
    fn decode(row: &Row, idx: usize) -> Result<Self, Error> {
        match row.col(idx)? {
            ColVal::Null => Ok(false),
            ColVal::Int(i) => Ok(*i != 0),
            other => Err(mismatch("INTEGER", other)),
        }
    }
}
impl Decode for Vec<u8> {
    fn decode(row: &Row, idx: usize) -> Result<Self, Error> {
        match row.col(idx)? {
            ColVal::Null => Ok(Vec::new()),
            // A BLOB value or the raw bytes of a TEXT value, matching sqlx
            // (`<Vec<u8>>` is compatible with both storage classes).
            ColVal::Blob(b) | ColVal::Text(b) => Ok(b.clone()),
            other => Err(mismatch("BLOB", other)),
        }
    }
}
impl Decode for serde_json::Value {
    fn decode(row: &Row, idx: usize) -> Result<Self, Error> {
        match row.col(idx)? {
            // A JSON value is stored and read as TEXT. A NULL reads as JSON
            // null; the backend never decodes a NULL column into a non-Option
            // `Value`, so this case is not reached in practice.
            ColVal::Null => Ok(serde_json::Value::Null),
            ColVal::Text(b) => {
                let s = std::str::from_utf8(b)
                    .map_err(|e| Error::protocol(format!("invalid UTF-8 in JSON column: {e}")))?;
                serde_json::from_str(s)
                    .map_err(|e| Error::protocol(format!("invalid JSON column: {e}")))
            }
            other => Err(mismatch("TEXT", other)),
        }
    }
}
impl<T: Decode> Decode for Option<T> {
    fn decode(row: &Row, idx: usize) -> Result<Self, Error> {
        match row.col(idx)? {
            ColVal::Null => Ok(None),
            _ => Ok(Some(T::decode(row, idx)?)),
        }
    }
}

// ─────────────────────────────── FromRow ───────────────────────────────

/// A type decodable from a whole result row, mirroring `sqlx::FromRow`.
///
/// The backend's catalog row structs implement it with a by-name column decode
/// (`row.try_get("<field>")`), matching the default column mapping of `sqlx`'s
/// own `FromRow` derive, and it is implemented for tuples (by position) below.
pub trait FromRow: Sized {
    /// Decode `self` from `row`.
    fn from_row(row: &Row) -> Result<Self, Error>;
}

macro_rules! tuple_from_row {
    ($($idx:tt : $ty:ident),+) => {
        impl<$($ty: Decode),+> FromRow for ($($ty,)+) {
            fn from_row(row: &Row) -> Result<Self, Error> {
                Ok(($($ty::decode(row, $idx)?,)+))
            }
        }
    };
}
tuple_from_row!(0: A);
tuple_from_row!(0: A, 1: B);
tuple_from_row!(0: A, 1: B, 2: C);
tuple_from_row!(0: A, 1: B, 2: C, 3: D);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P);
tuple_from_row!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q);

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

/// Bind the parameters positionally (1-based) to a prepared statement.
///
/// A statement with no placeholders is left unbound, so a parameter-less
/// trailing statement in a multi-statement script runs. A statement with
/// placeholders gets every parameter; binding beyond its placeholder count
/// returns the SQLite range error, as it does under `sqlx`.
fn bind_all(
    db: *mut ffi::sqlite3,
    stmt: *mut ffi::sqlite3_stmt,
    params: &[Param],
) -> Result<(), Error> {
    // SAFETY: `stmt` is a live prepared statement; reading its placeholder
    // count is a simple accessor.
    let placeholders = unsafe { ffi::sqlite3_bind_parameter_count(stmt) };
    if placeholders == 0 {
        return Ok(());
    }
    for (i, param) in params.iter().enumerate() {
        let pos = i32::try_from(i + 1)
            .map_err(|e| Error::protocol(format!("too many bound parameters: {e}")))?;
        bind_param(db, stmt, pos, param)?;
    }
    Ok(())
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
                        .map_err(|e| Error::protocol(format!("text too large to bind: {e}")))?,
                    ffi::SQLITE_TRANSIENT(),
                )
            }
            Param::Blob(b) => ffi::sqlite3_bind_blob(
                stmt,
                pos,
                b.as_ptr().cast::<std::ffi::c_void>(),
                i32::try_from(b.len())
                    .map_err(|e| Error::protocol(format!("blob too large to bind: {e}")))?,
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
/// as raw bytes (UTF-8 is validated at decode time, matching `sqlx`).
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

/// Rows affected and last insert id after a statement runs.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryResult {
    rows_affected: u64,
    last_insert_rowid: i64,
}

impl QueryResult {
    /// Rows changed by the statement (`sqlite3_changes`).
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
    /// The rowid of the last inserted row (`sqlite3_last_insert_rowid`).
    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid
    }
}

/// Build a [`QueryResult`] from the connection's post-step counters.
fn query_result(handle: &Handle) -> QueryResult {
    let db = handle.db;
    // SAFETY: `db` is live; both counters are simple reads.
    let (changes, rowid) =
        unsafe { (ffi::sqlite3_changes(db), ffi::sqlite3_last_insert_rowid(db)) };
    QueryResult {
        rows_affected: u64::try_from(changes).unwrap_or(0),
        last_insert_rowid: rowid,
    }
}

/// Prepare, bind and step every statement in `sql`, running the whole string
/// the way `sqlx` and `sqlite3_exec` do.
///
/// `collect` selects whether result rows are gathered (for a fetch) or
/// discarded after stepping the statement to completion (for an execute). An
/// empty or comment-only `sql` runs no statement and yields no rows and a
/// zeroed [`QueryResult`], matching `sqlx` (not the last error).
fn run_query(
    handle: &Handle,
    sql: &str,
    params: &[Param],
    collect: bool,
) -> Result<(Vec<Row>, QueryResult), Error> {
    let db = handle.db;
    let c_sql = CString::new(sql).map_err(|e| Error::protocol(e.to_string()))?;
    let mut rows: Vec<Row> = Vec::new();
    let mut cursor: *const c_char = c_sql.as_ptr();

    loop {
        let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
        let mut tail: *const c_char = ptr::null();
        // SAFETY: `db` is live; `cursor` points into the NUL-terminated
        // `c_sql` buffer, held for the whole loop; `-1` lets SQLite read to the
        // next NUL; the out-pointers are valid.
        let rc = unsafe { ffi::sqlite3_prepare_v2(db, cursor, -1, &raw mut stmt, &raw mut tail) };
        if rc != ffi::SQLITE_OK {
            return Err(db_error(db));
        }
        if stmt.is_null() {
            // Only whitespace or comments remain: nothing more to run.
            break;
        }
        let statement = Statement { stmt };
        bind_all(db, stmt, params)?;

        let mut names: Option<Arc<Vec<String>>> = None;
        loop {
            // SAFETY: `stmt` is a live prepared statement.
            let step = unsafe { ffi::sqlite3_step(stmt) };
            match step {
                ffi::SQLITE_ROW => {
                    if collect {
                        let names = names.get_or_insert_with(|| Arc::new(column_names(stmt)));
                        rows.push(Row {
                            names: Arc::clone(names),
                            vals: read_row(stmt),
                        });
                    }
                }
                ffi::SQLITE_DONE => break,
                _ => return Err(db_error(db)),
            }
        }
        drop(statement);
        cursor = tail;
    }

    Ok((rows, query_result(handle)))
}

/// A single statement with no parameters (pragmas and transaction control).
fn run_exec(handle: &Handle, sql: &str) -> Result<QueryResult, Error> {
    run_query(handle, sql, &[], false).map(|(_, result)| result)
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
        ex.ensure_available()?;
        let handle = ex.handle();
        run_query(&handle, &self.sql, &self.params, false).map(|(_, result)| result)
    }

    /// Fetch at most one row.
    pub async fn fetch_optional<E: Executor<Database = DB>>(
        self,
        ex: E,
    ) -> Result<Option<Row>, Error> {
        ex.ensure_available()?;
        let handle = ex.handle();
        let (rows, _) = run_query(&handle, &self.sql, &self.params, true)?;
        Ok(rows.into_iter().next())
    }

    /// Fetch exactly one row, erroring if none.
    pub async fn fetch_one<E: Executor<Database = DB>>(self, ex: E) -> Result<Row, Error> {
        self.fetch_optional(ex).await?.ok_or(Error::RowNotFound)
    }

    /// Fetch all rows.
    pub async fn fetch_all<E: Executor<Database = DB>>(self, ex: E) -> Result<Vec<Row>, Error> {
        ex.ensure_available()?;
        let handle = ex.handle();
        let (rows, _) = run_query(&handle, &self.sql, &self.params, true)?;
        Ok(rows)
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

    /// Fetch at most one typed row.
    pub async fn fetch_optional<E: Executor<Database = DB>>(
        self,
        ex: E,
    ) -> Result<Option<O>, Error> {
        ex.ensure_available()?;
        let handle = ex.handle();
        let (rows, _) = run_query(&handle, &self.sql, &self.params, true)?;
        match rows.first() {
            Some(row) => Ok(Some(O::from_row(row)?)),
            None => Ok(None),
        }
    }

    /// Fetch exactly one typed row.
    pub async fn fetch_one<E: Executor<Database = DB>>(self, ex: E) -> Result<O, Error> {
        self.fetch_optional(ex).await?.ok_or(Error::RowNotFound)
    }

    /// Fetch all typed rows.
    pub async fn fetch_all<E: Executor<Database = DB>>(self, ex: E) -> Result<Vec<O>, Error> {
        ex.ensure_available()?;
        let handle = ex.handle();
        let (rows, _) = run_query(&handle, &self.sql, &self.params, true)?;
        rows.iter().map(O::from_row).collect()
    }

    /// Stream typed rows, mirroring `sqlx::query_as(...).fetch(ex)`.
    ///
    /// The transport is synchronous, so the rows are materialised eagerly and
    /// returned as a ready-made stream; the caller's `TryStreamExt` loop is
    /// unchanged. The stream owns its items and borrows nothing from `ex`.
    // The executor is taken by value to match sqlx's `fetch(self, ex)`; the
    // executor types here are cheap references (`&SqlitePool`, `&mut
    // Transaction`), so this moves only a reference.
    #[allow(clippy::needless_pass_by_value)]
    pub fn fetch<'e, E: Executor<Database = DB>>(
        self,
        ex: E,
    ) -> futures::stream::BoxStream<'e, Result<O, Error>>
    where
        O: Send + 'e,
    {
        let collected: Result<Vec<O>, Error> = (|| {
            ex.ensure_available()?;
            let handle = ex.handle();
            let (rows, _) = run_query(&handle, &self.sql, &self.params, true)?;
            rows.iter().map(O::from_row).collect()
        })();
        match collected {
            Ok(items) => Box::pin(futures::stream::iter(items.into_iter().map(Ok))),
            Err(e) => Box::pin(futures::stream::once(async move { Err(e) })),
        }
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

    /// Fetch at most one scalar.
    pub async fn fetch_optional<E: Executor<Database = DB>>(
        self,
        ex: E,
    ) -> Result<Option<O>, Error> {
        ex.ensure_available()?;
        let handle = ex.handle();
        let (rows, _) = run_query(&handle, &self.sql, &self.params, true)?;
        match rows.first() {
            Some(row) => Ok(Some(O::decode(row, 0)?)),
            None => Ok(None),
        }
    }

    /// Fetch exactly one scalar.
    pub async fn fetch_one<E: Executor<Database = DB>>(self, ex: E) -> Result<O, Error> {
        self.fetch_optional(ex).await?.ok_or(Error::RowNotFound)
    }

    /// Fetch all scalars (column 0 of every row).
    pub async fn fetch_all<E: Executor<Database = DB>>(self, ex: E) -> Result<Vec<O>, Error> {
        ex.ensure_available()?;
        let handle = ex.handle();
        let (rows, _) = run_query(&handle, &self.sql, &self.params, true)?;
        rows.iter().map(|r| O::decode(r, 0)).collect()
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
        ex.ensure_available()?;
        let handle = ex.handle();
        run_query(&handle, &self.sql, &[], false).map(|(_, result)| result)
    }
}

// ───────────────────────── sqlx::sqlite::* paths ───────────────────────

/// Mirrors the `sqlx::sqlite` module the backend imports from.
pub mod sqlite {
    pub use super::{
        Row as SqliteRow, SqliteConnectOptions, SqliteConnection, SqlitePool, SqlitePoolOptions,
    };
}

/// Mirrors the `sqlx::pool` module path (`sqlx::pool::PoolConnection`).
pub mod pool {
    pub use super::PoolConnection;
}

/// Mirrors the `sqlx::error` module path (`sqlx::error::DatabaseError`).
pub mod error {
    pub use super::DatabaseError;
}
