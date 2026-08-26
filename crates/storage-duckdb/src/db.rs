// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A thin asynchronous facade over the synchronous `duckdb` crate.
//!
//! DuckDB has no `sqlx` driver, so this module supplies the small `sqlx`-shaped
//! surface the rest of the crate uses: a connection [`Pool`], positional
//! [`query`] / [`query_as`] / [`query_scalar`] builders with `.bind()`, and a
//! [`Transaction`] that commits explicitly and rolls back on drop. Everything
//! else in the crate is written against this module rather than against
//! `duckdb` directly, which keeps the storage code free of blocking calls.
//!
//! # Execution model
//!
//! `duckdb::Connection` is `Send` but not `Sync`, and every call on it blocks.
//! Each pooled connection therefore lives in a slot (`tokio::sync::Mutex<Option
//! <Connection>>`); running a statement takes the connection out of its slot,
//! moves it onto a `spawn_blocking` thread for the duration of the call, and
//! puts it back afterwards. A transaction holds its slot for its whole lifetime,
//! so every statement in it runs on the same connection.
//!
//! All connections in a pool are `try_clone()`s of one root connection, so they
//! share a single DuckDB database instance. That is what makes an in-memory
//! database usable from a pool of more than one connection (unlike SQLite, where
//! `:memory:` is private to the connection that opened it), and it is also what
//! DuckDB requires for file-backed databases: one process may hold one database
//! instance per file, so a second, independently opened handle on the same path
//! would be refused. [`Pool::open`] keeps a process-wide registry of root
//! connections keyed by canonical path for exactly that reason.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use duckdb::Connection;
use duckdb::types::Value;
use tokio::sync::{Mutex, OwnedMutexGuard};

// ── Errors ─────────────────────────────────────────────────────────────

/// Error type for every operation in this module.
#[derive(Debug)]
pub enum Error {
    /// The underlying DuckDB call failed.
    Db(duckdb::Error),
    /// A blocking task panicked or was cancelled; the connection it held is
    /// gone and will be re-created on next use.
    Worker(String),
    /// A column could not be converted to the requested Rust type.
    Decode(String),
    /// `fetch_one` found no row.
    RowNotFound,
    /// The pool has no usable connection and could not open one.
    Connection(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Db(e) => write!(f, "{e}"),
            Error::Worker(e) => write!(f, "database worker failed: {e}"),
            Error::Decode(e) => write!(f, "column decode error: {e}"),
            Error::RowNotFound => write!(
                f,
                "no rows returned by a query that expected at least one row"
            ),
            Error::Connection(e) => write!(f, "connection error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<duckdb::Error> for Error {
    fn from(e: duckdb::Error) -> Self {
        Error::Db(e)
    }
}

impl Error {
    /// The DuckDB error message, if this error carries one.
    pub fn message(&self) -> Option<&str> {
        match self {
            Error::Db(duckdb::Error::DuckDBFailure(_, Some(msg))) => Some(msg.as_str()),
            Error::Db(duckdb::Error::DuckDBFailure(_, None)) => None,
            Error::Db(_) => None,
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// ── Connection registry ────────────────────────────────────────────────

/// Shared root connection for one database path.
struct Root {
    conn: StdMutex<Connection>,
}

impl Root {
    fn clone_conn(&self) -> Result<Connection> {
        let guard = self
            .conn
            .lock()
            .map_err(|_| Error::Connection("root connection poisoned".to_owned()))?;
        guard.try_clone().map_err(Error::Db)
    }
}

/// Process-wide registry of root connections for file-backed databases.
///
/// DuckDB allows a single database instance per file per process; every pool
/// over the same file must clone from one root. In-memory databases are never
/// registered: each `Pool::open(":memory:")` is its own private database, which
/// is what callers (and the test-suite) expect.
fn registry() -> &'static StdMutex<HashMap<String, Arc<Root>>> {
    static REGISTRY: OnceLock<StdMutex<HashMap<String, Arc<Root>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| StdMutex::new(HashMap::new()))
}

/// Whether a configured path denotes an in-memory database.
pub fn is_memory_path(path: &str) -> bool {
    path == ":memory:" || path.is_empty()
}

/// Forget the registered root connection for `path`, closing it once every
/// pool cloned from it has been dropped. Used before deleting the database
/// file so DuckDB releases its lock.
pub fn forget_path(path: &str) {
    if let Ok(mut map) = registry().lock() {
        map.remove(&canonical_key(path));
    }
}

fn canonical_key(path: &str) -> String {
    std::fs::canonicalize(path)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| path.to_owned())
}

// ── Pool ───────────────────────────────────────────────────────────────

type Slot = Arc<Mutex<Option<Connection>>>;

struct PoolInner {
    root: Arc<Root>,
    slots: Vec<Slot>,
    next: AtomicUsize,
}

/// A fixed-size pool of connections over one DuckDB database.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("size", &self.inner.slots.len())
            .finish()
    }
}

impl Pool {
    /// Open (creating if necessary) the database at `path` with `size`
    /// connections. `":memory:"` opens a private in-memory database.
    pub async fn open(path: &str, size: u32) -> Result<Self> {
        let path = path.to_owned();
        let size = size.max(1) as usize;
        tokio::task::spawn_blocking(move || Self::open_blocking(&path, size))
            .await
            .map_err(|e| Error::Worker(e.to_string()))?
    }

    fn open_blocking(path: &str, size: usize) -> Result<Self> {
        let root = if is_memory_path(path) {
            Arc::new(Root {
                conn: StdMutex::new(Connection::open_in_memory()?),
            })
        } else {
            let mut map = registry()
                .lock()
                .map_err(|_| Error::Connection("registry poisoned".to_owned()))?;
            // Open first so the file exists for canonicalisation.
            let key = canonical_key(path);
            if let Some(existing) = map.get(&key) {
                Arc::clone(existing)
            } else {
                let conn = Connection::open(path)?;
                let key = canonical_key(path);
                let root = Arc::new(Root {
                    conn: StdMutex::new(conn),
                });
                map.insert(key, Arc::clone(&root));
                root
            }
        };
        let mut slots = Vec::with_capacity(size);
        for _ in 0..size {
            slots.push(Arc::new(Mutex::new(Some(root.clone_conn()?))));
        }
        Ok(Self {
            inner: Arc::new(PoolInner {
                root,
                slots,
                next: AtomicUsize::new(0),
            }),
        })
    }

    /// Open a pool that shares the database of `other` (a fresh set of
    /// connections onto the same instance).
    pub fn sibling(&self, size: u32) -> Result<Self> {
        let size = size.max(1) as usize;
        let mut slots = Vec::with_capacity(size);
        for _ in 0..size {
            slots.push(Arc::new(Mutex::new(Some(self.inner.root.clone_conn()?))));
        }
        Ok(Self {
            inner: Arc::new(PoolInner {
                root: Arc::clone(&self.inner.root),
                slots,
                next: AtomicUsize::new(0),
            }),
        })
    }

    /// Number of connections in the pool.
    pub fn size(&self) -> usize {
        self.inner.slots.len()
    }

    /// Acquire a connection slot, preferring an idle one.
    async fn acquire(&self) -> Result<Held> {
        let n = self.inner.slots.len();
        let start = self.inner.next.fetch_add(1, Ordering::Relaxed) % n;
        let mut guard = None;
        for i in 0..n {
            let slot = &self.inner.slots[(start + i) % n];
            if let Ok(g) = Arc::clone(slot).try_lock_owned() {
                guard = Some(g);
                break;
            }
        }
        let mut guard = match guard {
            Some(g) => g,
            None => Arc::clone(&self.inner.slots[start]).lock_owned().await,
        };
        if guard.is_none() {
            // A previous blocking task panicked and took the connection with
            // it; re-create it from the root.
            let root = Arc::clone(&self.inner.root);
            let conn = tokio::task::spawn_blocking(move || root.clone_conn())
                .await
                .map_err(|e| Error::Worker(e.to_string()))??;
            *guard = Some(conn);
        }
        Ok(Held { guard })
    }

    /// Begin a transaction on a pooled connection.
    pub async fn begin(&self) -> Result<Transaction> {
        let mut held = self.acquire().await?;
        held.run(Box::new(|c| c.execute_batch("BEGIN TRANSACTION")))
            .await?;
        Ok(Transaction {
            state: TxState {
                held: Some(held),
                finished: false,
            },
        })
    }
}

/// A connection slot held by a caller.
struct Held {
    guard: OwnedMutexGuard<Option<Connection>>,
}

/// A unit of blocking work run against a connection.
pub type Job<R> = Box<dyn FnOnce(&Connection) -> duckdb::Result<R> + Send + 'static>;

impl Held {
    async fn run<R: Send + 'static>(&mut self, job: Job<R>) -> Result<R> {
        let conn = self
            .guard
            .take()
            .ok_or_else(|| Error::Connection("connection slot is empty".to_owned()))?;
        let (conn, result) = tokio::task::spawn_blocking(move || {
            let r = job(&conn);
            (conn, r)
        })
        .await
        .map_err(|e| Error::Worker(e.to_string()))?;
        *self.guard = Some(conn);
        result.map_err(Error::Db)
    }
}

// ── Executor ───────────────────────────────────────────────────────────

/// Something a statement can run on: a pool (autocommit, any connection) or a
/// transaction (its pinned connection).
pub trait Executor: Send {
    fn run<R: Send + 'static>(self, job: Job<R>) -> impl Future<Output = Result<R>> + Send;
}

impl Executor for &Pool {
    async fn run<R: Send + 'static>(self, job: Job<R>) -> Result<R> {
        let mut held = self.acquire().await?;
        held.run(job).await
    }
}

impl Executor for &mut TxState {
    async fn run<R: Send + 'static>(self, job: Job<R>) -> Result<R> {
        let held = self
            .held
            .as_mut()
            .ok_or_else(|| Error::Connection("transaction already finished".to_owned()))?;
        held.run(job).await
    }
}

impl Executor for &mut Transaction {
    fn run<R: Send + 'static>(self, job: Job<R>) -> impl Future<Output = Result<R>> + Send {
        (&mut self.state).run(job)
    }
}

// ── Transaction ────────────────────────────────────────────────────────

/// Transaction state: the pinned connection and whether it has ended.
pub struct TxState {
    held: Option<Held>,
    finished: bool,
}

/// An open transaction. Dropping it without `commit` rolls it back.
pub struct Transaction {
    state: TxState,
}

impl Deref for Transaction {
    type Target = TxState;
    fn deref(&self) -> &TxState {
        &self.state
    }
}

impl DerefMut for Transaction {
    fn deref_mut(&mut self) -> &mut TxState {
        &mut self.state
    }
}

impl Transaction {
    /// Commit the transaction and release the connection.
    pub async fn commit(mut self) -> Result<()> {
        self.state.finished = true;
        let mut held = self
            .state
            .held
            .take()
            .ok_or_else(|| Error::Connection("transaction already finished".to_owned()))?;
        held.run(Box::new(|c| c.execute_batch("COMMIT"))).await
    }

    /// Roll the transaction back and release the connection.
    pub async fn rollback(mut self) -> Result<()> {
        self.state.finished = true;
        let mut held = self
            .state
            .held
            .take()
            .ok_or_else(|| Error::Connection("transaction already finished".to_owned()))?;
        held.run(Box::new(|c| c.execute_batch("ROLLBACK"))).await
    }
}

impl Drop for TxState {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Best-effort synchronous rollback. A ROLLBACK is cheap in DuckDB and
        // the alternative is leaking an open transaction on a pooled
        // connection, which would poison every later statement on it.
        if let Some(held) = self.held.as_mut()
            && let Some(conn) = held.guard.as_ref()
        {
            let _ = conn.execute_batch("ROLLBACK");
        }
    }
}

// ── Encoding (bind parameters) ─────────────────────────────────────────

/// Types that can be bound as a positional parameter.
pub trait Encode {
    fn encode(self) -> Value;
}

macro_rules! encode_int {
    ($($t:ty),*) => {$(
        impl Encode for $t {
            fn encode(self) -> Value { Value::BigInt(i64::try_from(self).unwrap_or(i64::MAX)) }
        }
        impl Encode for &$t {
            fn encode(self) -> Value { (*self).encode() }
        }
    )*};
}
encode_int!(i8, i16, i32, i64, u8, u16, u32, u64, usize);

impl Encode for f64 {
    fn encode(self) -> Value {
        Value::Double(self)
    }
}
impl Encode for &f64 {
    fn encode(self) -> Value {
        Value::Double(*self)
    }
}
impl Encode for f32 {
    fn encode(self) -> Value {
        Value::Float(self)
    }
}
impl Encode for bool {
    // Catalog booleans are stored as 0/1 integers (see `schema.rs`), so a bool
    // parameter must compare as an integer rather than a BOOLEAN.
    fn encode(self) -> Value {
        Value::BigInt(i64::from(self))
    }
}
impl Encode for &bool {
    fn encode(self) -> Value {
        (*self).encode()
    }
}
impl Encode for String {
    fn encode(self) -> Value {
        Value::Text(self)
    }
}
impl Encode for &String {
    fn encode(self) -> Value {
        Value::Text(self.clone())
    }
}
impl Encode for &str {
    fn encode(self) -> Value {
        Value::Text(self.to_owned())
    }
}
impl Encode for &&str {
    fn encode(self) -> Value {
        Value::Text((*self).to_owned())
    }
}
impl Encode for std::borrow::Cow<'_, str> {
    fn encode(self) -> Value {
        Value::Text(self.into_owned())
    }
}
impl Encode for &std::borrow::Cow<'_, str> {
    fn encode(self) -> Value {
        Value::Text(self.to_string())
    }
}
impl Encode for Vec<u8> {
    fn encode(self) -> Value {
        Value::Blob(self)
    }
}
impl Encode for &Vec<u8> {
    fn encode(self) -> Value {
        Value::Blob(self.clone())
    }
}
impl Encode for &[u8] {
    fn encode(self) -> Value {
        Value::Blob(self.to_vec())
    }
}
impl Encode for serde_json::Value {
    fn encode(self) -> Value {
        Value::Text(self.to_string())
    }
}
impl Encode for &serde_json::Value {
    fn encode(self) -> Value {
        Value::Text(self.to_string())
    }
}
impl Encode for time::OffsetDateTime {
    fn encode(self) -> Value {
        Value::Text(crate::duckdb_util::format_timestamp(self))
    }
}
impl Encode for &time::OffsetDateTime {
    fn encode(self) -> Value {
        (*self).encode()
    }
}
impl<T: Encode> Encode for Option<T> {
    fn encode(self) -> Value {
        match self {
            Some(v) => v.encode(),
            None => Value::Null,
        }
    }
}
impl<T> Encode for &Option<T>
where
    for<'a> &'a T: Encode,
{
    fn encode(self) -> Value {
        match self {
            Some(v) => v.encode(),
            None => Value::Null,
        }
    }
}
impl Encode for Value {
    fn encode(self) -> Value {
        self
    }
}

// ── Decoding (result columns) ──────────────────────────────────────────

/// Types that can be read from a result column.
pub trait Decode: Sized {
    fn decode(v: &Value) -> Result<Self>;
}

fn decode_err(want: &str, v: &Value) -> Error {
    Error::Decode(format!("expected {want}, got {v:?}"))
}

fn value_to_i128(v: &Value) -> Option<i128> {
    Some(match v {
        Value::Boolean(b) => i128::from(*b),
        Value::TinyInt(i) => i128::from(*i),
        Value::SmallInt(i) => i128::from(*i),
        Value::Int(i) => i128::from(*i),
        Value::BigInt(i) => i128::from(*i),
        Value::HugeInt(i) => *i,
        Value::UHugeInt(i) => i128::try_from(*i).ok()?,
        Value::UTinyInt(i) => i128::from(*i),
        Value::USmallInt(i) => i128::from(*i),
        Value::UInt(i) => i128::from(*i),
        Value::UBigInt(i) => i128::from(*i),
        Value::Double(d) if d.fract() == 0.0 => *d as i128,
        Value::Float(d) if d.fract() == 0.0 => *d as i128,
        Value::Decimal(d) if d.scale() == 0 => d.value(),
        Value::Text(s) => s.trim().parse::<i128>().ok()?,
        _ => return None,
    })
}

impl Decode for i64 {
    fn decode(v: &Value) -> Result<Self> {
        value_to_i128(v)
            .and_then(|i| i64::try_from(i).ok())
            .ok_or_else(|| decode_err("integer", v))
    }
}
impl Decode for i32 {
    fn decode(v: &Value) -> Result<Self> {
        value_to_i128(v)
            .and_then(|i| i32::try_from(i).ok())
            .ok_or_else(|| decode_err("integer", v))
    }
}
impl Decode for u64 {
    fn decode(v: &Value) -> Result<Self> {
        value_to_i128(v)
            .and_then(|i| u64::try_from(i).ok())
            .ok_or_else(|| decode_err("unsigned integer", v))
    }
}
impl Decode for u32 {
    fn decode(v: &Value) -> Result<Self> {
        value_to_i128(v)
            .and_then(|i| u32::try_from(i).ok())
            .ok_or_else(|| decode_err("unsigned integer", v))
    }
}
impl Decode for bool {
    fn decode(v: &Value) -> Result<Self> {
        match v {
            Value::Boolean(b) => Ok(*b),
            other => value_to_i128(other)
                .map(|i| i != 0)
                .ok_or_else(|| decode_err("boolean", v)),
        }
    }
}
impl Decode for f64 {
    fn decode(v: &Value) -> Result<Self> {
        match v {
            Value::Double(d) => Ok(*d),
            Value::Float(f) => Ok(f64::from(*f)),
            Value::Decimal(d) => Ok(d.value() as f64 / 10f64.powi(i32::from(d.scale()))),
            Value::Text(s) => s.trim().parse::<f64>().map_err(|_| decode_err("double", v)),
            other => value_to_i128(other)
                .map(|i| i as f64)
                .ok_or_else(|| decode_err("double", v)),
        }
    }
}
impl Decode for f32 {
    fn decode(v: &Value) -> Result<Self> {
        f64::decode(v).map(|d| d as f32)
    }
}
impl Decode for String {
    fn decode(v: &Value) -> Result<Self> {
        match v {
            Value::Text(s) | Value::Enum(s) => Ok(s.clone()),
            Value::Blob(b) => String::from_utf8(b.clone()).map_err(|_| decode_err("text", v)),
            Value::Boolean(b) => Ok(b.to_string()),
            Value::Double(d) => Ok(d.to_string()),
            Value::Float(d) => Ok(d.to_string()),
            other => value_to_i128(other)
                .map(|i| i.to_string())
                .ok_or_else(|| decode_err("text", v)),
        }
    }
}
impl Decode for Vec<u8> {
    fn decode(v: &Value) -> Result<Self> {
        match v {
            Value::Blob(b) | Value::Geometry(b) => Ok(b.clone()),
            Value::Text(s) => Ok(s.clone().into_bytes()),
            _ => Err(decode_err("blob", v)),
        }
    }
}
impl Decode for serde_json::Value {
    fn decode(v: &Value) -> Result<Self> {
        match v {
            Value::Text(s) => serde_json::from_str(s).map_err(|e| Error::Decode(e.to_string())),
            Value::Blob(b) => serde_json::from_slice(b).map_err(|e| Error::Decode(e.to_string())),
            _ => Err(decode_err("json text", v)),
        }
    }
}
impl Decode for time::OffsetDateTime {
    fn decode(v: &Value) -> Result<Self> {
        match v {
            Value::Text(s) => {
                crate::duckdb_util::parse_timestamp(s).map_err(|e| Error::Decode(e.to_string()))
            }
            Value::Timestamp(unit, raw) => {
                let nanos: i128 = match unit {
                    duckdb::types::TimeUnit::Second => i128::from(*raw) * 1_000_000_000,
                    duckdb::types::TimeUnit::Millisecond => i128::from(*raw) * 1_000_000,
                    duckdb::types::TimeUnit::Microsecond => i128::from(*raw) * 1_000,
                    duckdb::types::TimeUnit::Nanosecond => i128::from(*raw),
                };
                time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
                    .map_err(|e| Error::Decode(e.to_string()))
            }
            _ => Err(decode_err("timestamp", v)),
        }
    }
}
impl<T: Decode> Decode for Option<T> {
    fn decode(v: &Value) -> Result<Self> {
        match v {
            Value::Null => Ok(None),
            other => T::decode(other).map(Some),
        }
    }
}
impl Decode for Value {
    fn decode(v: &Value) -> Result<Self> {
        Ok(v.clone())
    }
}

// ── Rows ───────────────────────────────────────────────────────────────

/// A fully materialised result row.
pub type Row = Vec<Value>;

/// Types constructible from a result row, by column position.
pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self>;
}

fn col(row: &Row, i: usize) -> Result<&Value> {
    row.get(i)
        .ok_or_else(|| Error::Decode(format!("row has {} columns, wanted index {i}", row.len())))
}

macro_rules! impl_from_row_tuple {
    ($($idx:tt : $t:ident),+) => {
        impl<$($t: Decode),+> FromRow for ($($t,)+) {
            fn from_row(row: &Row) -> Result<Self> {
                Ok(($($t::decode(col(row, $idx)?)?,)+))
            }
        }
    };
}
impl_from_row_tuple!(0: A);
impl_from_row_tuple!(0: A, 1: B);
impl_from_row_tuple!(0: A, 1: B, 2: C);
impl_from_row_tuple!(0: A, 1: B, 2: C, 3: D);
impl_from_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E);
impl_from_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F);
impl_from_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G);
impl_from_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H);
impl_from_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I);
impl_from_row_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J);

/// Implement [`FromRow`] for a struct whose fields are selected in declaration
/// order (the crate's `*_COLUMNS` constants guarantee that order).
#[macro_export]
macro_rules! impl_from_row {
    ($name:ident { $($field:ident),+ $(,)? }) => {
        impl $crate::db::FromRow for $name {
            fn from_row(row: &$crate::db::Row) -> $crate::db::Result<Self> {
                let mut __i = 0usize;
                $(
                    let $field = $crate::db::Decode::decode(
                        row.get(__i).ok_or_else(|| $crate::db::Error::Decode(
                            format!("row has {} columns, wanted index {}", row.len(), __i)))?)?;
                    __i += 1;
                )+
                let _ = __i;
                Ok(Self { $($field),+ })
            }
        }
    };
}

// ── Statement helpers (blocking side) ──────────────────────────────────

fn run_query(conn: &Connection, sql: &str, params: &[Value]) -> duckdb::Result<Vec<Row>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query(duckdb::params_from_iter(params.iter()))?;
    let ncols = rows.as_ref().map_or(0, |s| s.column_count());
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let mut vals = Vec::with_capacity(ncols);
        for i in 0..ncols {
            vals.push(row.get_ref(i)?.to_owned());
        }
        out.push(vals);
    }
    Ok(out)
}

fn run_execute(conn: &Connection, sql: &str, params: &[Value]) -> duckdb::Result<u64> {
    let mut stmt = conn.prepare(sql)?;
    // A statement with a RETURNING clause produces a result set, and DuckDB
    // reports zero changed rows for it; count the returned rows instead so
    // `rows_affected` means the same thing it does for a plain statement.
    if has_returning_clause(sql) {
        let mut rows = stmt.query(duckdb::params_from_iter(params.iter()))?;
        let mut n = 0u64;
        while rows.next()?.is_some() {
            n += 1;
        }
        return Ok(n);
    }
    let n = stmt.execute(duckdb::params_from_iter(params.iter()))?;
    Ok(n as u64)
}

fn has_returning_clause(sql: &str) -> bool {
    sql.split_whitespace()
        .any(|w| w.eq_ignore_ascii_case("RETURNING"))
}

// ── Query builders ─────────────────────────────────────────────────────

/// Outcome of an `execute`.
#[derive(Debug, Clone, Copy)]
pub struct QueryResult {
    rows_affected: u64,
}

impl QueryResult {
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}

/// A statement with positional parameters, run for its side effects.
pub struct Query {
    sql: String,
    params: Vec<Value>,
}

/// Build a statement. Parameters are `?` placeholders bound with [`Query::bind`].
pub fn query(sql: &str) -> Query {
    Query {
        sql: sql.to_owned(),
        params: Vec::new(),
    }
}

impl Query {
    pub fn bind<T: Encode>(mut self, v: T) -> Self {
        self.params.push(v.encode());
        self
    }

    pub async fn execute<E: Executor>(self, exec: E) -> Result<QueryResult> {
        let Query { sql, params } = self;
        let n = exec
            .run(Box::new(move |c| run_execute(c, &sql, &params)))
            .await?;
        Ok(QueryResult { rows_affected: n })
    }

    pub async fn fetch_all<E: Executor>(self, exec: E) -> Result<Vec<Row>> {
        let Query { sql, params } = self;
        exec.run(Box::new(move |c| run_query(c, &sql, &params)))
            .await
    }
}

/// A statement whose rows decode into `T`.
pub struct QueryAs<T> {
    q: Query,
    _t: PhantomData<fn() -> T>,
}

/// Build a typed statement; rows decode via [`FromRow`].
pub fn query_as<T: FromRow>(sql: &str) -> QueryAs<T> {
    QueryAs {
        q: query(sql),
        _t: PhantomData,
    }
}

impl<T: FromRow + Send + 'static> QueryAs<T> {
    pub fn bind<V: Encode>(mut self, v: V) -> Self {
        self.q = self.q.bind(v);
        self
    }

    pub async fn fetch_all<E: Executor>(self, exec: E) -> Result<Vec<T>> {
        let rows = self.q.fetch_all(exec).await?;
        rows.iter().map(T::from_row).collect()
    }

    pub async fn fetch_optional<E: Executor>(self, exec: E) -> Result<Option<T>> {
        let rows = self.q.fetch_all(exec).await?;
        rows.first().map(T::from_row).transpose()
    }

    pub async fn fetch_one<E: Executor>(self, exec: E) -> Result<T> {
        self.fetch_optional(exec).await?.ok_or(Error::RowNotFound)
    }
}

/// A statement whose first column decodes into `T`.
pub struct QueryScalar<T> {
    q: Query,
    _t: PhantomData<fn() -> T>,
}

/// Build a single-column statement; the first column decodes via [`Decode`].
pub fn query_scalar<T: Decode>(sql: &str) -> QueryScalar<T> {
    QueryScalar {
        q: query(sql),
        _t: PhantomData,
    }
}

impl<T: Decode + Send + 'static> QueryScalar<T> {
    pub fn bind<V: Encode>(mut self, v: V) -> Self {
        self.q = self.q.bind(v);
        self
    }

    pub async fn fetch_all<E: Executor>(self, exec: E) -> Result<Vec<T>> {
        let rows = self.q.fetch_all(exec).await?;
        rows.iter().map(|r| T::decode(col(r, 0)?)).collect()
    }

    pub async fn fetch_optional<E: Executor>(self, exec: E) -> Result<Option<T>> {
        let rows = self.q.fetch_all(exec).await?;
        rows.first().map(|r| T::decode(col(r, 0)?)).transpose()
    }

    pub async fn fetch_one<E: Executor>(self, exec: E) -> Result<T> {
        self.fetch_optional(exec).await?.ok_or(Error::RowNotFound)
    }
}

/// A multi-statement script, run as a batch.
pub struct RawSql {
    sql: String,
}

/// Build a multi-statement batch (DDL scripts); no parameters.
pub fn raw_sql(sql: &str) -> RawSql {
    RawSql {
        sql: sql.to_owned(),
    }
}

impl RawSql {
    pub async fn execute<E: Executor>(self, exec: E) -> Result<()> {
        let sql = self.sql;
        exec.run(Box::new(move |c| c.execute_batch(&sql))).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trip_types() {
        let pool = Pool::open(":memory:", 2).await.unwrap();
        raw_sql(
            "CREATE TABLE t(k TEXT PRIMARY KEY, n BIGINT, d DOUBLE, b BLOB, j TEXT, flag BIGINT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let json = serde_json::json!({"a": [1, 2]});
        let r = query("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)")
            .bind("k1")
            .bind(42i64)
            .bind(1.5f64)
            .bind(vec![0u8, 1, 2])
            .bind(&json)
            .bind(true)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(r.rows_affected(), 1);
        let row: (String, i64, f64, Vec<u8>, serde_json::Value, bool) =
            query_as("SELECT k, n, d, b, j, flag FROM t WHERE k = ?")
                .bind("k1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(row, ("k1".to_owned(), 42, 1.5, vec![0, 1, 2], json, true));
        let count: i64 = query_scalar("SELECT COUNT(*) FROM t")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
        let exists: bool = query_scalar("SELECT EXISTS(SELECT 1 FROM t WHERE k = 'nope')")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(!exists);
        let missing: Option<(String,)> = query_as("SELECT k FROM t WHERE k = 'nope'")
            .fetch_optional(&pool)
            .await
            .unwrap();
        assert!(missing.is_none());
        let sum: i64 = query_scalar("SELECT SUM(n) FROM t")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(sum, 42);
    }

    #[tokio::test]
    async fn transaction_rolls_back_on_drop_and_commits_explicitly() {
        let pool = Pool::open(":memory:", 2).await.unwrap();
        raw_sql("CREATE TABLE t(k TEXT PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();
        {
            let mut tx = pool.begin().await.unwrap();
            query("INSERT INTO t VALUES ('a')")
                .execute(&mut *tx)
                .await
                .unwrap();
            // dropped without commit
        }
        let n: i64 = query_scalar("SELECT COUNT(*) FROM t")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 0);
        let mut tx = pool.begin().await.unwrap();
        query("INSERT INTO t VALUES ('a')")
            .execute(&mut *tx)
            .await
            .unwrap();
        let inner: &mut TxState = &mut tx;
        let seen: i64 = query_scalar("SELECT COUNT(*) FROM t")
            .fetch_one(inner)
            .await
            .unwrap();
        assert_eq!(seen, 1);
        tx.commit().await.unwrap();
        let n: i64 = query_scalar("SELECT COUNT(*) FROM t")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn in_memory_pool_shares_one_database_across_connections() {
        let pool = Pool::open(":memory:", 4).await.unwrap();
        raw_sql("CREATE TABLE t(k TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        for i in 0..8 {
            query("INSERT INTO t VALUES (?)")
                .bind(i)
                .execute(&pool)
                .await
                .unwrap();
        }
        let n: i64 = query_scalar("SELECT COUNT(*) FROM t")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 8);
        let sibling = pool.sibling(2).unwrap();
        let n: i64 = query_scalar("SELECT COUNT(*) FROM t")
            .fetch_one(&sibling)
            .await
            .unwrap();
        assert_eq!(n, 8);
    }

    #[tokio::test]
    async fn separate_in_memory_pools_are_independent() {
        let a = Pool::open(":memory:", 1).await.unwrap();
        let b = Pool::open(":memory:", 1).await.unwrap();
        raw_sql("CREATE TABLE only_a(k TEXT)")
            .execute(&a)
            .await
            .unwrap();
        let exists: bool = query_scalar(
            "SELECT EXISTS(SELECT 1 FROM duckdb_tables() WHERE table_name = 'only_a')",
        )
        .fetch_one(&b)
        .await
        .unwrap();
        assert!(!exists);
    }

    #[tokio::test]
    async fn returning_via_execute_counts_rows() {
        let pool = Pool::open(":memory:", 1).await.unwrap();
        raw_sql("CREATE TABLE t(k TEXT); INSERT INTO t VALUES ('a'), ('b')")
            .execute(&pool)
            .await
            .unwrap();
        let r = query("DELETE FROM t RETURNING k")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(r.rows_affected(), 2);
    }
}
