// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The rules of the crate's synchronous SQLite shim, as one table run on both
//! targets.
//!
//! On wasm the backend cannot use `sqlx` + the C `libsqlite3-sys` (a dedicated
//! OS thread per connection, a C sysroot), so the crate aliases itself to
//! `sqlx` and its shim drives `sqlite-wasm-rs`'s raw libsqlite3 C ABI
//! synchronously. Native builds keep the real `sqlx`, which is the behaviour
//! this backend is defined against.
//!
//! Each row runs its request on a fresh in-memory pool with one connection,
//! the pool the backend opens for an in-memory database, and renders the
//! outcome as `Ok(<value>)` or `Err[<variant>] <message>`. On native targets
//! the row runs against `sqlx-sqlite` (the version `Cargo.lock` pins) over the
//! C SQLite; on wasm32 it runs against the shim. A row has one expected
//! outcome for both, except the rows named `difference: ...`, which carry the
//! outcome `sqlx` gives and the outcome the shim gives; each stands for an
//! entry of the "Documented differences" list in the shim's module
//! documentation.
//!
//! The table's test, `each_row_gives_its_expected_outcome`, takes its verdict
//! from `compare_rows`, which runs the rows in order. For each row, one
//! statement compares the row's outcome with the outcome the row expects on
//! the target the test runs on, counts the row and writes a failure text for
//! each rule the row breaks: its outcome is not the one it expects; it is
//! named `difference: ...` and expects the same outcome on both targets, or is
//! not so named and expects two different ones; an earlier row has its name.
//! The test fails unless there is no failure text, the count is [`ROWS`] and
//! the number of difference rows counted is [`DIFFERENCE_ROWS`]. Both numbers
//! are written by hand, so the test also fails when a row is deleted from,
//! filtered out of or repeated in the table, and when the row loop skips a
//! row's statement with a `continue`, a `break`, or a `return` of the verdict
//! built so far or of an empty one. It does not check its expectations or its
//! own code: an expectation edited to match a changed outcome passes, as can
//! another edit to the code that runs or judges the rows (such as
//! `compare_rows` and `Verdict::record`) or to the final assertion, and two
//! changes that keep both numbers pass together (a row removed and [`ROWS`]
//! lowered, or a row skipped and another added). Such changes are for review.
//! The test does not read the shim's documentation.
//!
//! Natively the pool's acquire timeout is cut from `sqlx`'s 30 s default to
//! 50 ms, so the rows that find the connection checked out return
//! `PoolTimedOut` without the wait; the shim does not wait at all.
//!
//! The table is an integration test rather than an inline test module so the
//! wasm test binary compiles against only the crate, its dependencies and
//! `wasm-bindgen-test`: the crate's inline unit tests and the native
//! integration tests pull the engine, the backup store and tokio's net stack,
//! none of which build for wasm32. On wasm32 it runs under Node through
//! `wasm-bindgen-test`, and reaches the shim through the crate's
//! `#[doc(hidden)]` `wasm_sqlx_for_tests` module. Its rows that decode by
//! column name use structs declared with the crate's `#[doc(hidden)]`
//! `sqlite_row_struct!` macro, the one the backend declares its catalog row
//! structs with.
//!
//! The two tests after the table drive the backend itself, an in-memory
//! `SqliteEngine` opened as the WebAssembly adapter opens it, with requests
//! whose handling depends on the target: an `UpdateTable` that creates a
//! vector index, which the wasm32 build refuses because it cannot run the
//! index backfill (the test gives each target's expected outcome), and a scan
//! whose `Limit` does not fit wasm32's 32-bit `usize` (the same answer on
//! both).

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;

#[cfg(target_arch = "wasm32")]
use extenddb_storage_sqlite::wasm_sqlx_for_tests as sqlx;
use futures::StreamExt;

/// The number of rows `cases()` returns, written here by hand rather than
/// counted from `cases()`, so that a row deleted from or filtered out of
/// `cases()` fails the test. Adding or removing a row changes this number.
const ROWS: usize = 73;

/// The number of rows named `difference: ...`.
const DIFFERENCE_ROWS: usize = 6;

/// A row's rendered outcome, or the error of a setup step that failed.
type R = Result<String, sqlx::Error>;

/// One row of the table: its name (the stored value and the Rust type of a
/// decode row, the rule for the others), the expected outcome under `sqlx` and
/// under the shim, and the request.
struct Case {
    name: String,
    native: String,
    wasm: String,
    run: Box<dyn Fn() -> Pin<Box<dyn Future<Output = R>>>>,
}

/// A row whose request runs with `$pool`, a fresh pool, and whose outcome is
/// `$expected` on both targets.
macro_rules! row {
    ($name:expr, $expected:expr, |$pool:ident| $body:expr) => {{
        let expected: String = $expected.into();
        difference!($name, native: expected.clone(), wasm: expected, |$pool| $body)
    }};
}

/// A row whose outcome is `$native` under `sqlx` and `$wasm` under the shim.
macro_rules! difference {
    ($name:expr, native: $native:expr, wasm: $wasm:expr, |$pool:ident| $body:expr) => {
        Case {
            name: $name.into(),
            native: $native.into(),
            wasm: $wasm.into(),
            run: Box::new(|| {
                Box::pin(async {
                    let $pool = pool().await?;
                    R::Ok($body)
                })
            }),
        }
    };
}

/// A decode row: `SELECT <stored>` read back as `<type>` with `query_scalar`.
macro_rules! decode {
    ($stored:literal, $ty:ty, $expected:expr) => {
        row!(
            format!("{} as {}", $stored, stringify!($ty)),
            $expected,
            |pool| render(
                sqlx::query_scalar::<_, $ty>(concat!("SELECT ", $stored))
                    .fetch_one(&pool)
                    .await
            )
        )
    };
}

/// The pool the backend opens for an in-memory database: one connection, to
/// the location `sqlite::memory:`.
async fn pool() -> Result<sqlx::SqlitePool, sqlx::Error> {
    pool_options()
        .max_connections(1)
        .connect_with(in_memory()?)
        .await
}

/// The connect options of the location `sqlite::memory:`.
fn in_memory() -> Result<sqlx::sqlite::SqliteConnectOptions, sqlx::Error> {
    sqlx::sqlite::SqliteConnectOptions::from_str("sqlite::memory:")
}

/// Pool options with a 50 ms acquire timeout (see the module documentation).
#[cfg(not(target_arch = "wasm32"))]
fn pool_options() -> sqlx::sqlite::SqlitePoolOptions {
    sqlx::sqlite::SqlitePoolOptions::new().acquire_timeout(std::time::Duration::from_millis(50))
}

/// Pool options; the shim has no acquire timeout to set.
#[cfg(target_arch = "wasm32")]
fn pool_options() -> sqlx::sqlite::SqlitePoolOptions {
    sqlx::pool_options()
}

/// The variant name of an error: its `Debug` text up to the first character
/// that cannot be part of an identifier.
fn variant(e: &sqlx::Error) -> String {
    format!("{e:?}")
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or_default()
        .to_owned()
}

fn render<T: Debug>(result: Result<T, sqlx::Error>) -> String {
    match result {
        Ok(value) => format!("Ok({value:?})"),
        Err(e) => format!("Err[{}] {e}", variant(&e)),
    }
}

const DECODE: &str = "Err[ColumnDecode] error occurred while decoding column";
const OVERFLOW: &str = "Err[Database] error returned from database: (code: 1) integer overflow";
const TIMED_OUT: &str = "Err[PoolTimedOut] pool timed out while waiting for an open connection";
/// The shim's refusal of a `$0` placeholder.
const ZERO_REFUSED: &str =
    "Err[Protocol] encountered unexpected or invalid data: parameter $0 has no argument position";
const CLOSED: &str = "Err[PoolClosed] attempted to acquire a connection on a closed pool";
/// A statement whose second part fails at run time, after a first row.
const ROW_THEN_ERROR: &str = "SELECT 1; SELECT abs(-9223372036854775808)";

/// `sqlx`'s message for a value whose storage class the Rust type refuses.
fn mismatch(column: &str, rust: &str, as_sql: &str, found: &str) -> String {
    format!(
        "{DECODE} {column}: mismatched types; Rust type `{rust}` (as SQL type `{as_sql}`) is not \
         compatible with SQL type `{found}`"
    )
}

/// `rows_affected` of `sql`, run after `setup`.
async fn rows_affected(pool: &sqlx::SqlitePool, setup: &str, sql: &str) -> R {
    sqlx::raw_sql(setup).execute(pool).await?;
    Ok(match sqlx::query(sql).execute(pool).await {
        Ok(r) => format!("rows_affected={}", r.rows_affected()),
        Err(e) => render::<()>(Err(e)),
    })
}

/// The ids in table `t`, in rowid order.
async fn ids(pool: &sqlx::SqlitePool) -> String {
    render(
        sqlx::query_scalar::<_, Option<String>>("SELECT group_concat(id, ',') FROM t")
            .fetch_one(pool)
            .await,
    )
}

/// `SqliteConnectOptions::from_str(url).get_filename()`. The counter in the
/// name `sqlx` gives an in-memory database is replaced by `<n>`, since it
/// depends on how many locations the process parsed before.
fn filename(url: &str) -> String {
    match sqlx::sqlite::SqliteConnectOptions::from_str(url) {
        Ok(options) => {
            let name = options.get_filename().to_string_lossy().into_owned();
            match name.strip_prefix("file:sqlx-in-memory-") {
                Some(n) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => {
                    "Ok(\"file:sqlx-in-memory-<n>\")".to_owned()
                }
                _ => format!("Ok({name:?})"),
            }
        }
        Err(e) => render::<()>(Err(e)),
    }
}

/// The shim's refusal of a location other than `sqlite::memory:`, rendered.
fn refused_location(url: &str) -> String {
    format!(
        "Err[Configuration] error with configuration: The SQLite location {url:?} is not \
         supported by the ExtendDB WebAssembly build: it opens only an in-memory database \
         (sqlite::memory:)."
    )
}

/// The items of a `fetch` stream of one-column rows, rendered.
async fn stream(pool: &sqlx::SqlitePool, sql: &str) -> String {
    let items: Vec<String> = sqlx::query_as::<_, (i64,)>(sql)
        .fetch(pool)
        .map(render)
        .collect()
        .await;
    format!("[{}]", items.join(", "))
}

// Declared with the crate's `sqlite_row_struct!`, as the backend's catalog row
// structs are: natively each struct derives `sqlx::FromRow`; on wasm32 the
// macro generates the impl that reads each field from the column of the
// field's name through the shim.
extenddb_storage_sqlite::sqlite_row_struct! {
    struct Person {
        id: i64,
        name: String,
        nickname: Option<String>,
    }
}

extenddb_storage_sqlite::sqlite_row_struct! {
    /// An integer column `x`.
    struct IntegerX {
        x: i64,
    }
}

extenddb_storage_sqlite::sqlite_row_struct! {
    /// A text column `x`.
    struct TextX {
        x: String,
    }
}

extenddb_storage_sqlite::sqlite_row_struct! {
    /// An integer column `nope`.
    struct Nope {
        nope: i64,
    }
}

extenddb_storage_sqlite::sqlite_row_struct! {
    /// A text column named `type`, a Rust keyword, held in a raw identifier.
    struct Keyword {
        r#type: String,
    }
}

fn cases() -> Vec<Case> {
    let i32_overflow = i32::try_from(1_099_511_627_776_i64)
        .map(|_| ())
        .expect_err("2^40 does not fit in i32")
        .to_string();
    let string = "alloc::string::String";
    vec![
        // ── decoding: storage class against Rust type ──
        decode!("1.5", i64, mismatch("0", "i64", "INTEGER", "REAL")),
        decode!("1.5", i32, mismatch("0", "i32", "INTEGER", "REAL")),
        decode!("'yes'", bool, mismatch("0", "bool", "BOOLEAN", "TEXT")),
        decode!("1.5", String, mismatch("0", string, "TEXT", "REAL")),
        decode!("x'01'", String, mismatch("0", string, "TEXT", "BLOB")),
        decode!("5", String, mismatch("0", string, "TEXT", "INTEGER")),
        decode!("'hi'", i64, mismatch("0", "i64", "INTEGER", "TEXT")),
        decode!("1", f64, mismatch("0", "f64", "REAL", "INTEGER")),
        decode!(
            "1",
            serde_json::Value,
            mismatch("0", "serde_json::value::Value", "TEXT", "INTEGER")
        ),
        decode!(
            "1.5",
            Option<i64>,
            mismatch("0", "core::option::Option<i64>", "INTEGER", "REAL")
        ),
        decode!("2", bool, "Ok(true)"),
        decode!("'ab'", Vec<u8>, "Ok([97, 98])"),
        decode!("1099511627776", i32, format!("{DECODE} 0: {i32_overflow}")),
        decode!(
            "'{\"a\":1}'",
            serde_json::Value,
            "Ok(Object {\"a\": Number(1)})"
        ),
        decode!(
            "'nope'",
            serde_json::Value,
            format!("{DECODE} 0: expected ident at line 1 column 2")
        ),
        decode!(
            "CAST(x'fffe' AS TEXT)",
            String,
            format!("{DECODE} 0: invalid utf-8 sequence of 1 bytes from index 0")
        ),
        decode!("CAST(x'fffe' AS TEXT)", Vec<u8>, "Ok([255, 254])"),
        // ── decoding: NULL into a non-Option type ──
        decode!("NULL", i32, "Ok(0)"),
        decode!("NULL", i64, "Ok(0)"),
        decode!("NULL", f64, "Ok(0.0)"),
        decode!("NULL", bool, "Ok(false)"),
        decode!("NULL", String, "Ok(\"\")"),
        decode!("NULL", Vec<u8>, "Ok([])"),
        decode!("NULL", Option<i64>, "Ok(None)"),
        decode!(
            "NULL",
            serde_json::Value,
            format!("{DECODE} 0: EOF while parsing a value at line 1 column 0")
        ),
        row!("a bound NaN reads back as 0.0", "Ok(0.0)", |pool| {
            sqlx::raw_sql("CREATE TABLE m (v REAL)")
                .execute(&pool)
                .await?;
            sqlx::query("INSERT INTO m (v) VALUES (?)")
                .bind(f64::NAN)
                .execute(&pool)
                .await?;
            render(
                sqlx::query_scalar::<_, f64>("SELECT v FROM m")
                    .fetch_one(&pool)
                    .await,
            )
        }),
        // ── column lookup ──
        row!("a repeated column name finds the last", "Ok(2)", |pool| {
            render(
                sqlx::query_as::<_, IntegerX>("SELECT 1 AS x, 2 AS x")
                    .fetch_one(&pool)
                    .await
                    .map(|row| row.x),
            )
        }),
        row!(
            "a missing column name",
            "Err[ColumnNotFound] no column found for name: nope",
            |pool| render(
                sqlx::query_as::<_, Nope>("SELECT 1 AS x")
                    .fetch_one(&pool)
                    .await
                    .map(|row| row.nope)
            )
        ),
        row!(
            "a column index past the end",
            "Err[ColumnIndexOutOfBounds] column index out of bounds: the len is 2, but the \
             index is 2",
            |pool| render(
                sqlx::query_as::<_, (i64, i64, i64)>("SELECT 1, 2")
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!(
            "a mismatch read by name names the column",
            mismatch("\"x\"", string, "TEXT", "INTEGER"),
            |pool| render(
                sqlx::query_as::<_, TextX>("SELECT 1 AS x")
                    .fetch_one(&pool)
                    .await
                    .map(|row| row.x)
            )
        ),
        row!(
            "a raw identifier field reads the column of its name without r#",
            "Ok(\"k\")",
            |pool| render(
                sqlx::query_as::<_, Keyword>("SELECT 'k' AS \"type\"")
                    .fetch_one(&pool)
                    .await
                    .map(|row| row.r#type)
            )
        ),
        row!(
            "a tuple mismatch names its position",
            mismatch("1", string, "TEXT", "INTEGER"),
            |pool| render(
                sqlx::query_as::<_, (i64, String)>("SELECT 1, 2")
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!(
            "FromRow decodes by column name",
            "Ok((1, \"Ada\", None))",
            |pool| {
                let person: Result<Person, _> =
                    sqlx::query_as("SELECT NULL AS nickname, 'Ada' AS name, 1 AS id")
                        .fetch_one(&pool)
                        .await;
                render(person.map(|p| (p.id, p.name, p.nickname)))
            }
        ),
        // ── error text ──
        row!(
            "a database error shows its code",
            "Err[Database] error returned from database: (code: 1) no such table: missing",
            |pool| render(
                sqlx::query("SELECT * FROM missing")
                    .execute(&pool)
                    .await
                    .map(drop)
            )
        ),
        row!(
            "the Io error text",
            "error communicating with database: boom",
            |_pool| sqlx::Error::Io(std::io::Error::other("boom")).to_string()
        ),
        row!(
            "the WorkerCrashed error text",
            "attempted to communicate with a crashed background worker",
            |_pool| sqlx::Error::WorkerCrashed.to_string()
        ),
        // ── binding ──
        row!(
            "an argument past the placeholders is not bound",
            "Ok(1)",
            |pool| render(
                sqlx::query_scalar::<_, i64>("SELECT ?")
                    .bind(1_i64)
                    .bind(2_i64)
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!(
            "a placeholder with no argument is NULL",
            "Ok((1, None))",
            |pool| render(
                sqlx::query_as::<_, (i64, Option<i64>)>("SELECT ?, ?")
                    .bind(1_i64)
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!("a query with no arguments binds NULL", "Ok(None)", |pool| {
            render(
                sqlx::query_scalar::<_, Option<i64>>("SELECT ?")
                    .fetch_one(&pool)
                    .await,
            )
        }),
        row!(
            "?NNN takes the argument at NNN",
            "Ok((\"b\", \"a\"))",
            |pool| render(
                sqlx::query_as::<_, (String, String)>("SELECT ?2, ?1")
                    .bind("a")
                    .bind("b")
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!(
            "a bare ? after ?NNN takes the next argument in turn",
            "Ok((\"b\", \"b\", None))",
            |pool| render(
                sqlx::query_as::<_, (String, String, Option<String>)>("SELECT ?2, ?, ?")
                    .bind("a")
                    .bind("b")
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!("$N takes the argument at N", "Ok(7)", |pool| render(
            sqlx::query_scalar::<_, i64>("SELECT $1")
                .bind(7_i64)
                .fetch_one(&pool)
                .await
        )),
        row!(
            "a :name placeholder is refused",
            "Err[Protocol] encountered unexpected or invalid data: unsupported SQL parameter \
             format: :x (sqlx_sqlite::arguments:99)",
            |pool| render(
                sqlx::query_scalar::<_, i64>("SELECT :x")
                    .bind(7_i64)
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!(
            "an @name placeholder is refused",
            "Err[Protocol] encountered unexpected or invalid data: unsupported SQL parameter \
             format: @x (sqlx_sqlite::arguments:99)",
            |pool| render(
                sqlx::query_scalar::<_, i64>("SELECT @x")
                    .bind(7_i64)
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!(
            "a $name placeholder is refused",
            "Err[Protocol] encountered unexpected or invalid data: parameters with non-integer \
             names are not currently supported: abc (sqlx_sqlite::arguments:93)",
            |pool| render(
                sqlx::query_scalar::<_, i64>("SELECT $abc")
                    .bind(7_i64)
                    .fetch_one(&pool)
                    .await
            )
        ),
        difference!(
            "difference: fetch_one with a $0 placeholder",
            native: "Err[RowNotFound] no rows returned by a query that expected to return at least \
                     one row",
            wasm: ZERO_REFUSED,
            |pool| render(
                sqlx::query_scalar::<_, i64>("SELECT $0")
                    .bind(7_i64)
                    .fetch_one(&pool)
                    .await
            )
        ),
        difference!(
            "difference: fetch_optional with a $0 placeholder",
            native: "Ok(None)",
            wasm: ZERO_REFUSED,
            |pool| render(
                sqlx::query_scalar::<_, i64>("SELECT $0")
                    .bind(7_i64)
                    .fetch_optional(&pool)
                    .await
            )
        ),
        difference!(
            "difference: execute with a $0 placeholder",
            native: "Ok(0)",
            wasm: ZERO_REFUSED,
            |pool| render(
                sqlx::query("SELECT $0")
                    .bind(7_i64)
                    .execute(&pool)
                    .await
                    .map(|r| r.rows_affected())
            )
        ),
        row!(
            "arguments carry over from one statement to the next",
            "Ok(Some(\"1,2\"))",
            |pool| {
                sqlx::raw_sql("CREATE TABLE t (id INTEGER)")
                    .execute(&pool)
                    .await?;
                sqlx::query("INSERT INTO t VALUES (?); INSERT INTO t VALUES (?)")
                    .bind(1_i64)
                    .bind(2_i64)
                    .execute(&pool)
                    .await?;
                ids(&pool).await
            }
        ),
        // ── result counters ──
        row!(
            "rows affected add up over the statements",
            "rows_affected=3",
            |pool| rows_affected(
                &pool,
                "CREATE TABLE t (id INTEGER)",
                "INSERT INTO t VALUES (1), (2); INSERT INTO t VALUES (3)"
            )
            .await?
        ),
        row!(
            "empty SQL after a write reports nothing",
            "rows_affected=0",
            |pool| rows_affected(
                &pool,
                "CREATE TABLE t (id); INSERT INTO t VALUES (1), (2)",
                ""
            )
            .await?
        ),
        row!(
            "whitespace-only SQL after a write reports nothing",
            "rows_affected=0",
            |pool| rows_affected(
                &pool,
                "CREATE TABLE t (id); INSERT INTO t VALUES (1)",
                " \n "
            )
            .await?
        ),
        row!(
            "comment-only SQL after a write reports nothing",
            "rows_affected=0",
            |pool| rows_affected(
                &pool,
                "CREATE TABLE t (id); INSERT INTO t VALUES (1)",
                "-- c\n"
            )
            .await?
        ),
        row!(
            "a SELECT after a write reports the write's sqlite3_changes",
            "rows_affected=2",
            |pool| rows_affected(
                &pool,
                "CREATE TABLE t (id); INSERT INTO t VALUES (1), (2)",
                "SELECT 1"
            )
            .await?
        ),
        // ── fetch_optional / fetch_one stop at the first row ──
        row!(
            "fetch_optional runs nothing after the first row",
            "Ok(Some(1)) then Ok(None)",
            |pool| {
                sqlx::raw_sql("CREATE TABLE t (id INTEGER)")
                    .execute(&pool)
                    .await?;
                let first = render(
                    sqlx::query_scalar::<_, i64>(
                        "SELECT 1 UNION ALL SELECT 2; INSERT INTO t VALUES (99)",
                    )
                    .fetch_optional(&pool)
                    .await,
                );
                format!("{first} then {}", ids(&pool).await)
            }
        ),
        row!(
            "query_as fetch_one does not see a later error",
            "Ok((1,))",
            |pool| render(
                sqlx::query_as::<_, (i64,)>(ROW_THEN_ERROR)
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!(
            "query_scalar fetch_one does not see a later error",
            "Ok(1)",
            |pool| render(
                sqlx::query_scalar::<_, i64>(ROW_THEN_ERROR)
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!("fetch_optional sees an earlier error", OVERFLOW, |pool| {
            render(
                sqlx::query_scalar::<_, i64>("SELECT abs(-9223372036854775808); SELECT 1")
                    .fetch_optional(&pool)
                    .await,
            )
        }),
        row!("fetch_all sees a later error", OVERFLOW, |pool| render(
            sqlx::query_scalar::<_, i64>(ROW_THEN_ERROR)
                .fetch_all(&pool)
                .await
        )),
        // ── fetch yields in order ──
        row!(
            "fetch yields the rows before a database error, then the error",
            format!("[Ok((1,)), Ok((2,)), {OVERFLOW}]"),
            |pool| stream(
                &pool,
                "SELECT 1 UNION ALL SELECT 2; SELECT abs(-9223372036854775808); SELECT 3"
            )
            .await
        ),
        row!(
            "fetch yields a decode error in place and goes on",
            format!(
                "[Ok((1,)), {}, Ok((3,))]",
                mismatch("0", "i64", "INTEGER", "TEXT")
            ),
            |pool| stream(&pool, "SELECT 1 UNION ALL SELECT 'x' UNION ALL SELECT 3").await
        ),
        // ── the one connection while it is checked out ──
        row!(
            "pool statement, acquire, begin, begin_with during a transaction",
            format!("{TIMED_OUT} | {TIMED_OUT} | {TIMED_OUT} | {TIMED_OUT} | Ok(1)"),
            |pool| {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
                let statement = render(sqlx::query("SELECT 1").execute(&pool).await.map(drop));
                let acquire = render(pool.acquire().await.map(drop));
                let begin = render(pool.begin().await.map(drop));
                let begin_with = render(pool.begin_with("BEGIN IMMEDIATE").await.map(drop));
                let on_tx = render(
                    sqlx::query_scalar::<_, i64>("SELECT 1")
                        .fetch_one(&mut *tx)
                        .await,
                );
                format!("{statement} | {acquire} | {begin} | {begin_with} | {on_tx}")
            }
        ),
        row!(
            "a pool statement while a connection is acquired",
            format!("{TIMED_OUT} | Ok(1) | Ok(1)"),
            |pool| {
                let mut conn = pool.acquire().await?;
                let statement = render(sqlx::query("SELECT 1").execute(&pool).await.map(drop));
                let on_conn = render(
                    sqlx::query_scalar::<_, i64>("SELECT 1")
                        .fetch_one(&mut *conn)
                        .await,
                );
                drop(conn);
                let after = render(
                    sqlx::query_scalar::<_, i64>("SELECT 1")
                        .fetch_one(&pool)
                        .await,
                );
                format!("{statement} | {on_conn} | {after}")
            }
        ),
        row!(
            "commit keeps, rollback and drop discard, and each frees the pool",
            "Ok(Some(\"1\"))",
            |pool| {
                sqlx::raw_sql("CREATE TABLE t (id INTEGER)")
                    .execute(&pool)
                    .await?;
                let mut tx = pool.begin().await?;
                sqlx::query("INSERT INTO t VALUES (1)")
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                let mut tx = pool.begin().await?;
                sqlx::query("INSERT INTO t VALUES (2)")
                    .execute(&mut *tx)
                    .await?;
                tx.rollback().await?;
                let mut tx = pool.begin().await?;
                sqlx::query("INSERT INTO t VALUES (3)")
                    .execute(&mut *tx)
                    .await?;
                drop(tx);
                ids(&pool).await
            }
        ),
        difference!(
            "difference: with max_connections(2) there is still one connection",
            native: "Ok(())",
            wasm: TIMED_OUT,
            |_pool| {
                let pool = pool_options()
                    .max_connections(2)
                    .min_connections(2)
                    .idle_timeout(None)
                    .max_lifetime(None)
                    .connect_with(in_memory()?)
                    .await?;
                let _tx = pool.begin().await?;
                render(sqlx::query("SELECT 1").execute(&pool).await.map(drop))
            }
        ),
        // ── begin_with ──
        row!(
            "begin_with a statement that opens no transaction",
            "Err[BeginFailed] got unexpected connection status after attempting to begin \
             transaction",
            |pool| render(pool.begin_with("SELECT 1").await.map(drop))
        ),
        difference!(
            "difference: begin_with after a failed begin_with",
            native: "Err[InvalidSavePointStatement] attempted to call begin_with at non-zero \
                     transaction depth",
            wasm: "Ok(())",
            |pool| {
                let failed = pool.begin_with("SELECT 1").await.map(drop);
                assert!(failed.is_err(), "SELECT 1 opens no transaction");
                render(pool.begin_with("BEGIN IMMEDIATE").await.map(drop))
            }
        ),
        // ── close ──
        row!(
            "statement, acquire, begin and begin_with after close",
            format!("{CLOSED} | {CLOSED} | {CLOSED} | {CLOSED}"),
            |pool| {
                pool.close().await;
                let statement = render(sqlx::query("SELECT 1").execute(&pool).await.map(drop));
                let acquire = render(pool.acquire().await.map(drop));
                let begin = render(pool.begin().await.map(drop));
                let begin_with = render(pool.begin_with("BEGIN").await.map(drop));
                format!("{statement} | {acquire} | {begin} | {begin_with}")
            }
        ),
        // ── connect options ──
        row!(
            "the database file the backend finds for :memory:",
            "Ok(None)",
            |_pool| {
                use extenddb_storage::config::StorageConfig;
                let config = extenddb_storage_sqlite::SqliteConfig {
                    path: ":memory:".to_owned(),
                    pool_size: 1,
                };
                format!("{:?}", config.database_location())
            }
        ),
        difference!(
            "difference: a location other than sqlite::memory: is refused",
            native: "Ok(\"x.db\") | Ok(\"file:sqlx-in-memory-<n>\")",
            wasm: format!(
                "{} | {}",
                refused_location("sqlite://x.db?mode=rwc"),
                refused_location(":memory:")
            ),
            |_pool| format!(
                "{} | {}",
                filename("sqlite://x.db?mode=rwc"),
                filename(":memory:")
            )
        ),
        // ── connection settings ──
        row!("a connection's busy timeout", "Ok(5000)", |pool| render(
            sqlx::query_scalar::<_, i64>("PRAGMA busy_timeout")
                .fetch_one(&pool)
                .await
        )),
        row!(
            "a connection enforces foreign keys",
            "Ok(1)",
            |pool| render(
                sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                    .fetch_one(&pool)
                    .await
            )
        ),
        row!(
            "an insert that breaks a foreign key",
            "Err[Database] error returned from database: (code: 787) FOREIGN KEY constraint failed",
            |pool| {
                sqlx::raw_sql(
                    "CREATE TABLE p (id INTEGER PRIMARY KEY); \
                     CREATE TABLE c (pid INTEGER REFERENCES p (id))",
                )
                .execute(&pool)
                .await?;
                render(
                    sqlx::query("INSERT INTO c (pid) VALUES (1)")
                        .execute(&pool)
                        .await
                        .map(drop),
                )
            }
        ),
    ]
}

/// What `compare_rows` makes of the rows: how many it judged, how many of
/// those are named `difference: ...`, their names, and the failure texts.
#[derive(Default)]
struct Verdict {
    rows: usize,
    differences: usize,
    names: BTreeSet<String>,
    failures: Vec<String>,
}

impl Verdict {
    /// Judges the row `name`: adds a failure text for each rule it breaks,
    /// then counts it. `passed` is whether it gave the outcome it expects on
    /// this target, and `same` whether it expects the same outcome on both
    /// targets.
    fn record(&mut self, name: String, passed: bool, same: bool, expected: &str, actual: &str) {
        let difference = name.starts_with("difference: ");
        if !self.names.insert(name.clone()) {
            self.failures
                .push(format!("{name}\n    another row has this name"));
        }
        if difference == same {
            self.failures.push(format!(
                "{name}\n    a row is named `difference: ...` exactly when its native and wasm \
                 outcomes differ"
            ));
        }
        if !passed {
            self.failures.push(format!(
                "{name}\n    expected: {expected}\n    actual:   {actual}"
            ));
        }
        self.rows += 1;
        self.differences += usize::from(difference);
    }
}

/// Runs the rows of `cases()` in order and judges each one in the statement
/// that compares its outcome with the outcome it expects on this target.
async fn compare_rows() -> Verdict {
    let mut verdict = Verdict::default();
    for case in cases() {
        let same = case.native == case.wasm;
        let expected = if cfg!(target_arch = "wasm32") {
            case.wasm
        } else {
            case.native
        };
        let actual = match (case.run)().await {
            Ok(outcome) => outcome,
            Err(e) => format!("setup failed: {}", render::<()>(Err(e))),
        };
        verdict.record(case.name, actual == expected, same, &expected, &actual);
    }
    verdict
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn each_row_gives_its_expected_outcome() {
    let verdict = compare_rows().await;
    assert!(
        verdict.failures.is_empty()
            && verdict.rows == ROWS
            && verdict.differences == DIFFERENCE_ROWS,
        "{} rows judged (ROWS is {ROWS}), {} of them named `difference: ...` (DIFFERENCE_ROWS \
         is {DIFFERENCE_ROWS})\n{}",
        verdict.rows,
        verdict.differences,
        verdict.failures.join("\n")
    );
}

/// The account the backend tests below run as.
const ACCOUNT: &str = "000000000000";

/// A fresh in-memory engine, as the WebAssembly adapter opens it, holding the
/// PAY_PER_REQUEST table `t` keyed on the string `pk`.
async fn engine_with_table() -> extenddb_storage_sqlite::SqliteEngine {
    use extenddb_storage::TableEngine;
    let engine = extenddb_storage_sqlite::SqliteEngine::new_in_memory(
        "us-east-1",
        ACCOUNT,
        extenddb_core::limits::DEFAULT_MAX_ITEM_SIZE_BYTES,
    )
    .await
    .expect("in-memory engine");
    let input: extenddb_core::types::CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": "t",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("CreateTable input");
    engine
        .create_table(ACCOUNT, input)
        .await
        .expect("create table");
    engine
}

/// An `UpdateTable` that creates a vector index, alone and beside a global
/// secondary index create and a deletion-protection change, sent to the
/// backend on a fresh table: the answer, the indexes and deletion protection
/// the table has afterwards, and whether its description changed.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn an_update_table_that_creates_a_vector_index() {
    use extenddb_storage::TableEngine;
    let describe = |engine: &extenddb_storage_sqlite::SqliteEngine| {
        let input = extenddb_core::types::DescribeTableInput {
            table_name: "t".to_owned(),
        };
        let engine = engine.clone();
        async move {
            engine
                .describe_table(ACCOUNT, input)
                .await
                .expect("describe")
        }
    };
    let create_vector = serde_json::json!({"Create": {
        "IndexName": "vidx",
        "VectorAttribute": {"AttributeName": "emb"},
        "Dimensions": 3,
        "DistanceFunction": "COSINE",
        "Projection": {"ProjectionType": "ALL"}
    }});
    let refused = "Err(Unsupported(\"UpdateTable is not supported by the ExtendDB WebAssembly \
                   build: it cannot build a vector index; create the table with its vector \
                   indexes instead.\"))";
    let cases = [
        (
            "a vector index create",
            serde_json::json!({"TableName": "t", "VectorIndexUpdates": [create_vector.clone()]}),
            "Ok | vector [\"vidx\"] global [] deletion protection false | changed".to_owned(),
            format!("{refused} | vector [] global [] deletion protection false | unchanged"),
        ),
        (
            "a vector index create beside other changes",
            serde_json::json!({
                "TableName": "t",
                "AttributeDefinitions": [{"AttributeName": "g", "AttributeType": "S"}],
                "GlobalSecondaryIndexUpdates": [{"Create": {
                    "IndexName": "gsi",
                    "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }}],
                "DeletionProtectionEnabled": true,
                "VectorIndexUpdates": [create_vector]
            }),
            "Ok | vector [\"vidx\"] global [\"gsi\"] deletion protection true | changed".to_owned(),
            format!("{refused} | vector [] global [] deletion protection false | unchanged"),
        ),
    ];
    let mut failures = Vec::new();
    for (name, request, native, wasm) in cases {
        let engine = engine_with_table().await;
        let before = describe(&engine).await;
        let input: extenddb_core::types::UpdateTableInput =
            serde_json::from_value(request).expect("UpdateTable input");
        let answer = match engine.update_table(ACCOUNT, input).await {
            Ok(_) => "Ok".to_owned(),
            Err(e) => format!("Err({e:?})"),
        };
        let after = describe(&engine).await;
        let vector: Vec<&str> = after
            .vector_indexes
            .iter()
            .flatten()
            .map(|index| index.index_name.as_str())
            .collect();
        let global: Vec<&str> = after
            .global_secondary_indexes
            .iter()
            .flatten()
            .map(|index| index.index_name.as_str())
            .collect();
        let changed = if serde_json::to_value(&after).expect("after")
            == serde_json::to_value(&before).expect("before")
        {
            "unchanged"
        } else {
            "changed"
        };
        let actual = format!(
            "{answer} | vector {vector:?} global {global:?} deletion protection {} | {changed}",
            after.deletion_protection_enabled
        );
        let expected = if cfg!(target_arch = "wasm32") {
            wasm
        } else {
            native
        };
        if actual != expected {
            failures.push(format!(
                "{name}\n    expected: {expected}\n    actual:   {actual}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A scan whose `Limit` is above `u32::MAX` reads like one with no limit on
/// both targets: every item and no `LastEvaluatedKey`. `usize` is 32 bits on
/// wasm32, where the limit does not fit it.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn a_scan_limit_above_u32_max_returns_every_item() {
    use extenddb_storage::{DataEngine, TableEngine};
    let engine = engine_with_table().await;
    let key_info = engine.table_key_info(ACCOUNT, "t").await.expect("key info");
    for pk in ["a", "b", "c"] {
        let item: extenddb_core::types::Item =
            serde_json::from_value(serde_json::json!({"pk": {"S": pk}})).expect("item");
        engine
            .put_item(
                &key_info,
                item,
                false,
                None,
                &extenddb_core::expression::ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put");
    }
    let (items, last_key) = engine
        .scan(&key_info, Some(4_294_967_297), None, None, None, None)
        .await
        .expect("scan");
    assert_eq!(
        (items.len(), last_key),
        (3, None),
        "a Limit of 2^32 + 1 over three items"
    );
}
