// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! wasm32 runtime tests for the synchronous SQLite shim.
//!
//! On wasm the backend cannot use `sqlx` + the C `libsqlite3-sys` (a dedicated
//! OS thread per connection, a C sysroot), so the crate aliases itself to
//! `sqlx` and the bundled shim drives `sqlite-wasm-rs`'s raw libsqlite3 C ABI
//! synchronously. Native builds keep the real `sqlx`, which is the behaviour
//! this backend is defined against.
//!
//! The test is one table. Each row runs its request on a fresh in-memory pool
//! with one connection, the pool the backend opens for an in-memory database,
//! and renders the outcome as `Ok(<value>)` or `Err[<variant>] <message>`.
//! The expected outcome of a row is what `sqlx-sqlite` 0.8.6 returns for the
//! same calls natively, except in the rows named `difference: ...`, which pin
//! the shim's side of a difference listed in the shim's module documentation.
//!
//! The cases live in an integration test rather than the shim module so the
//! wasm test binary compiles against only the crate and `wasm-bindgen-test`:
//! the crate's inline unit tests and the native integration tests pull the
//! engine, the backup store and tokio's net stack, none of which build for
//! wasm32. They run under Node through `wasm-bindgen-test`; the shim's items
//! are reached through the crate's public surface (the crate re-exports the
//! shim at its root on wasm).
#![cfg(target_arch = "wasm32")]

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

use extenddb_storage_sqlite as sqlx;
use futures::StreamExt;
use sqlx::Connection;
use wasm_bindgen_test::wasm_bindgen_test;

/// A row's rendered outcome, or the error of a setup step that failed.
type R = Result<String, sqlx::Error>;

/// One row of the table: its name (the stored value and the Rust type of a
/// decode row, the rule for the others), the expected outcome, and the
/// request.
struct Case {
    name: String,
    expected: String,
    run: Box<dyn Fn() -> Pin<Box<dyn Future<Output = R>>>>,
}

/// A row whose request runs with `$pool`, a fresh pool.
macro_rules! row {
    ($name:expr, $expected:expr, |$pool:ident| $body:expr) => {
        Case {
            name: $name.into(),
            expected: $expected.into(),
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

/// The pool the backend opens for an in-memory database: one connection.
async fn pool() -> Result<sqlx::SqlitePool, sqlx::Error> {
    sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(":memory:")
        .await
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

/// `rows_affected` and `last_insert_rowid` of `sql`, run after `setup`.
async fn counters(pool: &sqlx::SqlitePool, setup: &str, sql: &str) -> R {
    sqlx::raw_sql(setup).execute(pool).await?;
    Ok(match sqlx::query(sql).execute(pool).await {
        Ok(r) => format!(
            "rows_affected={} last_insert_rowid={}",
            r.rows_affected(),
            r.last_insert_rowid()
        ),
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
/// name of an in-memory database is replaced by `<n>`, since it depends on how
/// many locations the process parsed before.
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

/// The items of a `fetch` stream of one-column rows, rendered.
async fn stream(pool: &sqlx::SqlitePool, sql: &str) -> String {
    let items: Vec<String> = sqlx::query_as::<_, (i64,)>(sql)
        .fetch(pool)
        .map(render)
        .collect()
        .await;
    format!("[{}]", items.join(", "))
}

struct Person {
    id: i64,
    name: String,
    nickname: Option<String>,
}

// The backend's catalog structs implement `FromRow` by decoding each field
// from the column of the same name; this stands in for one of them to pin
// that by-name decode path. The one-field-list declaration macro that
// generates this for the real structs lives with those structs.
impl sqlx::FromRow for Person {
    fn from_row(row: &sqlx::Row) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            nickname: row.try_get("nickname")?,
        })
    }
}

#[allow(clippy::too_many_lines)]
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
            let row = sqlx::query("SELECT 1 AS x, 2 AS x")
                .fetch_one(&pool)
                .await?;
            render(row.try_get::<i64, _>("x"))
        }),
        row!(
            "a missing column name",
            "Err[ColumnNotFound] no column found for name: nope",
            |pool| {
                let row = sqlx::query("SELECT 1 AS x").fetch_one(&pool).await?;
                render(row.try_get::<i64, _>("nope"))
            }
        ),
        row!(
            "a column index past the end",
            "Err[ColumnIndexOutOfBounds] column index out of bounds: the len is 2, but the \
             index is 5",
            |pool| {
                let row = sqlx::query("SELECT 1, 2").fetch_one(&pool).await?;
                render(row.try_get::<i64, _>(5_usize))
            }
        ),
        row!(
            "a mismatch read by name names the column",
            mismatch("\"x\"", string, "TEXT", "INTEGER"),
            |pool| {
                let row = sqlx::query("SELECT 1 AS x").fetch_one(&pool).await?;
                render(row.try_get::<String, _>("x"))
            }
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
        row!(
            "difference: a $0 placeholder is refused (sqlx's worker thread panics)",
            "Err[Protocol] encountered unexpected or invalid data: parameter $0 has no argument \
             position",
            |pool| render(
                sqlx::query_scalar::<_, i64>("SELECT $0")
                    .bind(7_i64)
                    .fetch_one(&pool)
                    .await
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
            "rows_affected=3 last_insert_rowid=3",
            |pool| counters(
                &pool,
                "CREATE TABLE t (id INTEGER)",
                "INSERT INTO t VALUES (1), (2); INSERT INTO t VALUES (3)"
            )
            .await?
        ),
        row!(
            "empty SQL after a write reports nothing",
            "rows_affected=0 last_insert_rowid=0",
            |pool| counters(
                &pool,
                "CREATE TABLE t (id); INSERT INTO t VALUES (1), (2)",
                ""
            )
            .await?
        ),
        row!(
            "whitespace-only SQL after a write reports nothing",
            "rows_affected=0 last_insert_rowid=0",
            |pool| counters(
                &pool,
                "CREATE TABLE t (id); INSERT INTO t VALUES (1)",
                " \n "
            )
            .await?
        ),
        row!(
            "comment-only SQL after a write reports nothing",
            "rows_affected=0 last_insert_rowid=0",
            |pool| counters(
                &pool,
                "CREATE TABLE t (id); INSERT INTO t VALUES (1)",
                "-- c\n"
            )
            .await?
        ),
        row!(
            "a SELECT after a write reports the write's sqlite3_changes",
            "rows_affected=2 last_insert_rowid=2",
            |pool| counters(
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
            "query fetch_one does not see a later error",
            "Ok(1)",
            |pool| render(
                sqlx::query(ROW_THEN_ERROR)
                    .fetch_one(&pool)
                    .await
                    .and_then(|row| row.try_get::<i64, _>(0_usize))
            )
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
        row!(
            "difference: with max_connections(2) there is still one connection",
            TIMED_OUT,
            |_pool| {
                let pool = sqlx::sqlite::SqlitePoolOptions::new()
                    .max_connections(2)
                    .min_connections(2)
                    .idle_timeout(None)
                    .max_lifetime(None)
                    .connect(":memory:")
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
        row!(
            "difference: begin_with after a failed begin_with runs \
             (sqlx: InvalidSavePointStatement)",
            "Ok(())",
            |pool| {
                let failed = pool.begin_with("SELECT 1").await.map(drop);
                assert!(failed.is_err(), "SELECT 1 opens no transaction");
                render(pool.begin_with("BEGIN IMMEDIATE").await.map(drop))
            }
        ),
        row!(
            "difference: a nested begin fails (sqlx opens a savepoint)",
            "Err[Database] error returned from database: (code: 1) cannot start a transaction \
             within a transaction",
            |pool| {
                let mut tx = pool.begin().await?;
                render(Connection::begin(&mut *tx).await.map(drop))
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
            ":memory: file name",
            "Ok(\"file:sqlx-in-memory-<n>\")",
            |_pool| filename(":memory:")
        ),
        row!(
            "sqlite::memory: file name",
            "Ok(\"file:sqlx-in-memory-<n>\")",
            |_pool| filename("sqlite::memory:")
        ),
        row!(
            "sqlite://:memory: file name",
            "Ok(\"file:sqlx-in-memory-<n>\")",
            |_pool| filename("sqlite://:memory:")
        ),
        row!(
            "file::memory:?cache=shared file name",
            "Ok(\"file::memory:\")",
            |_pool| filename("file::memory:?cache=shared")
        ),
        row!("file:/abs file name", "Ok(\"file:/abs\")", |_pool| {
            filename("file:/abs")
        }),
        row!("a percent-encoded file name", "Ok(\"a?b.db\")", |_pool| {
            filename("sqlite://a%3Fb.db?mode=rwc")
        }),
        row!("a stray % stays in the file name", "Ok(\"%zz\")", |_pool| {
            filename("%zz")
        }),
        row!("every leading scheme is removed", "Ok(\"x\")", |_pool| {
            filename("sqlite:sqlite:x")
        }),
        row!(
            "a file name that decodes to invalid UTF-8",
            "Err[Configuration] error with configuration: invalid utf-8 sequence of 1 bytes \
             from index 1",
            |_pool| filename("sqlite:x%ff")
        ),
        // The serve lock's `database_file` passes a location through
        // `sqlite_url` and reads the file name off these options on both
        // targets; these are the URLs it builds for the locations named.
        row!(
            "database_file(\"file::memory:?cache=shared\") reads \
             sqlite://file::memory:?cache=shared?mode=rwc",
            "Err[Configuration] error with configuration: unknown value \"shared?mode=rwc\" \
             for `cache`",
            |_pool| filename("sqlite://file::memory:?cache=shared?mode=rwc")
        ),
        row!(
            "database_file(\"file:/abs\") reads sqlite://file:/abs?mode=rwc",
            "Ok(\"file:/abs\")",
            |_pool| filename("sqlite://file:/abs?mode=rwc")
        ),
        // ── connect options: query parameters ──
        row!(
            "an unknown query parameter",
            "Err[Configuration] error with configuration: unknown query parameter `bogus` while \
             parsing connection URL",
            |_pool| filename("sqlite::memory:?bogus=1")
        ),
        row!(
            "an unknown mode",
            "Err[Configuration] error with configuration: unknown value \"bad\" for `mode`",
            |_pool| filename("sqlite://x?mode=bad")
        ),
        row!(
            "a mode with no value",
            "Err[Configuration] error with configuration: unknown value \"\" for `mode`",
            |_pool| filename("sqlite://x?mode")
        ),
        row!(
            "an unknown cache",
            "Err[Configuration] error with configuration: unknown value \"no\" for `cache`",
            |_pool| filename("sqlite://x?cache=no")
        ),
        row!(
            "an unknown immutable",
            "Err[Configuration] error with configuration: unknown value \"yes\" for `immutable`",
            |_pool| filename("sqlite://x?immutable=yes")
        ),
        row!(
            "known parameters, empty pairs and an encoded key",
            "Ok(\"x\") | Ok(\"x\") | Ok(\"x\")",
            |_pool| format!(
                "{} | {} | {}",
                filename("sqlite://x?vfs=a&mode=ro"),
                filename("sqlite://x?&&mode=rw"),
                filename("sqlite://x?mo%64e=memory")
            )
        ),
        row!(
            "a + in a key decodes to a space",
            "Err[Configuration] error with configuration: unknown query parameter `a b` while \
             parsing connection URL",
            |_pool| filename("sqlite://x?a+b=1")
        ),
    ]
}

#[wasm_bindgen_test]
async fn shim_follows_sqlx_sqlite() {
    let cases = cases();
    let total = cases.len();
    let mut failures = Vec::new();
    for case in cases {
        let actual = match (case.run)().await {
            Ok(outcome) => outcome,
            Err(e) => format!("setup failed: {}", render::<()>(Err(e))),
        };
        if actual != case.expected {
            failures.push(format!(
                "{}\n    expected: {}\n    actual:   {actual}",
                case.name, case.expected
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {total} rows differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
