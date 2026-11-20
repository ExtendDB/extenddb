// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! wasm32 runtime tests for the synchronous SQLite shim.
//!
//! On wasm the backend cannot use `sqlx` + the C `libsqlite3-sys` (a dedicated
//! OS thread per connection, a C sysroot), so the crate aliases itself to
//! `sqlx` and the bundled shim drives `sqlite-wasm-rs`'s raw libsqlite3 C ABI
//! synchronously. Native builds keep the real `sqlx`, which is the behaviour
//! this backend is defined against, so these cases pin the shim's own rules on
//! the one target where it is the backend: the storage-class type checks, NULL
//! decoding into a non-`Option`, invalid UTF-8, duplicate column names,
//! multi-statement and empty SQL, the pool-vs-open-transaction guard, rollback
//! on drop, `acquire`, and by-name `FromRow` decoding.
//!
//! The cases live in an integration test rather than the shim module so the
//! wasm test binary compiles against only the crate and `wasm-bindgen-test`:
//! the crate's inline unit tests and the native integration tests pull the
//! engine, the backup store and tokio's net stack, none of which build for
//! wasm32. They run under Node through `wasm-bindgen-test`; the shim's items
//! are reached through the crate's public surface (the crate re-exports the
//! shim at its root on wasm).
#![cfg(target_arch = "wasm32")]

use extenddb_storage_sqlite::{
    Error, FromRow, Row, SqlitePool, SqlitePoolOptions, query, query_as, query_scalar, raw_sql,
};
use wasm_bindgen_test::wasm_bindgen_test;

async fn mem_pool() -> SqlitePool {
    SqlitePoolOptions::new()
        .connect(":memory:")
        .await
        .expect("open in-memory database")
}

#[wasm_bindgen_test]
async fn round_trips_create_insert_query() {
    let pool = mem_pool().await;
    raw_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    query("INSERT INTO t (id, name) VALUES (?, ?)")
        .bind(1_i64)
        .bind("alice")
        .execute(&pool)
        .await
        .unwrap();

    // query_scalar in both inferred and turbofish forms.
    let count: i64 = query_scalar("SELECT COUNT(*) FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    let name = query_scalar::<_, String>("SELECT name FROM t WHERE id = ?")
        .bind(1_i64)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(name, "alice");

    // query_as tuple.
    let (id, name): (i64, String) = query_as("SELECT id, name FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((id, name), (1, "alice".to_owned()));
}

#[wasm_bindgen_test]
async fn decode_type_mismatch_is_an_error() {
    let pool = mem_pool().await;
    raw_sql("CREATE TABLE t (n INTEGER, s TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    query("INSERT INTO t VALUES (5, 'hi')")
        .execute(&pool)
        .await
        .unwrap();

    // Reading an INTEGER column as String is a mismatch, as under sqlx.
    let as_string: Result<String, _> = query_scalar("SELECT n FROM t").fetch_one(&pool).await;
    assert!(matches!(as_string, Err(Error::Protocol(_))));
    // Reading a TEXT column as i64 is a mismatch.
    let as_int: Result<i64, _> = query_scalar("SELECT s FROM t").fetch_one(&pool).await;
    assert!(matches!(as_int, Err(Error::Protocol(_))));
    // Reading an INTEGER column as f64 is a mismatch (f64 wants REAL).
    let as_real: Result<f64, _> = query_scalar("SELECT n FROM t").fetch_one(&pool).await;
    assert!(matches!(as_real, Err(Error::Protocol(_))));
}

#[wasm_bindgen_test]
async fn null_into_non_option_yields_default() {
    let pool = mem_pool().await;
    let text: String = query_scalar("SELECT NULL").fetch_one(&pool).await.unwrap();
    assert_eq!(text, "");
    let number: i64 = query_scalar("SELECT NULL").fetch_one(&pool).await.unwrap();
    assert_eq!(number, 0);
    let real: f64 = query_scalar("SELECT NULL").fetch_one(&pool).await.unwrap();
    assert_eq!(real, 0.0);
    let flag: bool = query_scalar("SELECT NULL").fetch_one(&pool).await.unwrap();
    assert!(!flag);
    // Option sees the NULL as None.
    let opt: Option<i64> = query_scalar("SELECT NULL").fetch_one(&pool).await.unwrap();
    assert_eq!(opt, None);
}

#[wasm_bindgen_test]
async fn nan_bound_reads_back_as_zero() {
    // SQLite stores a bound NaN as NULL, which reads back as the f64
    // default 0.0 — the behaviour the metrics path depends on.
    let pool = mem_pool().await;
    raw_sql("CREATE TABLE m (v REAL)")
        .execute(&pool)
        .await
        .unwrap();
    query("INSERT INTO m (v) VALUES (?)")
        .bind(f64::NAN)
        .execute(&pool)
        .await
        .unwrap();
    let v: f64 = query_scalar("SELECT v FROM m")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 0.0);
}

#[wasm_bindgen_test]
async fn invalid_utf8_text_errors_as_string_but_reads_as_bytes() {
    let pool = mem_pool().await;
    raw_sql("CREATE TABLE t (b)").execute(&pool).await.unwrap();
    // Store bytes 0xff 0xfe via a blob literal, then read the column as a
    // string (mismatch: it is stored as a BLOB) and as bytes.
    query("INSERT INTO t (b) VALUES (x'fffe')")
        .execute(&pool)
        .await
        .unwrap();
    let bytes: Vec<u8> = query_scalar("SELECT b FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(bytes, vec![0xff, 0xfe]);

    // A TEXT value with invalid UTF-8: CAST the blob to TEXT so its storage
    // class is TEXT, then decoding as String errors while bytes succeed.
    let as_string: Result<String, _> = query_scalar("SELECT CAST(x'fffe' AS TEXT)")
        .fetch_one(&pool)
        .await;
    assert!(matches!(as_string, Err(Error::Protocol(_))));
    let as_bytes: Vec<u8> = query_scalar("SELECT CAST(x'fffe' AS TEXT)")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(as_bytes, vec![0xff, 0xfe]);
}

#[wasm_bindgen_test]
async fn duplicate_column_name_resolves_to_last() {
    let pool = mem_pool().await;
    let row = query("SELECT 1 AS x, 2 AS x")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>("x"), 2);
}

#[wasm_bindgen_test]
async fn multi_statement_query_runs_every_statement() {
    let pool = mem_pool().await;
    raw_sql("CREATE TABLE t (id INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    // Two statements in one execute; both must run.
    query("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);")
        .execute(&pool)
        .await
        .unwrap();
    let count: i64 = query_scalar("SELECT COUNT(*) FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[wasm_bindgen_test]
async fn empty_and_comment_only_sql_is_not_an_error() {
    let pool = mem_pool().await;
    query("").execute(&pool).await.unwrap();
    query("   ").execute(&pool).await.unwrap();
    query("-- just a comment\n").execute(&pool).await.unwrap();
    let rows = query("-- nothing").fetch_all(&pool).await.unwrap();
    assert!(rows.is_empty());
}

#[wasm_bindgen_test]
async fn pool_statement_during_open_transaction_is_rejected() {
    let pool = mem_pool().await;
    raw_sql("CREATE TABLE t (id INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    // A statement on the transaction is fine.
    query("INSERT INTO t VALUES (1)")
        .execute(&mut *tx)
        .await
        .unwrap();
    // A statement through the pool while the transaction is open is rejected.
    let pooled = query("SELECT COUNT(*) FROM t").fetch_one(&pool).await;
    assert!(matches!(pooled, Err(Error::Protocol(_))));
    tx.commit().await.unwrap();
    // After commit the pool is usable again.
    let count: i64 = query_scalar("SELECT COUNT(*) FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[wasm_bindgen_test]
async fn transaction_rolls_back_on_drop() {
    let pool = mem_pool().await;
    raw_sql("CREATE TABLE t (id INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    {
        let mut tx = pool.begin().await.unwrap();
        query("INSERT INTO t VALUES (1)")
            .execute(&mut *tx)
            .await
            .unwrap();
        // tx dropped without commit → rollback.
    }
    let count: i64 = query_scalar("SELECT COUNT(*) FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[wasm_bindgen_test]
async fn acquire_yields_a_usable_connection() {
    let pool = mem_pool().await;
    raw_sql("CREATE TABLE t (id INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    let mut connection = pool.acquire().await.unwrap();
    query("INSERT INTO t VALUES (7)")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    let id: i64 = query_scalar("SELECT id FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(id, 7);
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
impl FromRow for Person {
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            nickname: row.try_get("nickname")?,
        })
    }
}

#[wasm_bindgen_test]
async fn from_row_decodes_by_column_name() {
    let pool = mem_pool().await;
    raw_sql("CREATE TABLE person (id INTEGER, name TEXT, nickname TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    query("INSERT INTO person VALUES (1, 'Ada', NULL)")
        .execute(&pool)
        .await
        .unwrap();
    // Column order deliberately differs from field order to prove the
    // decode is by name, not position.
    let person: Person = query_as("SELECT nickname, name, id FROM person")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(person.id, 1);
    assert_eq!(person.name, "Ada");
    assert_eq!(person.nickname, None);
}
