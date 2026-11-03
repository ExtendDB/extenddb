// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live checks of migration 6's rewrites against a scratch database.
//!
//! Each test needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL with no
//! database name, for example `postgresql://postgres@127.0.0.1:5432`), builds
//! a throwaway database, and drops it when it passes. Without the variable
//! every test reports a skip and passes. One database serves as both catalog
//! and data, created by the file bodies of every migration in id order, as
//! `apply_all_migrations` creates the one-database layout; the bodies'
//! rewrites ([`escape_catalog`], [`escape_data`]) then run in one
//! transaction, as the runner runs each body in its store's transaction. The
//! runner, the ledger and the sessions check over two databases are covered
//! by `tests/migrations_golden.rs`.

use extenddb_storage::migrations::{Body, MigrationError, MigrationId};
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::connect::RUNNER_APPLICATION_PREFIX;
use super::m0006_escape_control_chars::{escape_catalog, escape_data};

/// The `application_name` of the scratch pools: a runner's, so the sessions
/// check of migration 6 treats them as the migration's own.
pub(crate) fn test_application() -> String {
    format!("{RUNNER_APPLICATION_PREFIX}test")
}

pub(crate) struct Scratch {
    pub(crate) db: PgPool,
    pub(crate) admin: PgPool,
    pub(crate) db_name: String,
}

impl Scratch {
    pub(crate) async fn cleanup(self) {
        let Scratch { db, admin, db_name } = self;
        db.close().await;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
        ))
        .execute(&admin)
        .await
        .expect("drop the scratch database");
        admin.close().await;
    }
}

pub(crate) fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

pub(crate) fn skip(test: &str) {
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no \
         PostgreSQL to build a scratch database in."
    );
}

/// A scratch database holding the catalog and the data tables, with the file
/// bodies of every migration applied.
pub(crate) async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_esc_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin)
        .await
        .expect("create the scratch database");
    let db = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(
            format!("{base}/{db_name}")
                .parse::<PgConnectOptions>()
                .expect("scratch connection string")
                .application_name(&test_application()),
        )
        .await
        .expect("connect to the scratch database");
    for id in MigrationId::ALL {
        for body in super::migration(id).bodies() {
            if let Body::File { name, text } = &body.body {
                sqlx::raw_sql(text)
                    .execute(&db)
                    .await
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
            }
        }
    }
    Scratch { db, admin, db_name }
}

/// Run the catalog body's and the data body's rewrites on the connection of
/// `one` (a pool of one connection to the scratch database), in one
/// transaction that commits when both succeed, reading the catalog through
/// `catalog`; the notes of both, catalog first.
pub(crate) async fn run_on(catalog: &PgPool, one: &PgPool) -> Result<Vec<String>, MigrationError> {
    let mut conn = one.acquire().await.expect("acquire the connection");
    sqlx::raw_sql("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("BEGIN");
    let result = async {
        let mut notes = escape_catalog(&mut conn).await?;
        notes.extend(escape_data(catalog, &mut conn).await?);
        Ok(notes)
    }
    .await;
    let end = if result.is_ok() { "COMMIT" } else { "ROLLBACK" };
    sqlx::raw_sql(end).execute(&mut *conn).await.expect(end);
    result
}

/// [`run_on`] through a connection of its own to the database `db` reaches.
pub(crate) async fn run_pool(db: &PgPool) -> Result<Vec<String>, MigrationError> {
    let one = PgPoolOptions::new()
        .max_connections(1)
        .connect_with((*db.connect_options()).clone())
        .await
        .expect("connect a second session");
    let result = run_on(db, &one).await;
    one.close().await;
    result
}

/// [`run_pool`] on the scratch database.
pub(crate) async fn run(s: &Scratch) -> Result<Vec<String>, MigrationError> {
    run_pool(&s.db).await
}

/// Seed a data table, an index table, `gsi_pending`, `stream_records`, and
/// `backup_items` with three row kinds: raw U+0001 in key columns and
/// inside jsonb strings and object keys, the literal six-character text
/// backslash-u-0001 inside a jsonb string, and plain rows.
pub(crate) async fn seed(db: &PgPool) {
    let ks = json!([
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "RANGE"}
    ]);
    let ad = json!([
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "sk", "AttributeType": "S"}
    ]);
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind("111122223333")
        .bind("esc-check")
        .execute(db)
        .await
        .expect("seed the account row");
    sqlx::query(
        "INSERT INTO tables (account_id, table_name, key_schema, attribute_definitions, \
         table_status, table_arn, table_id) \
         VALUES ($1, 't', $2, $3, 'ACTIVE', \
         'arn:aws:dynamodb:us-east-1:111122223333:table/t', 't1')",
    )
    .bind("111122223333")
    .bind(&ks)
    .bind(&ad)
    .execute(db)
    .await
    .expect("seed the tables row");
    sqlx::query(
        "INSERT INTO indexes (table_id, index_id, index_name, index_type, key_schema, \
         projection) VALUES ('t1', 'i1', 'gsi1', 'GSI', $1, $2)",
    )
    .bind(json!([{"AttributeName": "gpk", "KeyType": "HASH"}]))
    .bind(json!({"ProjectionType": "ALL"}))
    .execute(db)
    .await
    .expect("seed the indexes row");

    sqlx::raw_sql(
        r#"CREATE TABLE "_ddb_t1" (
            pk TEXT COLLATE "C" NOT NULL,
            sk_s TEXT COLLATE "C",
            sk_n NUMERIC,
            sk_b BYTEA,
            item_data JSONB NOT NULL,
            PRIMARY KEY (pk, sk_s)
        );
        CREATE TABLE "_ddb_i1" (
            pk TEXT COLLATE "C" NOT NULL,
            sk_s TEXT COLLATE "C",
            sk_n NUMERIC,
            sk_b BYTEA,
            base_pk TEXT COLLATE "C" NOT NULL,
            base_sk_s TEXT COLLATE "C",
            base_sk_n NUMERIC,
            base_sk_b BYTEA,
            item_data JSONB NOT NULL,
            PRIMARY KEY (pk, base_pk, base_sk_s)
        );"#,
    )
    .execute(db)
    .await
    .expect("create the physical data and index tables");

    for (pk, sk, item) in [
        (
            "a\u{1}b",
            "s\u{1}",
            json!({"pk": {"S": "a\u{1}b"}, "k\u{1}": {"S": "v\u{1}"}}),
        ),
        ("lit", "s", json!({"note": {"S": "x\\u0001y"}})),
        ("plain", "s", json!({"pk": {"S": "plain"}})),
    ] {
        sqlx::query("INSERT INTO \"_ddb_t1\" (pk, sk_s, item_data) VALUES ($1, $2, $3)")
            .bind(pk)
            .bind(sk)
            .bind(item)
            .execute(db)
            .await
            .expect("seed a data table row");
    }
    for (pk, sk, base_pk, base_sk, item) in [
        (
            "g\u{1}",
            "gs\u{1}",
            "a\u{1}b",
            "s\u{1}",
            json!({"gpk": {"S": "g\u{1}"}}),
        ),
        ("zz", "zs", "plain", "s", json!({"gpk": {"S": "zz"}})),
    ] {
        sqlx::query(
            "INSERT INTO \"_ddb_i1\" (pk, sk_s, base_pk, base_sk_s, item_data) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(pk)
        .bind(sk)
        .bind(base_pk)
        .bind(base_sk)
        .bind(item)
        .execute(db)
        .await
        .expect("seed an index table row");
    }

    for (new_item, context) in [
        (json!({"pk": {"S": "a\u{1}b"}}), json!({"k\u{1}": 1})),
        (json!({"pk": {"S": "p"}}), json!({"k": 1})),
    ] {
        sqlx::query(
            "INSERT INTO gsi_pending (table_id, worker_partition, old_item, new_item, \
             index_context) VALUES ('t1', 0, NULL, $1, $2)",
        )
        .bind(new_item)
        .bind(context)
        .execute(db)
        .await
        .expect("seed a gsi_pending row");
    }

    sqlx::query(
        "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
         VALUES ('shard-1', 't1', '1')",
    )
    .execute(db)
    .await
    .expect("seed the stream shard");
    for (seq, record) in [
        ("1", json!({"Keys": {"pk": {"S": "a\u{1}b"}}})),
        ("2", json!({"Keys": {"pk": {"S": "p"}}})),
    ] {
        sqlx::query(
            "INSERT INTO stream_records (shard_id, sequence_number, table_id, event_name, \
             record_data) VALUES ('shard-1', $1, 't1', 'INSERT', $2)",
        )
        .bind(seq)
        .bind(record)
        .execute(db)
        .await
        .expect("seed a stream record");
    }

    sqlx::query(
        "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, \
         key_schema, attribute_definitions) \
         VALUES ('arn:aws:dynamodb:us-east-1:111122223333:table/t/backup/b1', 'b1', 't1', \
         't', '111122223333', $1, $2)",
    )
    .bind(&ks)
    .bind(&ad)
    .execute(db)
    .await
    .expect("seed the backups row");
    for (pk, sk, item) in [
        ("p\u{1}", Some("s\u{1}"), json!({"pk": {"S": "p\u{1}"}})),
        ("plain", None, json!({"pk": {"S": "plain"}})),
    ] {
        sqlx::query(
            "INSERT INTO backup_items (backup_arn, pk, sk, item_data) \
             VALUES ('arn:aws:dynamodb:us-east-1:111122223333:table/t/backup/b1', $1, $2, $3)",
        )
        .bind(pk)
        .bind(sk)
        .bind(item)
        .execute(db)
        .await
        .expect("seed a backup item");
    }
}

#[tokio::test]
async fn reencodes_legacy_rows() {
    if base_conn().is_none() {
        skip("reencodes_legacy_rows");
        return;
    }
    let s = scratch().await;
    seed(&s.db).await;

    let notes = run(&s).await.expect("run migration 6");
    assert_eq!(
        notes,
        vec![
            "backup_items: 3 row update(s)",
            "_ddb_t1 (table \"t\" of account 111122223333): 3 row update(s)",
            "_ddb_i1 (index \"gsi1\" of table \"t\" of account 111122223333): 5 row update(s)",
            "gsi_pending: 2 row update(s)",
            "stream_records: 1 row update(s)",
        ]
    );

    // Data table: raw U+0001 became U+0001 U+0002 in the key columns and
    // inside every jsonb string and object key; the literal-backslash row
    // and the plain row are byte-identical.
    let rows: Vec<(String, String, serde_json::Value)> =
        sqlx::query_as("SELECT pk, sk_s, item_data FROM \"_ddb_t1\" ORDER BY pk")
            .fetch_all(&s.db)
            .await
            .expect("read the data table");
    assert_eq!(
        rows,
        vec![
            (
                "a\u{1}\u{2}b".to_owned(),
                "s\u{1}\u{2}".to_owned(),
                json!({"pk": {"S": "a\u{1}\u{2}b"}, "k\u{1}\u{2}": {"S": "v\u{1}\u{2}"}}),
            ),
            (
                "lit".to_owned(),
                "s".to_owned(),
                json!({"note": {"S": "x\\u0001y"}}),
            ),
            (
                "plain".to_owned(),
                "s".to_owned(),
                json!({"pk": {"S": "plain"}}),
            ),
        ]
    );

    // Index table: pk, sk_s, base_pk, base_sk_s and item_data all re-encoded.
    let rows: Vec<(String, String, String, String, serde_json::Value)> = sqlx::query_as(
        "SELECT pk, sk_s, base_pk, base_sk_s, item_data FROM \"_ddb_i1\" ORDER BY pk",
    )
    .fetch_all(&s.db)
    .await
    .expect("read the index table");
    assert_eq!(
        rows,
        vec![
            (
                "g\u{1}\u{2}".to_owned(),
                "gs\u{1}\u{2}".to_owned(),
                "a\u{1}\u{2}b".to_owned(),
                "s\u{1}\u{2}".to_owned(),
                json!({"gpk": {"S": "g\u{1}\u{2}"}}),
            ),
            (
                "zz".to_owned(),
                "zs".to_owned(),
                "plain".to_owned(),
                "s".to_owned(),
                json!({"gpk": {"S": "zz"}}),
            ),
        ]
    );

    // gsi_pending: NULL old_item stays NULL, the raw row is re-encoded in
    // both jsonb columns, the plain row is untouched.
    let rows: Vec<(
        Option<serde_json::Value>,
        serde_json::Value,
        serde_json::Value,
    )> = sqlx::query_as("SELECT old_item, new_item, index_context FROM gsi_pending ORDER BY id")
        .fetch_all(&s.db)
        .await
        .expect("read gsi_pending");
    assert_eq!(
        rows,
        vec![
            (
                None,
                json!({"pk": {"S": "a\u{1}\u{2}b"}}),
                json!({"k\u{1}\u{2}": 1}),
            ),
            (None, json!({"pk": {"S": "p"}}), json!({"k": 1})),
        ]
    );

    // stream_records.
    let rows: Vec<(serde_json::Value,)> =
        sqlx::query_as("SELECT record_data FROM stream_records ORDER BY sequence_number")
            .fetch_all(&s.db)
            .await
            .expect("read stream_records");
    assert_eq!(
        rows,
        vec![
            (json!({"Keys": {"pk": {"S": "a\u{1}\u{2}b"}}}),),
            (json!({"Keys": {"pk": {"S": "p"}}}),),
        ]
    );

    // backup_items, including the NULL sk row.
    let rows: Vec<(String, Option<String>, serde_json::Value)> =
        sqlx::query_as("SELECT pk, sk, item_data FROM backup_items ORDER BY pk")
            .fetch_all(&s.db)
            .await
            .expect("read backup_items");
    assert_eq!(
        rows,
        vec![
            (
                "p\u{1}\u{2}".to_owned(),
                Some("s\u{1}\u{2}".to_owned()),
                json!({"pk": {"S": "p\u{1}\u{2}"}}),
            ),
            ("plain".to_owned(), None, json!({"pk": {"S": "plain"}}),),
        ]
    );

    s.cleanup().await;
}

/// The jsonb candidate predicate must not depend on the session's
/// standard_conforming_strings: with it off, a plain '\u0001' literal is a
/// single 0x01 byte that never appears in jsonb text and matches nothing,
/// so every jsonb row would be skipped while the migration recorded itself.
#[tokio::test]
async fn reencodes_jsonb_rows_with_standard_conforming_strings_off() {
    if base_conn().is_none() {
        skip("reencodes_jsonb_rows_with_standard_conforming_strings_off");
        return;
    }
    let s = scratch().await;
    seed(&s.db).await;

    let base = base_conn().unwrap();
    let scs_off = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::Executor::execute(&mut *conn, "SET standard_conforming_strings = off")
                    .await?;
                Ok(())
            })
        })
        .connect_with(
            format!("{base}/{}", s.db_name)
                .parse::<sqlx::postgres::PgConnectOptions>()
                .unwrap()
                .application_name(&test_application()),
        )
        .await
        .expect("connect with standard_conforming_strings off");

    run_on(&s.db, &scs_off).await.expect("run migration 6");

    let (pk, sk, item): (String, String, serde_json::Value) =
        sqlx::query_as("SELECT pk, sk_s, item_data FROM \"_ddb_t1\" WHERE pk LIKE 'a%'")
            .fetch_one(&s.db)
            .await
            .unwrap();
    assert_eq!(pk, "a\u{1}\u{2}b");
    assert_eq!(sk, "s\u{1}\u{2}");
    assert_eq!(
        item,
        json!({"pk": {"S": "a\u{1}\u{2}b"}, "k\u{1}\u{2}": {"S": "v\u{1}\u{2}"}}),
        "jsonb rows must be re-encoded whatever the session setting"
    );
    scs_off.close().await;
    s.cleanup().await;
}
