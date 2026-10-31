// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared by the migration integration tests: scratch database pairs, the
//! released fixtures, a fresh install seeded the way the fixtures were, and
//! the comparison of two installs with table and index ids mapped to names.
//!
//! Every test needs `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL
//! `postgres://user:pass@host:port` with no database, for a role that may
//! create databases; without it every test reports a skip and passes.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use extenddb_core::types::CreateTableInput;
use extenddb_storage::migrations::{MigrationId, RunReport, Runner, StoreKind};
use extenddb_storage::{MetadataEngine, TableEngine};
use extenddb_storage_postgres::migrations::{
    Definitions, PostgresMigrations, PostgresTarget, describe, migration,
};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

pub const ACCOUNT: &str = "123456789012";

/// The highest migration this binary carries. Tests derive what a complete
/// run does from it and from this backend's table, so adding a migration
/// needs no edit here; a literal id in a test means that migration itself.
pub const LATEST: u32 = MigrationId::LATEST.number();

/// The kinds of store `id` records rows in, catalog first, as this backend's
/// table declares them and the runner applies them.
fn store_kinds(id: MigrationId) -> Vec<StoreKind> {
    migration(id).store_kinds()
}

/// The ids a complete run records in a store of `kind`: every id in the
/// catalog, and in the data database the ids with a data body.
pub fn ids_in(kind: StoreKind) -> Vec<i32> {
    MigrationId::ALL
        .into_iter()
        .filter(|id| store_kinds(*id).contains(&kind))
        .map(|id| i32::try_from(id.number()).expect("an id fits the ledger's INTEGER"))
        .collect()
}

/// The steps a run from an empty or a legacy catalog applies: one for each
/// id in each store it records rows in.
pub fn step_count() -> usize {
    MigrationId::ALL
        .into_iter()
        .map(|id| store_kinds(id).len())
        .sum()
}

pub fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// Whether a missing server fails the test instead of skipping it: set
/// `EXTENDDB_TEST_PG_REQUIRED` to any value but `0` in a job that must run
/// these tests.
pub fn required() -> bool {
    std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0")
}

/// Report a skip, or fail when [`required`].
pub fn skip(test: &str) {
    assert!(
        !required(),
        "{test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, and EXTENDDB_TEST_PG_REQUIRED \
         says this job must run the PostgreSQL tests"
    );
    eprintln!("SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
}

/// Report a skip for a test that needs pgvector on a server without it.
pub fn skip_without_pgvector(test: &str) {
    eprintln!(
        "SKIP {test}: the server has no pgvector package, and \
         EXTENDDB_TEST_PGVECTOR_CONNECTION_STRING names no server that has it"
    );
}

/// Whether the test writes its expected files instead of comparing with
/// them: `EXTENDDB_BLESS_EXPECTED=1`.
pub fn bless() -> bool {
    std::env::var("EXTENDDB_BLESS_EXPECTED").as_deref() == Ok("1")
}

/// Whether the server has the pgvector package, which the vector fixtures
/// need.
pub async fn has_pgvector(base: &str) -> bool {
    let admin = pool(&format!("{base}/postgres"), 1).await;
    let found: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector')",
    )
    .fetch_one(&admin)
    .await
    .expect("read pg_available_extensions");
    admin.close().await;
    found
}

pub async fn pool(url: &str, size: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(size)
        .connect(url)
        .await
        .unwrap_or_else(|e| panic!("connect to {url}: {e}"))
}

/// A catalog and a data database, dropped by [`Scratch::drop_all`] or, after
/// a failed assertion, by the guard.
pub struct Scratch {
    pub base: String,
    pub catalog_name: String,
    pub data_name: String,
    _guard: Guard,
}

struct Guard {
    base: String,
    names: Vec<String>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        let (base, names) = (self.base.clone(), self.names.clone());
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async {
                if let Ok(admin) = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&format!("{base}/postgres"))
                    .await
                {
                    for n in &names {
                        let _ =
                            sqlx::query(&format!("DROP DATABASE IF EXISTS \"{n}\" WITH (FORCE)"))
                                .execute(&admin)
                                .await;
                    }
                    admin.close().await;
                }
            });
        })
        .join();
    }
}

impl Scratch {
    /// Two empty databases named `<prefix>_<random>_c` and `_d`.
    pub async fn new(base: &str, prefix: &str) -> Self {
        Self::with_names(base, prefix, "_c", "_d", &[]).await
    }

    /// Empty databases `<stem><catalog>` and `<stem><data>`, and one
    /// `<stem><suffix>` for each of `others`, all dropped with the scratch;
    /// `<stem>` is `<prefix>_<random>`.
    pub async fn with_names(
        base: &str,
        prefix: &str,
        catalog: &str,
        data: &str,
        others: &[&str],
    ) -> Self {
        let stem = format!(
            "{prefix}_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..10]
        );
        let (catalog_name, data_name) = (format!("{stem}{catalog}"), format!("{stem}{data}"));
        let mut names = vec![catalog_name.clone(), data_name.clone()];
        names.extend(others.iter().map(|suffix| format!("{stem}{suffix}")));
        let admin = pool(&format!("{base}/postgres"), 1).await;
        for n in &names {
            sqlx::query(&format!("CREATE DATABASE \"{n}\""))
                .execute(&admin)
                .await
                .expect("create a scratch database");
        }
        admin.close().await;
        Self {
            base: base.to_owned(),
            _guard: Guard {
                base: base.to_owned(),
                names,
            },
            catalog_name,
            data_name,
        }
    }

    pub fn catalog_url(&self) -> String {
        format!("{}/{}", self.base, self.catalog_name)
    }

    pub fn data_url(&self) -> String {
        format!("{}/{}", self.base, self.data_name)
    }

    pub fn options(&self) -> PgConnectOptions {
        self.catalog_url().parse().expect("a postgres URL")
    }

    /// The runner's target, which writes the data connection rows as `init`.
    pub fn target(&self) -> PostgresTarget {
        PostgresTarget::new(self.options(), &self.data_name)
            .with_data_connection_row(self.data_url())
    }

    pub async fn catalog(&self) -> PgPool {
        pool(&self.catalog_url(), 2).await
    }

    pub async fn data(&self) -> PgPool {
        pool(&self.data_url(), 2).await
    }

    /// Load a released fixture: `tests/fixtures/released/<state>/catalog.sql`
    /// and `data.sql`, each on a connection of its own.
    pub async fn load(&self, state: &str) {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/released")
            .join(state);
        for (file, url) in [
            ("catalog.sql", self.catalog_url()),
            ("data.sql", self.data_url()),
        ] {
            let sql = std::fs::read_to_string(dir.join(file))
                .unwrap_or_else(|e| panic!("{state}/{file}: {e}"));
            let p = pool(&url, 1).await;
            sqlx::raw_sql(&sql)
                .execute(&p)
                .await
                .unwrap_or_else(|e| panic!("load {state}/{file}: {e}"));
            p.close().await;
        }
        // The fixture names the data database it was dumped from.
        let catalog = self.catalog().await;
        sqlx::query("UPDATE settings SET value = $1 WHERE key = 'data_database_connection_string'")
            .bind(self.data_url())
            .execute(&catalog)
            .await
            .expect("point the catalog at the data database");
        sqlx::query("UPDATE settings SET value = $1 WHERE key = 'data_database_name'")
            .bind(&self.data_name)
            .execute(&catalog)
            .await
            .expect("name the data database");
        catalog.close().await;
    }

    /// Run the migrations up to `id` with no lock, as a test drives them.
    pub async fn migrate_through(&self, id: MigrationId) -> RunReport {
        let m = PostgresMigrations::open_unlocked(&self.target())
            .await
            .expect("open the runner");
        let report = Runner::new(&m, "test").migrate_through(id).await;
        m.close().await;
        report.unwrap_or_else(|e| panic!("migrate through {id}: {e}"))
    }

    /// `(id, database)` of every step a run from an empty or a legacy
    /// catalog applies, in the order the runner applies them: ids in order,
    /// and for each id the catalog first.
    pub fn all_steps(&self) -> Vec<(u32, String)> {
        MigrationId::ALL
            .into_iter()
            .flat_map(|id| {
                store_kinds(id).into_iter().map(move |kind| {
                    let database = match kind {
                        StoreKind::Catalog => self.catalog_name.clone(),
                        StoreKind::Data => self.data_name.clone(),
                        StoreKind::Account => panic!("PostgreSQL has no account store"),
                    };
                    (id.number(), database)
                })
            })
            .collect()
    }

    pub async fn status(&self) -> extenddb_storage::migrations::CatalogStatus {
        extenddb_storage_postgres::migrations::catalog_status(&self.target())
            .await
            .expect("read the status")
    }
}

fn seed_input(json: serde_json::Value) -> CreateTableInput {
    serde_json::from_value(json).expect("a CreateTable request")
}

/// The tables the released fixtures were seeded with, created through the
/// engine on an install this binary migrated: seed_tbl with a sort key, a GSI,
/// an LSI, a stream and TTL, seed_simple, and with `vector` vec_tbl with a
/// vector index. Per-table objects come from these requests; items and
/// backups are rows, not objects.
pub async fn seed_fresh(s: &Scratch, vector: bool) {
    let catalog = s.catalog().await;
    for (key, value) in [
        ("control_plane_delay_seconds", "0"),
        ("index_propagation_delay_ms", "0"),
    ] {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .bind(value)
        .execute(&catalog)
        .await
        .expect("set a delay");
    }
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, 'seed')")
        .bind(ACCOUNT)
        .execute(&catalog)
        .await
        .expect("the seed account");
    if vector {
        // `migrate` makes this attempt on every run, beside the ledger.
        let line = extenddb_storage_postgres::migrations::try_create_vector_extension(
            &s.data_url().parse().expect("a postgres URL"),
            None,
        )
        .await;
        assert!(line.starts_with("pgvector available"), "{line}");
    }
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: s.catalog_url(),
            pool_size: 4,
            max_item_size_bytes: 400_000,
        },
        "us-east-1",
    )
    .await
    .expect("open the engine");
    let mut requests = vec![
        serde_json::json!({
            "TableName": "seed_tbl",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
                {"AttributeName": "lsk", "AttributeType": "N"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            "GlobalSecondaryIndexes": [{"IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "ALL"}}],
            "LocalSecondaryIndexes": [{"IndexName": "lsi1",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "lsk", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "ALL"}}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
        }),
        serde_json::json!({
            "TableName": "seed_simple",
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "N"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    ];
    if vector {
        requests.push(serde_json::json!({
            "TableName": "vec_tbl",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [{"IndexName": "vidx", "Dimensions": 4, "DistanceFunction": "COSINE",
                "VectorAttribute": {"AttributeName": "emb"}, "Projection": {"ProjectionType": "ALL"}}]
        }));
    }
    for request in requests {
        engine
            .create_table(ACCOUNT, seed_input(request))
            .await
            .expect("CreateTable");
    }
    engine
        .update_ttl(ACCOUNT, "seed_tbl", "exp", true)
        .await
        .expect("UpdateTimeToLive");
    // The TTL worker builds the index in a server; here the engine does.
    engine
        .create_ttl_index(ACCOUNT, "seed_tbl", "exp")
        .await
        .expect("the TTL index");
    catalog.close().await;
}

/// `_ddb_<id>` names replaced with `T:<table>`, `I:<table>/<index>` or
/// `V:<table>/<index>` from the catalog's rows, longest id first.
pub async fn id_names(catalog: &PgPool) -> Vec<(String, String)> {
    let mut names: Vec<(String, String)> =
        sqlx::query_as("SELECT table_id, 'T:' || table_name FROM tables")
            .fetch_all(catalog)
            .await
            .expect("read tables");
    names.extend(
        sqlx::query_as::<_, (String, String)>(
            "SELECT i.index_id, 'I:' || t.table_name || '/' || i.index_name \
             FROM indexes i JOIN tables t ON t.table_id = i.table_id",
        )
        .fetch_all(catalog)
        .await
        .expect("read indexes"),
    );
    names.extend(
        sqlx::query_as::<_, (String, String)>(
            "SELECT v.index_id, 'V:' || t.table_name || '/' || v.index_name \
             FROM vector_indexes v JOIN tables t ON t.table_id = v.table_id",
        )
        .fetch_all(catalog)
        .await
        .unwrap_or_default(),
    );
    names
}

fn mapped(text: &str, names: &[(String, String)]) -> String {
    let mut out = text.to_owned();
    for (id, name) in names {
        out = out.replace(id.as_str(), name);
        // PostgreSQL truncates generated index names to 63 bytes, cutting ids.
        let short: String = id.replace('-', "_");
        out = out.replace(&short, name);
    }
    out
}

/// A database's per-table objects with ids mapped: columns, constraints, and
/// indexes keyed by table and definition with the index's own name removed,
/// because PostgreSQL generates some of them and truncates the id inside.
pub fn per_table(defs: &Definitions, names: &[(String, String)]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (key, column) in &defs.columns {
        if key.starts_with("_ddb_") {
            out.insert(format!("column {} {column:?}", mapped(key, names)));
        }
    }
    for (key, definition) in &defs.constraints {
        if key.starts_with("_ddb_") {
            let constraint = key.split_once('.').map_or("", |(_, c)| c);
            let table = key.split_once('.').map_or(key.as_str(), |(t, _)| t);
            let kind = if constraint.ends_with("_pkey") {
                "pkey"
            } else {
                constraint
            };
            out.insert(format!(
                "constraint {} {kind} {}",
                mapped(table, names),
                mapped(definition, names)
            ));
        }
    }
    for (name, index) in &defs.indexes {
        if index.table.starts_with("_ddb_") {
            let definition =
                index
                    .definition
                    .replacen(&format!("INDEX \"{name}\" "), "INDEX ? ", 1);
            let definition = definition.replacen(&format!("INDEX {name} "), "INDEX ? ", 1);
            out.insert(format!(
                "index {} {} valid={}",
                mapped(&index.table, names),
                mapped(&definition, names),
                index.valid
            ));
        }
    }
    out
}

/// The objects that are not per-table.
pub fn shared(defs: &Definitions) -> Definitions {
    Definitions {
        columns: defs
            .columns
            .iter()
            .filter(|(k, _)| !k.starts_with("_ddb_"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        indexes: defs
            .indexes
            .iter()
            .filter(|(_, i)| !i.table.starts_with("_ddb_"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        constraints: defs
            .constraints
            .iter()
            .filter(|(k, _)| !k.starts_with("_ddb_"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        sequences: defs.sequences.clone(),
    }
}

pub async fn settings(catalog: &PgPool) -> BTreeMap<String, String> {
    sqlx::query_as::<_, (String, String)>("SELECT key, value FROM settings")
        .fetch_all(catalog)
        .await
        .expect("read settings")
        .into_iter()
        .collect()
}

pub async fn describe_pool(p: &PgPool) -> Definitions {
    describe(p).await.expect("describe")
}

/// `(id, name, outcome)` of a store's ledger.
pub async fn ledger(p: &PgPool) -> Vec<(i32, String, String)> {
    sqlx::query_as("SELECT id, name, outcome FROM extenddb_migrations ORDER BY id")
        .fetch_all(p)
        .await
        .unwrap_or_default()
}

pub async fn legacy_rows(p: &PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT filename FROM schema_history ORDER BY filename")
        .fetch_all(p)
        .await
        .unwrap_or_default()
}

pub async fn count(p: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(p)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The statements an ownership note prints, as (who runs it, statement), in
/// the order the note numbers them: `(1) as <who>: <statement> (2) as ...`.
/// The last one ends with the note's line.
pub fn note_steps(note: &str) -> Vec<(String, String)> {
    let mut steps = Vec::new();
    let Some(start) = note.find("(1) as ") else {
        return steps;
    };
    let mut rest = &note[start..];
    let mut n = 1;
    while let Some(body) = rest.strip_prefix(&format!("({n}) as ")) {
        let (who, after) = body.split_once(": ").expect("who runs the statement");
        let next = format!(" ({}) as ", n + 1);
        let (statement, tail) = match after.find(&next) {
            Some(i) => (&after[..i], &after[i + 1..]),
            None => (after.lines().next().unwrap_or(after), ""),
        };
        steps.push((who.to_owned(), statement.trim().to_owned()));
        rest = tail;
        n += 1;
    }
    steps
}
