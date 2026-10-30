// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb catalog-check` against the data relations the engine creates.
//!
//! The check compares the relations in the data database with the names the
//! catalog's ids give through the backend's own naming functions. These tests
//! build a deployment the way `init` leaves it (catalog and data in separate
//! databases), create a table with a GSI, an LSI and a vector index through
//! the engine, and run the check and the drop through the backend's
//! `DiagnosticsStore`, which is what the command calls.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL
//! `postgres://user:pass@host:port`, no database); without it every test
//! reports a skip and passes. Each test creates its own databases and drops
//! them when its deployment is dropped, on a panic too.

use std::collections::{BTreeMap, BTreeSet};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DistanceFunction, GsiInput,
    Item, KeySchemaElement, KeyType, LsiInput, Projection, ProjectionType, ScalarAttributeType,
    TableKeyInfo, VectorAttribute, VectorIndexSpecification,
};
use extenddb_storage::bootstrapper::{BootstrapConfig, Bootstrapper};
use extenddb_storage::diagnostics::{CatalogRelation, DiagnosticsStore, OrphanDrop};
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{
    PostgresBootstrapper, PostgresCatalogStore, PostgresConfig, PostgresEngine, index_table_name,
    vector_table_name,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "orders";

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

fn skip(test: &str) {
    eprintln!("SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set.");
}

struct Parts {
    host: String,
    port: u16,
    user: String,
    pass: String,
}

fn parts(base: &str) -> Parts {
    let rest = base
        .strip_prefix("postgres://")
        .or_else(|| base.strip_prefix("postgresql://"))
        .expect("a postgres:// URL");
    let (creds, addr) = rest.split_once('@').expect("user:pass@host:port");
    let (user, pass) = creds.split_once(':').expect("user:pass");
    let (host, port) = addr.split_once(':').unwrap_or((addr, "5432"));
    Parts {
        host: host.to_owned(),
        port: port.parse().expect("port"),
        user: user.to_owned(),
        pass: pass.to_owned(),
    }
}

/// One deployment at a time across the binary: each holds an engine pool
/// with a floor of ten connections on a server other tests share.
async fn deployment_permit() -> tokio::sync::OwnedSemaphorePermit {
    static DEPLOYMENTS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    std::sync::Arc::clone(
        DEPLOYMENTS.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(1))),
    )
    .acquire_owned()
    .await
    .expect("the deployment semaphore is never closed")
}

/// A deployment initialised the way `init` does it: a catalog database and a
/// separate data database, with the data connection recorded in the catalog.
struct Deployment {
    base: String,
    catalog_db: String,
    data_db: String,
    catalog: PgPool,
    data: PgPool,
    engine: PostgresEngine,
    diagnostics: PostgresCatalogStore,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for Deployment {
    fn drop(&mut self) {
        // Runs on a panic too, so it cannot await on the test's runtime: a
        // thread of its own with its own runtime and connection.
        let base = self.base.clone();
        let names = [self.catalog_db.clone(), self.data_db.clone()];
        let dropped = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime");
            rt.block_on(async {
                let Ok(admin) = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&format!("{base}/postgres"))
                    .await
                else {
                    return;
                };
                for n in names {
                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{n}\" WITH (FORCE)"))
                        .execute(&admin)
                        .await;
                }
                admin.close().await;
            });
        })
        .join();
        if dropped.is_err() {
            eprintln!("cleanup of the test databases failed");
        }
    }
}

impl Deployment {
    async fn new() -> Self {
        let base = base_conn().expect("caller checks base_conn() first");
        let permit = deployment_permit().await;
        let stem = format!("eddb_cck_{}", uuid::Uuid::new_v4().simple())[..20].to_owned();
        let catalog_db = format!("{stem}_catalog");
        let data_db = format!("{stem}_data");
        let p = parts(&base);
        let init = PostgresBootstrapper::connect(BootstrapConfig {
            host: p.host.clone(),
            port: p.port,
            admin_user: p.user.clone(),
            admin_password: Some(p.pass.clone()),
            app_user: p.user.clone(),
            app_password: p.pass.clone(),
            catalog_db: catalog_db.clone(),
            data_db: data_db.clone(),
        })
        .await
        .expect("connect");
        init.create_catalog_db().await.expect("catalog db");
        init.create_data_db().await.expect("data db");
        init.run_catalog_migrations()
            .await
            .expect("catalog migrations");
        init.record_data_connection().await.expect("record");
        init.run_data_migrations().await.expect("data migrations");

        let catalog_url = format!("{base}/{catalog_db}");
        let catalog = PgPoolOptions::new()
            .max_connections(2)
            .connect(&catalog_url)
            .await
            .expect("catalog");
        let data = PgPoolOptions::new()
            .max_connections(2)
            .connect(&format!("{base}/{data_db}"))
            .await
            .expect("data");
        // No background workers run here, so transitions and index writes
        // must complete inline.
        for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
            sqlx::query("UPDATE settings SET value = '0' WHERE key = $1")
                .bind(key)
                .execute(&catalog)
                .await
                .expect("pin a delay to zero");
        }
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
            .bind(ACCOUNT)
            .bind(format!("acct-{stem}"))
            .execute(&catalog)
            .await
            .expect("seed the account row");
        let engine = PostgresEngine::new(
            &PostgresConfig {
                connection_string: catalog_url.clone(),
                pool_size: 10,
                max_item_size_bytes: 400_000,
            },
            REGION,
        )
        .await
        .expect("open a PostgresEngine");
        let diagnostics = PostgresCatalogStore::new(
            PgPoolOptions::new()
                .max_connections(2)
                .connect(&catalog_url)
                .await
                .expect("diagnostics pool"),
        );
        Self {
            base,
            catalog_db,
            data_db,
            catalog,
            data,
            engine,
            diagnostics,
            _permit: permit,
        }
    }

    /// Whether the data database can hold vector data.
    async fn pgvector(&self) -> bool {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'vector')")
            .fetch_one(&self.data)
            .await
            .expect("list the installed extensions")
    }

    /// Create `name` with GSI `gsi1`, LSI `lsi1` and, when pgvector is
    /// installed, vector index `vec1`; write three items.
    async fn create_table_with_indexes(&self, name: &str) -> TableKeyInfo {
        let string = |attr: &str| AttributeDefinition {
            attribute_name: attr.to_owned(),
            attribute_type: ScalarAttributeType::S,
        };
        let key = |attr: &str, key_type: KeyType| KeySchemaElement {
            attribute_name: attr.to_owned(),
            key_type,
        };
        let all = || Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        };
        let vector_indexes = self.pgvector().await.then(|| {
            vec![VectorIndexSpecification {
                index_name: "vec1".to_owned(),
                dimensions: 4,
                distance_function: DistanceFunction::Cosine,
                vector_attribute: VectorAttribute {
                    attribute_name: "emb".to_owned(),
                },
                search_schema: None,
                projection: Some(all()),
            }]
        });
        self.engine
            .create_table(
                ACCOUNT,
                CreateTableInput {
                    table_name: name.to_owned(),
                    key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
                    attribute_definitions: vec![
                        string("pk"),
                        string("sk"),
                        string("gpk"),
                        string("lsk"),
                    ],
                    billing_mode: Some(BillingMode::PayPerRequest),
                    global_secondary_indexes: Some(vec![GsiInput {
                        index_name: "gsi1".to_owned(),
                        key_schema: vec![key("gpk", KeyType::Hash)],
                        projection: all(),
                        provisioned_throughput: None,
                    }]),
                    local_secondary_indexes: Some(vec![LsiInput {
                        index_name: "lsi1".to_owned(),
                        key_schema: vec![key("pk", KeyType::Hash), key("lsk", KeyType::Range)],
                        projection: all(),
                    }]),
                    vector_indexes,
                    ..Default::default()
                },
            )
            .await
            .expect("create the table");
        let key_info = self
            .engine
            .table_key_info(ACCOUNT, name)
            .await
            .expect("read the key info");
        for n in 0..3 {
            self.engine
                .put_item(
                    &key_info,
                    item(n),
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await
                .expect("put an item");
        }
        key_info
    }

    /// The relations in the data database that carry the backend's prefix.
    async fn storage_relations(&self) -> BTreeSet<String> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT tablename::text FROM pg_tables \
             WHERE schemaname = current_schema() AND tablename LIKE '\\_ddb\\_%'",
        )
        .fetch_all(&self.data)
        .await
        .expect("list the data relations");
        rows.into_iter().map(|(n,)| n).collect()
    }

    /// Create a relation that no catalog row names, with the prefix the
    /// engine uses, and return its name.
    async fn plant_orphan(&self) -> String {
        let name = format!("_ddb_{}", uuid::Uuid::new_v4());
        sqlx::query(&format!(
            "CREATE TABLE \"{name}\" (pk TEXT PRIMARY KEY, item_data JSONB)"
        ))
        .execute(&self.data)
        .await
        .expect("plant the orphan");
        sqlx::query(&format!("INSERT INTO \"{name}\" VALUES ('x', '{{}}')"))
            .execute(&self.data)
            .await
            .expect("fill the orphan");
        name
    }

    async fn relation_exists(&self, name: &str) -> bool {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = $1)")
            .bind(name)
            .fetch_one(&self.data)
            .await
            .expect("look up a relation")
    }

    /// The unquoted name of `table`'s secondary index `index` relation, from
    /// its catalog id.
    async fn index_relation(&self, table: &str, index: &str) -> String {
        let (index_id,): (String,) = sqlx::query_as(
            "SELECT i.index_id FROM indexes i JOIN tables t ON t.table_id = i.table_id \
             WHERE t.account_id = $1 AND t.table_name = $2 AND i.index_name = $3",
        )
        .bind(ACCOUNT)
        .bind(table)
        .bind(index)
        .fetch_one(&self.catalog)
        .await
        .expect("read the index id");
        index_table_name(&index_id).trim_matches('"').to_owned()
    }

    /// The unquoted name of `table`'s vector index `index` relation.
    async fn vector_relation(&self, table: &str, index: &str) -> String {
        let (index_id,): (String,) = sqlx::query_as(
            "SELECT v.index_id FROM vector_indexes v JOIN tables t ON t.table_id = v.table_id \
             WHERE t.account_id = $1 AND t.table_name = $2 AND v.index_name = $3",
        )
        .bind(ACCOUNT)
        .bind(table)
        .bind(index)
        .fetch_one(&self.catalog)
        .await
        .expect("read the vector index id");
        vector_table_name(&index_id).trim_matches('"').to_owned()
    }

    async fn row_count(&self, relation: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{relation}\""))
            .fetch_one(&self.data)
            .await
            .expect("count the rows")
    }

    async fn set_status(&self, sql: &str, status: &str) {
        sqlx::query(sql)
            .bind(status)
            .execute(&self.catalog)
            .await
            .expect("set a status");
    }

    /// GetItem on every item written by `create_table_with_indexes`, and the
    /// row count of each index relation.
    async fn assert_data_intact(&self, key_info: &TableKeyInfo) {
        for n in 0..3 {
            let got = self
                .engine
                .get_item(key_info, &key(n))
                .await
                .expect("get an item");
            assert_eq!(got, Some(item(n)), "item {n} after the fix");
        }
        for index in ["gsi1", "lsi1"] {
            let relation = self.index_relation(TABLE, index).await;
            assert_eq!(self.row_count(&relation).await, 3, "{index} rows");
        }
        if self.pgvector().await {
            let relation = self.vector_relation(TABLE, "vec1").await;
            assert_eq!(self.row_count(&relation).await, 3, "vec1 rows");
        }
    }
}

fn key(n: u32) -> Item {
    BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(format!("p{n}"))),
        ("sk".to_owned(), AttributeValue::S("s".to_owned())),
    ])
}

fn item(n: u32) -> Item {
    let mut it = key(n);
    it.insert("gpk".to_owned(), AttributeValue::S("g".to_owned()));
    it.insert("lsk".to_owned(), AttributeValue::S(format!("l{n}")));
    it.insert(
        "emb".to_owned(),
        AttributeValue::L(
            ["0.1", "0.2", "0.3", &format!("{n}.4")]
                .iter()
                .map(|v| AttributeValue::N((*v).to_owned()))
                .collect(),
        ),
    );
    it
}

/// A table with a GSI, an LSI and a vector index has every relation the
/// engine created named by the catalog: nothing orphaned, nothing missing.
#[tokio::test]
async fn live_relations_are_neither_orphaned_nor_missing() {
    let test = "live_relations_are_neither_orphaned_nor_missing";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let relations = d.storage_relations().await;
    let expected = if d.pgvector().await { 4 } else { 3 };
    assert_eq!(relations.len(), expected, "{relations:?}");

    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check");
    assert!(check.orphaned.is_empty(), "orphaned: {:?}", check.orphaned);
    assert!(check.missing.is_empty(), "missing: {:?}", check.missing);
}

/// `--fix` drops the relation no catalog row names and nothing else: not the
/// live table's relations, not a table created after the check, not a table
/// without the backend's prefix. The live data is intact afterwards.
#[tokio::test]
async fn fix_drops_only_the_relation_no_catalog_row_names() {
    let test = "fix_drops_only_the_relation_no_catalog_row_names";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    let key_info = d.create_table_with_indexes(TABLE).await;
    let live = d.storage_relations().await;
    let orphan = d.plant_orphan().await;
    sqlx::query("CREATE TABLE operator_notes (note TEXT)")
        .execute(&d.data)
        .await
        .expect("an unrelated table");

    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check");
    assert_eq!(check.orphaned, vec![orphan.clone()]);
    assert!(check.missing.is_empty(), "missing: {:?}", check.missing);

    // A CreateTable that lands between the check and the drop: its relations
    // were not in the listing, and a stale candidate list naming them must
    // not get them dropped.
    d.create_table_with_indexes("later").await;
    let later: Vec<String> = d
        .storage_relations()
        .await
        .difference(&live)
        .filter(|r| **r != orphan)
        .cloned()
        .collect();
    assert_eq!(later.len(), if d.pgvector().await { 4 } else { 3 });
    let mut candidates = vec![orphan.clone(), "operator_notes".to_owned()];
    candidates.extend(later.iter().cloned());

    let outcomes = d
        .diagnostics
        .drop_orphaned_relations(candidates.clone())
        .await
        .expect("drop");
    let names: Vec<&String> = outcomes.iter().map(|(n, _)| n).collect();
    assert_eq!(names, candidates.iter().collect::<Vec<_>>());
    assert_eq!(outcomes[0].1, OrphanDrop::Dropped, "{outcomes:?}");
    assert_eq!(
        outcomes[1].1,
        OrphanDrop::Kept("not a data relation of this backend".to_owned()),
        "{outcomes:?}"
    );
    for (name, outcome) in &outcomes[2..] {
        match outcome {
            OrphanDrop::Kept(reason) => assert!(
                reason.starts_with("the catalog names it: ")
                    && reason.ends_with(&format!("table {ACCOUNT}/later")),
                "{name}: {reason}"
            ),
            other => panic!("{name}: {other:?}"),
        }
    }

    assert!(!d.relation_exists(&orphan).await, "the orphan was dropped");
    assert!(d.relation_exists("operator_notes").await);
    let after = d.storage_relations().await;
    assert!(live.is_subset(&after), "live relations survived: {after:?}");
    assert!(later.iter().all(|r| after.contains(r)), "{after:?}");
    d.assert_data_intact(&key_info).await;

    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check again");
    assert!(check.orphaned.is_empty(), "orphaned: {:?}", check.orphaned);
    assert!(check.missing.is_empty(), "missing: {:?}", check.missing);
}

/// A relation the catalog names and the data database lacks is reported
/// missing, with the index and table that own it; the relation that took
/// its place is reported orphaned.
#[tokio::test]
async fn a_relation_the_catalog_names_is_reported_missing() {
    let test = "a_relation_the_catalog_names_is_reported_missing";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let gsi = d.index_relation(TABLE, "gsi1").await;
    assert!(d.relation_exists(&gsi).await, "{gsi}");
    let elsewhere = format!("_ddb_{}", uuid::Uuid::new_v4());
    sqlx::query(&format!("ALTER TABLE \"{gsi}\" RENAME TO \"{elsewhere}\""))
        .execute(&d.data)
        .await
        .expect("rename the GSI relation");

    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check");
    assert_eq!(check.orphaned, vec![elsewhere.clone()]);
    assert_eq!(
        check.missing,
        vec![CatalogRelation {
            relation: gsi.clone(),
            owner: format!("index gsi1 of table {ACCOUNT}/{TABLE}"),
        }]
    );

    sqlx::query(&format!("ALTER TABLE \"{elsewhere}\" RENAME TO \"{gsi}\""))
        .execute(&d.data)
        .await
        .expect("rename it back");
    let check = d
        .diagnostics
        .check_storage_relations()
        .await
        .expect("check again");
    assert!(check.orphaned.is_empty(), "orphaned: {:?}", check.orphaned);
    assert!(check.missing.is_empty(), "missing: {:?}", check.missing);
}

/// Rows in every status name their relations: a table in CREATING, UPDATING
/// or DELETING and an index or vector index in CREATING or DELETING keep
/// their relations out of the orphan list. A DELETING row's relation is not
/// reported missing either, since DeleteTable removes it.
#[tokio::test]
async fn rows_in_every_status_count_as_live() {
    let test = "rows_in_every_status_count_as_live";
    if base_conn().is_none() {
        return skip(test);
    }
    let d = Deployment::new().await;
    d.create_table_with_indexes(TABLE).await;
    let table_status = "UPDATE tables SET table_status = $1";
    let index_status = "UPDATE indexes SET index_status = $1";
    let vector_status = "UPDATE vector_indexes SET index_status = $1";

    for status in ["CREATING", "UPDATING", "DELETING"] {
        d.set_status(table_status, status).await;
        let check = d
            .diagnostics
            .check_storage_relations()
            .await
            .expect("check");
        assert!(check.orphaned.is_empty(), "{status}: {:?}", check.orphaned);
        assert!(check.missing.is_empty(), "{status}: {:?}", check.missing);
    }
    d.set_status(table_status, "ACTIVE").await;

    for status in ["CREATING", "DELETING"] {
        d.set_status(index_status, status).await;
        d.set_status(vector_status, status).await;
        let check = d
            .diagnostics
            .check_storage_relations()
            .await
            .expect("check");
        assert!(check.orphaned.is_empty(), "{status}: {:?}", check.orphaned);
        assert!(check.missing.is_empty(), "{status}: {:?}", check.missing);
    }
}
