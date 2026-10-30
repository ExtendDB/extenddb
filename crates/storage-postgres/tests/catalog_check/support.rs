// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A deployment for the `catalog_check` tests, and the catalog and data
//! database operations they act with.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DistanceFunction, GsiInput,
    Item, KeySchemaElement, KeyType, LsiInput, Projection, ProjectionType, ScalarAttributeType,
    TableKeyInfo, VectorAttribute, VectorIndexSpecification,
};
use extenddb_storage::bootstrapper::{BootstrapConfig, Bootstrapper};
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{
    PostgresBootstrapper, PostgresCatalogStore, PostgresConfig, PostgresEngine, index_table_name,
    vector_table_name,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};

pub(crate) const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
pub(crate) const TABLE: &str = "orders";

pub(crate) fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

pub(crate) fn skip(test: &str) {
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
/// with a floor of ten connections on a server other tests share. It also
/// keeps the check's pause point to the one test that arms it.
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
pub(crate) struct Deployment {
    base: String,
    catalog_db: String,
    data_db: String,
    pub(crate) catalog: PgPool,
    pub(crate) data: PgPool,
    pub(crate) engine: PostgresEngine,
    pub(crate) diagnostics: PostgresCatalogStore,
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
    pub(crate) async fn new() -> Self {
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

    pub(crate) fn catalog_url(&self) -> String {
        format!("{}/{}", self.base, self.catalog_db)
    }

    fn data_url(&self) -> String {
        format!("{}/{}", self.base, self.data_db)
    }

    /// A second diagnostics store whose catalog sessions start every
    /// transaction at `isolation` unless it sets its own, as a database or
    /// role default would make them.
    pub(crate) async fn diagnostics_at(&self, isolation: &str) -> PostgresCatalogStore {
        // The server splits the startup options at spaces; a backslash keeps
        // one inside the value.
        let value = isolation.replace(' ', "\\ ");
        let options = self
            .catalog_url()
            .parse::<PgConnectOptions>()
            .expect("the catalog URL")
            .options([("default_transaction_isolation", value)]);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .expect("a pool at the isolation level");
        let level: String = sqlx::query_scalar("SHOW default_transaction_isolation")
            .fetch_one(&pool)
            .await
            .expect("read the default isolation");
        assert_eq!(level, isolation, "the sessions start at {isolation}");
        PostgresCatalogStore::new(pool)
    }

    /// Whether the data database can hold vector data.
    pub(crate) async fn pgvector(&self) -> bool {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'vector')")
            .fetch_one(&self.data)
            .await
            .expect("list the installed extensions")
    }

    /// Create `name` with GSI `gsi1`, LSI `lsi1` and, when pgvector is
    /// installed, vector index `vec1`; write three items.
    pub(crate) async fn create_table_with_indexes(&self, name: &str) -> TableKeyInfo {
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
    pub(crate) async fn storage_relations(&self) -> BTreeSet<String> {
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
    pub(crate) async fn plant_orphan(&self) -> String {
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

    /// Drop `relation` from the data database, as a server's drop would.
    pub(crate) async fn remove_relation(&self, relation: &str) {
        sqlx::query(&format!("DROP TABLE \"{relation}\""))
            .execute(&self.data)
            .await
            .expect("drop a data relation");
    }

    pub(crate) async fn relation_exists(&self, name: &str) -> bool {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = $1)")
            .bind(name)
            .fetch_one(&self.data)
            .await
            .expect("look up a relation")
    }

    /// The unquoted name of `table`'s secondary index `index` relation, from
    /// its catalog id.
    pub(crate) async fn index_relation(&self, table: &str, index: &str) -> String {
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
    pub(crate) async fn vector_relation(&self, table: &str, index: &str) -> String {
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

    pub(crate) async fn set_status(&self, sql: &str, status: &str) {
        sqlx::query(sql)
            .bind(status)
            .execute(&self.catalog)
            .await
            .expect("set a status");
    }

    /// GetItem on every item written by `create_table_with_indexes`, and the
    /// row count of each index relation.
    pub(crate) async fn assert_data_intact(&self, key_info: &TableKeyInfo) {
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

    /// Wait until a session waits for a `mode` lock on `relation` in the
    /// database `pool` connects to (`pg_locks` shows the request ungranted).
    pub(crate) async fn wait_for_lock_request(pool: &PgPool, relation: &str, mode: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_class c ON c.oid = l.relation \
                 WHERE l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
                   AND c.relname = $1 AND l.mode = $2 AND NOT l.granted)",
            )
            .bind(relation)
            .bind(mode)
            .fetch_one(pool)
            .await
            .expect("read pg_locks");
            if waiting {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "no session waited for {mode} on {relation} within 10 s"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Start adding GSI `index` to `TABLE` the way `UpdateTable` does it:
    /// lock the table row, insert the index row, and create and commit the
    /// index's relation while the catalog transaction stays open.
    pub(crate) async fn begin_index_add(&self, index: &str) -> IndexAdd {
        let mut conn = PgConnection::connect(&self.catalog_url())
            .await
            .expect("a catalog session for the index add");
        sqlx::query("BEGIN")
            .execute(&mut conn)
            .await
            .expect("begin");
        let (table_id,): (String,) = sqlx::query_as(
            "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2 FOR UPDATE",
        )
        .bind(ACCOUNT)
        .bind(TABLE)
        .fetch_one(&mut conn)
        .await
        .expect("lock the table row");
        let index_id = uuid::Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO indexes (table_id, index_name, index_id, index_type, key_schema, \
             projection, index_status) VALUES ($1, $2, $3, 'GSI', \
             '[{\"AttributeName\": \"gpk\", \"KeyType\": \"HASH\"}]', \
             '{\"ProjectionType\": \"ALL\"}', 'CREATING')",
        )
        .bind(&table_id)
        .bind(index)
        .bind(&index_id)
        .execute(&mut conn)
        .await
        .expect("insert the index row");
        let relation = index_table_name(&index_id).trim_matches('"').to_owned();
        let mut data = PgConnection::connect(&self.data_url())
            .await
            .expect("a data session for the index add");
        sqlx::query(&format!(
            "CREATE TABLE \"{relation}\" (pk TEXT PRIMARY KEY)"
        ))
        .execute(&mut data)
        .await
        .expect("create and commit the index relation");
        data.close().await.expect("close the data session");
        IndexAdd {
            conn,
            table_id,
            relation,
        }
    }
}

/// A GSI add in flight, from [`Deployment::begin_index_add`].
pub(crate) struct IndexAdd {
    conn: PgConnection,
    table_id: String,
    pub(crate) relation: String,
}

impl IndexAdd {
    /// The add's next step: write the table row, which needs `ROW EXCLUSIVE`
    /// on `tables` (an `UpdateTable` whose new index keys on a new attribute
    /// updates `attribute_definitions` here). Returns how long it took.
    pub(crate) async fn write_table_row(&mut self) -> Result<Duration, sqlx::Error> {
        let started = Instant::now();
        sqlx::query(
            "UPDATE tables SET attribute_definitions = attribute_definitions \
             WHERE table_id = $1",
        )
        .bind(&self.table_id)
        .execute(&mut self.conn)
        .await?;
        Ok(started.elapsed())
    }

    pub(crate) async fn commit(mut self) {
        sqlx::query("COMMIT")
            .execute(&mut self.conn)
            .await
            .expect("commit the index add");
        self.conn.close().await.expect("close the catalog session");
    }
}

/// A session of the data database holding `ACCESS SHARE` on `relation`, as
/// a reader such as `pg_dump` holds it, until it is dropped.
pub(crate) async fn hold_relation(d: &Deployment, relation: &str) -> PgConnection {
    let mut conn = PgConnection::connect(&d.data_url())
        .await
        .expect("a data session for the holder");
    sqlx::query("BEGIN")
        .execute(&mut conn)
        .await
        .expect("begin");
    sqlx::query(&format!("LOCK TABLE \"{relation}\" IN ACCESS SHARE MODE"))
        .execute(&mut conn)
        .await
        .expect("hold the relation");
    conn
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
