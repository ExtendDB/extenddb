// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage-level tests for the PostgreSQL vector index control plane.
//!
//! These run below the wire on purpose. This backend declares no vector search
//! capability, so the engine refuses every vector request before storage is
//! reached, which makes the wire the wrong place to test what the catalog does
//! with a vector index. The control-plane code exists now because the catalog
//! state that the search and build paths will read is created here, so it is
//! tested here, against a real PostgreSQL.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_vec_ctl_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.

use std::collections::{BTreeMap, HashMap};

use extenddb_core::expression::{self, ExpressionMaps};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DeleteTableInput,
    DeleteVectorIndexAction, DescribeTableInput, DistanceFunction, IndexStatus, Item,
    KeySchemaElement, KeyType, Projection, ProjectionType, ProvisionedThroughput,
    ScalarAttributeType, SearchSchemaElement, SearchSchemaElementType, UpdateTableInput,
    VectorAttribute, VectorIndexSpecification, VectorIndexUpdate,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";

/// Whether pgvector should be installed in the scratch database.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Pgvector {
    Install,
    Omit,
}

/// A throwaway database with the shipped schema applied and an engine on it.
struct Scratch {
    engine: PostgresEngine,
    catalog: PgPool,
    admin: PgPool,
    db_name: String,
    /// Databases created alongside the catalog, dropped by `cleanup` too.
    ///
    /// Cleanup owns every database it created, because a caller that drops one
    /// afterwards has no working admin pool to do it with: `PgPool` is an `Arc`
    /// around shared state, so closing one handle closes every clone and the
    /// `DROP` then fails with `PoolClosed`. An ignored error there leaks a
    /// database on every green run, which is invisible in CI and accumulates on a
    /// development server.
    extra_databases: Vec<String>,
    /// Held for the environment's lifetime, released on drop even when a test
    /// panics, so the next test does not start until this one's connections are
    /// gone. See [`environment_permit`].
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// One scratch environment at a time, across the whole test binary.
///
/// Each environment holds an engine pool with a floor of ten connections, and
/// these tests share a server with whatever else is running against it (in CI, an
/// ExtendDB instance started by an earlier step). Running twelve at once exhausts
/// `max_connections` and every test then fails on a pool timeout, which reports a
/// resource limit as a product bug. Serialising costs a few seconds.
async fn environment_permit() -> tokio::sync::OwnedSemaphorePermit {
    static ENVIRONMENTS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    std::sync::Arc::clone(
        ENVIRONMENTS.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(1))),
    )
    .acquire_owned()
    .await
    .expect("the environment semaphore is never closed")
}

impl Scratch {
    /// Drop the scratch database. Called only on the success path, so a failure
    /// leaves the database for inspection.
    async fn cleanup(self) {
        let Scratch {
            engine,
            catalog,
            admin,
            db_name,
            extra_databases,
            _permit,
        } = self;
        drop(engine);
        catalog.close().await;
        for name in extra_databases.iter().chain(std::iter::once(&db_name)) {
            sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
                .execute(&admin)
                .await
                .expect("drop a scratch database");
        }
        admin.close().await;
    }
}

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// Report the reason a test did nothing, loudly enough to notice in a log.
fn skip(test: &str) {
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch catalog in."
    );
}

/// Build a scratch database, apply the shipped migrations, and open an engine.
///
/// The migrations are the files the binary ships, included from the crate rather
/// than restated here: a test that built its own idea of the schema could pass
/// against a shape no deployment has.
async fn scratch(pgvector: Pgvector) -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let permit = environment_permit().await;
    let db_name = format!("eddb_vec_ctl_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();

    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin)
        .await
        .expect("create the scratch database");

    let url = format!("{base}/{db_name}");
    let catalog = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to the scratch database");

    if pgvector == Pgvector::Install {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
            .execute(&catalog)
            .await
            .expect("create the pgvector extension");
    }

    for sql in [
        include_str!("../migrations/001_schema.sql"),
        include_str!("../migrations/002_vector_indexes.sql"),
        include_str!("../data_migrations/001_data_schema.sql"),
        include_str!("../data_migrations/002_gsi_pending.sql"),
        include_str!("../data_migrations/003_idempotency_account_scope.sql"),
    ] {
        sqlx::raw_sql(sql)
            .execute(&catalog)
            .await
            .expect("apply a shipped migration");
    }

    // Zero control-plane delay so CreateTable and DeleteTable complete inline:
    // these tests run no background workers, so a scheduled transition would
    // never happen and every table would sit in CREATING.
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
        .execute(&catalog)
        .await
        .expect("pin the control-plane delay to zero");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{db_name}"))
        .execute(&catalog)
        .await
        .expect("seed the account row");

    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: url,
            // The engine clamps anything below its floor of ten, so ask for the
            // floor: a smaller number would only produce a warning and the same
            // pool.
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch database");

    Scratch {
        engine,
        catalog,
        admin,
        db_name,
        extra_databases: Vec::new(),
        _permit: permit,
    }
}

/// Build a scratch database with pgvector installed, or report why not.
///
/// The extension is a server package, so a PostgreSQL that does not ship it
/// cannot run the tests that need the `vector` type to exist. Those tests say so
/// and pass, rather than failing on an environment property; the control-plane
/// tests deliberately do not need it, so they always run.
async fn scratch_with_pgvector(test: &str) -> Option<Scratch> {
    let base = base_conn()?;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    let available: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = 'vector')",
    )
    .fetch_one(&admin)
    .await
    .expect("list the available extensions");
    admin.close().await;
    if !available {
        eprintln!(
            "SKIP {test}: this PostgreSQL server does not offer the pgvector extension, so the \
             capable path cannot be exercised here."
        );
        return None;
    }
    Some(scratch(Pgvector::Install).await)
}

fn hash_key(name: &str) -> Vec<KeySchemaElement> {
    vec![KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type: KeyType::Hash,
    }]
}

fn string_attr(name: &str) -> Vec<AttributeDefinition> {
    vec![AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    }]
}

fn projection(projection_type: ProjectionType) -> Projection {
    Projection {
        projection_type,
        non_key_attributes: None,
    }
}

/// A vector index specification: unscoped when `hash` is `None`.
fn vector_spec(name: &str, dimensions: u32, hash: Option<&str>) -> VectorIndexSpecification {
    VectorIndexSpecification {
        index_name: name.to_owned(),
        dimensions,
        distance_function: DistanceFunction::Cosine,
        vector_attribute: VectorAttribute {
            attribute_name: "emb".to_owned(),
        },
        search_schema: hash.map(|attr| {
            vec![SearchSchemaElement {
                attribute_name: attr.to_owned(),
                element_type: SearchSchemaElementType::Hash,
            }]
        }),
        projection: Some(projection(ProjectionType::All)),
    }
}

fn create_input(table: &str, vector_indexes: Vec<VectorIndexSpecification>) -> CreateTableInput {
    CreateTableInput {
        table_name: table.to_owned(),
        key_schema: hash_key("pk"),
        attribute_definitions: string_attr("pk"),
        billing_mode: Some(BillingMode::PayPerRequest),
        vector_indexes: (!vector_indexes.is_empty()).then_some(vector_indexes),
        ..Default::default()
    }
}

/// An `UpdateTableInput` that changes nothing, as a base for one that changes one
/// thing. The type has no `Default`, deliberately: every field is a distinct
/// control-plane change and defaulting them silently would be a bug magnet.
fn update_input(table: &str) -> UpdateTableInput {
    UpdateTableInput {
        table_name: table.to_owned(),
        billing_mode: None,
        provisioned_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: None,
        attribute_definitions: None,
        stream_specification: None,
        table_class: None,
        on_demand_throughput: None,
        vector_index_updates: None,
    }
}

fn delete_vector(index_name: &str) -> Option<Vec<VectorIndexUpdate>> {
    Some(vec![VectorIndexUpdate {
        create: None,
        delete: Some(DeleteVectorIndexAction {
            index_name: index_name.to_owned(),
        }),
    }])
}

async fn table_id(catalog: &PgPool, table: &str) -> String {
    sqlx::query_scalar("SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2")
        .bind(ACCOUNT)
        .bind(table)
        .fetch_one(catalog)
        .await
        .expect("read the table id")
}

async fn vector_row_count(catalog: &PgPool, table_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM vector_indexes WHERE table_id = $1")
        .bind(table_id)
        .fetch_one(catalog)
        .await
        .expect("count the vector index rows")
}

/// Put a vector index into a mid-build state that only the build path can reach,
/// so the phase-dependent delete rule can be tested before that path exists.
async fn set_index_phase(catalog: &PgPool, table_id: &str, index_name: &str, backfilling: bool) {
    sqlx::query(
        "UPDATE vector_indexes SET index_status = 'CREATING', backfilling = $3 \
         WHERE table_id = $1 AND index_name = $2",
    )
    .bind(table_id)
    .bind(index_name)
    .bind(backfilling)
    .execute(catalog)
    .await
    .expect("move the index into a CREATING phase");
}

#[tokio::test]
async fn create_table_records_a_vector_index_as_active_and_echoes_it() {
    if base_conn().is_none() {
        return skip("create_table_records_a_vector_index_as_active_and_echoes_it");
    }
    let s = scratch(Pgvector::Omit).await;

    let desc = s
        .engine
        .create_table(
            ACCOUNT,
            create_input("t_create", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a table with a vector index");

    // The response is what a client sees, so it carries the index rather than
    // requiring a follow-up describe.
    let echoed = desc
        .vector_indexes
        .as_ref()
        .expect("CreateTable must echo the vector index it created");
    assert_eq!(echoed.len(), 1);
    assert_eq!(echoed[0].index_name, "vidx");
    assert_eq!(echoed[0].dimensions, 4);
    assert_eq!(echoed[0].index_status, IndexStatus::Active);
    // A table created with the index is empty, so there is nothing to backfill
    // and the member is absent, which is the state the service reports.
    assert_eq!(echoed[0].backfilling, None);
    assert!(
        echoed[0].index_arn.ends_with("/index/vidx"),
        "{}",
        echoed[0].index_arn
    );

    // The catalog row must agree, including the ACTIVE-implies-no-backfilling
    // invariant the schema also enforces.
    let id = table_id(&s.catalog, "t_create").await;
    let row: (String, Option<bool>, i32, String, String) = sqlx::query_as(
        "SELECT index_status, backfilling, dimensions, distance_function, index_id \
         FROM vector_indexes WHERE table_id = $1 AND index_name = 'vidx'",
    )
    .bind(&id)
    .fetch_one(&s.catalog)
    .await
    .expect("read back the vector index row");
    assert_eq!(row.0, "ACTIVE");
    assert_eq!(row.1, None);
    assert_eq!(row.2, 4);
    assert_eq!(row.3, "COSINE");
    assert!(!row.4.is_empty(), "the index id must be assigned");

    s.cleanup().await;
}

#[tokio::test]
async fn describe_table_reports_scoped_and_unscoped_vector_indexes() {
    if base_conn().is_none() {
        return skip("describe_table_reports_scoped_and_unscoped_vector_indexes");
    }
    let s = scratch(Pgvector::Omit).await;

    let mut input = create_input(
        "t_describe",
        vec![
            vector_spec("scoped", 8, Some("pk")),
            VectorIndexSpecification {
                projection: Some(projection(ProjectionType::KeysOnly)),
                ..vector_spec("unscoped", 16, None)
            },
        ],
    );
    input.attribute_definitions = string_attr("pk");
    s.engine
        .create_table(ACCOUNT, input)
        .await
        .expect("create a table with two vector indexes");

    let desc = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "t_describe".to_owned(),
            },
        )
        .await
        .expect("describe the table");

    let mut indexes = desc
        .vector_indexes
        .expect("DescribeTable must report the stored vector indexes");
    indexes.sort_by(|a, b| a.index_name.cmp(&b.index_name));
    assert_eq!(indexes.len(), 2);

    let scoped = &indexes[0];
    assert_eq!(scoped.index_name, "scoped");
    assert_eq!(scoped.dimensions, 8);
    // A scoped index reports its search schema: a client needs it to know that
    // SearchConditionExpression is mandatory.
    let schema = scoped
        .search_schema
        .as_ref()
        .expect("a scoped index must report its search schema");
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].attribute_name, "pk");
    assert_eq!(schema[0].element_type, SearchSchemaElementType::Hash);
    assert_eq!(
        scoped.projection.as_ref().map(|p| p.projection_type),
        Some(ProjectionType::All)
    );

    let unscoped = &indexes[1];
    assert_eq!(unscoped.index_name, "unscoped");
    assert_eq!(unscoped.dimensions, 16);
    // Absent, not empty: an index with no HASH element spans the table, and the
    // two states are different to a client.
    assert_eq!(unscoped.search_schema, None);
    assert_eq!(
        unscoped.projection.as_ref().map(|p| p.projection_type),
        Some(ProjectionType::KeysOnly)
    );
    assert_eq!(unscoped.distance_function, DistanceFunction::Cosine);
    assert_eq!(unscoped.index_status, IndexStatus::Active);

    s.cleanup().await;
}

#[tokio::test]
async fn table_key_info_carries_the_vector_index_metadata_the_write_path_needs() {
    if base_conn().is_none() {
        return skip("table_key_info_carries_the_vector_index_metadata_the_write_path_needs");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_keyinfo", vec![vector_spec("vidx", 32, Some("pk"))]),
        )
        .await
        .expect("create a table with a vector index");

    let key_info = s
        .engine
        .table_key_info(ACCOUNT, "t_keyinfo")
        .await
        .expect("read the cached key info");

    // This is the switch that turns on engine-side vector write validation and
    // vector write capacity: both read this slice, and both are no-ops while it
    // is empty.
    assert_eq!(key_info.vector_indexes.len(), 1);
    let vi = &key_info.vector_indexes[0];
    assert_eq!(vi.index_name, "vidx");
    assert_eq!(vi.dimensions, 32);
    assert_eq!(vi.vector_attribute_name, "emb");
    assert_eq!(vi.search_schema.len(), 1);
    assert_eq!(vi.search_schema[0].attribute_name, "pk");
    assert_eq!(vi.projection.projection_type, ProjectionType::All);

    // A table with no vector index must report an empty slice rather than
    // inheriting the previous table's, so the write path stays free.
    s.engine
        .create_table(ACCOUNT, create_input("t_plain", vec![]))
        .await
        .expect("create a table with no vector index");
    let plain = s
        .engine
        .table_key_info(ACCOUNT, "t_plain")
        .await
        .expect("read the plain table's key info");
    assert!(plain.vector_indexes.is_empty());

    s.cleanup().await;
}

#[tokio::test]
async fn delete_table_removes_the_vector_index_rows() {
    if base_conn().is_none() {
        return skip("delete_table_removes_the_vector_index_rows");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_delete", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a table with a vector index");
    let id = table_id(&s.catalog, "t_delete").await;
    assert_eq!(vector_row_count(&s.catalog, &id).await, 1);

    s.engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: "t_delete".to_owned(),
            },
        )
        .await
        .expect("delete the table");

    // Left behind, these rows would resurface on a table that reused the id and
    // would keep the account's vector index count wrong.
    assert_eq!(vector_row_count(&s.catalog, &id).await, 0);

    s.cleanup().await;
}

#[tokio::test]
async fn update_table_deletes_an_active_vector_index_once() {
    if base_conn().is_none() {
        return skip("update_table_deletes_an_active_vector_index_once");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input(
                "t_update",
                vec![
                    vector_spec("keep", 4, Some("pk")),
                    vector_spec("drop", 4, Some("pk")),
                ],
            ),
        )
        .await
        .expect("create a table with two vector indexes");

    let desc = s
        .engine
        .update_table(
            ACCOUNT,
            UpdateTableInput {
                vector_index_updates: delete_vector("drop"),
                ..update_input("t_update")
            },
        )
        .await
        .expect("delete one vector index");

    // The response is what the engine's post-condition check reads, so the
    // deleted index must be gone from it and the surviving one still in it.
    let names: Vec<String> = desc
        .vector_indexes
        .expect("UpdateTable must report the surviving vector indexes")
        .into_iter()
        .map(|vi| vi.index_name)
        .collect();
    assert_eq!(names, vec!["keep".to_owned()]);

    // Deleting it again is not idempotent: the index is gone, so the second
    // request must report that rather than succeed silently.
    let err = s
        .engine
        .update_table(
            ACCOUNT,
            UpdateTableInput {
                vector_index_updates: delete_vector("drop"),
                ..update_input("t_update")
            },
        )
        .await
        .expect_err("deleting a vector index twice must fail");
    match err {
        StorageError::IndexNotFound(name) => assert_eq!(name, "drop"),
        other => panic!("expected IndexNotFound, got {other:?}"),
    }

    s.cleanup().await;
}

#[tokio::test]
async fn deleting_a_vector_index_in_the_allocation_phase_is_refused() {
    if base_conn().is_none() {
        return skip("deleting_a_vector_index_in_the_allocation_phase_is_refused");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_phase", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a table with a vector index");
    let id = table_id(&s.catalog, "t_phase").await;
    set_index_phase(&s.catalog, &id, "vidx", false).await;

    let err = s
        .engine
        .update_table(
            ACCOUNT,
            UpdateTableInput {
                vector_index_updates: delete_vector("vidx"),
                ..update_input("t_phase")
            },
        )
        .await
        .expect_err("a delete during resource allocation must be refused");

    // ResourceInUse, not Validation: the request is well formed and the resource
    // exists, so the client should retry rather than change the request. The
    // whole string is the measured one, including both resource names.
    match err {
        StorageError::ResourceInUse(msg) => assert_eq!(
            msg,
            extenddb_core::types::vector_index_delete_in_allocation_phase("t_phase", "vidx")
        ),
        other => panic!("expected ResourceInUse, got {other:?}"),
    }

    // The refusal must not have deleted anything on the way out.
    assert_eq!(vector_row_count(&s.catalog, &id).await, 1);

    s.cleanup().await;
}

#[tokio::test]
async fn deleting_a_vector_index_during_its_backfill_is_accepted() {
    if base_conn().is_none() {
        return skip("deleting_a_vector_index_during_its_backfill_is_accepted");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_backfilling", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a table with a vector index");
    let id = table_id(&s.catalog, "t_backfilling").await;
    set_index_phase(&s.catalog, &id, "vidx", true).await;

    // Same request, one phase later, opposite answer: the discriminator is the
    // backfilling flag and nothing else.
    s.engine
        .update_table(
            ACCOUNT,
            UpdateTableInput {
                vector_index_updates: delete_vector("vidx"),
                ..update_input("t_backfilling")
            },
        )
        .await
        .expect("a delete during the backfill phase must be accepted");
    assert_eq!(vector_row_count(&s.catalog, &id).await, 0);

    s.cleanup().await;
}

#[tokio::test]
async fn switching_a_table_with_vector_indexes_to_provisioned_is_refused() {
    if base_conn().is_none() {
        return skip("switching_a_table_with_vector_indexes_to_provisioned_is_refused");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_billing", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a pay-per-request table with a vector index");

    let switch = |table: &str| UpdateTableInput {
        billing_mode: Some(BillingMode::Provisioned),
        provisioned_throughput: Some(ProvisionedThroughput {
            read_capacity_units: 5,
            write_capacity_units: 5,
        }),
        ..update_input(table)
    };

    let err = s
        .engine
        .update_table(ACCOUNT, switch("t_billing"))
        .await
        .expect_err("a vector table must not leave PAY_PER_REQUEST");
    match err {
        StorageError::Validation(msg) => assert_eq!(
            msg,
            extenddb_core::types::VECTOR_INDEX_REQUIRES_PAY_PER_REQUEST
        ),
        other => panic!("expected Validation, got {other:?}"),
    }

    // Control: the guard must key on the vector indexes, not on the switch, so
    // an ordinary table still changes billing mode.
    s.engine
        .create_table(ACCOUNT, create_input("t_billing_plain", vec![]))
        .await
        .expect("create a plain pay-per-request table");
    s.engine
        .update_table(ACCOUNT, switch("t_billing_plain"))
        .await
        .expect("a table with no vector index may switch to provisioned");

    s.cleanup().await;
}

#[tokio::test]
async fn adding_a_vector_index_by_update_table_is_unsupported() {
    if base_conn().is_none() {
        return skip("adding_a_vector_index_by_update_table_is_unsupported");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(ACCOUNT, create_input("t_add", vec![]))
        .await
        .expect("create a table with no vector index");

    let err = s
        .engine
        .update_table(
            ACCOUNT,
            UpdateTableInput {
                vector_index_updates: Some(vec![VectorIndexUpdate {
                    create: Some(vector_spec("vidx", 4, Some("pk"))),
                    delete: None,
                }]),
                ..update_input("t_add")
            },
        )
        .await
        .expect_err("this backend cannot build a vector index yet");

    // Unsupported rather than Internal: the backend never claimed it could build
    // one, so this is a refusal the engine reports as a client error, not a
    // fault to page on.
    match err {
        StorageError::Unsupported(msg) => assert!(msg.contains("vidx"), "{msg}"),
        other => panic!("expected Unsupported, got {other:?}"),
    }

    // Nothing may be left behind: a catalog row with no storage behind it would
    // be an index that reports ACTIVE and answers nothing.
    let id = table_id(&s.catalog, "t_add").await;
    assert_eq!(vector_row_count(&s.catalog, &id).await, 0);

    s.cleanup().await;
}

#[tokio::test]
async fn restoring_a_backup_that_carries_vector_indexes_is_refused() {
    if base_conn().is_none() {
        return skip("restoring_a_backup_that_carries_vector_indexes_is_refused");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_source", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a table with a vector index");
    let details = s
        .engine
        .create_backup(ACCOUNT, "t_source", "b_vector")
        .await
        .expect("back up a table with a vector index");

    // Refused, not silently degraded: restore does not carry indexes across, so
    // succeeding here would hand back a table whose declared index is missing
    // and whose client only finds out on the first search.
    let err = s
        .engine
        .restore_table_from_backup(ACCOUNT, "t_restored", &details.backup_arn)
        .await
        .expect_err("restoring a vector-indexed backup must be refused");
    match err {
        StorageError::Unsupported(msg) => {
            assert!(msg.contains("vector index"), "{msg}");
            assert!(msg.contains(&details.backup_arn), "{msg}");
        }
        other => panic!("expected Unsupported, got {other:?}"),
    }

    // The refusal must leave no half-created target table behind.
    let target: Option<(String,)> =
        sqlx::query_as("SELECT table_name FROM tables WHERE account_id = $1 AND table_name = $2")
            .bind(ACCOUNT)
            .bind("t_restored")
            .fetch_optional(&s.catalog)
            .await
            .expect("look for the target table");
    assert!(target.is_none(), "no target table may be created");

    // Control: a backup with no vector indexes still restores, so the refusal is
    // keyed on the snapshot rather than on backups in general.
    s.engine
        .create_table(ACCOUNT, create_input("t_plain_source", vec![]))
        .await
        .expect("create a plain table");
    let plain = s
        .engine
        .create_backup(ACCOUNT, "t_plain_source", "b_plain")
        .await
        .expect("back up a plain table");
    s.engine
        .restore_table_from_backup(ACCOUNT, "t_plain_restored", &plain.backup_arn)
        .await
        .expect("a backup with no vector indexes must still restore");

    s.cleanup().await;
}

#[tokio::test]
async fn a_wrong_dimension_vector_written_by_update_item_is_rejected() {
    if base_conn().is_none() {
        return skip("a_wrong_dimension_vector_written_by_update_item_is_rejected");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_write", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a table with a four-dimension vector index");

    let key_info = s
        .engine
        .table_key_info(ACCOUNT, "t_write")
        .await
        .expect("read the key info");
    let key: Item = BTreeMap::from([("pk".to_owned(), AttributeValue::S("a".to_owned()))]);

    // The update path already handed `key_info.vector_indexes` to the evaluator
    // before this work; it was a no-op only because the slice was always empty.
    // So this is the behaviour that populating the slice turns on, with no call
    // site changed.
    let three = AttributeValue::L(vec![
        AttributeValue::N("1".to_owned()),
        AttributeValue::N("2".to_owned()),
        AttributeValue::N("3".to_owned()),
    ]);
    let err = update_emb(&s.engine, &key_info, &key, three)
        .await
        .expect_err("a three-element vector must not enter a four-dimension index");
    match err {
        StorageError::Validation(msg) => {
            assert!(msg.contains("emb"), "{msg}");
            assert!(msg.contains('4') && msg.contains('3'), "{msg}");
        }
        other => panic!("expected Validation, got {other:?}"),
    }

    // Control: the right number of components is accepted, so the check is on
    // the dimension rather than on writing a list at all.
    let four = AttributeValue::L(vec![
        AttributeValue::N("1".to_owned()),
        AttributeValue::N("2".to_owned()),
        AttributeValue::N("3".to_owned()),
        AttributeValue::N("4".to_owned()),
    ]);
    update_emb(&s.engine, &key_info, &key, four)
        .await
        .expect("a four-element vector must be accepted");

    s.cleanup().await;
}

/// `SET emb = :v` against one item, through the storage update path.
async fn update_emb(
    engine: &PostgresEngine,
    key_info: &extenddb_core::types::TableKeyInfo,
    key: &Item,
    value: AttributeValue,
) -> Result<Option<Item>, StorageError> {
    let tokens = expression::tokenize("SET emb = :v").expect("tokenize the update expression");
    let actions = expression::parse_update(&tokens).expect("parse the update expression");
    // Keys carry no leading colon: the engine strips it before building the
    // maps, and the resolver adds it back when it reports an unknown one.
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::from([("v".to_owned(), value)]));
    engine
        .update_item(key_info, key, &actions, false, false, None, &maps, None)
        .await
        .map(|(item, _)| item)
}

#[tokio::test]
async fn describe_table_refuses_a_vector_index_whose_stored_status_is_unrecognised() {
    let test = "describe_table_refuses_a_vector_index_whose_stored_status_is_unrecognised";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_badstatus", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a table with a vector index");
    let id = table_id(&s.catalog, "t_badstatus").await;

    // A value no build writes, standing in for a corrupt row or one written by a
    // future version. `IndexStatus` carries a catch-all variant for forward
    // compatibility when parsing a service response, which is the opposite of
    // what is wanted when reading our own catalog: a status we cannot understand
    // must not be handed to a client as if we could.
    sqlx::query("UPDATE vector_indexes SET index_status = 'ACITVE' WHERE table_id = $1")
        .bind(&id)
        .execute(&s.catalog)
        .await
        .expect("write an unrecognised status");

    let err = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "t_badstatus".to_owned(),
            },
        )
        .await
        .expect_err("an unrecognised stored status must not be described");
    match err {
        StorageError::Internal(msg) => assert!(msg.contains("ACITVE"), "{msg}"),
        other => panic!("expected Internal, got {other:?}"),
    }

    s.cleanup().await;
}

#[tokio::test]
async fn describe_table_reports_a_creating_index_as_backfilling() {
    if base_conn().is_none() {
        return skip("describe_table_reports_a_creating_index_as_backfilling");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_creating", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a table with a vector index");
    let id = table_id(&s.catalog, "t_creating").await;
    set_index_phase(&s.catalog, &id, "vidx", true).await;

    let desc = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "t_creating".to_owned(),
            },
        )
        .await
        .expect("describe a table whose index is still building");

    // The only path that round-trips a non-ACTIVE status and a present
    // Backfilling member, which is the pair the wire contract ties together.
    let indexes = desc.vector_indexes.expect("the index must be reported");
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].index_status, IndexStatus::Creating);
    assert_eq!(indexes[0].backfilling, Some(true));

    s.cleanup().await;
}

#[tokio::test]
async fn an_empty_search_schema_is_stored_and_reported_as_absent() {
    if base_conn().is_none() {
        return skip("an_empty_search_schema_is_stored_and_reported_as_absent");
    }
    let s = scratch(Pgvector::Omit).await;

    // Core accepts `SearchSchema: []` on the request, and every path downstream
    // treats it as unscoped. The service reports an absent member or a populated
    // one, never an empty list, so storing `[]` would create a third state that
    // DescribeTable echoes back and no client expects.
    let mut spec = vector_spec("vidx", 4, None);
    spec.search_schema = Some(Vec::new());
    let desc = s
        .engine
        .create_table(ACCOUNT, create_input("t_emptyschema", vec![spec]))
        .await
        .expect("create a table with an empty search schema");
    assert_eq!(
        desc.vector_indexes.as_ref().unwrap()[0].search_schema,
        None,
        "the CreateTable echo must not report an empty list"
    );

    let id = table_id(&s.catalog, "t_emptyschema").await;
    let stored: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT search_schema FROM vector_indexes WHERE table_id = $1")
            .bind(&id)
            .fetch_one(&s.catalog)
            .await
            .expect("read the stored search schema");
    assert_eq!(
        stored, None,
        "the catalog must hold NULL, not an empty list"
    );

    let described = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "t_emptyschema".to_owned(),
            },
        )
        .await
        .expect("describe the table");
    assert_eq!(
        described.vector_indexes.unwrap()[0].search_schema,
        None,
        "DescribeTable must report the member as absent"
    );

    s.cleanup().await;
}

#[tokio::test]
async fn a_backup_snapshot_carries_the_wire_shape_behind_a_version() {
    if base_conn().is_none() {
        return skip("a_backup_snapshot_carries_the_wire_shape_behind_a_version");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_snapshot", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a table with a vector index");
    let details = s
        .engine
        .create_backup(ACCOUNT, "t_snapshot", "b_snapshot")
        .await
        .expect("back up the table");

    let snapshot: serde_json::Value =
        sqlx::query_scalar("SELECT vector_indexes FROM backups WHERE backup_arn = $1")
            .bind(&details.backup_arn)
            .fetch_one(&s.catalog)
            .await
            .expect("read the snapshot");

    // A snapshot outlives the schema that produced it, so it carries a version
    // and the wire's own names rather than a copy of the catalog row. A later
    // column rename must not change the meaning of snapshots already on disk.
    assert_eq!(snapshot["Version"], serde_json::json!(1));
    let indexes = snapshot["VectorIndexes"]
        .as_array()
        .expect("the snapshot must carry an index list");
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0]["IndexName"], serde_json::json!("vidx"));
    assert_eq!(indexes[0]["Dimensions"], serde_json::json!(4));
    assert_eq!(indexes[0]["DistanceFunction"], serde_json::json!("COSINE"));
    assert_eq!(
        indexes[0]["VectorAttribute"]["AttributeName"],
        serde_json::json!("emb")
    );
    // Build state belongs to the table it came from, not to a definition being
    // restored, so it is deliberately not in the snapshot.
    for absent in ["index_status", "backfilling", "build_owner"] {
        assert!(
            indexes[0].get(absent).is_none(),
            "the snapshot must not carry {absent}"
        );
    }

    s.cleanup().await;
}

#[tokio::test]
async fn a_backup_taken_before_the_snapshot_column_existed_still_restores() {
    if base_conn().is_none() {
        return skip("a_backup_taken_before_the_snapshot_column_existed_still_restores");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(ACCOUNT, create_input("t_legacy", vec![]))
        .await
        .expect("create a plain table");
    let details = s
        .engine
        .create_backup(ACCOUNT, "t_legacy", "b_legacy")
        .await
        .expect("back up the table");

    // Every backup taken before this migration has NULL here, and the refusal
    // rests on reading that as "no vector indexes" rather than as "unknown".
    // Without a test, tightening the Option handling would break every legacy
    // restore on an upgraded deployment and nothing would notice.
    sqlx::query("UPDATE backups SET vector_indexes = NULL WHERE backup_arn = $1")
        .bind(&details.backup_arn)
        .execute(&s.catalog)
        .await
        .expect("blank the snapshot the way a pre-migration backup has it");

    s.engine
        .restore_table_from_backup(ACCOUNT, "t_legacy_restored", &details.backup_arn)
        .await
        .expect("a pre-migration backup must still restore");

    s.cleanup().await;
}

#[tokio::test]
async fn a_vector_index_at_the_maximum_dimension_round_trips() {
    if base_conn().is_none() {
        return skip("a_vector_index_at_the_maximum_dimension_round_trips");
    }
    let s = scratch(Pgvector::Omit).await;

    // 4096 is the largest dimension core accepts. The catalog column is a 32-bit
    // integer and the wire type is unsigned, so the value crosses two narrowing
    // conversions on the way out and back; the boundary is where a wrong cast
    // would show.
    let desc = s
        .engine
        .create_table(
            ACCOUNT,
            create_input("t_maxdim", vec![vector_spec("vidx", 4096, Some("pk"))]),
        )
        .await
        .expect("create a table with a maximum-dimension vector index");
    assert_eq!(desc.vector_indexes.as_ref().unwrap()[0].dimensions, 4096);

    let described = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "t_maxdim".to_owned(),
            },
        )
        .await
        .expect("describe the table");
    assert_eq!(described.vector_indexes.unwrap()[0].dimensions, 4096);

    let key_info = s
        .engine
        .table_key_info(ACCOUNT, "t_maxdim")
        .await
        .expect("read the key info");
    assert_eq!(key_info.vector_indexes[0].dimensions, 4096);

    s.cleanup().await;
}

#[tokio::test]
async fn describe_table_refuses_a_vector_index_with_a_corrupt_payload() {
    if base_conn().is_none() {
        return skip("describe_table_refuses_a_vector_index_with_a_corrupt_payload");
    }
    let s = scratch(Pgvector::Omit).await;

    s.engine
        .create_table(
            ACCOUNT,
            create_input("t_corrupt", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect("create a table with a vector index");
    let id = table_id(&s.catalog, "t_corrupt").await;

    // Well-formed JSON that is not a vector attribute. The comment on the decode
    // path promises a loud failure rather than a defaulted description, because a
    // client builds a search from what it reads here.
    sqlx::query(
        "UPDATE vector_indexes SET vector_attribute = '{\"Nonsense\": true}'::jsonb \
         WHERE table_id = $1",
    )
    .bind(&id)
    .execute(&s.catalog)
    .await
    .expect("corrupt the payload");

    let err = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "t_corrupt".to_owned(),
            },
        )
        .await
        .expect_err("a payload that cannot be decoded must not be described");
    match err {
        StorageError::Internal(msg) => assert!(msg.contains("vector_attribute"), "{msg}"),
        other => panic!("expected Internal, got {other:?}"),
    }

    // The same row must also fail the write path's cache fill, rather than
    // quietly yielding a table with no vector indexes and skipping validation.
    let err = s
        .engine
        .table_key_info(ACCOUNT, "t_corrupt")
        .await
        .expect_err("the cached key info must not silently drop the index");
    assert!(matches!(err, StorageError::Internal(_)), "{err:?}");

    s.cleanup().await;
}

#[tokio::test]
async fn the_probe_reads_the_data_database_and_not_the_catalog() {
    let test = "the_probe_reads_the_data_database_and_not_the_catalog";
    if base_conn().is_none() {
        return skip(test);
    }
    let Some(base) = base_conn() else { return };

    // In production the catalog and the data database are separate, and pgvector
    // is installed on the data one, because that is where vector storage lives.
    // Every other test here runs against a scratch database that serves as both,
    // so the engine would pass them all while probing the wrong database. This is
    // the only test that can tell the two apart.
    let Some(catalog) = scratch_with_pgvector_omitted_but_data_installed(&base, test).await else {
        return;
    };

    assert!(
        catalog.engine.vector_capable(),
        "the extension is installed on the data database, so the capability must be reported \
         even though the catalog database does not have it"
    );

    // Both databases go through cleanup, which owns every database it created and
    // is the only place holding an admin pool that still works: closing one handle
    // of a `PgPool` closes every clone, so a drop attempted after cleanup would
    // fail with `PoolClosed` and, if its error were discarded, leak silently.
    assert_eq!(
        catalog.extra_databases.len(),
        1,
        "the data database must be registered for cleanup"
    );
    catalog.cleanup().await;
}

/// Build a scratch catalog whose data database is a *different* database, with
/// pgvector installed only on the data one.
///
/// Returns the scratch environment, whose `cleanup` drops both databases, or
/// `None` when the server has no pgvector to install.
async fn scratch_with_pgvector_omitted_but_data_installed(
    base: &str,
    test: &str,
) -> Option<Scratch> {
    let probe = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    let available: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = 'vector')",
    )
    .fetch_one(&probe)
    .await
    .expect("list the available extensions");
    probe.close().await;
    if !available {
        eprintln!(
            "SKIP {test}: this PostgreSQL server does not offer the pgvector extension, so the \
             data-database probe cannot be exercised here."
        );
        return None;
    }

    // The catalog, deliberately without the extension.
    let catalog = scratch(Pgvector::Omit).await;
    let data_db = format!("{}_data", catalog.db_name);
    sqlx::query(&format!("CREATE DATABASE \"{data_db}\""))
        .execute(&catalog.admin)
        .await
        .expect("create the separate data database");

    let data_url = format!("{base}/{data_db}");
    let data = PgPoolOptions::new()
        .max_connections(1)
        .connect(&data_url)
        .await
        .expect("connect to the data database");
    sqlx::query("CREATE EXTENSION vector")
        .execute(&data)
        .await
        .expect("install pgvector on the data database only");
    data.close().await;

    // Registering the connection string is what makes the engine open a second
    // pool instead of reusing the catalog's.
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
         ON CONFLICT (key) DO UPDATE SET value = $1",
    )
    .bind(&data_url)
    .execute(&catalog.catalog)
    .await
    .expect("register the data database connection string");

    // Re-open the engine so the constructor sees the setting and probes the data
    // database.
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: format!("{base}/{}", catalog.db_name),
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("re-open the engine with a separate data database");

    Some(Scratch {
        engine,
        extra_databases: vec![data_db],
        ..catalog
    })
}

#[tokio::test]
async fn the_engine_detects_whether_the_data_database_has_pgvector() {
    if base_conn().is_none() {
        return skip("the_engine_detects_whether_the_data_database_has_pgvector");
    }

    // The negative case is the one that must hold on every server, including one
    // that has never heard of pgvector: no extension, no capability, and a
    // catalog that otherwise works normally.
    let without = scratch(Pgvector::Omit).await;
    assert!(
        !without.engine.vector_capable(),
        "a data database without the extension must not report vector capability"
    );
    without
        .engine
        .create_table(ACCOUNT, create_input("t_novector", vec![]))
        .await
        .expect("a server without pgvector must still serve ordinary tables");
    without.cleanup().await;

    let Some(with) =
        scratch_with_pgvector("the_engine_detects_whether_the_data_database_has_pgvector").await
    else {
        return;
    };
    assert!(
        with.engine.vector_capable(),
        "a data database with the extension installed must report vector capability"
    );
    with.cleanup().await;
}

#[tokio::test]
async fn losing_pgvector_after_startup_refuses_a_vector_index_rather_than_recording_it() {
    let test = "losing_pgvector_after_startup_refuses_a_vector_index_rather_than_recording_it";
    if base_conn().is_none() {
        return skip(test);
    }
    let Some(s) = scratch_with_pgvector(test).await else {
        return;
    };

    // The engine probed the extension once, at construction, and cached the
    // answer. Dropping it now is the window the second layer exists for: a DBA
    // dropping the extension, or a failover onto a server without it.
    assert!(s.engine.vector_capable());
    sqlx::query("DROP EXTENSION vector")
        .execute(&s.catalog)
        .await
        .expect("drop the pgvector extension");

    let err = s
        .engine
        .create_table(
            ACCOUNT,
            create_input("t_vanished", vec![vector_spec("vidx", 4, Some("pk"))]),
        )
        .await
        .expect_err("a vector index must not be recorded once the extension is gone");

    // Unsupported, so the engine answers 400 with the reason. Without the
    // mapping this is a raw PostgreSQL error and a 500, which tells the caller
    // nothing actionable.
    match err {
        StorageError::Unsupported(msg) => {
            assert!(msg.contains("pgvector"), "{msg}");
            assert!(msg.contains("data database"), "{msg}");
        }
        other => panic!("expected Unsupported, got {other:?}"),
    }

    // No table and no index row: the refusal happens before anything is written.
    let table: Option<(String,)> =
        sqlx::query_as("SELECT table_name FROM tables WHERE account_id = $1 AND table_name = $2")
            .bind(ACCOUNT)
            .bind("t_vanished")
            .fetch_optional(&s.catalog)
            .await
            .expect("look for the table");
    assert!(table.is_none(), "no table may be created");

    s.cleanup().await;
}
