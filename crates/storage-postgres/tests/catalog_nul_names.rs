// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Catalog round trip of attribute names containing U+0000 on PostgreSQL.
//!
//! DynamoDB accepts U+0000 in a key attribute name and `DescribeTable`
//! returns it byte-identical. The catalog stores `key_schema`,
//! `attribute_definitions` and index `projection` as `jsonb`, which rejects
//! the `\u0000` escape, so those columns are written through the escape in
//! `catalog_json` and read back through its inverse. This file pins the
//! round trip at the storage layer: create with the character in a table key
//! name, a GSI key name, an LSI key name and a projected attribute name;
//! describe; read the key info the data plane uses; add and remove a GSI on
//! the attribute; describe the table's stream; back the table up, read the
//! backup's description and restore source, restore it into a second table and
//! describe that; delete. Every decode site the catalog has for these columns
//! (DescribeTable, key info, UpdateTable, DescribeStream, backup capture,
//! DescribeBackup, the restore source and the restore target) returns the names
//! byte for byte. It also checks the stored column holds no U+0000, which is
//! what a PostgreSQL `jsonb` column cannot hold.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component, pointing at a server whose role may create and drop databases.
//! Without it every test reports a skip and passes.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateGsiAction, CreateTableInput, DeleteGsiAction,
    DeleteTableInput, DescribeStreamInput, DescribeTableInput, GlobalSecondaryIndexUpdate,
    GsiInput, KeySchemaElement, KeyType, LsiInput, Projection, ProjectionType, ScalarAttributeType,
    StreamSpecification, StreamViewType, UpdateTableInput,
};
use extenddb_storage::{
    BackupEngine, RestoreRequest, RestoreTableOverrides, StreamEngine, TableEngine,
};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";

const PK: &str = "p\u{0}k";
const SK: &str = "s\u{0}k";
const GK: &str = "g\u{0}k";
const LK: &str = "l\u{0}k";
const PROJECTED: &str = "n\u{0}k";

/// A catalog database and a data database, as `init` creates them. Two
/// databases rather than one: the catalog's `stream_shards` references
/// `tables`, and the data database's copy does not, so a single database
/// would refuse the shard rows the engine writes before the catalog
/// transaction commits.
struct Scratch {
    engine: Option<PostgresEngine>,
    db: Option<PgPool>,
    base: String,
    db_name: String,
}

impl Scratch {
    fn engine(&self) -> &PostgresEngine {
        self.engine.as_ref().expect("engine")
    }

    fn db(&self) -> &PgPool {
        self.db.as_ref().expect("db")
    }
}

/// Drops the scratch database on every exit, a failed assertion included, so a
/// failing run leaves nothing behind on a shared server.
impl Drop for Scratch {
    fn drop(&mut self) {
        let base = self.base.clone();
        let names = [self.db_name.clone(), format!("{}_d", self.db_name)];
        let engine = self.engine.take();
        let db = self.db.take();
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async move {
                drop(engine);
                if let Some(db) = db {
                    db.close().await;
                }
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

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// Every `.sql` file under `dir`, in filename order.
fn shipped_migrations(dir: &str) -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files: Vec<_> = std::fs::read_dir(root.join(dir))
        .expect("read the migrations directory")
        .map(|e| e.expect("directory entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    files.sort();
    files
        .iter()
        .map(|f| std::fs::read_to_string(f).expect("read a migration file"))
        .collect()
}

async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_nul_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    for n in [db_name.clone(), format!("{db_name}_d")] {
        sqlx::query(&format!("CREATE DATABASE \"{n}\""))
            .execute(&admin)
            .await
            .expect("create a scratch database");
    }
    admin.close().await;
    let url = format!("{base}/{db_name}");
    let db = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to the scratch catalog");
    for sql in shipped_migrations("migrations") {
        sqlx::raw_sql(&sql)
            .execute(&db)
            .await
            .expect("apply a catalog migration");
    }
    let data_url = format!("{base}/{db_name}_d");
    let data = PgPoolOptions::new()
        .max_connections(1)
        .connect(&data_url)
        .await
        .expect("connect to the scratch data database");
    for sql in shipped_migrations("data_migrations") {
        sqlx::raw_sql(&sql)
            .execute(&data)
            .await
            .expect("apply a data migration");
    }
    data.close().await;
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(&data_url)
    .execute(&db)
    .await
    .expect("point the catalog at the data database");
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
        .execute(&db)
        .await
        .expect("pin the control-plane delay to zero");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{db_name}"))
        .execute(&db)
        .await
        .expect("seed the account row");
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: url,
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch database");
    Scratch {
        engine: Some(engine),
        db: Some(db),
        base,
        db_name,
    }
}

fn s_attr(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    }
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn include(names: &[&str]) -> Projection {
    Projection {
        projection_type: ProjectionType::Include,
        non_key_attributes: Some(names.iter().map(|n| (*n).to_owned()).collect()),
    }
}

/// A (PK, SK) table with a GSI on GK and an LSI on LK, both projecting PROJECTED.
fn input(table: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: table.to_owned(),
        key_schema: vec![key(PK, KeyType::Hash), key(SK, KeyType::Range)],
        attribute_definitions: vec![s_attr(PK), s_attr(SK), s_attr(GK), s_attr(LK)],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "gsi1".to_owned(),
            key_schema: vec![key(GK, KeyType::Hash)],
            projection: include(&[PROJECTED]),
            provisioned_throughput: None,
        }]),
        local_secondary_indexes: Some(vec![LsiInput {
            index_name: "lsi1".to_owned(),
            key_schema: vec![key(PK, KeyType::Hash), key(LK, KeyType::Range)],
            projection: include(&[PROJECTED]),
        }]),
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        }),
        ..Default::default()
    }
}

/// An `UpdateTableInput` carrying only a GSI update.
fn gsi_update(
    table: &str,
    attribute_definitions: Option<Vec<AttributeDefinition>>,
    update: GlobalSecondaryIndexUpdate,
) -> UpdateTableInput {
    UpdateTableInput {
        table_name: table.to_owned(),
        billing_mode: None,
        table_throughput_mode: None,
        provisioned_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: Some(vec![update]),
        attribute_definitions,
        stream_specification: None,
        table_class: None,
        on_demand_throughput: None,
        vector_index_updates: None,
    }
}

fn names(ks: &[KeySchemaElement]) -> Vec<&str> {
    ks.iter().map(|k| k.attribute_name.as_str()).collect()
}

fn def_names(ad: &[AttributeDefinition]) -> Vec<&str> {
    let mut v: Vec<&str> = ad.iter().map(|a| a.attribute_name.as_str()).collect();
    v.sort_unstable();
    v
}

async fn stored_catalog_text(db: &PgPool, table: &str) -> String {
    let (ks, ad, idx): (String, String, String) = sqlx::query_as(
        "SELECT t.key_schema::text, t.attribute_definitions::text, \
         COALESCE(string_agg(i.key_schema::text || i.projection::text, ' '), '') \
         FROM tables t LEFT JOIN indexes i ON i.table_id = t.table_id \
         WHERE t.account_id = $1 AND t.table_name = $2 \
         GROUP BY t.key_schema, t.attribute_definitions",
    )
    .bind(ACCOUNT)
    .bind(table)
    .fetch_one(db)
    .await
    .expect("read the stored catalog columns");
    format!("{ks} {ad} {idx}")
}

#[tokio::test]
async fn key_attribute_names_with_nul_round_trip_through_the_catalog() {
    let Some(_) = base_conn() else {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    };
    let sc = scratch().await;
    let table = "nul_names";

    let created = sc
        .engine()
        .create_table(ACCOUNT, input(table))
        .await
        .expect("CreateTable with U+0000 in key attribute names");
    assert_eq!(names(&created.key_schema), [PK, SK]);

    // The stored jsonb holds no U+0000 and no \u0000 escape: that is the
    // byte the column cannot hold, so its absence is what makes the insert
    // succeed. The names are present in escaped form.
    let stored = stored_catalog_text(sc.db(), table).await;
    assert!(
        !stored.contains('\u{0}'),
        "stored catalog text carries a NUL: {stored}"
    );
    assert!(
        !stored.contains("\\u0000"),
        "stored catalog text carries \\u0000: {stored}"
    );
    // `jsonb::text` renders control characters as JSON escapes. This pins the
    // escape alphabet (U+0000 as U+0001 U+0001) on purpose: it is the mapping
    // the item data columns use for `item_data`, so one decoder can read both
    // this column and those later; a change here would split the two.
    assert!(
        stored.contains("p\\u0001\\u0001k"),
        "escaped key name absent from: {stored}"
    );

    let desc = sc
        .engine()
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: table.to_owned(),
            },
        )
        .await
        .expect("DescribeTable");
    assert_eq!(names(&desc.key_schema), [PK, SK]);
    assert_eq!(
        def_names(&desc.attribute_definitions),
        def_names(&input(table).attribute_definitions)
    );
    let gsi = &desc.global_secondary_indexes.as_deref().expect("GSIs")[0];
    assert_eq!(names(&gsi.key_schema), [GK]);
    assert_eq!(
        gsi.projection.non_key_attributes.as_deref(),
        Some(&[PROJECTED.to_owned()][..])
    );
    let lsi = &desc.local_secondary_indexes.as_deref().expect("LSIs")[0];
    assert_eq!(names(&lsi.key_schema), [PK, LK]);
    assert_eq!(
        lsi.projection.non_key_attributes.as_deref(),
        Some(&[PROJECTED.to_owned()][..])
    );

    // The key info the data plane and authorization read.
    let info = sc
        .engine()
        .table_key_info(ACCOUNT, table)
        .await
        .expect("table_key_info");
    assert_eq!(names(&info.key_schema), [PK, SK]);
    assert_eq!(
        def_names(&info.attribute_definitions),
        def_names(&input(table).attribute_definitions)
    );
    assert_eq!(names(&info.global_secondary_indexes[0].key_schema), [GK]);
    assert_eq!(names(&info.local_secondary_indexes[0].key_schema), [PK, LK]);

    // DescribeStream reads the key schema from the catalog on its own path.
    let stream_arn = desc.latest_stream_arn.clone().expect("the stream ARN");
    let stream = sc
        .engine()
        .describe_stream(
            ACCOUNT,
            &DescribeStreamInput {
                stream_arn,
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .expect("DescribeStream");
    assert_eq!(names(&stream.key_schema), [PK, SK]);

    // The backup copies the key schema, the definitions and every index into
    // backups and backup_definitions; DescribeBackup and the restore source
    // read them back; the restore target is created from the restore source.
    let backup = sc
        .engine()
        .create_backup(ACCOUNT, table, "bk367")
        .await
        .expect("CreateBackup");
    let described = sc
        .engine()
        .describe_backup(ACCOUNT, &backup.backup_arn)
        .await
        .expect("DescribeBackup");
    assert_eq!(names(&described.source_table_details.key_schema), [PK, SK]);
    let source = sc
        .engine()
        .backup_restore_source(ACCOUNT, &backup.backup_arn)
        .await
        .expect("restore source");
    assert_eq!(names(&source.key_schema), [PK, SK]);
    assert_eq!(
        def_names(&source.attribute_definitions),
        def_names(&input(table).attribute_definitions)
    );
    let definition = source.definition.as_ref().expect("the backup definition");
    assert_eq!(
        names(&definition.global_secondary_indexes[0].key_schema),
        [GK]
    );
    assert_eq!(
        definition.global_secondary_indexes[0]
            .projection
            .non_key_attributes
            .as_deref(),
        Some(&[PROJECTED.to_owned()][..])
    );
    assert_eq!(
        names(&definition.local_secondary_indexes[0].key_schema),
        [PK, LK]
    );
    assert_eq!(
        definition.local_secondary_indexes[0]
            .projection
            .non_key_attributes
            .as_deref(),
        Some(&[PROJECTED.to_owned()][..])
    );
    let restored_name = "nul_names_restored";
    let restore_input = source.restore_input(restored_name, &RestoreTableOverrides::default());
    extenddb_core::validation::validate_create_table(
        &restore_input,
        &extenddb_core::limits::LimitsConfig::default(),
    )
    .expect("the restore input validates as CreateTable would");
    sc.engine()
        .restore_table_from_backup(
            ACCOUNT,
            RestoreRequest {
                backup_arn: backup.backup_arn.clone(),
                source_table_arn: source.source_table_arn.clone(),
                restore_date_time: source.backup_creation_date_time,
                input: restore_input,
            },
        )
        .await
        .expect("RestoreTableFromBackup");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(90);
    loop {
        let status: Option<String> = sqlx::query_scalar(
            "SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(ACCOUNT)
        .bind(restored_name)
        .fetch_optional(sc.db())
        .await
        .expect("restore target status");
        if status.as_deref() == Some("ACTIVE") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "restore target still {status:?} after 90 s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let restored = sc
        .engine()
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: restored_name.to_owned(),
            },
        )
        .await
        .expect("DescribeTable on the restore target");
    assert_eq!(names(&restored.key_schema), [PK, SK]);
    assert_eq!(
        def_names(&restored.attribute_definitions),
        def_names(&input(table).attribute_definitions)
    );
    let rgsi = &restored.global_secondary_indexes.as_deref().expect("GSIs")[0];
    assert_eq!(names(&rgsi.key_schema), [GK]);
    assert_eq!(
        rgsi.projection.non_key_attributes.as_deref(),
        Some(&[PROJECTED.to_owned()][..])
    );
    let rlsi = &restored.local_secondary_indexes.as_deref().expect("LSIs")[0];
    assert_eq!(names(&rlsi.key_schema), [PK, LK]);
    assert_eq!(
        rlsi.projection.non_key_attributes.as_deref(),
        Some(&[PROJECTED.to_owned()][..])
    );

    // Remove the GSI, then add one back on the same U+0000 attribute.
    let after_delete = sc
        .engine()
        .update_table(
            ACCOUNT,
            gsi_update(
                table,
                None,
                GlobalSecondaryIndexUpdate {
                    create: None,
                    update: None,
                    delete: Some(DeleteGsiAction {
                        index_name: "gsi1".to_owned(),
                    }),
                },
            ),
        )
        .await
        .expect("UpdateTable delete GSI");
    assert!(
        after_delete
            .global_secondary_indexes
            .as_deref()
            .unwrap_or(&[])
            .is_empty()
    );
    // The pruned definitions were rewritten through the escape and read back.
    assert_eq!(def_names(&after_delete.attribute_definitions), [LK, PK, SK]);

    let after_create = sc
        .engine()
        .update_table(
            ACCOUNT,
            gsi_update(
                table,
                Some(vec![s_attr(GK)]),
                GlobalSecondaryIndexUpdate {
                    create: Some(CreateGsiAction {
                        index_name: "gsi2".to_owned(),
                        key_schema: vec![key(GK, KeyType::Hash)],
                        projection: include(&[PROJECTED]),
                        provisioned_throughput: None,
                    }),
                    update: None,
                    delete: None,
                },
            ),
        )
        .await
        .expect("UpdateTable create GSI on a U+0000 attribute");
    let gsi2 = &after_create
        .global_secondary_indexes
        .as_deref()
        .expect("GSIs")[0];
    assert_eq!(gsi2.index_name, "gsi2");
    assert_eq!(names(&gsi2.key_schema), [GK]);
    assert_eq!(
        def_names(&after_create.attribute_definitions),
        [GK, LK, PK, SK]
    );

    let deleted = sc
        .engine()
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: table.to_owned(),
            },
        )
        .await
        .expect("DeleteTable");
    assert_eq!(names(&deleted.key_schema), [PK, SK]);
}

#[tokio::test]
async fn plain_names_are_stored_exactly_as_before() {
    let Some(_) = base_conn() else {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    };
    let sc = scratch().await;
    let table = "plain_names";
    let mut plain = input(table);
    plain.key_schema = vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)];
    plain.attribute_definitions = vec![s_attr("pk"), s_attr("sk"), s_attr("gk"), s_attr("lk")];
    plain.global_secondary_indexes = Some(vec![GsiInput {
        index_name: "gsi1".to_owned(),
        key_schema: vec![key("gk", KeyType::Hash)],
        projection: include(&["nk"]),
        provisioned_throughput: None,
    }]);
    plain.local_secondary_indexes = Some(vec![LsiInput {
        index_name: "lsi1".to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("lk", KeyType::Range)],
        projection: include(&["nk"]),
    }]);
    sc.engine()
        .create_table(ACCOUNT, plain)
        .await
        .expect("CreateTable");

    // No U+0001 anywhere: an ordinary name is written as itself.
    let stored = stored_catalog_text(sc.db(), table).await;
    assert!(
        !stored.contains('\u{1}'),
        "escape marker in ordinary catalog text: {stored}"
    );
    assert!(
        stored.contains(r#"{"KeyType": "HASH", "AttributeName": "pk"}"#)
            || stored.contains(r#"{"AttributeName": "pk", "KeyType": "HASH"}"#)
    );
}
