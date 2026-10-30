// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The catalog migration `004_escape_catalog_control_chars.sql` against a
//! catalog an earlier release wrote.
//!
//! Earlier releases stored attribute names raw in the catalog's `jsonb`
//! columns; this release stores them through the `catalog_json` escape, whose
//! decoder reads U+0001 followed by U+0001 or U+0002 as an escape pair. The
//! migration re-encodes every stored document holding a raw U+0001 once, so a
//! legacy name `a<U+0001><U+0001>z` still reads back as itself. Pinned here:
//!
//! - a 0.0.4 catalog holding such names in all seven columns is refused by
//!   `serve` (version gate), migrated by `migrate --yes`, and then returns the
//!   names byte-identical through `DescribeTable` and `DescribeBackup`;
//! - a second `migrate --yes` changes nothing;
//! - a catalog stamped 0.0.5 by hand without the record is refused by `serve`
//!   naming the file;
//! - the migration refuses to run while another session holds the catalog.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL
//! `postgres://user:pass@host:port`, no database); without it every test
//! reports a skip and passes. Each test creates and drops its own databases.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DescribeTableInput, GsiInput,
    KeySchemaElement, KeyType, Projection, ProjectionType, ScalarAttributeType,
};
use extenddb_storage::bootstrapper::{BootstrapConfig, Bootstrapper};
use extenddb_storage::server_components::BackendError;
use extenddb_storage::{BackupEngine, TableEngine};
use extenddb_storage_postgres::{PostgresBootstrapper, PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

const ACCOUNT: &str = "123456789012";
/// A name an earlier release could store raw and this release's decoder
/// would misread without the migration.
const LEGACY: &str = "a\u{1}\u{1}z";
const LEGACY_GSI_KEY: &str = "g\u{1}\u{2}k";
const LEGACY_PROJECTED: &str = "n\u{1}\u{1}";

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

fn bootstrap_config(base: &str, catalog: &str, data: &str) -> BootstrapConfig {
    let rest = base
        .strip_prefix("postgres://")
        .or_else(|| base.strip_prefix("postgresql://"))
        .expect("a postgres:// URL");
    let (creds, addr) = rest.split_once('@').expect("user:pass@host:port");
    let (user, pass) = creds.split_once(':').expect("user:pass");
    let (host, port) = addr.split_once(':').unwrap_or((addr, "5432"));
    BootstrapConfig {
        host: host.to_owned(),
        port: port.parse().expect("port"),
        admin_user: user.to_owned(),
        admin_password: Some(pass.to_owned()),
        app_user: user.to_owned(),
        app_password: pass.to_owned(),
        catalog_db: catalog.to_owned(),
        data_db: data.to_owned(),
    }
}

/// Drops the scratch databases on every exit, a panic included.
struct Scratch {
    base: String,
    names: [String; 2],
    catalog: Option<PgPool>,
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let base = self.base.clone();
        let names = self.names.clone();
        let pool = self.catalog.take();
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async move {
                if let Some(p) = pool {
                    p.close().await;
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

impl Scratch {
    fn pool(&self) -> &PgPool {
        self.catalog.as_ref().expect("catalog pool")
    }

    /// A 0.0.4 catalog and data database the way the previous release's
    /// `init` left them (001 to 003 applied and recorded, every data migration
    /// recorded), holding one table with a GSI and one backup of it, with the
    /// legacy names written raw into all seven columns the way that release
    /// wrote them.
    async fn legacy() -> Self {
        let base = base_conn().expect("caller checks base_conn() first");
        let stem = format!("eddb_esc_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
        let names = [format!("{stem}_c"), format!("{stem}_d")];
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{base}/postgres"))
            .await
            .expect("admin");
        for n in &names {
            sqlx::query(&format!("CREATE DATABASE \"{n}\""))
                .execute(&admin)
                .await
                .expect("create scratch database");
        }
        admin.close().await;
        // The scratch pool carries the migrate command's application name so
        // the exclusive-access check does not count it; a real operator session
        // is simulated separately with a plain connection.
        let opts: PgConnectOptions = format!("{base}/{}", names[0]).parse().expect("catalog url");
        let catalog = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(opts.application_name("extenddb-migrate"))
            .await
            .expect("catalog");
        let s = Self {
            base,
            names,
            catalog: Some(catalog),
        };
        for (file, sql) in [
            (
                "001_schema.sql",
                include_str!("../migrations/001_schema.sql"),
            ),
            (
                "002_vector_indexes.sql",
                include_str!("../migrations/002_vector_indexes.sql"),
            ),
            (
                "003_backup_definitions.sql",
                include_str!("../migrations/003_backup_definitions.sql"),
            ),
        ] {
            sqlx::raw_sql(sql).execute(s.pool()).await.expect(file);
            sqlx::query("INSERT INTO schema_history (filename) VALUES ($1)")
                .bind(file)
                .execute(s.pool())
                .await
                .expect("record");
        }
        let data = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{}/{}", s.base, s.names[1]))
            .await
            .expect("data");
        sqlx::query(
            "CREATE TABLE schema_history (filename TEXT PRIMARY KEY, \
             applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
        )
        .execute(&data)
        .await
        .expect("data ledger");
        for (file, sql) in [
            (
                "001_data_schema.sql",
                include_str!("../data_migrations/001_data_schema.sql"),
            ),
            (
                "002_gsi_pending.sql",
                include_str!("../data_migrations/002_gsi_pending.sql"),
            ),
            (
                "003_idempotency_account_scope.sql",
                include_str!("../data_migrations/003_idempotency_account_scope.sql"),
            ),
            (
                "004_vector_index_state.sql",
                include_str!("../data_migrations/004_vector_index_state.sql"),
            ),
            ("003_gsi_base_key_index", ""),
        ] {
            if !sql.is_empty() {
                sqlx::raw_sql(sql).execute(&data).await.expect(file);
            }
            sqlx::query("INSERT INTO schema_history (filename) VALUES ($1)")
                .bind(file)
                .execute(&data)
                .await
                .expect("record");
        }
        data.close().await;
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(format!("{}/{}", s.base, s.names[1]))
        .execute(s.pool())
        .await
        .expect("point the catalog at the data database");
        sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
            .execute(s.pool())
            .await
            .expect("no control-plane delay");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
            .bind(ACCOUNT)
            .bind(format!("acct-{stem}"))
            .execute(s.pool())
            .await
            .expect("account");

        // The table and backup rows come from this release's engine (the
        // shapes are the same); the seven columns are then set by SQL to the
        // raw documents the previous release wrote.
        let engine = s.engine().await;
        engine
            .create_table(
                ACCOUNT,
                CreateTableInput {
                    table_name: "legacy".to_owned(),
                    key_schema: vec![key("pk", KeyType::Hash)],
                    attribute_definitions: vec![s_attr("pk"), s_attr("gk")],
                    billing_mode: Some(BillingMode::PayPerRequest),
                    global_secondary_indexes: Some(vec![GsiInput {
                        index_name: "gsi1".to_owned(),
                        key_schema: vec![key("gk", KeyType::Hash)],
                        projection: Projection {
                            projection_type: ProjectionType::Include,
                            non_key_attributes: Some(vec!["nk".to_owned()]),
                        },
                        provisioned_throughput: None,
                    }]),
                    ..Default::default()
                },
            )
            .await
            .expect("create the table");
        engine
            .create_backup(ACCOUNT, "legacy", "bk")
            .await
            .expect("create the backup");
        drop(engine);

        let raw_ks = serde_json::json!([{"AttributeName": LEGACY, "KeyType": "HASH"}]);
        let raw_ad = serde_json::json!([
            {"AttributeName": LEGACY, "AttributeType": "S"},
            {"AttributeName": LEGACY_GSI_KEY, "AttributeType": "S"}
        ]);
        let raw_gsi_ks = serde_json::json!([{"AttributeName": LEGACY_GSI_KEY, "KeyType": "HASH"}]);
        let raw_proj = serde_json::json!({
            "ProjectionType": "INCLUDE",
            "NonKeyAttributes": [LEGACY_PROJECTED]
        });
        for (sql, values) in [
            (
                "UPDATE tables SET key_schema = $1, attribute_definitions = $2",
                (&raw_ks, &raw_ad),
            ),
            (
                "UPDATE indexes SET key_schema = $1, projection = $2",
                (&raw_gsi_ks, &raw_proj),
            ),
            (
                "UPDATE backups SET key_schema = $1, attribute_definitions = $2",
                (&raw_ks, &raw_ad),
            ),
        ] {
            sqlx::query(sql)
                .bind(values.0)
                .bind(values.1)
                .execute(s.pool())
                .await
                .expect(sql);
        }
        let mut definition: serde_json::Value =
            sqlx::query_scalar("SELECT definition FROM backup_definitions")
                .fetch_one(s.pool())
                .await
                .expect("the backup definition");
        definition["GlobalSecondaryIndexes"][0]["KeySchema"] = raw_gsi_ks.clone();
        definition["GlobalSecondaryIndexes"][0]["Projection"] = raw_proj.clone();
        sqlx::query("UPDATE backup_definitions SET definition = $1")
            .bind(&definition)
            .execute(s.pool())
            .await
            .expect("write the raw definition");
        assert_eq!(s.catalog_version().await, "0.0.4");
        s
    }

    fn bootstrapper(&self) -> PostgresBootstrapper {
        PostgresBootstrapper::new(bootstrap_config(&self.base, &self.names[0], &self.names[1]))
    }

    async fn engine(&self) -> PostgresEngine {
        PostgresEngine::new(
            &PostgresConfig {
                connection_string: format!("{}/{}", self.base, self.names[0]),
                pool_size: 4,
                max_item_size_bytes: 400_000,
            },
            "us-east-1",
        )
        .await
        .expect("engine")
    }

    async fn catalog_version(&self) -> String {
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_version'")
            .fetch_one(self.pool())
            .await
            .expect("catalog_version")
    }

    /// The seven columns' stored text, for before-and-after comparisons.
    async fn stored_text(&self) -> String {
        sqlx::query_scalar(
            "SELECT (SELECT string_agg(key_schema::text || attribute_definitions::text, '|' \
                      ORDER BY table_name) FROM tables) \
                 || ' ' || (SELECT string_agg(key_schema::text || projection::text, '|' \
                      ORDER BY index_name) FROM indexes) \
                 || ' ' || (SELECT string_agg(key_schema::text || attribute_definitions::text, '|' \
                      ORDER BY backup_arn) FROM backups) \
                 || ' ' || (SELECT string_agg(definition::text, '|' ORDER BY backup_arn) \
                      FROM backup_definitions)",
        )
        .fetch_one(self.pool())
        .await
        .expect("stored text")
    }

    async fn backup_arn(&self) -> String {
        sqlx::query_scalar("SELECT backup_arn FROM backups")
            .fetch_one(self.pool())
            .await
            .expect("backup arn")
    }
}

fn names(ks: &[KeySchemaElement]) -> Vec<&str> {
    ks.iter().map(|k| k.attribute_name.as_str()).collect()
}

#[tokio::test]
async fn legacy_names_with_control_characters_survive_the_upgrade() {
    if base_conn().is_none() {
        eprintln!("SKIP legacy_names_with_control_characters_survive_the_upgrade: no PostgreSQL");
        return;
    }
    let s = Scratch::legacy().await;
    let before = s.stored_text().await;
    // The raw pair is in the stored text (jsonb::text renders it as \u0001).
    assert!(before.contains("a\\u0001\\u0001z"), "{before}");

    // Without the migration this binary's decoder would misread the names,
    // so serve refuses the 0.0.4 catalog outright.
    let engine = s.engine().await;
    match engine.prepare_catalog_for_serve().await {
        Err(BackendError::CatalogVersionMismatch { expected, found }) => {
            assert_eq!((expected.as_str(), found.as_str()), ("0.0.5", "0.0.4"));
        }
        other => panic!("serve must refuse the 0.0.4 catalog: {other:?}"),
    }
    drop(engine);

    let b = s.bootstrapper();
    let pending = b.pending_data_migrations().await.expect("pending");
    assert!(
        pending.contains(&"catalog 004_escape_catalog_control_chars.sql".to_owned()),
        "{pending:?}"
    );
    b.run_data_migrations().await.expect("migrate --yes");
    assert_eq!(s.catalog_version().await, "0.0.5");
    let after = s.stored_text().await;
    assert!(!after.contains("a\\u0001\\u0001z"), "re-encoded: {after}");
    assert!(after.contains("a\\u0001\\u0002\\u0001\\u0002z"), "{after}");

    // A second run changes nothing: the ledger records the file.
    assert!(
        b.pending_data_migrations()
            .await
            .expect("pending")
            .is_empty()
    );
    b.run_data_migrations().await.expect("second migrate --yes");
    assert_eq!(
        s.stored_text().await,
        after,
        "a second run must not rewrite"
    );

    let engine = s.engine().await;
    engine
        .prepare_catalog_for_serve()
        .await
        .expect("serve starts on the migrated catalog");
    let desc = engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "legacy".to_owned(),
            },
        )
        .await
        .expect("DescribeTable");
    assert_eq!(names(&desc.key_schema), [LEGACY]);
    let mut defs: Vec<&str> = desc
        .attribute_definitions
        .iter()
        .map(|a| a.attribute_name.as_str())
        .collect();
    defs.sort_unstable();
    assert_eq!(defs, [LEGACY, LEGACY_GSI_KEY]);
    let gsi = &desc.global_secondary_indexes.as_deref().expect("GSIs")[0];
    assert_eq!(names(&gsi.key_schema), [LEGACY_GSI_KEY]);
    assert_eq!(
        gsi.projection.non_key_attributes.as_deref(),
        Some(&[LEGACY_PROJECTED.to_owned()][..])
    );
    // backups.key_schema and attribute_definitions (DescribeBackup and the
    // restore source) and backup_definitions.definition (the restore source).
    let backup = engine
        .describe_backup(ACCOUNT, &s.backup_arn().await)
        .await
        .expect("DescribeBackup");
    assert_eq!(names(&backup.source_table_details.key_schema), [LEGACY]);
    let source = engine
        .backup_restore_source(ACCOUNT, &s.backup_arn().await)
        .await
        .expect("restore source");
    assert_eq!(names(&source.key_schema), [LEGACY]);
    let mut defs: Vec<&str> = source
        .attribute_definitions
        .iter()
        .map(|a| a.attribute_name.as_str())
        .collect();
    defs.sort_unstable();
    assert_eq!(defs, [LEGACY, LEGACY_GSI_KEY]);
    let definition = source.definition.expect("the backup definition");
    let bgsi = &definition.global_secondary_indexes[0];
    assert_eq!(names(&bgsi.key_schema), [LEGACY_GSI_KEY]);
    assert_eq!(
        bgsi.projection.non_key_attributes.as_deref(),
        Some(&[LEGACY_PROJECTED.to_owned()][..])
    );
    drop(engine);
}

#[tokio::test]
async fn a_catalog_stamped_current_without_the_record_is_refused_at_serve() {
    if base_conn().is_none() {
        eprintln!("SKIP a_catalog_stamped_current_without_the_record_is_refused_at_serve");
        return;
    }
    let s = Scratch::legacy().await;
    sqlx::query("UPDATE settings SET value = '0.0.5' WHERE key = 'catalog_version'")
        .execute(s.pool())
        .await
        .expect("stamp");
    let engine = s.engine().await;
    match engine.prepare_catalog_for_serve().await {
        Err(BackendError::InitializationFailed(m)) => {
            assert!(m.contains("004_escape_catalog_control_chars.sql"), "{m}");
            assert!(m.contains("extenddb migrate --yes"), "{m}");
        }
        other => panic!("serve must refuse the unrecorded migration: {other:?}"),
    }
    drop(engine);
}

#[tokio::test]
async fn the_migration_refuses_to_run_while_another_session_holds_the_catalog() {
    if base_conn().is_none() {
        eprintln!("SKIP the_migration_refuses_to_run_while_another_session_holds_the_catalog");
        return;
    }
    let s = Scratch::legacy().await;
    let before = s.stored_text().await;
    // An operator's psql, or a server still running: a session without the
    // migrate command's application name.
    let opts: PgConnectOptions = format!("{}/{}", s.base, s.names[0])
        .parse()
        .expect("catalog url");
    let other = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .connect_with(opts.application_name("psql"))
        .await
        .expect("other session");
    let _: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&other)
        .await
        .expect("hold the session open");
    let b = s.bootstrapper();
    let err = b
        .check_exclusive_access_for_pending_migrations()
        .await
        .expect_err("another session holds the catalog");
    let text = format!("{err:?}");
    assert!(
        text.contains("other connection(s) hold the catalog database"),
        "{text}"
    );
    assert!(text.contains("psql"), "{text}");
    let err = b
        .run_data_migrations()
        .await
        .expect_err("the migration itself refuses too");
    assert!(
        format!("{err:?}").contains("other connection(s) hold the catalog database"),
        "{err:?}"
    );
    assert_eq!(s.stored_text().await, before, "a refusal changes nothing");
    assert_eq!(s.catalog_version().await, "0.0.4");
    other.close().await;
    b.run_data_migrations()
        .await
        .expect("migrate --yes once the session is gone");
    assert_eq!(s.catalog_version().await, "0.0.5");
}
