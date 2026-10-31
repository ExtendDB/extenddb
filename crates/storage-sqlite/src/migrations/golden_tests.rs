// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Golden tests for SQLite migrations 2 to 5, each run on a released catalog
//! with the damage or the content its body handles, and migration 1's
//! accepted and converged pre-release shapes.

use std::path::Path;

use extenddb_storage::migrations::{CatalogStatus, MigrationError, exit};

use super::tests::{
    DEV_0_1_7, DEV_0_1_14, copies, count, exec, load_dump, plain_pool, reconverge, run, scalar,
    status, temp_db, version_row,
};

/// Rows of a query, one tab-separated line each: text as it is (JSON with
/// sorted keys, because the workspace build enables serde_json's insertion
/// order and a single-crate build does not), integers and reals in decimal,
/// blobs in hex, and NULL as `NULL`.
async fn dump(db: &Path, sql: &str) -> String {
    let pool = plain_pool(db).await;
    let rows = sqlx::query(sql).fetch_all(&pool).await.expect(sql);
    pool.close().await;
    let mut out = format!("-- {sql}\n");
    for row in rows {
        use sqlx::Row;
        let fields: Vec<String> = (0..row.columns().len())
            .map(|i| {
                if let Ok(text) = row.try_get::<Option<String>, _>(i) {
                    return text.map_or_else(
                        || "NULL".to_owned(),
                        |text| match serde_json::from_str::<serde_json::Value>(&text) {
                            Ok(value) if value.is_object() || value.is_array() => canonical(&value),
                            _ => text,
                        },
                    );
                }
                if let Ok(n) = row.try_get::<Option<i64>, _>(i) {
                    return n.map_or_else(|| "NULL".to_owned(), |n| n.to_string());
                }
                if let Ok(x) = row.try_get::<Option<f64>, _>(i) {
                    return x.map_or_else(|| "NULL".to_owned(), |x| x.to_string());
                }
                let bytes = row.try_get::<Option<Vec<u8>>, _>(i).expect("a blob");
                bytes.map_or_else(
                    || "NULL".to_owned(),
                    |b| b.iter().map(|byte| format!("{byte:02x}")).collect(),
                )
            })
            .collect();
        out.push_str(&fields.join("\t"));
        out.push('\n');
    }
    out
}

/// A JSON value with object keys in sorted order.
fn canonical(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            let members: Vec<String> = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::Value::from(k.as_str()),
                        canonical(&map[k])
                    )
                })
                .collect();
            format!("{{{}}}", members.join(","))
        }
        serde_json::Value::Array(items) => {
            format!(
                "[{}]",
                items.iter().map(canonical).collect::<Vec<_>>().join(",")
            )
        }
        other => other.to_string(),
    }
}

/// Compare a body's result with its committed dump under
/// `tests/fixtures/golden/`. `EXTENDDB_BLESS_EXPECTED=1` rewrites the file.
fn golden(name: &str, actual: &str) {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("tests/fixtures/golden/{name}.txt"));
    if super::tests::bless() {
        std::fs::create_dir_all(path.parent().expect("dir")).expect("golden dir");
        std::fs::write(&path, actual).expect("write golden");
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        expected,
        actual,
        "{name} differs from {}; regenerate with EXTENDDB_BLESS_EXPECTED=1",
        path.display()
    );
}

#[tokio::test]
async fn the_golden_dump_writes_every_column_type() {
    let db = temp_db("dump");
    exec(
        &db,
        "CREATE TABLE t (a TEXT, b INTEGER, c REAL, d BLOB, e TEXT)",
    )
    .await;
    exec(&db, "INSERT INTO t VALUES ('x', 7, 1.5, x'0aff', NULL)").await;
    assert_eq!(
        dump(&db, "SELECT a, b, c, d, e FROM t").await,
        "-- SELECT a, b, c, d, e FROM t\nx\t7\t1.5\t0aff\tNULL\n"
    );
}

/// The account and IAM user every dev-image fixture holds.
async fn principal(db: &Path) -> (String, String) {
    let pool = plain_pool(db).await;
    let row: (String, String) =
        sqlx::query_as("SELECT account_id, user_name FROM iam_users LIMIT 1")
            .fetch_one(&pool)
            .await
            .expect("an IAM user");
    pool.close().await;
    row
}

async fn put_policy(db: &Path, principal_type: &str, principal_name: &str, name: &str, doc: &str) {
    let (account, _) = principal(db).await;
    let pool = plain_pool(db).await;
    sqlx::query(
        "INSERT INTO iam_policies (account_id, principal_type, principal_name, policy_name, \
         policy_document) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&account)
    .bind(principal_type)
    .bind(principal_name)
    .bind(name)
    .bind(doc)
    .execute(&pool)
    .await
    .expect("policy");
    pool.close().await;
}

const TAGGED: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"dynamodb:ResourceTag/env":"prod"}}}]}"#;
const COLLIDING: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/env":"prod","dynamodb:ResourceTag/env":"dev"}}}]}"#;

/// The six places a policy is stored: user, group and role inline policies,
/// user and role permissions boundaries, and a session policy.
async fn tag_every_place(db: &Path) {
    let (account, user) = principal(db).await;
    let pool = plain_pool(db).await;
    sqlx::query("INSERT INTO iam_groups (account_id, group_name, group_arn) VALUES (?, 'devs', ?)")
        .bind(&account)
        .bind(format!("arn:aws:iam::{account}:group/devs"))
        .execute(&pool)
        .await
        .expect("group");
    sqlx::query(
        "INSERT INTO iam_roles (account_id, role_name, role_arn, trust_policy) \
         VALUES (?, 'app', ?, '{}')",
    )
    .bind(&account)
    .bind(format!("arn:aws:iam::{account}:role/app"))
    .execute(&pool)
    .await
    .expect("role");
    for (principal_type, name) in [("user", user.as_str()), ("role", "app")] {
        sqlx::query(
            "INSERT INTO iam_permissions_boundaries (account_id, principal_type, \
             principal_name, policy_document) VALUES (?, ?, ?, ?)",
        )
        .bind(&account)
        .bind(principal_type)
        .bind(name)
        .bind(TAGGED)
        .execute(&pool)
        .await
        .expect("boundary");
    }
    sqlx::query(
        "INSERT INTO iam_sessions (session_token, access_key_id, secret_key_encrypted, \
         account_id, role_name, session_name, session_policy, expires_at) \
         VALUES ('token-1', 'ASIAFIXTURE0001', x'00', ?, 'app', 'session', ?, \
         '2999-01-01T00:00:00.000Z')",
    )
    .bind(&account)
    .bind(TAGGED)
    .execute(&pool)
    .await
    .expect("session");
    pool.close().await;
    put_policy(db, "user", &user, "tagged-user", TAGGED).await;
    put_policy(db, "group", "devs", "tagged-group", TAGGED).await;
    put_policy(db, "role", "app", "tagged-role", TAGGED).await;
}

#[tokio::test]
async fn migration_2_rewrites_resource_tag_condition_keys_in_every_place() {
    let db = temp_db("m2");
    load_dump(&db, DEV_0_1_14).await;
    tag_every_place(&db).await;
    run(&db).await.expect("migrate");
    for (place, sql) in [
        (
            "inline",
            "SELECT COUNT(*) FROM iam_policies WHERE policy_document LIKE '%dynamodb:ResourceTag%'",
        ),
        (
            "boundary",
            "SELECT COUNT(*) FROM iam_permissions_boundaries WHERE policy_document LIKE '%dynamodb:ResourceTag%'",
        ),
        (
            "session",
            "SELECT COUNT(*) FROM iam_sessions WHERE session_policy LIKE '%dynamodb:ResourceTag%'",
        ),
    ] {
        assert_eq!(
            count(&db, sql).await,
            0,
            "{place} policies keep the dynamodb: form"
        );
    }
    let document = scalar(
        &db,
        "SELECT policy_document FROM iam_policies WHERE policy_name = 'tagged-role'",
    )
    .await
    .expect("policy");
    let document: serde_json::Value = serde_json::from_str(&document).expect("json");
    assert_eq!(
        document["Statement"][0]["Condition"],
        serde_json::json!({"StringEquals": {"aws:ResourceTag/env": "prod"}})
    );
    let notes = scalar(&db, "SELECT notes FROM extenddb_migrations WHERE id = 2")
        .await
        .expect("row");
    assert_eq!(notes.lines().count(), 6, "{notes}");
    let mut text = dump(
        &db,
        "SELECT account_id, principal_type, principal_name, policy_name, policy_document \
         FROM iam_policies ORDER BY principal_type, policy_name",
    )
    .await;
    text.push_str(
        &dump(
            &db,
            "SELECT account_id, principal_type, principal_name, policy_document \
             FROM iam_permissions_boundaries ORDER BY principal_type",
        )
        .await,
    );
    text.push_str(
        &dump(
            &db,
            "SELECT session_token, role_name, session_policy FROM iam_sessions \
             ORDER BY session_token",
        )
        .await,
    );
    text.push_str(&dump(&db, "SELECT notes FROM extenddb_migrations WHERE id = 2").await);
    golden("0002_resource_tag_condition_keys", &text);
}

/// A collision is found by `detect` before anything is written: no copy, no
/// ledger, and the released version row untouched.
#[tokio::test]
async fn migration_2_collisions_stop_the_run_before_any_write() {
    let db = temp_db("m2-collision");
    load_dump(&db, DEV_0_1_14).await;
    let (_, user) = principal(&db).await;
    put_policy(&db, "user", &user, "colliding", COLLIDING).await;
    let found = status(&db).await;
    let CatalogStatus::NeedsAttention { items } = &found else {
        panic!("{found:?}");
    };
    assert!(items[0].contains("policy colliding"), "{items:?}");
    assert_eq!(found.exit_code(), exit::ATTENTION);
    let err = run(&db).await.expect_err("refused");
    assert!(
        matches!(
            err,
            MigrationError::Status(CatalogStatus::NeedsAttention { .. })
        ),
        "{err:?}"
    );
    assert_eq!(err.exit_code(), exit::ATTENTION);
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'extenddb_migrations'"
        )
        .await,
        0
    );
    assert_eq!(version_row(&db).await.as_deref(), Some("0.0.3"));
    assert!(copies(&db).is_empty(), "no copy before a refusal");
}

#[tokio::test]
async fn migration_3_restores_sort_key_definitions_in_tables_and_backups() {
    let db = temp_db("m3");
    load_dump(&db, DEV_0_1_14).await;
    // The pre-fix UpdateTable kept only the definitions its request named.
    let damaged = r#"[{"AttributeName":"gpk","AttributeType":"S"},{"AttributeName":"gsk","AttributeType":"N"},{"AttributeName":"lsk","AttributeType":"S"}]"#;
    let key_schema =
        r#"[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]"#;
    let pool = plain_pool(&db).await;
    let (table_id, account): (String, String) =
        sqlx::query_as("SELECT table_id, account_id FROM tables")
            .fetch_one(&pool)
            .await
            .expect("table");
    sqlx::query("UPDATE tables SET attribute_definitions = ?")
        .bind(damaged)
        .execute(&pool)
        .await
        .expect("damage");
    // b1: an item gives the type. b2: no item and no source table. b3: no
    // item, and the source table's key column gives the type.
    for (arn, source, with_item) in [
        ("arn:b1", table_id.as_str(), true),
        ("arn:b2", "6f9619ff-8b86-4011-b42d-00c04fc964ff", false),
        ("arn:b3", table_id.as_str(), false),
    ] {
        sqlx::query(
            "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, key_schema, \
             attribute_definitions) VALUES (?, 'b', ?, 'cu_orders', ?, ?, ?)",
        )
        .bind(arn)
        .bind(source)
        .bind(&account)
        .bind(key_schema)
        .bind(damaged)
        .execute(&pool)
        .await
        .expect("backup");
        if with_item {
            sqlx::query(
                "INSERT INTO backup_items (backup_arn, pk, sk, item_data) VALUES (?, 'a', NULL, ?)",
            )
            .bind(arn)
            .bind(r#"{"pk":{"S":"a"},"sk":{"S":"1"}}"#)
            .execute(&pool)
            .await
            .expect("item");
        }
    }
    pool.close().await;

    run(&db).await.expect("migrate");
    let restored = scalar(&db, "SELECT attribute_definitions FROM tables")
        .await
        .expect("row");
    assert!(
        restored.contains(r#"{"AttributeName":"sk","AttributeType":"S"}"#),
        "{restored}"
    );
    for arn in ["arn:b1", "arn:b3"] {
        let repaired = scalar(
            &db,
            &format!("SELECT attribute_definitions FROM backups WHERE backup_arn = '{arn}'"),
        )
        .await
        .expect("backup");
        assert!(
            repaired.contains(r#"{"AttributeName":"sk","AttributeType":"S"}"#),
            "{arn}: {repaired}"
        );
    }
    let b2 = scalar(
        &db,
        "SELECT attribute_definitions FROM backups WHERE backup_arn = 'arn:b2'",
    )
    .await
    .expect("b2");
    assert_eq!(
        b2, damaged,
        "a backup with no items and no source table is left as it is"
    );
    let notes = scalar(&db, "SELECT notes FROM extenddb_migrations WHERE id = 3")
        .await
        .expect("row");
    let expected_notes = [
        "restored cu_orders: sk (S)",
        "cu_orders: partition key definition(s) [pk] are absent",
        "restored backup arn:b1 of cu_orders: sk (S)",
        "backup arn:b2 of cu_orders: sort key(s) [sk] have no attribute definition",
        "restored backup arn:b3 of cu_orders: sk (S)",
    ];
    for expected in expected_notes {
        assert!(notes.contains(expected), "{expected} not in {notes}");
    }
    let mut text = dump(
        &db,
        "SELECT table_name, attribute_definitions FROM tables ORDER BY table_name",
    )
    .await;
    text.push_str(
        &dump(
            &db,
            "SELECT backup_arn, attribute_definitions FROM backups ORDER BY backup_arn",
        )
        .await,
    );
    text.push_str(&dump(&db, "SELECT notes FROM extenddb_migrations WHERE id = 3").await);
    golden("0003_sort_key_definitions", &text);
}

/// Pre-release shapes of `table_restores`: the extra default a pre-release
/// build gave `restore_date_time` is accepted with a note, and a table
/// without `source_table_arn` gains the column.
#[tokio::test]
async fn migration_1_accepts_and_converges_pre_release_table_restores() {
    let db = temp_db("pre-release-default");
    load_dump(&db, DEV_0_1_14).await;
    exec(
        &db,
        "CREATE TABLE table_restores (\
             table_id TEXT PRIMARY KEY REFERENCES tables(table_id) ON DELETE CASCADE, \
             source_backup_arn TEXT NOT NULL, \
             restore_date_time TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), \
             source_table_arn TEXT)",
    )
    .await;
    run(&db).await.expect("accepted");
    let notes = scalar(&db, "SELECT notes FROM extenddb_migrations WHERE id = 1")
        .await
        .expect("row");
    assert!(
        notes.contains("keeps the default a pre-release build gave it"),
        "{notes}"
    );

    let db = temp_db("pre-release-no-arn");
    load_dump(&db, DEV_0_1_14).await;
    exec(
        &db,
        "CREATE TABLE table_restores (\
             table_id TEXT PRIMARY KEY REFERENCES tables(table_id) ON DELETE CASCADE, \
             source_backup_arn TEXT NOT NULL, \
             restore_date_time TEXT NOT NULL); \
         INSERT INTO table_restores SELECT table_id, 'arn:backup', '2026-01-01T00:00:00Z' \
             FROM tables;",
    )
    .await;
    run(&db).await.expect("converged");
    assert_eq!(status(&db).await.label(), "Current");
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM pragma_table_info('table_restores') \
             WHERE name = 'source_table_arn'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(&db, "SELECT source_backup_arn FROM table_restores")
            .await
            .as_deref(),
        Some("arn:backup"),
        "the row is kept"
    );
    let notes = scalar(&db, "SELECT notes FROM extenddb_migrations WHERE id = 1")
        .await
        .expect("row");
    assert!(
        notes.contains("added table_restores.source_table_arn"),
        "{notes}"
    );
}

#[tokio::test]
async fn migration_4_adds_the_base_key_index_to_index_tables_created_before_v0_1_8() {
    let db = temp_db("m4");
    load_dump(&db, DEV_0_1_7).await;
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'idx_base_key_%'"
        )
        .await,
        0
    );
    // A catalog row whose index table is gone is skipped and noted, and so
    // is the index table of a table being deleted.
    exec(
        &db,
        "INSERT INTO indexes (table_id, index_id, index_name, index_type, key_schema, projection) \
         SELECT table_id, '6f9619ff-8b86-4011-b42d-00c04fc964ff', 'gone', 'GSI', '[]', '{}' FROM tables; \
         CREATE TEMP TABLE going AS SELECT * FROM tables; \
         UPDATE going SET table_id = '0b7e9a52-3c1d-4f6e-9a2b-5d8c7e6f1a40', \
             table_name = 'going', table_status = 'DELETING'; \
         INSERT INTO tables SELECT * FROM going; \
         INSERT INTO indexes (table_id, index_id, index_name, index_type, key_schema, projection) \
         VALUES ('0b7e9a52-3c1d-4f6e-9a2b-5d8c7e6f1a40', '9d1f3b6a-2e4c-4a8b-b7d0-6c5e4f3a2b10', \
             'by_gpk', 'GSI', '[]', '{}'); \
         CREATE TABLE \"_ddb_9d1f3b6a-2e4c-4a8b-b7d0-6c5e4f3a2b10\" (\
             pk TEXT NOT NULL, base_pk TEXT NOT NULL, base_sk_s TEXT NOT NULL, \
             PRIMARY KEY (pk, base_pk, base_sk_s));",
    )
    .await;
    run(&db).await.expect("migrate");
    let pool = plain_pool(&db).await;
    let created: Vec<(String, String)> = sqlx::query_as(
        "SELECT i.index_name, m.sql FROM indexes i \
         JOIN sqlite_master m ON m.name = 'idx_base_key_' || i.index_id ORDER BY i.index_name",
    )
    .fetch_all(&pool)
    .await
    .expect("indexes");
    let ids: Vec<(String, String)> = sqlx::query_as(
        "SELECT index_name, index_id FROM indexes WHERE table_id = \
         (SELECT table_id FROM tables WHERE table_name = 'cu_orders') ORDER BY index_name",
    )
    .fetch_all(&pool)
    .await
    .expect("ids");
    pool.close().await;
    assert_eq!(created.len(), 2, "{created:?}");
    for (name, sql) in &created {
        let id = &ids.iter().find(|(n, _)| n == name).expect("id").1;
        // The statement CreateTable writes for a new index table.
        assert_eq!(
            sql,
            &format!("CREATE INDEX \"idx_base_key_{id}\" ON \"_ddb_{id}\" (base_pk, base_sk_s)")
        );
    }
    let notes = scalar(&db, "SELECT notes FROM extenddb_migrations WHERE id = 4")
        .await
        .expect("row");
    for expected in [
        "cu_orders index gone: skipped, its index table",
        "cu_orders index by_gpk: created idx_base_key_",
        "going index by_gpk: skipped, the table is being deleted",
    ] {
        assert!(notes.contains(expected), "{expected} not in {notes}");
    }
    let mut text = dump(
        &db,
        "SELECT name, sql FROM sqlite_master WHERE name LIKE 'idx_base_key_%' ORDER BY name",
    )
    .await;
    text.push_str(&dump(&db, "SELECT notes FROM extenddb_migrations WHERE id = 4").await);
    golden("0004_index_table_base_key", &text);
}

/// Migration 5 on a released catalog holding two restore registrations: one
/// whose table is ACTIVE (a finished restore) and one whose table is CREATING
/// (a restore the stop interrupted). The job record is added and backfilled,
/// the interrupted one RUNNING with the epoch heartbeat and generation 0, the
/// index and maintenance tables exist, the catalog id is written once, and a
/// second run changes nothing.
#[tokio::test]
async fn migration_5_adds_the_catalog_index_and_backfills_the_registrations() {
    let db = temp_db("m5");
    load_dump(&db, DEV_0_1_14).await;
    // The dump predates `table_restores`; create it as that release did.
    exec(
        &db,
        "CREATE TABLE table_restores (\
             table_id TEXT PRIMARY KEY REFERENCES tables(table_id) ON DELETE CASCADE, \
             source_backup_arn TEXT NOT NULL, \
             source_table_arn TEXT, \
             restore_date_time TEXT NOT NULL); \
         CREATE TEMP TABLE interrupted AS SELECT * FROM tables; \
         UPDATE interrupted SET table_id = '4e1c0d2b-7a9f-4c3e-8b6d-2f5a1c9e7d30', \
             table_name = 'interrupted', table_status = 'CREATING', status_transition_at = NULL; \
         INSERT INTO tables SELECT * FROM interrupted; \
         INSERT INTO table_restores (table_id, source_backup_arn, source_table_arn, restore_date_time) \
         SELECT table_id, 'arn:aws:dynamodb:us-east-1:000000000000:table/cu_orders/backup/01700000000000-0000000a', \
                'arn:aws:dynamodb:us-east-1:000000000000:table/cu_orders', '2026-01-01T00:00:00.000Z' \
         FROM tables WHERE table_name IN ('cu_orders', 'interrupted');",
    )
    .await;
    run(&db).await.expect("migrate");
    let catalog_id = scalar(&db, "SELECT value FROM settings WHERE key = 'catalog_id'")
        .await
        .expect("a catalog id");
    assert!(uuid::Uuid::parse_str(&catalog_id).is_ok(), "{catalog_id}");
    assert_eq!(count(&db, "SELECT COUNT(*) FROM backup_index").await, 0);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM backup_maintenance").await,
        0
    );
    let notes = scalar(&db, "SELECT notes FROM extenddb_migrations WHERE id = 5")
        .await
        .expect("row");
    for expected in [
        "added table_restores.status",
        "added table_restores.completed_at",
        "backfilled 2 restore registration(s) as legacy restores, 1 of them RUNNING",
        "wrote settings.catalog_id",
    ] {
        assert!(notes.contains(expected), "{expected} not in {notes}");
    }
    let mut text = dump(
        &db,
        "SELECT t.table_name, t.table_status, r.status, r.kind, r.owner_generation, \
         r.heartbeat_at, r.store_kind, r.completed_at = t.creation_date_time \
         FROM table_restores r JOIN tables t ON t.table_id = r.table_id ORDER BY t.table_name",
    )
    .await;
    text.push_str(
        &dump(
            &db,
            "SELECT name FROM pragma_table_info('table_restores') ORDER BY cid",
        )
        .await,
    );
    text.push_str(
        &dump(
            &db,
            "SELECT name, type FROM sqlite_master WHERE name LIKE 'backup_%' OR name LIKE \
             'idx_backup_%' OR name = 'idx_table_restores_status' ORDER BY name",
        )
        .await,
    );
    text.push_str(&dump(&db, "SELECT notes FROM extenddb_migrations WHERE id = 5").await);
    golden("0005_backup_catalog_index", &text);

    // A second run (through reconverge, which runs every convergent body
    // again) adds nothing and keeps the catalog id.
    reconverge(&db).await.expect("reconverge");
    assert_eq!(
        scalar(&db, "SELECT value FROM settings WHERE key = 'catalog_id'")
            .await
            .as_deref(),
        Some(catalog_id.as_str())
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM pragma_table_info('table_restores')"
        )
        .await,
        16
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT status FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
             WHERE t.table_name = 'interrupted'"
        )
        .await
        .as_deref(),
        Some("RUNNING")
    );
}
