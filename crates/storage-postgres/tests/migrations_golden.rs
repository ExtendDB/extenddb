// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Golden tests of the PostgreSQL code bodies: each runs on a released
//! fixture prepared with the damage it repairs, and the rows it changed and
//! its ledger notes are compared with `tests/fixtures/golden/000N_<name>.txt`.
//! `EXTENDDB_BLESS_EXPECTED=1` writes the files instead. See
//! `migrations_support` for the database these tests need.

mod migrations_support;

use std::fmt::Write as _;
use std::time::Duration;

use extenddb_storage::migrations::{CatalogStatus, MigrationError, MigrationId};
use extenddb_storage_postgres::migrations::migrate;
use migrations_support::{ACCOUNT, Scratch, base_conn, id_names, skip};
use sqlx::PgPool;

fn golden(name: &str, actual: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/golden")
        .join(name);
    if migrations_support::bless() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    assert_eq!(actual, expected, "{name} differs from its golden file");
}

async fn notes(catalog: &PgPool, id: i32) -> String {
    sqlx::query_scalar("SELECT notes FROM extenddb_migrations WHERE id = $1")
        .bind(id)
        .fetch_one(catalog)
        .await
        .unwrap()
}

const TAGGED: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"dynamodb:ResourceTag/env":"prod"}}}]}"#;

/// A `dynamodb:ResourceTag` policy in every place a policy is stored.
async fn tag_every_policy(catalog: &PgPool) {
    for sql in [
        "UPDATE iam_policies SET policy_document = $1::jsonb WHERE principal_name = 'tester'",
        "INSERT INTO iam_groups (account_id, group_name, group_arn) VALUES ('123456789012', 'g1', 'arn:aws:iam::123456789012:group/g1')",
        "INSERT INTO iam_policies (account_id, principal_type, principal_name, policy_name, policy_document) VALUES ('123456789012', 'group', 'g1', 'gp', $1::jsonb)",
        "INSERT INTO iam_roles (account_id, role_name, role_arn, trust_policy) VALUES ('123456789012', 'r1', 'arn:aws:iam::123456789012:role/r1', '{}')",
        "INSERT INTO iam_policies (account_id, principal_type, principal_name, policy_name, policy_document) VALUES ('123456789012', 'role', 'r1', 'rp', $1::jsonb)",
        "INSERT INTO iam_permissions_boundaries (account_id, principal_type, principal_name, policy_document) VALUES ('123456789012', 'user', 'tester', $1::jsonb)",
        "INSERT INTO iam_permissions_boundaries (account_id, principal_type, principal_name, policy_document) VALUES ('123456789012', 'role', 'r1', $1::jsonb)",
        "INSERT INTO iam_sessions (session_token, access_key_id, secret_key_encrypted, account_id, role_name, session_name, session_policy, expires_at) VALUES ('tok', 'ASIAEXAMPLE', '\\x00', '123456789012', 'r1', 's', $1::jsonb, now() + interval '1 day')",
    ] {
        let q = sqlx::query(sql);
        let q = if sql.contains("$1") {
            q.bind(TAGGED)
        } else {
            q
        };
        q.execute(catalog)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[tokio::test]
async fn migration_2_runs_again_after_a_lock_timeout() {
    let Some(base) = base_conn() else {
        return skip("migration 2 lock timeout");
    };
    let s = Scratch::new(&base, "pgm_g2lock").await;
    s.load("0.1.14").await;
    s.migrate_through(MigrationId::Baseline).await;
    let catalog = s.catalog().await;
    tag_every_policy(&catalog).await;
    // Another session holds the policies table in SHARE mode for 7 s: the
    // body's reads go on, and its first write waits past the runner's 5 s
    // lock timeout. The management store's error keeps SQLSTATE 55P03, so
    // the runner runs the body again, and the second attempt writes once the
    // lock is released.
    let holder = migrations_support::pool(&s.catalog_url(), 1).await;
    let mut held = holder.begin().await.unwrap();
    sqlx::query("LOCK TABLE iam_policies IN SHARE MODE")
        .execute(&mut *held)
        .await
        .unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(7)).await;
        held.commit().await.unwrap();
        holder.close().await;
    });
    let started = std::time::Instant::now();
    s.migrate_through(MigrationId::ResourceTagConditionKeys)
        .await;
    release.await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_secs(5),
        "the body did not wait for the lock: {:?}",
        started.elapsed()
    );
    let left: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM iam_policies WHERE policy_document::text LIKE '%dynamodb:ResourceTag%'",
    )
    .fetch_one(&catalog)
    .await
    .unwrap();
    assert_eq!(left, 0, "a policy was not rewritten");
    catalog.close().await;
}

#[tokio::test]
async fn migration_2_rewrites_resource_tag_condition_keys_in_every_place() {
    let Some(base) = base_conn() else {
        return skip("golden 2");
    };
    let s = Scratch::new(&base, "pgm_g2").await;
    s.load("0.1.14").await;
    s.migrate_through(MigrationId::Baseline).await;
    let catalog = s.catalog().await;
    tag_every_policy(&catalog).await;
    s.migrate_through(MigrationId::ResourceTagConditionKeys)
        .await;
    let mut out = String::new();
    // Sorted by byte order (`COLLATE "C"`), so the file is the same whatever
    // the database's collation and C library.
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT * FROM (SELECT 'policy' AS kind, principal_type || ' ' || principal_name || ' ' || policy_name AS name, policy_document::text, '' \
         FROM iam_policies UNION ALL \
         SELECT 'boundary', principal_type || ' ' || principal_name, policy_document::text, '' FROM iam_permissions_boundaries UNION ALL \
         SELECT 'session', session_token, session_policy::text, '' FROM iam_sessions) AS r \
         ORDER BY kind COLLATE \"C\", name COLLATE \"C\"",
    )
    .fetch_all(&catalog)
    .await
    .unwrap();
    for (kind, name, document, _) in rows {
        writeln!(out, "{kind} {name}: {document}").unwrap();
    }
    // The body visits policies in the order the management store lists
    // them, which follows the database's collation, so the notes are sorted.
    let mut rewrote: Vec<String> = notes(&catalog, 2)
        .await
        .lines()
        .map(str::to_owned)
        .collect();
    rewrote.sort();
    writeln!(out, "notes:\n{}", rewrote.join("\n")).unwrap();
    golden("0002_resource_tag_condition_keys.txt", &out);
    catalog.close().await;
}

#[tokio::test]
async fn migration_2_collisions_stop_the_run_before_any_write() {
    let Some(base) = base_conn() else {
        return skip("golden 2 collision");
    };
    let s = Scratch::new(&base, "pgm_g2c").await;
    s.load("0.1.14").await;
    let catalog = s.catalog().await;
    let both = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"dynamodb:ResourceTag/env":"prod","aws:ResourceTag/env":"dev"}}}]}"#;
    sqlx::query(
        "UPDATE iam_policies SET policy_document = $1::jsonb WHERE principal_name = 'tester'",
    )
    .bind(both)
    .execute(&catalog)
    .await
    .unwrap();
    assert!(matches!(
        s.status().await,
        CatalogStatus::NeedsAttention { .. }
    ));
    let err = migrate(&s.target(), "test", Duration::from_secs(5))
        .await
        .expect_err("refused");
    assert!(
        matches!(
            err,
            MigrationError::Status(CatalogStatus::NeedsAttention { .. })
        ),
        "{err}"
    );
    assert_eq!(err.exit_code(), 5);
    let ledger: bool =
        sqlx::query_scalar("SELECT to_regclass('public.extenddb_migrations') IS NOT NULL")
            .fetch_one(&catalog)
            .await
            .unwrap();
    assert!(!ledger, "a refused run wrote the ledger");
    catalog.close().await;
}

#[tokio::test]
async fn migration_3_restores_sort_key_definitions_in_tables_and_backups() {
    let Some(base) = base_conn() else {
        return skip("golden 3");
    };
    let s = Scratch::new(&base, "pgm_g3").await;
    s.load("0.1.14").await;
    s.migrate_through(MigrationId::ResourceTagConditionKeys)
        .await;
    let catalog = s.catalog().await;
    // The pre-fix UpdateTable kept only the GSI's definitions: the table's
    // sort key `sk` and partition key `pk` are gone, from the table and from
    // the backup taken of it.
    let damaged = r#"[{"AttributeName":"gpk","AttributeType":"S"},{"AttributeName":"lsk","AttributeType":"N"}]"#;
    for sql in [
        "UPDATE tables SET attribute_definitions = $1::jsonb WHERE table_name = 'seed_tbl'",
        "UPDATE backups SET attribute_definitions = $1::jsonb WHERE table_name = 'seed_tbl'",
    ] {
        sqlx::query(sql)
            .bind(damaged)
            .execute(&catalog)
            .await
            .unwrap();
    }
    // A backup with no items whose source table is gone gives no type.
    sqlx::query(
        "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, key_schema, attribute_definitions) \
         VALUES ('arn:orphan', 'orphan', 'gone', 'gone_tbl', $1, \
         '[{\"AttributeName\":\"pk\",\"KeyType\":\"HASH\"},{\"AttributeName\":\"sk\",\"KeyType\":\"RANGE\"}]', '[]')",
    )
    .bind(ACCOUNT)
    .execute(&catalog)
    .await
    .unwrap();
    s.migrate_through(MigrationId::SortKeyDefinitions).await;
    let mut out = String::new();
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT * FROM (SELECT 'table ' || table_name AS name, attribute_definitions::text FROM tables UNION ALL \
         SELECT 'backup ' || backup_name, attribute_definitions::text FROM backups) AS r \
         ORDER BY name COLLATE \"C\"",
    )
    .fetch_all(&catalog)
    .await
    .unwrap();
    for (name, defs) in rows {
        writeln!(out, "{name}: {defs}").unwrap();
    }
    let arn: String =
        sqlx::query_scalar("SELECT backup_arn FROM backups WHERE backup_name = 'seed_backup'")
            .fetch_one(&catalog)
            .await
            .unwrap();
    writeln!(
        out,
        "notes:\n{}",
        notes(&catalog, 3).await.replace(&arn, "<seed_backup>")
    )
    .unwrap();
    golden("0003_sort_key_definitions.txt", &out);
    catalog.close().await;
}

#[tokio::test]
async fn migration_4_adds_the_base_key_index_and_skips_what_it_cannot_reach() {
    let Some(base) = base_conn() else {
        return skip("golden 4");
    };
    let s = Scratch::new(&base, "pgm_g4").await;
    // Index tables created by 0.1.5 have no base key index.
    s.load("0.1.5").await;
    s.migrate_through(MigrationId::SortKeyDefinitions).await;
    let catalog = s.catalog().await;
    // A table being deleted, and a table whose index table is gone.
    for (name, status, table_id, index_id) in [
        (
            "going",
            "DELETING",
            "00000000-0000-4000-8000-000000000001",
            "00000000-0000-4000-8000-0000000000a1",
        ),
        (
            "lost",
            "ACTIVE",
            "00000000-0000-4000-8000-000000000002",
            "00000000-0000-4000-8000-0000000000a2",
        ),
    ] {
        sqlx::query(
            "INSERT INTO tables (account_id, table_name, key_schema, attribute_definitions, table_status, table_arn, table_id) \
             VALUES ($1, $2, '[]', '[]', $3, 'arn:' || $2, $4)",
        )
        .bind(ACCOUNT)
        .bind(name)
        .bind(status)
        .bind(table_id)
        .execute(&catalog)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO indexes (table_id, index_id, index_name, index_type, key_schema, projection) \
             VALUES ($1, $2, $3 || '_gsi', 'GSI', '[]', '{}')",
        )
        .bind(table_id)
        .bind(index_id)
        .bind(name)
        .execute(&catalog)
        .await
        .unwrap();
    }
    s.migrate_through(MigrationId::IndexTableBaseKey).await;
    let data = s.data().await;
    let names = id_names(&catalog).await;
    let mut lines: Vec<String> = sqlx::query_as::<_, (String, bool)>(
        "SELECT pg_get_indexdef(i.indexrelid), i.indisvalid FROM pg_index i \
         JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname LIKE '%\\_base\\_key\\_idx'",
    )
    .fetch_all(&data)
    .await
    .unwrap()
    .into_iter()
    .map(|(def, valid)| {
        let mut def = def;
        for (id, name) in &names {
            def = def.replace(id.as_str(), name);
        }
        format!("{def} valid={valid}")
    })
    .collect();
    lines.sort();
    let mut out = lines.join("\n");
    let mut note = notes(&data, 4).await;
    for (id, name) in &names {
        note = note.replace(id.as_str(), name);
    }
    write!(out, "\nnotes:\n{note}\n").unwrap();
    golden("0004_index_table_base_key.txt", &out);
    catalog.close().await;
    data.close().await;
}
