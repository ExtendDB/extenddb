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

use extenddb_storage::migrations::{CatalogStatus, MigrationError, MigrationId, Runner};
use extenddb_storage_postgres::migrations::{PostgresMigrations, PostgresTarget, migrate};
use migrations_support::{ACCOUNT, LATEST, Scratch, base_conn, id_names, skip};
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
/// Both forms on one key with different values, which migration 2 refuses.
const COLLIDING: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"dynamodb:ResourceTag/env":"prod","aws:ResourceTag/env":"dev"}}}]}"#;

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
    sqlx::query(
        "UPDATE iam_policies SET policy_document = $1::jsonb WHERE principal_name = 'tester'",
    )
    .bind(COLLIDING)
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

/// The text of every stored policy, sorted by byte order.
async fn every_policy_document(catalog: &PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT kind || ' ' || name || ': ' || document FROM (SELECT 'policy' AS kind, \
         principal_type || ' ' || principal_name || ' ' || policy_name AS name, \
         policy_document::text AS document FROM iam_policies UNION ALL \
         SELECT 'boundary', principal_type || ' ' || principal_name, policy_document::text \
         FROM iam_permissions_boundaries UNION ALL \
         SELECT 'session', session_token, session_policy::text FROM iam_sessions) AS r \
         ORDER BY kind COLLATE \"C\", name COLLATE \"C\"",
    )
    .fetch_all(catalog)
    .await
    .unwrap()
}

/// Record what a release before 1.0 leaves when it writes to a migrated
/// catalog, then reconverge: migration 2 is named as skipped, and the
/// catalog is current again.
async fn reconverge_after_an_older_release_wrote(s: &Scratch, catalog: &PgPool) {
    sqlx::query("INSERT INTO settings (key, value) VALUES ('resource_tag_migration', 'applied')")
        .execute(catalog)
        .await
        .unwrap();
    assert_eq!(s.status().await.label(), "WrittenByOlderRelease");
    let m = PostgresMigrations::open_unlocked(&s.target())
        .await
        .unwrap();
    let report = Runner::new(&m, "test").reconverge().await;
    m.close().await;
    let report = report.expect("reconverge");
    assert!(report.applied.iter().all(|step| step.id != 2), "{report:?}");
    assert!(
        report
            .notes
            .iter()
            .any(|n| n.starts_with("Skipped 2 resource_tag_condition_keys:")),
        "{report:?}"
    );
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
}

/// From 1.0 on a `dynamodb:ResourceTag` key written through the API has no
/// effect, as on the service. Migration 2 is not convergent, so a reconverge
/// leaves a policy written after it, in each place a policy is stored, with
/// the text it was written with, and so with the decision it had.
#[tokio::test]
async fn a_reconverge_leaves_policies_written_after_migration_2_as_they_are() {
    let Some(base) = base_conn() else {
        return skip("reconverge after migration 2");
    };
    let s = Scratch::new(&base, "pgm_g2rc").await;
    s.load("0.1.14").await;
    s.migrate_through(MigrationId::LATEST).await;
    let catalog = s.catalog().await;
    tag_every_policy(&catalog).await;
    let before = every_policy_document(&catalog).await;
    assert!(
        before
            .iter()
            .any(|d| d.contains("dynamodb:ResourceTag/env")),
        "{before:?}"
    );
    reconverge_after_an_older_release_wrote(&s, &catalog).await;
    assert_eq!(every_policy_document(&catalog).await, before);
    catalog.close().await;
}

/// A policy written after migration 2 with both forms on one key, which
/// the rewrite would refuse, does not stop a reconverge.
#[tokio::test]
async fn a_reconverge_is_not_stopped_by_a_policy_migration_2_would_refuse() {
    let Some(base) = base_conn() else {
        return skip("reconverge after migration 2, both forms");
    };
    let s = Scratch::new(&base, "pgm_g2rcc").await;
    s.load("0.1.14").await;
    s.migrate_through(MigrationId::LATEST).await;
    let catalog = s.catalog().await;
    sqlx::query(
        "UPDATE iam_policies SET policy_document = $1::jsonb WHERE principal_name = 'tester'",
    )
    .bind(COLLIDING)
    .execute(&catalog)
    .await
    .unwrap();
    let before = every_policy_document(&catalog).await;
    reconverge_after_an_older_release_wrote(&s, &catalog).await;
    assert_eq!(every_policy_document(&catalog).await, before);
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

/// The physical tables of `seed_tbl` and their text key columns.
async fn seed_tbl_tables(catalog: &PgPool) -> Vec<(String, &'static [&'static str])> {
    let table: String =
        sqlx::query_scalar("SELECT table_id FROM tables WHERE table_name = 'seed_tbl'")
            .fetch_one(catalog)
            .await
            .unwrap();
    let indexes: Vec<String> =
        sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = $1 ORDER BY index_name")
            .bind(&table)
            .fetch_all(catalog)
            .await
            .unwrap();
    let mut tables: Vec<(String, &'static [&'static str])> =
        vec![(format!("_ddb_{table}"), &["pk", "sk_s"])];
    for index in indexes {
        tables.push((
            format!("_ddb_{index}"),
            &["pk", "sk_s", "base_pk", "base_sk_s"],
        ));
    }
    tables
}

/// The attribute every legacy row gets: a name and a value holding U+0001
/// as an earlier release stored it, the value followed by U+0002.
const LEGACY_NOTE: &str = "item_data || jsonb_build_object('note' || chr(1), \
     jsonb_build_object('S', 'v' || chr(1) || chr(2)))";

/// Each row of `table` as `<table> pk=.. sk=.. <note attributes>`, with the
/// strings in Rust's escaped form, so a control character is visible.
async fn rows_of(pool: &PgPool, table: &str, label: &str, key: [&str; 2]) -> Vec<String> {
    let rows: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(&format!(
        "SELECT {}, {}, (SELECT string_agg(key || '=' || (value->>'S'), ',' ORDER BY key) \
         FROM jsonb_each(item_data) WHERE key LIKE 'note%') FROM \"{table}\"",
        key[0], key[1]
    ))
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|(pk, sk, note)| format!("{label} pk={pk:?} sk={sk:?} {note:?}"))
        .collect()
}

/// Migration 6 on the 0.1.14 fixture whose strings an earlier release
/// stored with a raw U+0001: in the key columns of `seed_tbl`'s table, GSI
/// and LSI, in their documents, and in the backup's `backup_items`.
#[tokio::test]
async fn migration_6_escapes_the_strings_an_earlier_release_stored() {
    let Some(base) = base_conn() else {
        return skip("golden 6");
    };
    let s = Scratch::new(&base, "pgm_g6").await;
    s.load("0.1.14").await;
    s.migrate_through(MigrationId::DropContinuousBackups).await;
    let catalog = s.catalog().await;
    let data = s.data().await;
    let tables = seed_tbl_tables(&catalog).await;
    for (table, columns) in &tables {
        let keys: Vec<String> = columns
            .iter()
            .map(|c| format!("{c} = {c} || chr(1)"))
            .collect();
        sqlx::query(&format!(
            "UPDATE \"{table}\" SET {}, item_data = {LEGACY_NOTE}",
            keys.join(", ")
        ))
        .execute(&data)
        .await
        .unwrap();
    }
    sqlx::query(&format!(
        "UPDATE backup_items SET pk = pk || chr(1), sk = sk || chr(1), item_data = {LEGACY_NOTE}"
    ))
    .execute(&catalog)
    .await
    .unwrap();
    // A status read does not look for other sessions: this test's own are
    // open.
    assert_eq!(s.status().await.label(), "Behind");
    catalog.close().await;
    data.close().await;
    s.migrate_through(MigrationId::EscapeControlChars).await;
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });

    let catalog = s.catalog().await;
    let data = s.data().await;
    let names = id_names(&catalog).await;
    let mut lines = Vec::new();
    for (table, columns) in &tables {
        let mut label = table.clone();
        for (id, name) in &names {
            label = label.replace(id.as_str(), name);
        }
        let mut rows = rows_of(&data, table, &label, [columns[0], columns[1]]).await;
        if columns.len() > 2 {
            // An index row also names its base item.
            rows = rows_of(
                &data,
                table,
                &format!("{label} base"),
                ["base_pk", "base_sk_s"],
            )
            .await
            .into_iter()
            .chain(rows)
            .collect();
        }
        lines.extend(rows);
    }
    lines.extend(rows_of(&catalog, "backup_items", "backup_items", ["pk", "sk"]).await);
    lines.sort();
    let mut out = lines.join("\n");
    for (pool, store) in [(&catalog, "catalog"), (&data, "data")] {
        let mut note = notes(pool, 6).await;
        for (id, name) in &names {
            note = note.replace(id.as_str(), name);
        }
        write!(out, "\n{store} notes:\n{note}").unwrap();
    }
    out.push('\n');
    golden("0006_escape_control_chars.txt", &out);
    catalog.close().await;
    data.close().await;
}

/// The id-6 rows of the ledger of the database `pool` reaches.
async fn ledger_rows_of_6(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM extenddb_migrations WHERE id = 6")
        .fetch_one(pool)
        .await
        .unwrap()
}

/// While another session holds the data database of a store with tables,
/// `migrate --yes` stops with exit 5 before its first write, naming the
/// session, and migrates once the session is gone.
#[tokio::test]
async fn migration_6_stops_before_any_write_while_another_session_holds_the_data() {
    let Some(base) = base_conn() else {
        return skip("migration 6 sessions check");
    };
    let s = Scratch::new(&base, "pgm_g6s").await;
    s.load("0.1.14").await;
    s.migrate_through(MigrationId::DropContinuousBackups).await;
    // A server of an earlier release, still connected.
    let server = migrations_support::pool(&s.data_url(), 1).await;
    sqlx::query("SELECT 1").execute(&server).await.unwrap();
    // The preview runs beside it.
    assert_eq!(s.status().await.label(), "Behind");
    let err = migrate(
        &s.target_checking_sessions(),
        "test",
        Duration::from_secs(5),
    )
    .await
    .expect_err("refused");
    assert_eq!(err.exit_code(), 5, "{err:?}");
    let text = format!("{err:?}");
    assert!(
        text.contains(&format!(
            "1 other session(s) hold the data database {}",
            s.data_name
        )),
        "{text}"
    );
    let catalog = s.catalog().await;
    let data = s.data().await;
    assert_eq!(
        ledger_rows_of_6(&catalog).await,
        0,
        "the refused run wrote the catalog ledger"
    );
    assert_eq!(
        ledger_rows_of_6(&data).await,
        0,
        "the refused run wrote the data ledger"
    );
    catalog.close().await;
    data.close().await;
    server.close().await;
    // Connection strings that name an application, as an operator's can: the
    // runner names its own sessions over them, so the check does not count
    // them.
    let catalog = s.catalog().await;
    sqlx::query("UPDATE settings SET value = $1 WHERE key = 'data_database_connection_string'")
        .bind(format!("{}?application_name=psql", s.data_url()))
        .execute(&catalog)
        .await
        .unwrap();
    catalog.close().await;
    let named = || {
        PostgresTarget::new(
            format!("{}?application_name=psql", s.catalog_url())
                .parse()
                .unwrap(),
            &s.data_name,
        )
    };
    // PostgreSQL removes a closed session from pg_stat_activity a moment
    // after the close returns.
    let mut result = migrate(&named(), "test", Duration::from_secs(5)).await;
    for _ in 0..50 {
        if result.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        result = migrate(&named(), "test", Duration::from_secs(5)).await;
    }
    result.expect("migrate once the session is gone");
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
}
