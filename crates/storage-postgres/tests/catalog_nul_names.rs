// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The catalog round trip of attribute names that hold U+0000 or U+0001.
//!
//! DynamoDB accepts U+0000 in a key attribute name, an attribute definition,
//! an index key attribute name and a projected attribute name, and
//! `DescribeTable` returns each name byte for byte (measured 2026-09-18,
//! 2026-10-24 and 2026-11-04). PostgreSQL `jsonb` refuses U+0000, so the
//! catalog stores these names through the escape of `catalog_json` (U+0000 as
//! U+0001 U+0001, U+0001 as U+0001 U+0002). The table here holds U+0000, a
//! raw U+0001 pair (the stored form of U+0000, which the decoder must not
//! read as U+0000) and U+0001 U+0002 (the stored form of U+0001) in its key
//! schema, its GSI and LSI keys and its projected names. Each place that
//! reads the names back returns them byte for byte: `DescribeTable`, the key
//! info of the data plane, `DescribeStream`, `DescribeBackup`, the restore
//! source, a restored table, the `backup_index` row that `backup
//! migrate-legacy` writes, `UpdateTable` and `DeleteTable`.
//!
//! See `catalog_nul_support` for the database these tests need.

mod catalog_nul_support;

use catalog_nul_support::{
    ACCOUNT, Options, REGION, base_conn, def_names, documents, include, key, names, s_attr,
    scratch, skip, stored_text, table_documents, wait_active,
};
use extenddb_core::backup_format::BackupArn;
use extenddb_core::types::{
    BillingMode, CreateTableInput, DeleteTableInput, DescribeStreamInput, DescribeTableInput,
    GsiInput, KeyType, LsiInput, StreamSpecification, StreamViewType, UpdateTableInput,
};
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, Replaced, StoreIdentity, StoreKind,
};
use extenddb_storage::backup_definition::{LegacyVectorState, source_table_from_legacy};
use extenddb_storage::{
    BackupCatalog, JobPermit, LegacyBackupReader, RestoreRequest, RestoreTableOverrides,
    StreamEngine, TableEngine,
};

/// U+0000 in the partition key.
const PK: &str = "p\u{0}k";
/// A raw U+0001 pair in the sort key.
const SK: &str = "s\u{1}\u{1}k";
/// U+0000 in the GSI partition key.
const GK: &str = "g\u{0}k";
/// U+0001 U+0002 in the GSI sort key.
const GSK: &str = "h\u{1}\u{2}k";
/// Both characters in the LSI sort key.
const LK: &str = "l\u{0}\u{1}k";
/// The projected names of both indexes.
const PROJECTED: [&str; 2] = ["n\u{0}k", "m\u{1}k"];

fn input(table: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: table.to_owned(),
        key_schema: vec![key(PK, KeyType::Hash), key(SK, KeyType::Range)],
        attribute_definitions: vec![s_attr(PK), s_attr(SK), s_attr(GK), s_attr(GSK), s_attr(LK)],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "gsi1".to_owned(),
            key_schema: vec![key(GK, KeyType::Hash), key(GSK, KeyType::Range)],
            projection: include(&PROJECTED),
            provisioned_throughput: None,
            warm_throughput: None,
        }]),
        local_secondary_indexes: Some(vec![LsiInput {
            index_name: "lsi1".to_owned(),
            key_schema: vec![key(PK, KeyType::Hash), key(LK, KeyType::Range)],
            projection: include(&PROJECTED),
        }]),
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        }),
        ..Default::default()
    }
}

fn projected(p: &extenddb_core::types::Projection) -> Vec<&str> {
    p.non_key_attributes
        .iter()
        .flatten()
        .map(String::as_str)
        .collect()
}

/// Every name of the table, in its stored form, and no U+0000.
fn assert_stored_escaped(text: &str, what: &str) {
    assert!(!text.contains("\\u0000"), "{what} holds U+0000: {text}");
    for name in [PK, SK, GK, GSK, LK, PROJECTED[0], PROJECTED[1]] {
        let stored = format!("\"{}\"", stored_text(name));
        assert!(text.contains(&stored), "{what}: no {stored} in {text}");
    }
}

fn update(table: &str, json: serde_json::Value) -> UpdateTableInput {
    let mut json = json;
    json["TableName"] = serde_json::Value::String(table.to_owned());
    serde_json::from_value(json).expect("an UpdateTable request")
}

#[tokio::test]
async fn names_with_u0000_and_u0001_round_trip_through_the_catalog() {
    let test = "names_with_u0000_and_u0001_round_trip_through_the_catalog";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(test, Options::default()).await.expect("a scratch");
    let table = "nul_names";
    let created = s
        .engine
        .create_table(ACCOUNT, input(table))
        .await
        .expect("CreateTable with U+0000 and U+0001 in attribute names");
    assert_eq!(names(&created.key_schema), [PK, SK]);
    assert_stored_escaped(&table_documents(&s.catalog, table).await, "the table rows");

    let desc = s
        .engine
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
    assert_eq!(names(&gsi.key_schema), [GK, GSK]);
    assert_eq!(projected(&gsi.projection), PROJECTED);
    let lsi = &desc.local_secondary_indexes.as_deref().expect("LSIs")[0];
    assert_eq!(names(&lsi.key_schema), [PK, LK]);
    assert_eq!(projected(&lsi.projection), PROJECTED);

    // The key info that the data plane and authorization read.
    let info = s
        .engine
        .table_key_info(ACCOUNT, table)
        .await
        .expect("table_key_info");
    assert_eq!(names(&info.key_schema), [PK, SK]);
    assert_eq!(
        def_names(&info.attribute_definitions),
        def_names(&input(table).attribute_definitions)
    );
    assert_eq!(
        names(&info.global_secondary_indexes[0].key_schema),
        [GK, GSK]
    );
    assert_eq!(
        projected(&info.global_secondary_indexes[0].projection),
        PROJECTED
    );
    assert_eq!(names(&info.local_secondary_indexes[0].key_schema), [PK, LK]);

    // DescribeStream reads the key schema on its own path.
    let stream = s
        .engine
        .describe_stream(
            ACCOUNT,
            &DescribeStreamInput {
                stream_arn: desc.latest_stream_arn.clone().expect("the stream ARN"),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .expect("DescribeStream");
    assert_eq!(names(&stream.key_schema), [PK, SK]);

    // A backup copies the documents into `backups` and `backup_definitions`.
    let backup = s
        .engine
        .create(ACCOUNT, table, "bk367", JobPermit::detached())
        .await
        .expect("CreateBackup");
    let stored = documents(
        &s.catalog,
        "SELECT b.key_schema::text || ' ' || b.attribute_definitions::text || ' ' || \
         d.definition::text FROM backups b \
         JOIN backup_definitions d ON d.backup_arn = b.backup_arn WHERE b.backup_arn = $1",
        &backup.backup_arn,
    )
    .await
    .join(" ");
    assert_stored_escaped(&stored, "the backup rows");
    let described = s
        .engine
        .describe(ACCOUNT, &backup.backup_arn)
        .await
        .expect("DescribeBackup");
    assert_eq!(names(&described.source_table_details.key_schema), [PK, SK]);
    let source = s
        .engine
        .restore_source(ACCOUNT, &backup.backup_arn)
        .await
        .expect("the restore source");
    assert_eq!(names(&source.key_schema), [PK, SK]);
    assert_eq!(
        def_names(&source.attribute_definitions),
        def_names(&input(table).attribute_definitions)
    );
    let definition = source.definition.as_ref().expect("the backup definition");
    let bgsi = &definition.global_secondary_indexes[0];
    assert_eq!(names(&bgsi.key_schema), [GK, GSK]);
    assert_eq!(projected(&bgsi.projection), PROJECTED);
    let blsi = &definition.local_secondary_indexes[0];
    assert_eq!(names(&blsi.key_schema), [PK, LK]);
    assert_eq!(projected(&blsi.projection), PROJECTED);

    // The restore target is created from the restore source.
    let restored_name = "nul_names_restored";
    let restore_input = source.restore_input(restored_name, &RestoreTableOverrides::default());
    extenddb_core::validation::validate_create_table(
        &restore_input,
        &extenddb_core::limits::LimitsConfig::default(),
    )
    .expect("the restore input validates as CreateTable does");
    s.engine
        .restore(
            ACCOUNT,
            RestoreRequest {
                backup_arn: backup.backup_arn.clone(),
                source_table_arn: source.source_table_arn.clone(),
                restore_date_time: source.backup_creation_date_time,
                input: restore_input,
            },
            JobPermit::detached(),
        )
        .await
        .expect("RestoreTableFromBackup");
    wait_active(&s.catalog, restored_name).await;
    assert_stored_escaped(
        &table_documents(&s.catalog, restored_name).await,
        "the restored table rows",
    );
    let restored = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: restored_name.to_owned(),
            },
        )
        .await
        .expect("DescribeTable of the restored table");
    assert_eq!(names(&restored.key_schema), [PK, SK]);
    assert_eq!(
        def_names(&restored.attribute_definitions),
        def_names(&input(table).attribute_definitions)
    );
    let rgsi = &restored.global_secondary_indexes.as_deref().expect("GSIs")[0];
    assert_eq!(names(&rgsi.key_schema), [GK, GSK]);
    assert_eq!(projected(&rgsi.projection), PROJECTED);
    let rlsi = &restored.local_secondary_indexes.as_deref().expect("LSIs")[0];
    assert_eq!(names(&rlsi.key_schema), [PK, LK]);
    assert_eq!(projected(&rlsi.projection), PROJECTED);

    // `backup migrate-legacy` replaces the in-catalog backup by its
    // `backup_index` row, whose source table carries the names.
    let arn = BackupArn::parse(&backup.backup_arn).expect("the backup ARN");
    let migrated =
        source_table_from_legacy(&source, LegacyVectorState::NoneRecorded, REGION, ACCOUNT)
            .expect("the source table of the index row");
    let row = BackupRow {
        backup_id: arn.backup_id().to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_name: table.to_owned(),
        backup_arn: backup.backup_arn.clone(),
        backup_name: "bk367".to_owned(),
        status: BackupStatus::Available,
        store: StoreIdentity {
            store_kind: StoreKind::Filesystem,
            store_root: "/srv/extenddb-backups".to_owned(),
            store_id: "store-1".to_owned(),
        },
        size_bytes: 0,
        item_count: source.item_count,
        created_at: source.backup_creation_date_time,
        completed_at: None,
        failure_reason: None,
        source_table_details_json: serde_json::to_value(&migrated).expect("JSON"),
        owner_generation: 0,
        heartbeat_at: 0.0,
    };
    assert_eq!(
        s.engine
            .replace_with_index_row(&row)
            .await
            .expect("the replacement"),
        Replaced::Replaced
    );
    let stored = documents(
        &s.catalog,
        "SELECT source_table_details_json::text FROM backup_index WHERE backup_arn = $1",
        &backup.backup_arn,
    )
    .await
    .join(" ");
    assert_stored_escaped(&stored, "the backup_index row");
    let read = s
        .engine
        .read_backup_row(&arn)
        .await
        .expect("read the backup_index row")
        .expect("the backup_index row");
    let schema = read.source_table().expect("the source table");
    assert_eq!(names(&schema.key_schema), [PK, SK]);

    // Deleting the GSI prunes its definitions, compared and rewritten in
    // the stored form; a GSI is then created on the U+0000 attribute again.
    let after_delete = s
        .engine
        .update_table(
            ACCOUNT,
            update(
                table,
                serde_json::json!({
                    "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "gsi1"}}]
                }),
            ),
        )
        .await
        .expect("UpdateTable deleting the GSI");
    assert!(
        after_delete
            .global_secondary_indexes
            .as_deref()
            .unwrap_or(&[])
            .is_empty()
    );
    assert_eq!(def_names(&after_delete.attribute_definitions), {
        let mut v = vec![PK, SK, LK];
        v.sort_unstable();
        v
    });
    let after_create = s
        .engine
        .update_table(
            ACCOUNT,
            update(
                table,
                serde_json::json!({
                    "AttributeDefinitions": [{"AttributeName": GK, "AttributeType": "S"}],
                    "GlobalSecondaryIndexUpdates": [{"Create": {
                        "IndexName": "gsi2",
                        "KeySchema": [{"AttributeName": GK, "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": PROJECTED}
                    }}]
                }),
            ),
        )
        .await
        .expect("UpdateTable creating a GSI on the U+0000 attribute");
    let gsi2 = &after_create
        .global_secondary_indexes
        .as_deref()
        .expect("GSIs")[0];
    assert_eq!(gsi2.index_name, "gsi2");
    assert_eq!(names(&gsi2.key_schema), [GK]);
    assert_eq!(projected(&gsi2.projection), PROJECTED);
    assert_eq!(def_names(&after_create.attribute_definitions), {
        let mut v = vec![PK, SK, LK, GK];
        v.sort_unstable();
        v
    });
    let text = table_documents(&s.catalog, table).await;
    assert!(!text.contains("\\u0000"), "{text}");
    assert!(text.contains(&format!("\"{}\"", stored_text(GK))), "{text}");

    let deleted = s
        .engine
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
async fn names_without_either_character_are_stored_as_before() {
    let test = "names_without_either_character_are_stored_as_before";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch(test, Options::default()).await.expect("a scratch");
    let table = "plain_names";
    let plain = CreateTableInput {
        table_name: table.to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: vec![s_attr("pk"), s_attr("sk"), s_attr("gk")],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "gsi1".to_owned(),
            key_schema: vec![key("gk", KeyType::Hash)],
            projection: include(&["nk"]),
            provisioned_throughput: None,
            warm_throughput: None,
        }]),
        ..Default::default()
    };
    s.engine
        .create_table(ACCOUNT, plain.clone())
        .await
        .expect("CreateTable");
    // The stored documents are the plain serialization.
    let (ks, ad): (serde_json::Value, serde_json::Value) = sqlx::query_as(
        "SELECT key_schema, attribute_definitions FROM tables WHERE table_name = $1",
    )
    .bind(table)
    .fetch_one(&s.catalog)
    .await
    .expect("read the table row");
    assert_eq!(ks, serde_json::to_value(&plain.key_schema).unwrap());
    assert_eq!(
        ad,
        serde_json::to_value(&plain.attribute_definitions).unwrap()
    );
    let (iks, proj): (serde_json::Value, serde_json::Value) =
        sqlx::query_as("SELECT key_schema, projection FROM indexes WHERE index_name = 'gsi1'")
            .fetch_one(&s.catalog)
            .await
            .expect("read the index row");
    let gsi = &plain.global_secondary_indexes.as_ref().unwrap()[0];
    assert_eq!(iks, serde_json::to_value(&gsi.key_schema).unwrap());
    assert_eq!(proj, serde_json::to_value(&gsi.projection).unwrap());
    let text = table_documents(&s.catalog, table).await;
    assert!(!text.contains("\\u0001"), "{text}");
}
