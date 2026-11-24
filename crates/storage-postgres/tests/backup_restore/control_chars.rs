// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use futures::StreamExt;

use super::*;

/// Every stored row of one physical table, each as the JSON text of the
/// whole row, in a stable order. Two tables with the same stored form compare
/// equal; a difference in any column (an escaped key text, an `item_data`
/// document) shows up.
async fn stored_rows(s: &Scratch, physical_id: &str) -> Vec<String> {
    sqlx::query_scalar(&format!(
        "SELECT to_jsonb(t)::text FROM \"_ddb_{physical_id}\" t ORDER BY 1"
    ))
    .fetch_all(&s.db)
    .await
    .expect("read stored rows")
}

/// Items holding U+0001 in key values and index key values, and U+0000 and
/// U+0001 in nested values and attribute names: the documents are stored
/// through the control-character escape, and the key columns hold the key
/// text as it is. The legacy reader decodes the backup's items, and a restore
/// copies `backup_items` text into the target as it is and derives the key
/// columns and index rows from it, so those must come out in the same stored
/// form as the source's (documents escaped once, not twice), and the restored
/// items must read back equal to what was written. One item, stored as a
/// write before the GSI existed would store it, holds U+0000 in the GSI's key
/// `g`, which the key columns cannot hold: the source's index has no row for
/// it, and the restore leaves it out of the index too.
#[tokio::test]
async fn restore_keeps_control_characters_in_keys_indexes_and_items() {
    if base_conn().is_none() {
        eprintln!("SKIP restore_keeps_control_characters_in_keys_indexes_and_items: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let src = indexed_source_with(&s, "nul_src", 0).await;
    let key_info = |table_name: &str, table_id: &str, desc: &TableDescription| TableKeyInfo {
        table_name: table_name.to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_id: table_id.to_owned(),
        key_schema: desc.key_schema.clone(),
        base_key_schema: desc.key_schema.clone(),
        attribute_definitions: desc.attribute_definitions.clone(),
        ..Default::default()
    };
    let src_desc = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "nul_src".to_owned(),
            },
        )
        .await
        .expect("describe the source");
    let src_key_info = key_info("nul_src", &src, &src_desc);
    let mut items: Vec<Item> = [
        serde_json::json!({
            "pk": {"S": "p\u{1}a"}, "sk": {"N": "1"},
            "g": {"S": "g\u{1}\u{2}"}, "l": {"S": "l\u{1}\u{1}x"},
            "n\u{0}ame": {"S": "v\u{0}"},
            "m": {"M": {"k\u{1}": {"S": "\u{0}"}, "plain": {"N": "2"}}},
            "list": {"L": [{"S": "\u{1}\u{2}"}, {"SS": ["a\u{0}", "b"]}]}
        }),
        serde_json::json!({
            "pk": {"S": "p\u{1}a"}, "sk": {"N": "2"},
            "g": {"S": "g\u{1}\u{2}"}, "l": {"S": "l\u{1}"}, "v": {"S": "\u{0}"}
        }),
        serde_json::json!({"pk": {"S": "\u{1}"}, "sk": {"N": "3"}, "l": {"S": "\u{1}\u{2}"}}),
        serde_json::json!({"pk": {"S": "plain"}, "sk": {"N": "4"}, "g": {"S": "g"}}),
    ]
    .into_iter()
    .map(|v| serde_json::from_value(v).expect("item"))
    .collect();
    for item in &items {
        s.engine
            .put_item(
                &src_key_info,
                item.clone(),
                false,
                None,
                &extenddb_core::expression::ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put");
    }

    let nul_in_gsi_key: Item = serde_json::from_value(serde_json::json!({
        "pk": {"S": "nul"}, "sk": {"N": "5"}, "g": {"S": "g\u{0}"}
    }))
    .expect("item");
    sqlx::query(&format!(
        "INSERT INTO \"_ddb_{src}\" (pk, sk_n, item_data) VALUES ('nul', 5, $1)"
    ))
    .bind(extenddb_storage::util::escape_json_strings(
        serde_json::to_value(&nul_in_gsi_key).expect("item json"),
    ))
    .execute(&s.db)
    .await
    .expect("store the item as a write before the GSI would");
    items.push(nul_in_gsi_key);

    let backup = s
        .engine
        .create(ACCOUNT, "nul_src", "b", JobPermit::detached())
        .await
        .expect("backup");

    // The legacy reader, which carries an in-catalog backup into a store,
    // reads the items back as written.
    let text = |item: &Item| serde_json::to_string(item).expect("item text");
    let mut written: Vec<String> = items.iter().map(text).collect();
    written.sort();
    let opened = s
        .engine
        .items(ACCOUNT, &backup.backup_arn)
        .await
        .expect("open the legacy reader");
    let mut read: Vec<String> = opened
        .items
        .map(|item| text(&item.expect("an item")))
        .collect()
        .await;
    read.sort();
    assert_eq!(read, written);

    let desc = restore(&s, "nul_dst", &backup.backup_arn)
        .await
        .expect("restore");
    assert_eq!(table_status(&s, "nul_dst").await.as_deref(), Some("ACTIVE"));

    // The base table and each index table hold the same stored rows as the
    // source's, so every key column kept its text and every document was
    // escaped exactly once.
    assert_eq!(
        stored_rows(&s, &desc.table_id).await,
        stored_rows(&s, &src).await
    );
    let src_idx = index_ids(&s, &src).await;
    let dst_idx = index_ids(&s, &desc.table_id).await;
    assert_eq!(src_idx.len(), 2);
    for ((name, a), (_, b)) in src_idx.iter().zip(&dst_idx) {
        let want = stored_rows(&s, a).await;
        assert!(!want.is_empty(), "index {name} has rows at the source");
        assert_eq!(stored_rows(&s, b).await, want, "index {name}");
    }

    // The restored items read back equal to what was written.
    let dst_desc = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "nul_dst".to_owned(),
            },
        )
        .await
        .expect("describe the restored table");
    let dst_key_info = key_info("nul_dst", &desc.table_id, &dst_desc);
    for item in &items {
        let key: Item = item
            .iter()
            .filter(|(k, _)| *k == "pk" || *k == "sk")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let got = s
            .engine
            .get_item(&dst_key_info, &key)
            .await
            .expect("point read");
        assert_eq!(got.as_ref(), Some(item));
    }
    s.cleanup().await;
}
