// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use extenddb_storage::util::{escape_json_strings, unescape_json_strings};

use super::*;

/// `bulk_load` writes each item's `item_data` in the stored form that the
/// write path gives it (`data::item_to_json`): an item with U+0001 in its
/// key and U+0000 and U+0001 in its names and values loads, and its row holds
/// the escape once.
#[tokio::test]
async fn bulk_load_stores_control_characters_through_the_escape() {
    if !pg_available("bulk_load_stores_control_characters_through_the_escape") {
        return;
    }
    let s = scratch().await;
    let row = available_backup(&s, "Music").await;
    let (_desc, key, claim) = register(&s, &row, "Loaded", None).await;
    let written = serde_json::json!({
        "pk": {"S": "p\u{1}a"},
        "sk": {"N": "1"},
        "g": {"S": "g\u{1}"},
        "n\u{0}ame": {"M": {"k\u{1}": {"S": "\u{0}\u{1}\u{2}"}}}
    });
    let item: Item = serde_json::from_value(written.clone()).expect("item");
    let applied = s
        .engine
        .bulk_load(
            &key,
            &*claim,
            BulkLoadFile {
                file_index: 0,
                start_at: 0,
                reapplying: false,
            },
            stream(vec![item]),
        )
        .await
        .expect("load the file");
    assert_eq!(applied, 1);
    let stored: serde_json::Value =
        sqlx::query_scalar(&format!("SELECT item_data FROM \"_ddb_{}\"", key.table_id))
            .fetch_one(&s.db)
            .await
            .expect("read the base row");
    assert_eq!(stored, escape_json_strings(written.clone()));
    assert_eq!(unescape_json_strings(stored), written);
    s.cleanup().await;
}
