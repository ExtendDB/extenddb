// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Secondary-index key attribute validation on writes. A write whose GSI key
//! attribute has the wrong type, or is an empty string, must be rejected with a
//! ValidationException, matching real DynamoDB.

use crate::test_base::*;
use std::collections::HashMap;

#[tokio::test]
async fn put_item_rejects_wrong_typed_index_key() {
    let c = client();
    let t = tables().await;
    // simple_key_string_gsi has a GSI on gsiHashKey (type S). Supplying a
    // numeric value for it is a type mismatch on the index key.
    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&format!("idxk_{}", ts())));
    item.insert(GSI_HASH_KEY.into(), n(123));
    item.insert(GSI_RANGE_KEY.into(), s("ok"));

    let err = c
        .put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .expect_err("wrong-typed index key must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
    let m = err_msg(&err);
    assert!(
        m.contains("Index Key") && m.contains("IndexName"),
        "expected index-key type-mismatch message, got: {m}"
    );
}

#[tokio::test]
async fn put_item_rejects_empty_index_key() {
    let c = client();
    let t = tables().await;
    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&format!("idxe_{}", ts())));
    item.insert(GSI_HASH_KEY.into(), s("")); // empty string index key
    item.insert(GSI_RANGE_KEY.into(), s("ok"));

    let err = c
        .put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .expect_err("empty index key must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
}

#[tokio::test]
async fn put_item_accepts_valid_index_key() {
    let c = client();
    let t = tables().await;
    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&format!("idxok_{}", ts())));
    item.insert(GSI_HASH_KEY.into(), s("h"));
    item.insert(GSI_RANGE_KEY.into(), s("r"));
    c.put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .expect("valid index key must be accepted");
}
