// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the DynamoDB-at-home storage backend.
//!
//! These tests exercise a real `DynamoEngine` against a live DynamoDB endpoint.
//! They are gated on the `DDB_LOCAL_ENDPOINT` environment variable: if it is
//! unset, each test prints a skip message and returns immediately, so
//! `cargo test -p extenddb-storage-dynamodb` stays green in CI without the
//! variable.
//!
//! # Running against DynamoDB Local
//!
//! 1. Start DynamoDB Local:
//!    ```sh
//!    docker run -d -p 8000:8000 amazon/dynamodb-local
//!    ```
//!
//! 2. Run the integration tests (data plane only — no Postgres needed):
//!    ```sh
//!    DDB_LOCAL_ENDPOINT=http://localhost:8000 \
//!      AWS_ACCESS_KEY_ID=dummy \
//!      AWS_SECRET_ACCESS_KEY=dummy \
//!      AWS_REGION=us-east-1 \
//!      AWS_DEFAULT_REGION=us-east-1 \
//!      cargo test -p extenddb-storage-dynamodb --test integration -- --nocapture
//!    ```
//!
//! Note: `serve` additionally requires a Postgres catalog. These tests only
//! exercise the data plane (table lifecycle + item CRUD + query + conditions).

use std::collections::HashMap;

use extenddb_core::expression::{
    ExpressionMaps, parse_condition, parse_key_condition, parse_update, tokenize,
};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, CreateTableInput, DeleteTableInput, DescribeTableInput,
    Item, KeySchemaElement, KeyType, ScalarAttributeType, TableStatus,
};
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_dynamodb::{DynamoEngine, config::DynamoStorageConfig};

// ---------------------------------------------------------------------------
// Helper: read DDB_LOCAL_ENDPOINT (returns None if unset → test skips)
// ---------------------------------------------------------------------------

fn endpoint() -> Option<String> {
    std::env::var("DDB_LOCAL_ENDPOINT").ok()
}

// ---------------------------------------------------------------------------
// Helper: build a DynamoStorageConfig pointing at the local endpoint
// ---------------------------------------------------------------------------

fn make_config(ep: &str) -> DynamoStorageConfig {
    let toml_str = format!(
        r#"
region = "us-east-1"
endpoint_url = "{ep}"
table_prefix = "it_"
catalog_connection_string = "postgresql://unused"
pool_size = 20
"#
    );
    let t: toml::Table = toml::from_str(&toml_str).expect("config toml parse");
    DynamoStorageConfig::from_table(&t).expect("DynamoStorageConfig::from_table")
}

// ---------------------------------------------------------------------------
// Helper: create a simple string Item
// ---------------------------------------------------------------------------

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_owned())
}

fn item(pairs: &[(&str, AttributeValue)]) -> Item {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// Helper: build a CreateTableInput for a table with pk (S, HASH) + sk (S, RANGE)
// ---------------------------------------------------------------------------

fn create_pk_sk_table(logical_name: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: logical_name.to_owned(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        billing_mode: None, // defaults to PAY_PER_REQUEST
        provisioned_throughput: None,
        global_secondary_indexes: None,
        local_secondary_indexes: None,
        stream_specification: None,
        sse_specification: None,
        tags: None,
        deletion_protection_enabled: None,
        table_class: None,
        on_demand_throughput: None,
    }
}

// ---------------------------------------------------------------------------
// Helper: empty ExpressionMaps
// ---------------------------------------------------------------------------

fn empty_maps() -> ExpressionMaps {
    ExpressionMaps::new(HashMap::new(), HashMap::new())
}

// ---------------------------------------------------------------------------
// Helper: pre-delete a table if it exists (ignore TableNotFound)
// ---------------------------------------------------------------------------

async fn try_delete_table(engine: &DynamoEngine, account_id: &str, table_name: &str) {
    let _ = engine
        .delete_table(
            account_id,
            DeleteTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await;
}

// ============================================================================
// Test 1: Table lifecycle + item CRUD
// ============================================================================
//
// - create_table  → assert returns; describe_table → assert status active
// - table_key_info for data ops
// - put_item, get_item, update_item SET, delete_item

#[tokio::test]
async fn test_table_lifecycle_and_item_crud() {
    let Some(ep) = endpoint() else {
        eprintln!("skipping test_table_lifecycle_and_item_crud: DDB_LOCAL_ENDPOINT unset");
        return;
    };

    let cfg = make_config(&ep);
    let engine = DynamoEngine::from_config(&cfg).await;
    let account_id = "000000000001";
    let table_name = "it_crud";

    // Clean up from a prior run (ignore errors)
    try_delete_table(&engine, account_id, table_name).await;

    // --- create_table ---
    let desc = engine
        .create_table(account_id, create_pk_sk_table(table_name))
        .await
        .expect("create_table failed");
    assert_eq!(
        desc.table_name, table_name,
        "logical table name should match"
    );
    eprintln!("[crud] create_table OK, table_name={}", desc.table_name);

    // --- describe_table ---
    let desc2 = engine
        .describe_table(
            account_id,
            DescribeTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await
        .expect("describe_table failed");
    assert_eq!(desc2.table_name, table_name);
    assert_eq!(desc2.table_status, TableStatus::Active);
    eprintln!("[crud] describe_table OK, status={:?}", desc2.table_status);

    // --- table_key_info ---
    let key_info = engine
        .table_key_info(account_id, table_name)
        .await
        .expect("table_key_info failed");
    assert_eq!(key_info.table_name, table_name);
    eprintln!("[crud] table_key_info OK");

    // --- put_item: {pk: "u#1", sk: "p#1", name: "Bob", n: N"42"} ---
    let full_item = item(&[
        ("pk", s("u#1")),
        ("sk", s("p#1")),
        ("name", s("Bob")),
        ("n", n("42")),
    ]);
    engine
        .put_item(&key_info, full_item, false, None, &empty_maps(), None)
        .await
        .expect("put_item failed");
    eprintln!("[crud] put_item OK");

    // --- get_item by key {pk, sk} → assert Some and name == "Bob" ---
    let key = item(&[("pk", s("u#1")), ("sk", s("p#1"))]);
    let got = engine
        .get_item(&key_info, &key)
        .await
        .expect("get_item failed");
    let got_item = got.expect("get_item returned None, expected item");
    assert_eq!(got_item.get("name"), Some(&s("Bob")), "name should be Bob");
    eprintln!("[crud] get_item OK: name={:?}", got_item.get("name"));

    // --- update_item: SET n = :v where :v = N"43" ---
    let update_tokens = tokenize("SET n = :v").expect("tokenize update");
    let actions = parse_update(&update_tokens).expect("parse_update");
    let mut values: HashMap<String, AttributeValue> = HashMap::new();
    values.insert("v".to_owned(), n("43"));
    let update_maps = ExpressionMaps::new(HashMap::new(), values);
    engine
        .update_item(
            &key_info,
            &key,
            &actions,
            false,
            false,
            None,
            &update_maps,
            None,
        )
        .await
        .expect("update_item failed");
    eprintln!("[crud] update_item OK");

    // Verify n changed to 43
    let got2 = engine
        .get_item(&key_info, &key)
        .await
        .expect("get_item after update failed")
        .expect("get_item after update returned None");
    assert_eq!(got2.get("n"), Some(&n("43")), "n should be 43 after update");
    eprintln!("[crud] verify update OK: n={:?}", got2.get("n"));

    // --- delete_item ---
    engine
        .delete_item(&key_info, &key, false, None, &empty_maps(), None)
        .await
        .expect("delete_item failed");
    eprintln!("[crud] delete_item OK");

    // Verify item is gone
    let got3 = engine
        .get_item(&key_info, &key)
        .await
        .expect("get_item after delete failed");
    assert!(got3.is_none(), "item should be None after delete");
    eprintln!("[crud] verify delete OK: item is None");

    // --- delete_table ---
    engine
        .delete_table(
            account_id,
            DeleteTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await
        .expect("delete_table failed");
    eprintln!("[crud] delete_table OK");
}

// ============================================================================
// Test 2: Query with begins_with sort condition
// ============================================================================
//
// - create table "it_query"; put 3 items with same pk "u#1", sk "p#01","p#02","x#03"
// - query pk = :pk AND begins_with(sk, :pre) where :pre = "p#"
// - assert exactly 2 items returned

#[tokio::test]
async fn test_query_begins_with() {
    let Some(ep) = endpoint() else {
        eprintln!("skipping test_query_begins_with: DDB_LOCAL_ENDPOINT unset");
        return;
    };

    let cfg = make_config(&ep);
    let engine = DynamoEngine::from_config(&cfg).await;
    let account_id = "000000000001";
    let table_name = "it_query";

    try_delete_table(&engine, account_id, table_name).await;

    engine
        .create_table(account_id, create_pk_sk_table(table_name))
        .await
        .expect("create_table failed");
    eprintln!("[query] table created");

    let key_info = engine
        .table_key_info(account_id, table_name)
        .await
        .expect("table_key_info failed");

    // Put 3 items
    for (sk_val, extra) in [("p#01", "alpha"), ("p#02", "beta"), ("x#03", "gamma")] {
        let full_item = item(&[("pk", s("u#1")), ("sk", s(sk_val)), ("extra", s(extra))]);
        engine
            .put_item(&key_info, full_item, false, None, &empty_maps(), None)
            .await
            .expect("put_item failed");
    }
    eprintln!("[query] put 3 items OK");

    // Build key condition: pk = :pk AND begins_with(sk, :pre)
    let kc_str = "pk = :pk AND begins_with(sk, :pre)";
    let kc_tokens = tokenize(kc_str).expect("tokenize key condition");
    let kc = parse_key_condition(&kc_tokens).expect("parse_key_condition");

    let mut values: HashMap<String, AttributeValue> = HashMap::new();
    values.insert("pk".to_owned(), s("u#1"));
    values.insert("pre".to_owned(), s("p#"));
    let kc_maps = ExpressionMaps::new(HashMap::new(), values);

    let (items, lek) = engine
        .query(&key_info, &kc, &kc_maps, true, None, None, None)
        .await
        .expect("query failed");

    eprintln!(
        "[query] query returned {} items, lek={:?}",
        items.len(),
        lek
    );
    assert_eq!(
        items.len(),
        2,
        "expected exactly 2 items from begins_with(sk, 'p#')"
    );

    // Verify sorted ascending
    let sk0 = items[0].get("sk").expect("sk in first item");
    let sk1 = items[1].get("sk").expect("sk in second item");
    assert_eq!(sk0, &s("p#01"), "first item sk should be p#01");
    assert_eq!(sk1, &s("p#02"), "second item sk should be p#02");
    eprintln!("[query] order OK: {:?} < {:?}", sk0, sk1);

    engine
        .delete_table(
            account_id,
            DeleteTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await
        .expect("delete_table failed");
    eprintln!("[query] delete_table OK");
}

// ============================================================================
// Test 3: Conditional put fails → ConditionFailed
// ============================================================================
//
// - table "it_cond"; put item; then put SAME key with attribute_not_exists(pk)
//   → assert the result is Err(StorageError::ConditionFailed(_))

#[tokio::test]
async fn test_conditional_put_fails() {
    let Some(ep) = endpoint() else {
        eprintln!("skipping test_conditional_put_fails: DDB_LOCAL_ENDPOINT unset");
        return;
    };

    let cfg = make_config(&ep);
    let engine = DynamoEngine::from_config(&cfg).await;
    let account_id = "000000000001";
    let table_name = "it_cond";

    try_delete_table(&engine, account_id, table_name).await;

    engine
        .create_table(account_id, create_pk_sk_table(table_name))
        .await
        .expect("create_table failed");
    eprintln!("[cond] table created");

    let key_info = engine
        .table_key_info(account_id, table_name)
        .await
        .expect("table_key_info failed");

    let first_item = item(&[("pk", s("c#1")), ("sk", s("s#1")), ("val", s("first"))]);

    // First put (no condition) — should succeed
    engine
        .put_item(
            &key_info,
            first_item.clone(),
            false,
            None,
            &empty_maps(),
            None,
        )
        .await
        .expect("first put_item failed");
    eprintln!("[cond] first put OK");

    // Second put with attribute_not_exists(pk) condition — should fail
    let cond_tokens = tokenize("attribute_not_exists(pk)").expect("tokenize condition");
    let cond_expr = parse_condition(&cond_tokens).expect("parse_condition");

    let second_item = item(&[("pk", s("c#1")), ("sk", s("s#1")), ("val", s("second"))]);
    let result = engine
        .put_item(
            &key_info,
            second_item,
            false,
            Some(&cond_expr),
            &empty_maps(),
            None,
        )
        .await;

    eprintln!("[cond] conditional put result: {:?}", result);

    match result {
        Err(extenddb_storage::error::StorageError::ConditionFailed(_)) => {
            eprintln!("[cond] got expected ConditionFailed");
        }
        Err(other) => panic!("expected ConditionFailed, got different error: {other:?}"),
        Ok(_) => panic!("expected ConditionFailed, but put_item succeeded"),
    }

    // Verify original item is still there unchanged
    let key = item(&[("pk", s("c#1")), ("sk", s("s#1"))]);
    let got = engine
        .get_item(&key_info, &key)
        .await
        .expect("get_item failed")
        .expect("item should still exist");
    assert_eq!(
        got.get("val"),
        Some(&s("first")),
        "original value should remain"
    );
    eprintln!("[cond] original item intact, val={:?}", got.get("val"));

    engine
        .delete_table(
            account_id,
            DeleteTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await
        .expect("delete_table failed");
    eprintln!("[cond] delete_table OK");
}
