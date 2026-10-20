// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for `dynamodb:LeadingKeys` extraction.
//!
//! Expectations mirror behaviour measured against live `DynamoDB` on
//! 2026-10-20.

use super::*;
use extenddb_core::types::{
    IndexInfo, IndexType, KeySchemaElement, KeyType, Projection, ProjectionType, TableKeyInfo,
};
use serde_json::json;

const MAX_TOKENS: usize = 4096;

fn ks(name: &str, kt: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type: kt,
    }
}

fn proj() -> Projection {
    Projection {
        projection_type: ProjectionType::All,
        non_key_attributes: None,
    }
}

/// A table with base key (pk HASH, sk RANGE), an LSI sharing pk, and a GSI on
/// its own partition key gpk.
fn main_table() -> TableKeyInfo {
    TableKeyInfo {
        table_name: "t".to_owned(),
        key_schema: vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)],
        base_key_schema: vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)],
        local_secondary_indexes: vec![IndexInfo {
            index_name: "lsi1".to_owned(),
            index_id: "lsi1".to_owned(),
            index_type: IndexType::Lsi,
            key_schema: vec![ks("pk", KeyType::Hash), ks("lsi_sk", KeyType::Range)],
            projection: proj(),
        }],
        global_secondary_indexes: vec![IndexInfo {
            index_name: "gsi1".to_owned(),
            index_id: "gsi1".to_owned(),
            index_type: IndexType::Gsi,
            key_schema: vec![ks("gpk", KeyType::Hash), ks("gsk", KeyType::Range)],
            projection: proj(),
        }],
        ..Default::default()
    }
}

fn num_table() -> TableKeyInfo {
    TableKeyInfo {
        table_name: "n".to_owned(),
        key_schema: vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)],
        base_key_schema: vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)],
        ..Default::default()
    }
}

// ---- item operations ----------------------------------------------------

#[test]
fn get_item_extracts_partition_key() {
    let input = json!({"TableName": "t", "Key": {"pk": {"S": "alice"}, "sk": {"S": "x"}}});
    let r = single_table_leading_keys(&input, "GetItem", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["alice".to_owned()]));
}

#[test]
fn put_item_extracts_partition_key_from_item() {
    let input = json!({"TableName": "t", "Item": {"pk": {"S": "alice"}, "sk": {"S": "x"}}});
    let r = single_table_leading_keys(&input, "PutItem", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["alice".to_owned()]));
}

#[test]
fn number_partition_key_is_the_canonical_string() {
    // Live DynamoDB compares an N key of 10 as the string "10".
    let input = json!({"TableName": "n", "Key": {"pk": {"N": "10"}, "sk": {"S": "x"}}});
    let r = single_table_leading_keys(&input, "GetItem", Some(&num_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["10".to_owned()]));
}

#[test]
fn binary_partition_key_is_the_base64_text() {
    let input = json!({"TableName": "b", "Key": {"pk": {"B": "AQI="}, "sk": {"S": "x"}}});
    let r = single_table_leading_keys(&input, "GetItem", Some(&num_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["AQI=".to_owned()]));
}

// ---- query: the defect ---------------------------------------------------

#[test]
fn query_kce_extracts_partition_key() {
    let input = json!({
        "TableName": "t",
        "KeyConditionExpression": "pk = :v",
        "ExpressionAttributeValues": {":v": {"S": "alice"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["alice".to_owned()]));
}

#[test]
fn query_kce_resolves_expression_attribute_name() {
    let input = json!({
        "TableName": "t",
        "KeyConditionExpression": "#p = :v",
        "ExpressionAttributeNames": {"#p": "pk"},
        "ExpressionAttributeValues": {":v": {"S": "bob"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["bob".to_owned()]));
}

#[test]
fn query_kce_with_sort_key_condition() {
    let input = json!({
        "TableName": "t",
        "KeyConditionExpression": "pk = :v AND sk = :s",
        "ExpressionAttributeValues": {":v": {"S": "alice"}, ":s": {"S": "x"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["alice".to_owned()]));
}

#[test]
fn query_kce_sort_key_before_partition_key() {
    // resolve_pk_sk must still pick the partition key when the clauses are swapped.
    let input = json!({
        "TableName": "t",
        "KeyConditionExpression": "sk = :s AND pk = :v",
        "ExpressionAttributeValues": {":v": {"S": "alice"}, ":s": {"S": "x"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["alice".to_owned()]));
}

#[test]
fn query_legacy_key_conditions() {
    let input = json!({
        "TableName": "t",
        "KeyConditions": {"pk": {"ComparisonOperator": "EQ",
                                 "AttributeValueList": [{"S": "alice"}]}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["alice".to_owned()]));
}

#[test]
fn query_on_lsi_uses_base_partition_key() {
    let input = json!({
        "TableName": "t",
        "IndexName": "lsi1",
        "KeyConditionExpression": "pk = :v",
        "ExpressionAttributeValues": {":v": {"S": "alice"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["alice".to_owned()]));
}

#[test]
fn query_on_gsi_uses_index_partition_key() {
    // Measured: the service binds LeadingKeys to the queried GSI's partition key.
    let input = json!({
        "TableName": "t",
        "IndexName": "gsi1",
        "KeyConditionExpression": "gpk = :v",
        "ExpressionAttributeValues": {":v": {"S": "galice"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["galice".to_owned()]));
}

// ---- query: shapes the engine rejects (key left absent) -----------------

#[test]
fn query_missing_key_condition_is_absent() {
    let input = json!({"TableName": "t"});
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, None);
}

#[test]
fn query_non_equality_partition_key_is_absent() {
    let input = json!({
        "TableName": "t",
        "KeyConditionExpression": "pk > :v",
        "ExpressionAttributeValues": {":v": {"S": "alice"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, None);
}

#[test]
fn query_malformed_key_condition_is_absent() {
    let input = json!({
        "TableName": "t",
        "KeyConditionExpression": "pk :: :v",
        "ExpressionAttributeValues": {":v": {"S": "alice"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, None);
}

#[test]
fn query_unresolved_name_is_absent() {
    // #p with no ExpressionAttributeNames entry: the engine rejects it.
    let input = json!({
        "TableName": "t",
        "KeyConditionExpression": "#p = :v",
        "ExpressionAttributeValues": {":v": {"S": "alice"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, None);
}

#[test]
fn query_condition_only_on_sort_key_is_absent() {
    let input = json!({
        "TableName": "t",
        "KeyConditionExpression": "sk = :s",
        "ExpressionAttributeValues": {":s": {"S": "x"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, None);
}

#[test]
fn query_unknown_index_is_absent() {
    let input = json!({
        "TableName": "t",
        "IndexName": "nope",
        "KeyConditionExpression": "pk = :v",
        "ExpressionAttributeValues": {":v": {"S": "alice"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&main_table()), MAX_TOKENS);
    assert_eq!(r, None);
}

#[test]
fn query_without_key_info_is_absent() {
    let input = json!({
        "TableName": "t",
        "KeyConditionExpression": "pk = :v",
        "ExpressionAttributeValues": {":v": {"S": "alice"}},
    });
    assert_eq!(
        single_table_leading_keys(&input, "Query", None, MAX_TOKENS),
        None
    );
}

// ---- scan sets no leading keys ------------------------------------------

#[test]
fn scan_sets_no_leading_keys() {
    let input = json!({"TableName": "t"});
    assert_eq!(
        single_table_leading_keys(&input, "Scan", Some(&main_table()), MAX_TOKENS),
        None
    );
}

// ---- batch: per-table key sets ------------------------------------------

fn key_info_map() -> HashMap<String, TableKeyInfo> {
    HashMap::from([("t".to_owned(), main_table())])
}

#[test]
fn batch_get_collects_all_keys_for_a_table() {
    let input = json!({"RequestItems": {"t": {"Keys": [
        {"pk": {"S": "alice"}, "sk": {"S": "x"}},
        {"pk": {"S": "bob"}, "sk": {"S": "y"}},
    ]}}});
    let t = batch_transact_targets("BatchGetItem", &input, &key_info_map()).unwrap();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].action, "BatchGetItem");
    assert_eq!(t[0].table, "t");
    assert_eq!(
        t[0].leading_keys,
        Some(vec!["alice".to_owned(), "bob".to_owned()])
    );
}

#[test]
fn batch_write_collects_put_and_delete_keys() {
    let input = json!({"RequestItems": {"t": [
        {"PutRequest": {"Item": {"pk": {"S": "alice"}, "sk": {"S": "x"}}}},
        {"DeleteRequest": {"Key": {"pk": {"S": "bob"}, "sk": {"S": "y"}}}},
    ]}});
    let t = batch_transact_targets("BatchWriteItem", &input, &key_info_map()).unwrap();
    assert_eq!(t.len(), 1);
    assert_eq!(
        t[0].leading_keys,
        Some(vec!["alice".to_owned(), "bob".to_owned()])
    );
}

// ---- transaction: one authorization per item ----------------------------

#[test]
fn transact_get_one_target_per_item() {
    let input = json!({"TransactItems": [
        {"Get": {"TableName": "t", "Key": {"pk": {"S": "alice"}, "sk": {"S": "x"}}}},
        {"Get": {"TableName": "t", "Key": {"pk": {"S": "bob"}, "sk": {"S": "y"}}}},
    ]});
    let t = batch_transact_targets("TransactGetItems", &input, &key_info_map()).unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t[0].action, "GetItem");
    assert_eq!(t[0].leading_keys, Some(vec!["alice".to_owned()]));
    assert_eq!(t[1].leading_keys, Some(vec!["bob".to_owned()]));
}

#[test]
fn transact_write_maps_each_sub_op_to_its_action_and_key() {
    let input = json!({"TransactItems": [
        {"Put": {"TableName": "t", "Item": {"pk": {"S": "alice"}, "sk": {"S": "x"}}}},
        {"Delete": {"TableName": "t", "Key": {"pk": {"S": "bob"}, "sk": {"S": "y"}}}},
        {"Update": {"TableName": "t", "Key": {"pk": {"S": "carol"}, "sk": {"S": "z"}}}},
        {"ConditionCheck": {"TableName": "t", "Key": {"pk": {"S": "dave"}, "sk": {"S": "w"}}}},
    ]});
    let t = batch_transact_targets("TransactWriteItems", &input, &key_info_map()).unwrap();
    let got: Vec<(&str, Option<Vec<String>>)> = t
        .iter()
        .map(|a| (a.action.as_str(), a.leading_keys.clone()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("PutItem", Some(vec!["alice".to_owned()])),
            ("DeleteItem", Some(vec!["bob".to_owned()])),
            ("UpdateItem", Some(vec!["carol".to_owned()])),
            ("ConditionCheckItem", Some(vec!["dave".to_owned()])),
        ]
    );
}

#[test]
fn non_batch_operation_has_no_targets() {
    let input = json!({"TableName": "t"});
    assert_eq!(
        batch_transact_targets("GetItem", &input, &key_info_map()),
        None
    );
}
