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

#[test]
fn n_leading_key_is_normalized_on_item_operations() {
    // GetItem compares the normalized number: under a policy fencing to "10",
    // a request pk of 010, 10.0, 1E1 or +10 is allowed (measured 2026-10-20).
    for spelling in ["010", "10.0", "1E1", "+10"] {
        let input = json!({"TableName": "n", "Key": {"pk": {"N": spelling}, "sk": {"S": "x"}}});
        let r = single_table_leading_keys(&input, "GetItem", Some(&num_table()), MAX_TOKENS);
        assert_eq!(r, Some(vec!["10".to_owned()]), "spelling {spelling}");
    }
}

#[test]
fn n_leading_key_zero_spellings_normalize_to_zero() {
    // Under a fence to "0" the service allows -0, 00 and 0.0 on GetItem; each
    // normalizes to "0" (measured 2026-10-20).
    for spelling in ["-0", "00", "0.0"] {
        let input = json!({"TableName": "n", "Key": {"pk": {"N": spelling}, "sk": {"S": "x"}}});
        let r = single_table_leading_keys(&input, "GetItem", Some(&num_table()), MAX_TOKENS);
        assert_eq!(r, Some(vec!["0".to_owned()]), "spelling {spelling}");
    }
}

#[test]
fn query_n_leading_key_is_normalized() {
    let input = json!({
        "TableName": "n",
        "KeyConditionExpression": "pk = :v",
        "ExpressionAttributeValues": {":v": {"N": "1E1"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&num_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["10".to_owned()]));
}

#[test]
fn batch_get_n_leading_keys_are_normalized() {
    let ki = HashMap::from([("n".to_owned(), Some(num_table()))]);
    let bg = json!({"RequestItems": {"n": {"Keys": [
        {"pk": {"N": "010"}, "sk": {"S": "x"}},
        {"pk": {"N": "1E1"}, "sk": {"S": "y"}},
    ]}}});
    let t = batch_transact_targets("BatchGetItem", &bg, &ki).unwrap();
    assert_eq!(
        t[0].leading_keys,
        Some(vec!["10".to_owned(), "10".to_owned()])
    );
}

#[test]
fn batch_write_n_leading_keys_keep_the_written_scale() {
    // BatchWriteItem put and delete requests both compare the number as Java's
    // BigDecimal prints it (measured 2026-10-20, 25 of 25 attempts each).
    let ki = HashMap::from([("n".to_owned(), Some(num_table()))]);
    let bw = json!({"RequestItems": {"n": [
        {"PutRequest": {"Item": {"pk": {"N": "10.0"}, "sk": {"S": "x"}}}},
        {"DeleteRequest": {"Key": {"pk": {"N": "1E1"}, "sk": {"S": "y"}}}},
        {"PutRequest": {"Item": {"pk": {"N": "010"}, "sk": {"S": "z"}}}},
    ]}});
    let t = batch_transact_targets("BatchWriteItem", &bw, &ki).unwrap();
    assert_eq!(
        t[0].leading_keys,
        Some(vec!["10.0".to_owned(), "1E+1".to_owned(), "10".to_owned()])
    );
}

#[test]
fn transact_n_leading_keys_keep_the_written_scale() {
    // TransactGetItems and every TransactWriteItems action compare the number
    // as Java's BigDecimal prints it: under a fence of "10", 10.0 and 1E1 are
    // denied and 010, +10 and 1.0E1 are allowed (measured 2026-10-20).
    let ki = HashMap::from([("n".to_owned(), Some(num_table()))]);
    for (spelling, expected) in [
        ("10.0", "10.0"),
        ("1E1", "1E+1"),
        ("010", "10"),
        ("+10", "10"),
        ("1.0E1", "10"),
    ] {
        let tg = json!({"TransactItems": [
            {"Get": {"TableName": "n", "Key": {"pk": {"N": spelling}, "sk": {"S": "x"}}}},
        ]});
        let t = batch_transact_targets("TransactGetItems", &tg, &ki).unwrap();
        assert_eq!(
            t[0].leading_keys,
            Some(vec![expected.to_owned()]),
            "{spelling}"
        );

        let tw = json!({"TransactItems": [
            {"Put": {"TableName": "n", "Item": {"pk": {"N": spelling}, "sk": {"S": "x"}}}},
            {"Update": {"TableName": "n", "Key": {"pk": {"N": spelling}, "sk": {"S": "y"}}}},
            {"Delete": {"TableName": "n", "Key": {"pk": {"N": spelling}, "sk": {"S": "z"}}}},
            {"ConditionCheck": {"TableName": "n", "Key": {"pk": {"N": spelling}, "sk": {"S": "w"}}}},
        ]});
        let t = batch_transact_targets("TransactWriteItems", &tw, &ki).unwrap();
        for target in &t {
            assert_eq!(
                target.leading_keys,
                Some(vec![expected.to_owned()]),
                "{spelling} {}",
                target.action
            );
        }
    }
}

#[test]
fn java_decimal_string_matches_the_measured_service_values() {
    // Each pair is a request spelling and the LeadingKeys value the service
    // matched on TransactGetItems and BatchWriteItem puts (measured 2026-10-20).
    for (spelling, expected) in [
        ("100E-2", "1.00"),
        ("0.0000001", "1E-7"),
        ("0.000001", "0.000001"),
        ("123E2", "1.23E+4"),
        ("-5.50", "-5.50"),
        ("0.00", "0.00"),
        ("0E5", "0E+5"),
        (".5", "0.5"),
        ("5.", "5"),
        ("+1.5E+3", "1.5E+3"),
        ("3e1", "3E+1"),
        ("12.3400E-1", "1.23400"),
        ("-0", "0"),
        ("1.00E-130", "1.00E-130"),
        ("7E0", "7"),
        ("80E-1", "8.0"),
        ("10.0", "10.0"),
        ("1E1", "1E+1"),
        ("1.0E1", "10"),
        ("010", "10"),
        ("+10", "10"),
    ] {
        assert_eq!(
            java_decimal_string(spelling).as_deref(),
            Some(expected),
            "spelling {spelling}"
        );
    }
}

#[test]
fn malformed_n_leading_key_is_absent() {
    // An N the engine would reject as malformed is not a determinable partition
    // value, so the key is left absent and the engine returns ValidationException.
    let input = json!({"TableName": "n", "Key": {"pk": {"N": "1.2.3"}, "sk": {"S": "x"}}});
    let r = single_table_leading_keys(&input, "GetItem", Some(&num_table()), MAX_TOKENS);
    assert_eq!(r, None);
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

/// A table carrying a GSI whose partition key has two HASH attributes (g1, g2).
/// ExtendDB allows this shape and so does live `DynamoDB` (a two-HASH GSI is
/// accepted; a two-HASH base table is rejected). Measured 2026-10-20.
fn multi_hash_gsi_table() -> TableKeyInfo {
    TableKeyInfo {
        table_name: "m".to_owned(),
        key_schema: vec![ks("pk", KeyType::Hash)],
        base_key_schema: vec![ks("pk", KeyType::Hash)],
        global_secondary_indexes: vec![IndexInfo {
            index_name: "gsi2".to_owned(),
            index_id: "gsi2".to_owned(),
            index_type: IndexType::Gsi,
            key_schema: vec![ks("g1", KeyType::Hash), ks("g2", KeyType::Hash)],
            projection: proj(),
        }],
        ..Default::default()
    }
}

#[test]
fn query_on_multi_hash_gsi_uses_first_hash_attribute_only() {
    // Measured 2026-10-20: for a Query on a GSI whose partition key has two
    // HASH attributes, live DynamoDB evaluates dynamodb:LeadingKeys against the
    // FIRST HASH attribute's value only. Under ForAllValues:StringEquals
    // ["alice"] it allowed a query of g1=alice with any g2 value and denied
    // g1=bob, so the second HASH attribute's value is not part of LeadingKeys.
    let input = json!({
        "TableName": "m",
        "IndexName": "gsi2",
        "KeyConditionExpression": "g1 = :a AND g2 = :b",
        "ExpressionAttributeValues": {":a": {"S": "alice"}, ":b": {"S": "red"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&multi_hash_gsi_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["alice".to_owned()]));
}

#[test]
fn query_on_multi_hash_gsi_is_clause_order_independent() {
    // The same query with the two HASH clauses written in the other order still
    // yields the first HASH attribute's value.
    let input = json!({
        "TableName": "m",
        "IndexName": "gsi2",
        "KeyConditionExpression": "g2 = :b AND g1 = :a",
        "ExpressionAttributeValues": {":a": {"S": "alice"}, ":b": {"S": "blue"}},
    });
    let r = single_table_leading_keys(&input, "Query", Some(&multi_hash_gsi_table()), MAX_TOKENS);
    assert_eq!(r, Some(vec!["alice".to_owned()]));
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

fn key_info_map() -> HashMap<String, Option<TableKeyInfo>> {
    HashMap::from([("t".to_owned(), Some(main_table()))])
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

// ---- transaction targets are de-duplicated -------------------------------

#[test]
fn transact_targets_with_the_same_action_table_and_key_are_evaluated_once() {
    // A hundred ConditionChecks on one partition need one evaluation; distinct
    // keys and distinct actions each keep their own target, in the order of
    // their first occurrence.
    let mut items: Vec<Value> = (0..100)
        .map(|i| {
            json!({"ConditionCheck": {"TableName": "t",
                "Key": {"pk": {"S": "alice"}, "sk": {"S": format!("s{i}")}},
                "ConditionExpression": "attribute_exists(pk)"}})
        })
        .collect();
    items.push(json!({"Put": {"TableName": "t",
        "Item": {"pk": {"S": "alice"}, "sk": {"S": "p"}}}}));
    items.push(json!({"ConditionCheck": {"TableName": "t",
        "Key": {"pk": {"S": "bob"}, "sk": {"S": "x"}},
        "ConditionExpression": "attribute_exists(pk)"}}));
    let input = json!({ "TransactItems": items });
    let t = batch_transact_targets("TransactWriteItems", &input, &key_info_map()).unwrap();
    let got: Vec<(&str, Option<Vec<String>>)> = t
        .iter()
        .map(|a| (a.action.as_str(), a.leading_keys.clone()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("ConditionCheckItem", Some(vec!["alice".to_owned()])),
            ("PutItem", Some(vec!["alice".to_owned()])),
            ("ConditionCheckItem", Some(vec!["bob".to_owned()])),
        ]
    );

    let gets: Vec<Value> = (0..3)
        .map(|i| {
            json!({"Get": {"TableName": "t",
            "Key": {"pk": {"S": "alice"}, "sk": {"S": format!("s{i}")}}}})
        })
        .collect();
    let t = batch_transact_targets(
        "TransactGetItems",
        &json!({ "TransactItems": gets }),
        &key_info_map(),
    )
    .unwrap();
    assert_eq!(t.len(), 1);
}

#[test]
fn a_table_missing_from_the_snapshot_leaves_the_key_absent() {
    // A `None` entry is a table authorization found missing: its targets carry
    // no LeadingKeys, and the engine, bound to the same snapshot, returns
    // ResourceNotFoundException for it.
    let ki = HashMap::from([("t".to_owned(), None)]);
    let input = json!({"TransactItems": [
        {"Get": {"TableName": "t", "Key": {"pk": {"S": "bob"}, "sk": {"S": "x"}}}},
    ]});
    let t = batch_transact_targets("TransactGetItems", &input, &ki).unwrap();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].leading_keys, None);
}

// ---- the key condition token limit ---------------------------------------

use extenddb_core::expression::{ExpressionKind, tokenize_for};

/// The token-count limit is the one place authorization (`tokenize_with_limit`)
/// and the engine's Query path (`tokenize_for` with
/// `ExpressionKind::KeyCondition`, called from `parse_key_condition_expr`)
/// could disagree. Both enforce the same limit through the same `push_token`,
/// so a key condition at the limit is accepted by both and one over it is
/// rejected by both. The engine side here is the engine's tokenizer call.
#[test]
fn token_limit_boundary_matches_between_engine_and_auth() {
    let kce = "pk = :v";
    let input = json!({"TableName": "t",
        "KeyConditionExpression": kce,
        "ExpressionAttributeValues": {":v": {"S": "alice"}}});
    let main = main_table();

    // "pk = :v" is three tokens. At the limit both accept and agree.
    assert!(tokenize_for(kce, 3, ExpressionKind::KeyCondition).is_ok());
    assert_eq!(
        single_table_leading_keys(&input, "Query", Some(&main), 3),
        Some(vec!["alice".to_owned()])
    );

    // One token under the limit: both reject (the engine errors, authorization
    // leaves the key absent).
    assert!(
        tokenize_for(kce, 2, ExpressionKind::KeyCondition).is_err(),
        "the engine must reject over the token limit"
    );
    assert_eq!(
        single_table_leading_keys(&input, "Query", Some(&main), 2),
        None,
        "authorization must leave the key absent over the token limit"
    );
}
