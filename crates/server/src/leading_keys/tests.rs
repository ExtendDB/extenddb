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

// ---- differential: the engine and authorization agree on the partition key -

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionKind, parse_key_condition, tokenize_for, validate_no_reserved_words,
};
use extenddb_core::limits::LimitsConfig;

/// The partition key value the engine would execute a Query against, or an
/// error when the engine rejects the request before any data flows. This
/// mirrors `crates/engine/src/query.rs` and `crates/engine/src/
/// expression_helpers.rs`: it parses the key condition with the same shared
/// `extenddb_core::expression` primitives the engine uses (`tokenize_for`,
/// `validate_no_reserved_words`, `parse_key_condition`), resolves PK/SK against
/// the queried key schema's first element, validates the partition key is
/// present, reclassifies multi-part HASH schemas, and reads the partition value
/// from the placeholder. If this yields a value, authorization must extract the
/// same one, or a Query the engine serves would reach the evaluator with
/// LeadingKeys absent (a bypass) or bound to the wrong partition.
fn engine_partition_value(
    input: &Value,
    key_info: &TableKeyInfo,
    limits: &LimitsConfig,
) -> Result<String, DynamoDbError> {
    let schema: &[KeySchemaElement] = match input.get("IndexName").and_then(Value::as_str) {
        Some(ix) => {
            &key_info
                .global_secondary_indexes
                .iter()
                .chain(key_info.local_secondary_indexes.iter())
                .find(|i| i.index_name == ix)
                .ok_or_else(|| DynamoDbError::ValidationException("unknown index".to_owned()))?
                .key_schema
        }
        None => &key_info.key_schema,
    };
    let pk_attr = schema
        .first()
        .ok_or_else(|| DynamoDbError::ValidationException("empty key schema".to_owned()))?
        .attribute_name
        .clone();
    let names = names_map(input);
    let values = values_map(input);

    let mut kc = if let Some(kce) = input.get("KeyConditionExpression").and_then(Value::as_str) {
        let tokens = tokenize_for(
            kce,
            limits.max_expression_tokens,
            ExpressionKind::KeyCondition,
        )?;
        if limits.enforce_reserved_keywords {
            validate_no_reserved_words(&tokens)?;
        }
        parse_key_condition(&tokens)?
    } else if let Some(conds) = input.get("KeyConditions").and_then(Value::as_object) {
        // Mirror desugar_key_conditions for the hash key: EQ with one value.
        let hc = conds.get(&pk_attr).ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "Query condition missed key schema element: {pk_attr}"
            ))
        })?;
        if hc.get("ComparisonOperator").and_then(Value::as_str) != Some("EQ") {
            return Err(DynamoDbError::ValidationException(
                "Query key condition not supported".to_owned(),
            ));
        }
        let list = hc
            .get("AttributeValueList")
            .and_then(Value::as_array)
            .ok_or_else(|| DynamoDbError::ValidationException("missing value list".to_owned()))?;
        if list.len() != 1 {
            return Err(DynamoDbError::ValidationException(
                "EQ requires exactly one value".to_owned(),
            ));
        }
        return extract_typed_value(&list[0])
            .ok_or_else(|| DynamoDbError::ValidationException("bad value".to_owned()));
    } else {
        return Err(DynamoDbError::ValidationException(
            "Either the KeyConditions or KeyConditionExpression parameter must be specified"
                .to_owned(),
        ));
    };

    kc.resolve_pk_sk(&pk_attr, &names)?;
    if resolve_attr_name(&kc.pk_path, &names).as_deref() != Some(pk_attr.as_str()) {
        return Err(DynamoDbError::ValidationException(format!(
            "Query condition missed key schema element: {pk_attr}"
        )));
    }
    if extenddb_core::types::is_multipart_key_schema(schema) {
        let hash_elems = extenddb_core::types::hash_key_elements(schema);
        let hash_attrs: Vec<&str> = hash_elems
            .iter()
            .map(|e| e.attribute_name.as_str())
            .collect();
        kc.resolve_multipart(&hash_attrs, &names)?;
        let provided = 1 + kc.extra_pk_conditions.len();
        if provided != hash_attrs.len() {
            return Err(DynamoDbError::ValidationException(
                "Query condition missed a HASH key schema element".to_owned(),
            ));
        }
    }
    match &kc.pk_value {
        Expr::Placeholder(n) => {
            let v = values
                .get(n)
                .ok_or_else(|| DynamoDbError::ValidationException("unresolved value".to_owned()))?;
            extract_typed_value(v)
                .ok_or_else(|| DynamoDbError::ValidationException("bad value".to_owned()))
        }
        _ => Err(DynamoDbError::ValidationException(
            "non-placeholder value".to_owned(),
        )),
    }
}

/// For a Query the engine accepts and executes against partition value V,
/// authorization must extract exactly `Some(vec![V])`. Run over a corpus of
/// key-condition shapes. A shape the engine rejects imposes no requirement (the
/// engine returns ValidationException before any data flows), so only the
/// accept direction is asserted.
#[test]
fn engine_and_auth_agree_on_partition_key_over_corpus() {
    let limits = LimitsConfig::default();
    let main = main_table();
    let mh = multi_hash_gsi_table();

    // (label, input, key_info)
    let cases: Vec<(&str, Value, &TableKeyInfo)> = vec![
        (
            "pk eq placeholder",
            json!({"TableName": "t",
            "KeyConditionExpression": "pk = :v",
            "ExpressionAttributeValues": {":v": {"S": "alice"}}}),
            &main,
        ),
        (
            "hash name ref",
            json!({"TableName": "t",
            "KeyConditionExpression": "#p = :v",
            "ExpressionAttributeNames": {"#p": "pk"},
            "ExpressionAttributeValues": {":v": {"S": "bob"}}}),
            &main,
        ),
        (
            "single parens",
            json!({"TableName": "t",
            "KeyConditionExpression": "(pk = :v)",
            "ExpressionAttributeValues": {":v": {"S": "alice"}}}),
            &main,
        ),
        (
            "pk and sk eq",
            json!({"TableName": "t",
            "KeyConditionExpression": "pk = :v AND sk = :s",
            "ExpressionAttributeValues": {":v": {"S": "alice"}, ":s": {"S": "x"}}}),
            &main,
        ),
        (
            "sk before pk",
            json!({"TableName": "t",
            "KeyConditionExpression": "sk = :s AND pk = :v",
            "ExpressionAttributeValues": {":v": {"S": "alice"}, ":s": {"S": "x"}}}),
            &main,
        ),
        (
            "no spaces",
            json!({"TableName": "t",
            "KeyConditionExpression": "pk=:v",
            "ExpressionAttributeValues": {":v": {"S": "alice"}}}),
            &main,
        ),
        (
            "extra spaces",
            json!({"TableName": "t",
            "KeyConditionExpression": "pk   =   :v",
            "ExpressionAttributeValues": {":v": {"S": "alice"}}}),
            &main,
        ),
        (
            "begins_with sk",
            json!({"TableName": "t",
            "KeyConditionExpression": "pk = :v AND begins_with(sk, :p)",
            "ExpressionAttributeValues": {":v": {"S": "alice"}, ":p": {"S": "pre"}}}),
            &main,
        ),
        (
            "between sk",
            json!({"TableName": "t",
            "KeyConditionExpression": "pk = :v AND sk BETWEEN :lo AND :hi",
            "ExpressionAttributeValues": {":v": {"S": "alice"}, ":lo": {"S": "a"},
                                          ":hi": {"S": "z"}}}),
            &main,
        ),
        (
            "both name refs",
            json!({"TableName": "t",
            "KeyConditionExpression": "#p = :v AND #s = :s",
            "ExpressionAttributeNames": {"#p": "pk", "#s": "sk"},
            "ExpressionAttributeValues": {":v": {"S": "alice"}, ":s": {"S": "x"}}}),
            &main,
        ),
        (
            "number key",
            json!({"TableName": "n",
            "KeyConditionExpression": "pk = :v",
            "ExpressionAttributeValues": {":v": {"N": "10"}}}),
            &main,
        ),
        (
            "legacy eq",
            json!({"TableName": "t", "KeyConditions": {
            "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "alice"}]}}}),
            &main,
        ),
        (
            "legacy eq with sk",
            json!({"TableName": "t", "KeyConditions": {
            "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "alice"}]},
            "sk": {"ComparisonOperator": "BEGINS_WITH",
                   "AttributeValueList": [{"S": "x"}]}}}),
            &main,
        ),
        (
            "lsi base pk",
            json!({"TableName": "t", "IndexName": "lsi1",
            "KeyConditionExpression": "pk = :v",
            "ExpressionAttributeValues": {":v": {"S": "alice"}}}),
            &main,
        ),
        (
            "gsi index pk",
            json!({"TableName": "t", "IndexName": "gsi1",
            "KeyConditionExpression": "gpk = :v",
            "ExpressionAttributeValues": {":v": {"S": "galice"}}}),
            &main,
        ),
        (
            "multi hash gsi",
            json!({"TableName": "m", "IndexName": "gsi2",
            "KeyConditionExpression": "g1 = :a AND g2 = :b",
            "ExpressionAttributeValues": {":a": {"S": "alice"}, ":b": {"S": "blue"}}}),
            &mh,
        ),
        (
            "multi hash gsi swapped",
            json!({"TableName": "m", "IndexName": "gsi2",
            "KeyConditionExpression": "g2 = :b AND g1 = :a",
            "ExpressionAttributeValues": {":a": {"S": "alice"}, ":b": {"S": "blue"}}}),
            &mh,
        ),
        // Shapes the engine rejects: no accept-direction assertion, kept to show
        // the corpus exercises rejects too.
        (
            "non-eq pk",
            json!({"TableName": "t",
            "KeyConditionExpression": "pk > :v",
            "ExpressionAttributeValues": {":v": {"S": "alice"}}}),
            &main,
        ),
        (
            "malformed",
            json!({"TableName": "t",
            "KeyConditionExpression": "pk :: :v",
            "ExpressionAttributeValues": {":v": {"S": "alice"}}}),
            &main,
        ),
        (
            "sk only",
            json!({"TableName": "t",
            "KeyConditionExpression": "sk = :s",
            "ExpressionAttributeValues": {":s": {"S": "x"}}}),
            &main,
        ),
        (
            "unresolved name",
            json!({"TableName": "t",
            "KeyConditionExpression": "#p = :v",
            "ExpressionAttributeValues": {":v": {"S": "alice"}}}),
            &main,
        ),
        (
            "legacy non-eq pk",
            json!({"TableName": "t", "KeyConditions": {
            "pk": {"ComparisonOperator": "LE", "AttributeValueList": [{"S": "alice"}]}}}),
            &main,
        ),
        (
            "legacy begins_with pk",
            json!({"TableName": "t", "KeyConditions": {
            "pk": {"ComparisonOperator": "BEGINS_WITH",
                   "AttributeValueList": [{"S": "alice"}]}}}),
            &main,
        ),
        (
            "multi hash gsi missing g2",
            json!({"TableName": "m", "IndexName": "gsi2",
            "KeyConditionExpression": "g1 = :a",
            "ExpressionAttributeValues": {":a": {"S": "alice"}}}),
            &mh,
        ),
    ];

    let mut accepted = 0;
    for (label, input, ki) in &cases {
        let eng = engine_partition_value(input, ki, &limits);
        let auth =
            single_table_leading_keys(input, "Query", Some(ki), limits.max_expression_tokens);
        if let Ok(v) = &eng {
            accepted += 1;
            assert_eq!(
                auth,
                Some(vec![v.clone()]),
                "case '{label}': engine executes partition {v:?} but authorization extracted {auth:?}"
            );
        }
    }
    // Guard against a vacuous test: the corpus must exercise accepts.
    assert!(accepted >= 15, "expected many accept cases, got {accepted}");
}

/// The token-count limit is the one place authorization (`tokenize_with_limit`)
/// and the engine (`tokenize_for`) could disagree. Both enforce the same limit
/// through the same `push_token`, so a key condition at the limit is accepted
/// by both and one over it is rejected by both.
#[test]
fn token_limit_boundary_matches_between_engine_and_auth() {
    let input = json!({"TableName": "t",
        "KeyConditionExpression": "pk = :v",
        "ExpressionAttributeValues": {":v": {"S": "alice"}}});
    let main = main_table();

    // "pk = :v" is three tokens. At the limit both accept and agree.
    let at_limit = LimitsConfig {
        max_expression_tokens: 3,
        ..LimitsConfig::default()
    };
    let eng = engine_partition_value(&input, &main, &at_limit);
    let auth =
        single_table_leading_keys(&input, "Query", Some(&main), at_limit.max_expression_tokens);
    assert_eq!(eng.ok(), Some("alice".to_owned()));
    assert_eq!(auth, Some(vec!["alice".to_owned()]));

    // One token under the limit: both reject (engine errors, authorization
    // leaves the key absent).
    let over = LimitsConfig {
        max_expression_tokens: 2,
        ..LimitsConfig::default()
    };
    let eng = engine_partition_value(&input, &main, &over);
    let auth = single_table_leading_keys(&input, "Query", Some(&main), over.max_expression_tokens);
    assert!(eng.is_err(), "engine must reject over the token limit");
    assert_eq!(
        auth, None,
        "authorization must leave the key absent over the token limit"
    );
}
