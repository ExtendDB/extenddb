// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for `dynamodb:Attributes` extraction.
//!
//! Rows marked "Measured" mirror the service's answers of 2026-10-31,
//! 2026-11-01, 2026-11-09 and 2026-11-10; the parent module's docs list what
//! was measured. The 2026-11-09 rows were measured on a table keyed on `pk`
//! and `sk` with a global and a local secondary index: the key attributes of
//! a queried GSI, of a scanned GSI and the sort key of a scanned LSI on a
//! `Scan` with no `ExclusiveStartKey`, a table's sort key, and the key
//! condition of a `Query` and the `Keys` of a `BatchGetItem` on a table that
//! does not exist. The 2026-11-10 rows were measured on a table of the same
//! shape: a later page of a `Scan` of a GSI and of an LSI. Other unmeasured
//! parameters follow the rule measured for the same parameter on another
//! operation.

use std::collections::HashMap;

use super::*;
use extenddb_core::types::{
    IndexInfo, IndexType, KeySchemaElement, KeyType, Projection, ProjectionType,
};
use serde_json::json;

fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn index(name: &str, index_type: IndexType, key_schema: Vec<KeySchemaElement>) -> IndexInfo {
    IndexInfo {
        index_name: name.to_owned(),
        index_id: name.to_owned(),
        index_type,
        key_schema,
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
    }
}

/// A table keyed on `pk` alone, as the measured table was.
fn hash_table() -> TableKeyInfo {
    TableKeyInfo {
        table_name: "t".to_owned(),
        key_schema: vec![ks("pk", KeyType::Hash)],
        base_key_schema: vec![ks("pk", KeyType::Hash)],
        ..Default::default()
    }
}

/// A table keyed on `pk` and `sk`, with a GSI on `gpk`/`gsk` and an LSI on
/// `pk`/`lsk`.
fn indexed_table() -> TableKeyInfo {
    TableKeyInfo {
        table_name: "i".to_owned(),
        key_schema: vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)],
        base_key_schema: vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)],
        global_secondary_indexes: vec![index(
            "gsi1",
            IndexType::Gsi,
            vec![ks("gpk", KeyType::Hash), ks("gsk", KeyType::Range)],
        )],
        local_secondary_indexes: vec![index(
            "lsi1",
            IndexType::Lsi,
            vec![ks("pk", KeyType::Hash), ks("lsk", KeyType::Range)],
        )],
        ..Default::default()
    }
}

fn single(operation: &str, input: &Value, key_info: Option<&TableKeyInfo>) -> Option<Vec<String>> {
    single_table_attributes(operation, input, key_info, &LimitsConfig::default())
}

fn names(list: &[&str]) -> Option<Vec<String>> {
    Some(list.iter().map(|s| (*s).to_owned()).collect())
}

// ---- writes ---------------------------------------------------------------

#[test]
fn put_item_holds_the_key_schema_and_every_item_attribute() {
    // Measured: PutItem {pk, ssn} matches a ForAnyValue Deny on ssn, and
    // {pk, name} does not.
    let input = json!({"TableName": "t", "Item": {"pk": {"S": "a"}, "ssn": {"S": "1"}}});
    assert_eq!(
        single("PutItem", &input, Some(&hash_table())),
        names(&["pk", "ssn"])
    );
    let input = json!({"TableName": "t", "Item": {"pk": {"S": "a"}}});
    assert_eq!(
        single("PutItem", &input, Some(&hash_table())),
        names(&["pk"])
    );
}

#[test]
fn put_item_adds_the_names_its_condition_reads() {
    let input = json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}},
        "ConditionExpression":
            "attribute_not_exists(#p) AND (size(secret.a) > :n OR ssn IN (:x, :y))",
        "ExpressionAttributeNames": {"#p": "pk"},
    });
    assert_eq!(
        single("PutItem", &input, Some(&hash_table())),
        names(&["pk", "secret", "ssn"])
    );
    let legacy = json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}},
        "Expected": {"ssn": {"Exists": false}},
    });
    assert_eq!(
        single("PutItem", &legacy, Some(&hash_table())),
        names(&["pk", "ssn"])
    );
}

#[test]
fn update_item_holds_the_attribute_set_names() {
    // Measured: `SET ssn = :v` matches a Deny on ssn; `SET #n = :v` with
    // `#n` = `name` does not.
    let input = json!({
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET ssn = :v",
    });
    assert_eq!(
        single("UpdateItem", &input, Some(&hash_table())),
        names(&["pk", "ssn"])
    );
    let input = json!({
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET #n = :v",
        "ExpressionAttributeNames": {"#n": "name"},
    });
    assert_eq!(
        single("UpdateItem", &input, Some(&hash_table())),
        names(&["name", "pk"])
    );
}

#[test]
fn update_item_holds_every_action_target_but_not_the_paths_a_value_reads() {
    // Measured: REMOVE, ADD and DELETE of ssn match a Deny on ssn, and a SET
    // value that reads ssn (`ssn`, `#s`, `if_not_exists`, `list_append`,
    // `ssn + :one`) does not.
    let input = json!({
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET a = b + :v, c.d = list_append(e, :l), \
            f = if_not_exists(g[0].h, :z) REMOVE i.j[2] ADD k :n DELETE #m :s",
        "ConditionExpression": "attribute_exists(x.y)",
        "ExpressionAttributeNames": {"#m": "mset"},
    });
    assert_eq!(
        single("UpdateItem", &input, Some(&hash_table())),
        names(&["a", "c", "f", "i", "k", "mset", "pk", "x"])
    );
    let copy = json!({
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET #n = #s, o = if_not_exists(ssn, :d), p = :one + ssn",
        "ExpressionAttributeNames": {"#n": "name", "#s": "ssn"},
    });
    assert_eq!(
        single("UpdateItem", &copy, Some(&hash_table())),
        names(&["name", "o", "p", "pk"])
    );
}

#[test]
fn update_item_holds_the_legacy_attribute_updates_and_expected() {
    let input = json!({
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "AttributeUpdates": {"ssn": {"Action": "DELETE"}},
        "Expected": {"name": {"Value": {"S": "n"}}},
    });
    assert_eq!(
        single("UpdateItem", &input, Some(&hash_table())),
        names(&["name", "pk", "ssn"])
    );
}

#[test]
fn delete_item_holds_the_key_and_condition_names_only() {
    // Measured: DeleteItem of an item that holds ssn is allowed under a Deny
    // on ssn, so the stored item's attributes are not in the key.
    let input = json!({"TableName": "t", "Key": {"pk": {"S": "a"}}});
    assert_eq!(
        single("DeleteItem", &input, Some(&hash_table())),
        names(&["pk"])
    );
    let input = json!({
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "ConditionExpression": "ssn = :v",
    });
    assert_eq!(
        single("DeleteItem", &input, Some(&hash_table())),
        names(&["pk", "ssn"])
    );
}

#[test]
fn return_values_and_select_add_no_names() {
    let input = json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}},
        "ReturnValues": "ALL_OLD",
    });
    assert_eq!(
        single("PutItem", &input, Some(&hash_table())),
        names(&["pk"])
    );
    let input = json!({"TableName": "t", "Select": "ALL_ATTRIBUTES"});
    assert_eq!(single("Scan", &input, Some(&hash_table())), names(&["pk"]));
}

// ---- reads ----------------------------------------------------------------

#[test]
fn get_item_holds_attributes_to_get_as_written() {
    // Measured: AttributesToGet=[ssn] matches a Deny on ssn. Each entry is a
    // top-level name, so `a.b` is one name.
    let input = json!({
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "AttributesToGet": ["ssn", "a.b"],
    });
    assert_eq!(
        single("GetItem", &input, Some(&hash_table())),
        names(&["a.b", "pk", "ssn"])
    );
}

#[test]
fn get_item_holds_the_top_level_name_of_each_projected_path() {
    // Measured: `secret.a` and `#s.a` (`#s` = `secret`) both match a Deny on
    // secret.
    let input = json!({
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "ProjectionExpression": "secret.a, #s.b, l[0].c, #n",
        "ExpressionAttributeNames": {"#s": "other", "#n": "name"},
    });
    assert_eq!(
        single("GetItem", &input, Some(&hash_table())),
        names(&["l", "name", "other", "pk", "secret"])
    );
}

#[test]
fn get_item_with_no_projection_holds_only_the_key_schema() {
    // Measured: GetItem with no projection is allowed under a Deny on ssn and
    // returns the whole item.
    let input = json!({"TableName": "i", "Key": {"pk": {"S": "a"}, "sk": {"S": "b"}}});
    assert_eq!(
        single("GetItem", &input, Some(&indexed_table())),
        names(&["pk", "sk"])
    );
}

#[test]
fn a_table_that_does_not_exist_adds_no_key_schema() {
    let input = json!({"TableName": "gone", "Key": {"pk": {"S": "a"}}});
    assert_eq!(single("GetItem", &input, None), None);
    let input =
        json!({"TableName": "gone", "Key": {"pk": {"S": "a"}}, "ProjectionExpression": "ssn"});
    assert_eq!(single("GetItem", &input, None), names(&["ssn"]));
}

#[test]
fn query_holds_the_table_and_index_keys_and_its_read_parameters() {
    // Measured: a Query of a GSI whose key condition names only the GSI's
    // partition key is refused under an Allow whose ForAllValues list holds
    // the table's keys, the GSI's partition key and the projected name, and
    // allowed once the list holds the GSI's sort key too. Only the index's
    // key schema gives `gsk` here.
    let gsi = json!({"TableName": "i", "IndexName": "gsi1", "KeyConditionExpression": "gpk = :g"});
    assert_eq!(
        single("Query", &gsi, Some(&indexed_table())),
        names(&["gpk", "gsk", "pk", "sk"])
    );
    let input = json!({
        "TableName": "i",
        "IndexName": "gsi1",
        "KeyConditionExpression": "gpk = :g",
        "FilterExpression": "ssn = :s",
        "ProjectionExpression": "#n.first",
        "ExpressionAttributeNames": {"#n": "name"},
        "ExclusiveStartKey": {"pk": {"S": "a"}, "sk": {"S": "b"}, "gpk": {"S": "g"}, "gsk": {"S": "h"}},
    });
    assert_eq!(
        single("Query", &input, Some(&indexed_table())),
        names(&["gpk", "gsk", "name", "pk", "sk", "ssn"])
    );
    let lsi = json!({"TableName": "i", "IndexName": "lsi1", "KeyConditionExpression": "pk = :p"});
    assert_eq!(
        single("Query", &lsi, Some(&indexed_table())),
        names(&["lsk", "pk", "sk"])
    );
}

#[test]
fn query_holds_the_legacy_filter_and_attributes_to_get() {
    let input = json!({
        "TableName": "i",
        "KeyConditions": {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "a"}]}},
        "QueryFilter": {"ssn": {"ComparisonOperator": "NOT_NULL"}},
        "AttributesToGet": ["name"],
    });
    assert_eq!(
        single("Query", &input, Some(&indexed_table())),
        names(&["name", "pk", "sk", "ssn"])
    );
}

#[test]
fn a_query_of_a_table_that_does_not_exist_holds_its_key_condition_names() {
    // Measured: on a table that does not exist, a Query whose
    // KeyConditionExpression is `#n = :p` with `#n` for `name` is refused
    // under a Deny on name, and one whose KeyConditions name pk is refused
    // under a Deny on pk, with AccessDeniedException rather than
    // ResourceNotFoundException.
    let input = json!({
        "TableName": "gone",
        "KeyConditionExpression": "#n = :p",
        "ExpressionAttributeNames": {"#n": "name"},
        "ExpressionAttributeValues": {":p": {"S": "p1"}},
    });
    assert_eq!(single("Query", &input, None), names(&["name"]));
    let legacy = json!({
        "TableName": "gone",
        "KeyConditions": {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "p1"}]}},
    });
    assert_eq!(single("Query", &legacy, None), names(&["pk"]));
    // Every clause of the condition, a BETWEEN and a begins_with included.
    let two = json!({
        "TableName": "gone",
        "KeyConditionExpression": "pk = :p AND sk BETWEEN :a AND :b",
    });
    assert_eq!(single("Query", &two, None), names(&["pk", "sk"]));
    let prefix = json!({
        "TableName": "gone",
        "KeyConditionExpression": "begins_with(#s, :a) AND #p = :p",
        "ExpressionAttributeNames": {"#s": "sort", "#p": "part"},
    });
    assert_eq!(single("Query", &prefix, None), names(&["part", "sort"]));
    // A key condition the key condition parser refuses gives none. On this
    // table, which does not exist, the engine answers
    // ResourceNotFoundException before it reads the key condition.
    let refused = json!({"TableName": "gone", "KeyConditionExpression": "pk > :v"});
    assert_eq!(single("Query", &refused, None), None);
    let refused = json!({"TableName": "gone", "KeyConditionExpression": "pk = :p OR sk = :s"});
    assert_eq!(single("Query", &refused, None), None);
}

#[test]
fn a_key_condition_with_a_reserved_word_gives_no_names() {
    // The engine's key condition parser refuses `name = :p`, with the
    // reserved word `name` written bare, when enforce_reserved_keywords is
    // set (the default), so the key condition adds no name. With the check
    // off, the engine accepts it and the key holds `name`.
    let input = json!({"TableName": "gone", "KeyConditionExpression": "name = :p"});
    assert_eq!(single("Query", &input, None), None);
    let placeholder = json!({
        "TableName": "gone",
        "KeyConditionExpression": "#n = :p",
        "ExpressionAttributeNames": {"#n": "name"},
    });
    assert_eq!(single("Query", &placeholder, None), names(&["name"]));
    let lax = LimitsConfig {
        enforce_reserved_keywords: false,
        ..LimitsConfig::default()
    };
    assert_eq!(
        single_table_attributes("Query", &input, None, &lax),
        names(&["name"])
    );
}

#[test]
fn a_key_condition_on_a_table_that_exists_adds_no_name_the_key_lacks() {
    let input = json!({
        "TableName": "i",
        "IndexName": "gsi1",
        "KeyConditionExpression": "gpk = :g AND gsk < :s",
    });
    assert_eq!(
        single("Query", &input, Some(&indexed_table())),
        names(&["gpk", "gsk", "pk", "sk"])
    );
    let input = json!({"TableName": "t", "KeyConditionExpression": "pk = :p"});
    assert_eq!(single("Query", &input, Some(&hash_table())), names(&["pk"]));
}

#[test]
fn scan_with_no_named_attribute_holds_the_table_key_attributes() {
    // Measured: a Deny on pk refuses a Scan with no projection, and a Deny
    // gated on Null true refuses none, so the key holds pk. A Scan with
    // Limit=2 and no projection is allowed under a Deny on ssn and returns
    // whole items.
    let input = json!({"TableName": "t"});
    assert_eq!(single("Scan", &input, Some(&hash_table())), names(&["pk"]));
    let input = json!({"TableName": "t", "Limit": 2});
    assert_eq!(single("Scan", &input, Some(&hash_table())), names(&["pk"]));
}

#[test]
fn a_select_count_scan_holds_the_table_key_attributes() {
    // Measured: an Allow of Scan gated on ForAllValues ["name"], or on Null
    // true, grants no Select COUNT Scan.
    let input = json!({"TableName": "t", "Select": "COUNT"});
    assert_eq!(single("Scan", &input, Some(&hash_table())), names(&["pk"]));
}

#[test]
fn scan_holds_the_sort_key_of_the_table() {
    let input = json!({"TableName": "i", "Limit": 1});
    assert_eq!(
        single("Scan", &input, Some(&indexed_table())),
        names(&["pk", "sk"])
    );
}

#[test]
fn scan_on_an_index_holds_the_table_key_attributes_and_not_the_index_keys() {
    // Measured, on a Scan with no ExclusiveStartKey: a Scan of a GSI is
    // allowed under an Allow whose ForAllValues list holds the table's keys
    // and not the GSI's, and under a Deny on the GSI's partition key, and an
    // Allow whose list holds the GSI's keys and not the table's refuses it. A
    // Scan of an LSI projecting name is allowed under an Allow over pk, sk and
    // name, so the LSI's sort key is not in the key.
    let gsi = json!({"TableName": "i", "IndexName": "gsi1"});
    assert_eq!(
        single("Scan", &gsi, Some(&indexed_table())),
        names(&["pk", "sk"])
    );
    let projected = json!({
        "TableName": "i",
        "IndexName": "gsi1",
        "ProjectionExpression": "#n",
        "ExpressionAttributeNames": {"#n": "name"},
    });
    assert_eq!(
        single("Scan", &projected, Some(&indexed_table())),
        names(&["name", "pk", "sk"])
    );
    let lsi = json!({"TableName": "i", "IndexName": "lsi1"});
    assert_eq!(
        single("Scan", &lsi, Some(&indexed_table())),
        names(&["pk", "sk"])
    );
    let unknown = json!({"TableName": "i", "IndexName": "nope"});
    assert_eq!(
        single("Scan", &unknown, Some(&indexed_table())),
        names(&["pk", "sk"])
    );
}

#[test]
fn a_later_page_of_an_index_scan_holds_the_index_keys_of_its_start_key() {
    // Measured, on a later page: a Scan of a GSI or an LSI whose start key
    // holds the table's and the index's key attributes is refused under an
    // Allow whose ForAllValues list holds the table's keys, and under a Deny
    // on the GSI's partition key or the LSI's sort key. The key holds the
    // start key's names as written.
    let gsi = json!({
        "TableName": "i",
        "IndexName": "gsi1",
        "ExclusiveStartKey": {"pk": {"S": "a"}, "sk": {"S": "b"}, "gpk": {"S": "g"}, "gsk": {"S": "h"}},
    });
    assert_eq!(
        single("Scan", &gsi, Some(&indexed_table())),
        names(&["gpk", "gsk", "pk", "sk"])
    );
    let lsi = json!({
        "TableName": "i",
        "IndexName": "lsi1",
        "ExclusiveStartKey": {"pk": {"S": "a"}, "sk": {"S": "b"}, "lsk": {"S": "l"}},
    });
    assert_eq!(
        single("Scan", &lsi, Some(&indexed_table())),
        names(&["lsk", "pk", "sk"])
    );
}

#[test]
fn a_scan_of_a_table_that_does_not_exist_names_only_what_it_gives() {
    assert_eq!(single("Scan", &json!({"TableName": "gone"}), None), None);
    let input = json!({"TableName": "gone", "ExclusiveStartKey": {"pk": {"S": "a"}}});
    assert_eq!(single("Scan", &input, None), names(&["pk"]));
}

#[test]
fn scan_holds_its_filter_projection_and_start_key_names() {
    // Measured: Scan with AttributesToGet=[ssn] matches a Deny on ssn.
    let input = json!({"TableName": "t", "AttributesToGet": ["ssn"]});
    assert_eq!(
        single("Scan", &input, Some(&hash_table())),
        names(&["pk", "ssn"])
    );
    let input = json!({
        "TableName": "t",
        "FilterExpression": "begins_with(#c, :p)",
        "ProjectionExpression": "name",
        "ExpressionAttributeNames": {"#c": "code"},
        "ExclusiveStartKey": {"pk": {"S": "a"}},
        "ScanFilter": {"age": {"ComparisonOperator": "NULL"}},
    });
    assert_eq!(
        single("Scan", &input, Some(&hash_table())),
        names(&["age", "code", "name", "pk"])
    );
}

#[test]
fn search_vectors_holds_its_projected_paths() {
    let input = json!({
        "TableName": "t",
        "IndexName": "vi",
        "ProjectionExpression": "#v.x, name",
        "ExpressionAttributeNames": {"#v": "vec"},
    });
    assert_eq!(
        single("SearchVectors", &input, Some(&hash_table())),
        names(&["name", "vec"])
    );
}

#[test]
fn an_operation_that_touches_no_item_has_no_attributes() {
    let input = json!({"TableName": "t", "ProjectionExpression": "ssn"});
    assert_eq!(single("DescribeTable", &input, Some(&hash_table())), None);
    assert_eq!(single("ListTables", &json!({}), None), None);
}

// ---- expressions the engine refuses ---------------------------------------

#[test]
fn an_unresolved_name_placeholder_is_skipped() {
    let input = json!({
        "TableName": "t",
        "Key": {"pk": {"S": "a"}},
        "ProjectionExpression": "#missing, ssn",
    });
    assert_eq!(
        single("GetItem", &input, Some(&hash_table())),
        names(&["pk", "ssn"])
    );
}

#[test]
fn an_expression_the_engine_refuses_gives_no_names() {
    let input = json!({
        "TableName": "t",
        "Item": {"pk": {"S": "a"}},
        "ConditionExpression": "ssn = = :v",
    });
    assert_eq!(
        single("PutItem", &input, Some(&hash_table())),
        names(&["pk"])
    );

    let tight = LimitsConfig {
        max_expression_tokens: 3,
        ..LimitsConfig::default()
    };
    let input = json!({"TableName": "gone", "ProjectionExpression": "a, b, c"});
    assert_eq!(
        single_table_attributes("GetItem", &input, None, &tight),
        None
    );

    let small = LimitsConfig {
        max_expression_length_bytes: 4,
        ..LimitsConfig::default()
    };
    let input = json!({"TableName": "gone", "ProjectionExpression": "abcde"});
    assert_eq!(
        single_table_attributes("GetItem", &input, None, &small),
        None
    );
    let input = json!({"TableName": "gone", "ProjectionExpression": "abcd"});
    assert_eq!(
        single_table_attributes("GetItem", &input, None, &small),
        names(&["abcd"])
    );
}

// ---- batch and transaction requests ---------------------------------------

#[test]
fn batch_get_item_holds_the_key_schema_and_the_projection_per_table() {
    // Measured: BatchGetItem with ProjectionExpression=ssn matches a Deny on
    // ssn; with no projection it does not.
    let body = json!({
        "Keys": [{"pk": {"S": "a"}}, {"pk": {"S": "b"}}],
        "ProjectionExpression": "ssn",
    });
    let limits = LimitsConfig::default();
    assert_eq!(
        batch_table_attributes("BatchGetItem", &body, Some(&hash_table()), &limits),
        names(&["pk", "ssn"])
    );
    let body = json!({"Keys": [{"pk": {"S": "a"}}], "AttributesToGet": ["name"]});
    assert_eq!(
        batch_table_attributes("BatchGetItem", &body, Some(&hash_table()), &limits),
        names(&["name", "pk"])
    );
    let body = json!({"Keys": [{"pk": {"S": "a"}}]});
    assert_eq!(
        batch_table_attributes("BatchGetItem", &body, Some(&hash_table()), &limits),
        names(&["pk"])
    );
}

#[test]
fn batch_get_item_keys_add_no_names_of_their_own() {
    // Measured: on a table that does not exist, a BatchGetItem whose only key
    // names pk is answered ResourceNotFoundException under a Deny on pk, as a
    // GetItem of the same key is. The names in Keys are not in the key; the
    // table's key schema is, as for GetItem.
    let limits = LimitsConfig::default();
    let body = json!({"Keys": [{"pk": {"S": "a"}}, {"pk": {"S": "b"}}]});
    assert_eq!(
        batch_table_attributes("BatchGetItem", &body, None, &limits),
        None
    );
    let body = json!({"Keys": [{"pk": {"S": "a"}}], "ProjectionExpression": "ssn"});
    assert_eq!(
        batch_table_attributes("BatchGetItem", &body, None, &limits),
        names(&["ssn"])
    );
    // A key naming an attribute outside the key schema is refused by the
    // engine; the key holds the schema, not the name.
    let body = json!({"Keys": [{"pk": {"S": "a"}, "extra": {"S": "x"}}]});
    assert_eq!(
        batch_table_attributes("BatchGetItem", &body, Some(&hash_table()), &limits),
        names(&["pk"])
    );
}

#[test]
fn batch_write_item_holds_every_put_item_and_delete_key() {
    // Measured: a PutRequest of {pk, ssn} matches a Deny on ssn.
    let body = json!([
        {"PutRequest": {"Item": {"pk": {"S": "a"}, "ssn": {"S": "1"}}}},
        {"PutRequest": {"Item": {"pk": {"S": "b"}, "name": {"S": "n"}}}},
        {"DeleteRequest": {"Key": {"pk": {"S": "c"}}}},
    ]);
    let limits = LimitsConfig::default();
    assert_eq!(
        batch_table_attributes("BatchWriteItem", &body, Some(&hash_table()), &limits),
        names(&["name", "pk", "ssn"])
    );
    assert_eq!(
        batch_table_attributes("Scan", &body, Some(&hash_table()), &limits),
        None
    );
}

#[test]
fn transaction_items_hold_what_their_single_item_operation_holds() {
    // Measured: a TransactGetItems Get with ProjectionExpression=ssn and a
    // TransactWriteItems Put of {pk, ssn} match a Deny on ssn.
    let ki = Some(hash_table());
    let limits = LimitsConfig::default();
    let item = |sub: &str, body: Value| transact_item_attributes(sub, &body, ki.as_ref(), &limits);
    assert_eq!(
        item(
            "Get",
            json!({"Key": {"pk": {"S": "a"}}, "ProjectionExpression": "ssn"})
        ),
        names(&["pk", "ssn"])
    );
    assert_eq!(
        item("Get", json!({"Key": {"pk": {"S": "a"}}})),
        names(&["pk"])
    );
    assert_eq!(
        item(
            "Put",
            json!({"Item": {"pk": {"S": "a"}, "ssn": {"S": "1"}}})
        ),
        names(&["pk", "ssn"])
    );
    assert_eq!(
        item(
            "Update",
            json!({"Key": {"pk": {"S": "a"}}, "UpdateExpression": "REMOVE ssn"})
        ),
        names(&["pk", "ssn"])
    );
    assert_eq!(
        item(
            "Delete",
            json!({"Key": {"pk": {"S": "a"}}, "ConditionExpression": "attribute_exists(ssn)"})
        ),
        names(&["pk", "ssn"])
    );
    assert_eq!(
        item(
            "ConditionCheck",
            json!({"Key": {"pk": {"S": "a"}}, "ConditionExpression": "#s = :v",
                   "ExpressionAttributeNames": {"#s": "ssn"}})
        ),
        names(&["pk", "ssn"])
    );
    assert_eq!(item("Other", json!({"Key": {"pk": {"S": "a"}}})), None);
}

#[test]
fn batch_and_transaction_targets_carry_the_attributes() {
    let key_info: HashMap<String, Option<TableKeyInfo>> =
        HashMap::from([("t".to_owned(), Some(hash_table()))]);
    let limits = LimitsConfig::default();

    let batch = json!({"RequestItems": {"t": [
        {"PutRequest": {"Item": {"pk": {"S": "a"}, "ssn": {"S": "1"}}}},
    ]}});
    let targets =
        crate::leading_keys::batch_transact_targets("BatchWriteItem", &batch, &key_info, &limits)
            .expect("a batch request has targets");
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].attributes, names(&["pk", "ssn"]));

    // Two checks on one key with different conditions are two evaluations;
    // a repeat of the first is not a third.
    let check = |expr: &str| {
        json!({"ConditionCheck": {"TableName": "t", "Key": {"pk": {"S": "a"}},
                                  "ConditionExpression": expr}})
    };
    let transact = json!({"TransactItems": [
        check("attribute_exists(ssn)"),
        check("attribute_exists(name)"),
        check("attribute_exists(ssn)"),
    ]});
    let targets = crate::leading_keys::batch_transact_targets(
        "TransactWriteItems",
        &transact,
        &key_info,
        &limits,
    )
    .expect("a transaction has targets");
    let attributes: Vec<_> = targets.iter().map(|t| t.attributes.clone()).collect();
    assert_eq!(
        attributes,
        vec![names(&["pk", "ssn"]), names(&["name", "pk"])]
    );
}
