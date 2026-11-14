// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the write executor.

use super::*;
use extenddb_core::types::{AttributeDefinition, KeySchemaElement, KeyType};

/// A pk(N)/sk(S) table.
fn number_string_table() -> TableKeyInfo {
    let key = |name: &str, key_type| KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    };
    let definition = |name: &str, attribute_type| AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type,
    };
    TableKeyInfo {
        table_name: "t".to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: vec![
            definition("pk", ScalarAttributeType::N),
            definition("sk", ScalarAttributeType::S),
        ],
        ..TableKeyInfo::default()
    }
}

fn key_type_answer(key: &Value) -> Option<String> {
    common::check_key_types(key.as_object().unwrap(), &number_string_table())
        .err()
        .map(|e| e.message().to_owned())
}

fn overlap_answer(sql: &str) -> Option<String> {
    match extenddb_core::partiql::parse(sql).unwrap() {
        Statement::Update(update) => check_overlapping_paths(&update).err(),
        other => panic!("expected an UPDATE, got {other:?}"),
    }
}

#[test]
fn a_deep_write_condition_is_written_for_the_classic_parser() {
    let condition_of = |sql: &str| match extenddb_core::partiql::parse(sql).unwrap() {
        Statement::Update(update) => update.where_clause.unwrap(),
        other => panic!("expected an UPDATE, got {other:?}"),
    };
    // 99 levels of NOT and AND hold 298 operators, within the service's
    // 300; written as nested as the statement, the text passed the
    // classic parser's 150 levels.
    let deep = "NOT (a = 1 AND ".repeat(99) + "a = 1" + &")".repeat(99);
    let condition = condition_of(&format!("UPDATE \"t\" SET q = 1 WHERE {deep}"));
    let text = std::thread::Builder::new()
        .stack_size(128 << 10)
        .spawn(move || ExprBuilder::default().condition(&condition, &[]).unwrap())
        .unwrap()
        .join()
        .unwrap();
    let wrapped = format!("attribute_exists(#pk) AND ({text})");
    let limits = extenddb_core::limits::LimitsConfig::default();
    assert!(crate::expression_helpers::parse_condition_expr(&wrapped, &limits).is_ok());
    // An OR at the top is left for the caller to put in parentheses.
    let or = condition_of("UPDATE \"t\" SET q = 1 WHERE a = 1 OR NOT (b = 1 OR c = 2)");
    assert_eq!(
        ExprBuilder::default().condition(&or, &[]).unwrap(),
        "#n0 = :v0 OR NOT #n1 = :v1 AND NOT #n2 = :v2"
    );
}

#[test]
fn update_paths_that_overlap_are_refused_naming_the_later_one_first() {
    for (assignments, one, two) in [
        ("SET q = 1, q = 2", "[q]", "[q]"),
        ("SET q = 1 REMOVE q", "[q]", "[q]"),
        ("SET m = {'a': 1}, m.a = 2", "[m, a]", "[m]"),
        (
            "SET ss = set_add(ss, <<'b'>>), ss = set_add(ss, <<'c'>>)",
            "[ss]",
            "[ss]",
        ),
        (
            "SET ss = 'plain', ss = set_add(ss, <<'b'>>)",
            "[ss]",
            "[ss]",
        ),
        ("SET ss = set_delete(ss, <<'a'>>) REMOVE ss", "[ss]", "[ss]"),
    ] {
        let sql = format!("UPDATE \"t\" {assignments} WHERE pk = 'a'");
        assert_eq!(
            overlap_answer(&sql),
            Some(format!(
                "Two document paths overlap with each other; must remove or rewrite one \
                 of these paths; path one: {one}, path two: {two}"
            )),
            "{sql}"
        );
    }
    assert!(overlap_answer("UPDATE \"t\" SET l[0] = 1, l = [2] WHERE pk = 'a'").is_some());
}

#[test]
fn update_paths_that_name_one_element_two_ways_conflict() {
    for (assignments, one, two) in [
        ("SET a.b = 1, a[0] = 2", "[a, [0]]", "[a, b]"),
        ("SET a[0] = 2, a.b = 1", "[a, b]", "[a, [0]]"),
        ("SET m.x.b = 1 REMOVE m.x[3]", "[m, x, [3]]", "[m, x, b]"),
        ("SET a[1].b = 1, a.c = 2", "[a, c]", "[a, [1], b]"),
    ] {
        let sql = format!("UPDATE \"t\" {assignments} WHERE pk = 'a'");
        assert_eq!(
            overlap_answer(&sql),
            Some(format!(
                "Two document paths conflict with each other; must remove or rewrite one \
                 of these paths; path one: {one}, path two: {two}"
            )),
            "{sql}"
        );
    }
    // Paths that part at two members, or at two elements, do not.
    assert_eq!(
        overlap_answer("UPDATE \"t\" SET a.b = 1, a.c[0] = 2 WHERE pk = 'a'"),
        None
    );
    assert_eq!(
        overlap_answer("UPDATE \"t\" SET a[0].b = 1, a[1] = 2 WHERE pk = 'a'"),
        None
    );
}

#[test]
fn update_paths_that_do_not_overlap_pass() {
    for assignments in [
        "SET m.a = 1, m.b = 2",
        "SET l[0] = 1, l[1] = 2",
        "SET a = 1 REMOVE b",
        "SET ab = 1, a = 2",
        "SET ss = set_add(ss, <<'b'>>), ns = set_delete(ns, <<1>>)",
    ] {
        let sql = format!("UPDATE \"t\" {assignments} WHERE pk = 'a'");
        assert_eq!(overlap_answer(&sql), None, "{sql}");
    }
}

#[test]
fn a_write_key_of_the_wrong_type_names_the_sort_key_first() {
    let message = |name: &str| {
        Some(format!(
            "Key attribute's data type should match its data type in table's schema: Key {name}"
        ))
    };
    assert_eq!(
        key_type_answer(&json!({"pk": {"N": "1"}, "sk": {"S": "a"}})),
        None
    );
    assert_eq!(
        key_type_answer(&json!({"pk": {"S": "a"}, "sk": {"S": "a"}})),
        message("pk")
    );
    assert_eq!(
        key_type_answer(&json!({"pk": {"N": "1"}, "sk": {"N": "1"}})),
        message("sk")
    );
    assert_eq!(
        key_type_answer(&json!({"sk": {"N": "1"}, "pk": {"S": "a"}})),
        message("sk")
    );
    assert_eq!(
        key_type_answer(&json!({"pk": {"S": "a"}, "sk": {"N": "1"}})),
        message("sk")
    );
}

/// The key and residual count a write's WHERE gives on the pk/sk table,
/// or its refusal.
fn write_key_answer(condition: &str) -> Result<(Value, usize), String> {
    let sql = format!("DELETE FROM \"t\" WHERE {condition}");
    let Statement::Delete(delete) = extenddb_core::partiql::parse(&sql).unwrap() else {
        panic!("expected a DELETE");
    };
    split_key_where(&delete.where_clause, "pk", Some("sk"), &[])
        .map(|(key, residual)| (Value::Object(key), residual.len()))
        .map_err(|e| e.message().to_owned())
}

#[test]
fn a_write_key_equality_may_have_the_value_on_either_side() {
    let key = json!({"pk": {"N": "1"}, "sk": {"S": "x"}});
    for (condition, residual) in [
        ("pk = 1 AND sk = 'x'", 0),
        ("1 = pk AND 'x' = sk", 0),
        ("pk = 1 AND pk = 1 AND sk = 'x'", 0),
        ("pk = 1 AND sk = 'x' AND 'x' = sk", 0),
        ("pk = 1 AND sk = 'x' AND sk <> 'y'", 1),
        ("pk = 1 AND sk = 'x' AND sk = a", 1),
    ] {
        assert_eq!(
            write_key_answer(condition),
            Ok((key.clone(), residual)),
            "{condition}"
        );
    }
}

#[test]
fn a_write_key_with_two_values_is_refused_naming_the_key() {
    let multiple = |name: &str| {
        Err(format!(
            "Multiple conditions on same key {name}. Only single item Update/Insert/Delete \
             are supported"
        ))
    };
    assert_eq!(
        write_key_answer("pk = 'a' AND 1 = pk AND sk = 'x'"),
        multiple("pk")
    );
    assert_eq!(
        write_key_answer("pk = 'a' AND sk = 'x' AND sk = 'y'"),
        multiple("sk")
    );
    assert_eq!(
        write_key_answer("pk = 'a' AND sk > 'x'"),
        Err("Where clause does not contain a mandatory equality on all key attributes".to_owned())
    );
}
