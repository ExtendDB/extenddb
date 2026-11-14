// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the order of the refusals of a statement around the walk of its condition.

use super::*;

#[test]
fn a_set_clause_comes_after_the_operator_count() {
    let many = many();
    for value in [
        "1e126",
        "[1, 1e126]",
        "{'k': 1e126}",
        "?",
        "+b",
        "b IS NULL",
        "[?]",
    ] {
        let sql = format!("UPDATE \"tbl\" SET q = {value} WHERE pk = 'a' AND {many}");
        assert_eq!(refusal(&sql, 0), too_many_operators(), "{value}");
    }
    // Also when the clause is written after the condition.
    let sql = format!("FROM \"tbl\" WHERE pk = 'a' AND {many} SET q = +b");
    assert_eq!(refusal(&sql, 0), too_many_operators());
    // And after the walk within the operator limit.
    assert_eq!(
        refusal("UPDATE \"tbl\" SET q = +b WHERE pk = 'a' AND c = 1 + 1", 0),
        PLUS
    );
    // The SET value is still checked when the condition is within it.
    assert_eq!(
        refusal("UPDATE \"tbl\" SET q = +b WHERE pk = 'a'", 0),
        "Incorrect number of operands for operator or function; operator or function: +, \
         number of operands: 1"
    );
    assert!(parse("UPDATE \"tbl\" SET q = 1e126 WHERE pk = 'a'").is_ok());
}

#[test]
fn the_table_name_rule_comes_first_for_a_request() {
    let many = many();
    let members: Vec<String> = (0..101).map(|i| i.to_string()).collect();
    for condition in [
        "b = 1 + 1".to_owned(),
        many.clone(),
        format!("c = 1e126 AND {many}"),
        format!("a IN [{}]", members.join(", ")),
        "c = ?".to_owned(),
        "a IN 1".to_owned(),
    ] {
        let sql = format!("SELECT * FROM \"tt\" WHERE {condition}");
        assert_eq!(refusal(&sql, 0), NAME_RULE, "{condition}");
    }
    // An empty FROM path component and a syntax error come before it.
    assert_eq!(
        refusal("SELECT * FROM \"\" WHERE b = 1 + 1", 0),
        "Path component cannot be an empty string"
    );
    assert_eq!(
        refusal("SELECT * FROM \"tt\" WHERE b = 1 +", 0),
        format!("{WF}Missing right-hand side expression of infix operator")
    );
    // Without a request the rule is left to the checks made once the
    // statement parses.
    assert!(parse("SELECT * FROM \"tt\" WHERE pk = 'a'").is_ok());
    assert_eq!(err("SELECT * FROM \"tt\" WHERE b = 1 + 1"), PLUS);
}

#[test]
fn an_alias_comes_before_the_walk() {
    let sql = format!("SELECT * FROM \"tbl\" AS x WHERE c = 1e126 AND {}", many());
    assert_eq!(err(&sql), "Aliasing is not supported");
}

#[test]
fn the_refusals_found_while_reading_still_come_before_the_operator_count() {
    let many = "NOT ".repeat(300) + "a = 1";
    for (tail, expected) in [
        (
            "a = 1 AND b = 1 + 1 AND c = 1e126".to_owned(),
            PLUS.to_owned(),
        ),
        (
            "a = 1 AND foo(b) AND c = 1e126".to_owned(),
            format!(
                "Unrecognized function: foo at 1:{}:3",
                SELECT.len() + many.len() + " AND a = 1 AND ".len() + 1
            ),
        ),
        ("a = 1 AND b = MISSING".to_owned(), MISSING_VALUE.to_owned()),
        (format!("a = 1 AND b = {}", deep()), nesting()),
        (
            "a = 1 AND".to_owned(),
            format!("{WF}Missing right-hand side expression of infix operator"),
        ),
        // The argument count of a call is checked after the operator count.
        ("a = 1 AND size(b, c) = 1".to_owned(), too_many_operators()),
    ] {
        let sql = format!("{SELECT}{many} AND {tail}");
        assert_eq!(err(&sql), expected, "{tail}");
    }
}

#[test]
fn a_select_with_a_call_in_its_select_list_leaves_its_condition_to_the_engine() {
    for condition in ["pk = 1e126".to_owned(), "b = 1 + 1".to_owned(), many()] {
        let sql = format!("SELECT COUNT(*) FROM \"tbl\" WHERE {condition}");
        let Ok(Statement::Select(select)) = parse_for_request(&sql, 0) else {
            panic!("{sql}");
        };
        assert_eq!(
            select.projection_error.as_deref(),
            Some("Unexpected path component at 1:8:5"),
            "{condition}"
        );
    }
    // What comes before the condition still comes first.
    assert_eq!(
        refusal("SELECT COUNT(*) FROM \"tbl\" AS x WHERE b = 1 + 1", 0),
        "Aliasing is not supported"
    );
    assert_eq!(
        refusal("SELECT COUNT(*) FROM \"tt\" WHERE b = 1 + 1", 0),
        NAME_RULE
    );
}

#[test]
fn the_table_name_rule_comes_before_a_select_list_alias_and_an_update_of_an_index() {
    for sql in [
        "SELECT a AS b FROM \"tt\" WHERE pk = 'a'",
        "UPDATE \"tt\".\"idx\" SET q = 1 WHERE pk = 'a'",
        "FROM \"tt\".\"idx\" WHERE pk = 'a' SET q = 1",
    ] {
        assert_eq!(refusal(sql, 0), NAME_RULE, "{sql}");
    }
    // Both still come before the walk, and an alias on the table before
    // the rule.
    assert_eq!(
        refusal("SELECT a AS b FROM \"tbl\" WHERE b = 1 + 1", 0),
        "Aliasing is not supported"
    );
    assert_eq!(
        refusal("UPDATE \"tbl\".\"idx\" SET q = 1 WHERE b = 1 + 1", 0),
        "This operation is not supported on an index"
    );
    assert_eq!(
        refusal("UPDATE \"tt\" AS x SET q = 1 WHERE pk = 'a'", 0),
        "Aliasing is not supported"
    );
    // A DELETE of an index is not measured against the rule.
    assert_eq!(
        refusal("DELETE FROM \"tt\".\"idx\" WHERE pk = 'a'", 0),
        "This operation is not supported on an index"
    );
}

#[test]
fn execute_statement_refuses_exists_before_the_table_name_rule() {
    use crate::partiql::parser::{EXISTS_OUTSIDE_TRANSACTION, parse_for_execute_statement};
    for sql in [
        "EXISTS(SELECT * FROM \"tt\" WHERE pk = 'a' AND b = 1 + 1)",
        "EXISTS(SELECT * FROM \"tbl\" WHERE pk = 'a' AND b = 1e126)",
    ] {
        assert_eq!(
            parse_for_execute_statement(sql, 0).unwrap_err().message,
            EXISTS_OUTSIDE_TRANSACTION,
            "{sql}"
        );
        // Elsewhere the statement keeps the order of its checks.
        assert_ne!(refusal(sql, 0), EXISTS_OUTSIDE_TRANSACTION, "{sql}");
    }
    assert_eq!(
        refusal(
            "EXISTS(SELECT * FROM \"tt\" WHERE pk = 'a' AND b = 1 + 1)",
            0
        ),
        NAME_RULE
    );
    // An alias on the table and a syntax error come first.
    assert_eq!(
        parse_for_execute_statement("EXISTS(SELECT * FROM \"tbl\" x WHERE pk = 'a')", 0)
            .unwrap_err()
            .message,
        "Aliasing is not supported"
    );
}

#[test]
fn a_limit_clause_comes_after_the_table_name_rule_and_before_the_walk() {
    let members: Vec<String> = (0..101).map(|i| i.to_string()).collect();
    for condition in [
        "c = 1e126".to_owned(),
        "c = ?".to_owned(),
        "b = 1 + 1".to_owned(),
        many(),
        format!("a IN [{}]", members.join(", ")),
    ] {
        let sql = format!("{SELECT}{condition} LIMIT 2");
        // The token after LIMIT is named.
        let column = sql.find("LIMIT 2").unwrap() + "LIMIT ".len() + 1;
        assert_eq!(
            refusal(&sql, 0),
            format!("Unsupported clause: LIMIT at 1:{column}:1"),
            "{condition}"
        );
    }
    assert_eq!(
        refusal("SELECT * FROM \"tt\" WHERE pk = 'a' LIMIT 2", 0),
        NAME_RULE
    );
}

#[test]
fn a_select_list_call_comes_before_an_in_with_no_list() {
    let call = "Unexpected path component at 1:8:5";
    for sql in [
        "SELECT COUNT(*) FROM \"tbl\" WHERE a IN 1",
        "SELECT COUNT(*) FROM \"tbl\" WHERE c = 1e126 AND a IN 1",
    ] {
        assert_eq!(refusal(sql, 0), call, "{sql}");
    }
    // Without a call, and in an EXISTS, the IN keeps its refusal.
    assert_eq!(refusal(&format!("{SELECT}a IN 1"), 0), IN_OPERANDS);
    assert_eq!(
        refusal("EXISTS(SELECT COUNT(*) FROM \"tbl\" WHERE a IN 1)", 0),
        IN_OPERANDS
    );
    // What comes before the condition still comes first.
    assert_eq!(
        refusal("SELECT COUNT(*) FROM \"tt\" WHERE a IN 1", 0),
        NAME_RULE
    );
}

#[test]
fn a_sign_in_a_set_clause_comes_after_its_numbers() {
    const SIGN: &str = "Incorrect number of operands for operator or function; operator or \
                        function: +, number of operands: 1";
    for sets in [
        "q = 1e126, r = +b",
        "r = +b, q = 1e126",
        "r = +b, q = [1e126]",
    ] {
        let sql = format!("UPDATE \"tbl\" SET {sets} WHERE pk = 'a'");
        let expected = if sets.contains('[') {
            format!("{OVERFLOW_MESSAGE} under root[0]")
        } else {
            overflow()
        };
        assert_eq!(refusal(&sql, 0), expected, "{sets}");
    }
    assert_eq!(
        refusal("UPDATE \"tbl\" SET r = +b, q = 1 WHERE pk = 'a'", 0),
        SIGN
    );
    // The other refusals of the clause keep their order.
    assert_eq!(
        refusal("UPDATE \"tbl\" SET r = +b, s = b IS NULL WHERE pk = 'a'", 0),
        SIGN
    );
    assert_eq!(
        refusal(
            "UPDATE \"tbl\" SET s = b IS NULL, q = 1e126, r = +b WHERE pk = 'a'",
            0
        ),
        "IS operator is not supported in update expressions"
    );
    // A number alone is left to the checks made once the statement
    // parses.
    assert!(parse("UPDATE \"tbl\" SET q = 1e126 WHERE pk = 'a'").is_ok());
}

#[test]
fn a_negative_zero_decimal_keeps_its_place_among_the_refusals_of_a_value() {
    let zero = |sql: &str, path: &str| {
        let column = sql.find("-0.0").unwrap() + 2;
        format!("Negative zero decimal is not supported under key {path} at 1:{column}:3")
    };
    // With a parameter inside a literal, the one written first.
    let sql = "INSERT INTO \"tbl\" VALUE {'pk':'a','x':[-0.0, ?]}";
    assert_eq!(refusal(sql, 1), zero(sql, "root[0]"));
    let sql = "INSERT INTO \"tbl\" VALUE {'pk':'a','x':[?, -0.0]}";
    assert_eq!(refusal(sql, 1), parameter_in_a_list(sql));
    // Ahead of a sign before a SET value that is not a number.
    let sql = "UPDATE \"tbl\" SET x = +b, y = -0.0 WHERE pk = 'a'";
    assert_eq!(refusal(sql, 0), zero(sql, "root"));
    // After what the walk of the condition meets.
    assert_eq!(
        refusal(
            "UPDATE \"tbl\" SET x = -0.0 WHERE pk = 'a' AND a = 1 + 1",
            0
        ),
        PLUS
    );
}
