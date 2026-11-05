// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the WHERE condition: its predicates, operators and groups.

use super::*;

#[test]
fn empty_in_list_is_rejected() {
    let m = err("SELECT * FROM \"t\" WHERE pk='a' AND sk IN []");
    assert!(
        m.starts_with("IN operator must have a left hand argument"),
        "{m}"
    );
}

#[test]
fn where_with_nothing_after_it_is_an_unexpected_term() {
    for sql in [
        "SELECT * FROM \"t\" WHERE",
        "SELECT * FROM \"t\" WHERE;",
        "SELECT * FROM \"t\" WHERE   ",
        "SELECT pk FROM \"t\" WHERE",
        "UPDATE \"t\" SET a = 1 WHERE",
        "DELETE FROM \"t\" WHERE",
        "SELECT * FROM \"t\" WHERE NOT",
        "SELECT * FROM \"t\" WHERE (",
        "SELECT * FROM \"t\".\"gsi\" WHERE",
    ] {
        assert_eq!(err(sql), wf("Unexpected term"), "{sql}");
    }
}

#[test]
fn an_infix_operator_with_nothing_after_it_is_missing_its_right_side() {
    for sql in [
        "SELECT * FROM \"t\" WHERE pk = 'p1' AND",
        "SELECT * FROM \"t\" WHERE pk = 'p1' OR",
        "SELECT * FROM \"t\" WHERE pk =",
        "SELECT * FROM \"t\" WHERE pk = 'p' AND n <",
        "SELECT * FROM \"t\" WHERE pk = 'p' AND n = 1 +",
        "SELECT * FROM \"t\" WHERE pk = 'p' AND n IN",
        "SELECT * FROM \"t\" WHERE pk = 'p' AND n BETWEEN",
    ] {
        assert_eq!(
            err(sql),
            wf("Missing right-hand side expression of infix operator"),
            "{sql}"
        );
    }
    // A semicolon after the operator is a term that cannot start.
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'p1' AND;"),
        wf("Unexpected term")
    );
}

#[test]
fn a_term_that_cannot_start_is_named_by_what_it_is() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE ORDER BY pk"),
        wf("Unexpected keyword")
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE IS NULL"),
        wf("Unexpected operator")
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = LIKE"),
        wf("Unexpected operator")
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'a' AND value = 1"),
        wf("Unexpected keyword")
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE = 1"),
        wf("Unexpected operator")
    );
    assert_eq!(err("SELECT * FROM \"t\" WHERE , 1"), wf("Unexpected term"));
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE asc = 1"),
        wf("Unexpected term")
    );
}

#[test]
fn a_keyword_function_name_is_still_a_call() {
    assert!(parse("SELECT * FROM \"t\" WHERE pk = 'a' AND size(s) = 3").is_ok());
}

#[test]
fn is_with_a_name_that_is_not_a_type() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'a' AND x IS FOO"),
        wf("Expected type name")
    );
}

#[test]
fn is_null_and_type_tests_parse_as_filters() {
    match where_of("SELECT * FROM \"t\" WHERE pk = 'a' AND nul IS NULL") {
        WhereExpr::And(_, b) => {
            assert!(matches!(*b, WhereExpr::IsType(_, TypeName::Null, false)));
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        where_of("SELECT * FROM \"t\" WHERE s IS NOT NULL"),
        WhereExpr::IsType(_, TypeName::Null, true)
    ));
    assert!(matches!(
        where_of("SELECT * FROM \"t\" WHERE flag IS BOOL"),
        WhereExpr::IsType(_, TypeName::Boolean, false)
    ));
    assert!(matches!(
        where_of("SELECT * FROM \"t\" WHERE s IS string"),
        WhereExpr::IsType(_, TypeName::String, false)
    ));
    assert!(matches!(
        where_of("SELECT * FROM \"t\" WHERE s IS MISSING"),
        WhereExpr::IsMissing(_, true)
    ));
}

#[test]
fn an_is_filter_on_a_type_with_no_filter_is_refused_at_its_left_side() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'a' AND x IS INT4"),
        "Unsupported data type for IS operator: Integer4Type. Supported data types are \
         [MISSING, NULL, BOOLEAN, BLOB, STRING, NUMERIC, STRUCT, TUPLE, LIST] at 1:38:1"
    );
    // A length after the type is read.
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'a' AND sv IS VARCHAR(10)"),
        "Unsupported data type for IS operator: CharacterVaryingType. Supported data \
         types are [MISSING, NULL, BOOLEAN, BLOB, STRING, NUMERIC, STRUCT, TUPLE, LIST] \
         at 1:38:2"
    );
}

#[test]
fn a_between_that_breaks_off() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE n BETWEEN 1 AND"),
        wf("Expected expression after AND")
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE n BETWEEN 1"),
        wf("Expected AND keyword")
    );
    assert_eq!(err("SELECT * FROM \"t\" WHERE n BETWEEN"), wf(MISSING_RHS));
}

#[test]
fn like_with_nothing_after_it_is_missing_its_right_side() {
    assert_eq!(err("SELECT * FROM \"t\" WHERE s LIKE"), wf(MISSING_RHS));
}

#[test]
fn a_condition_takes_no_arithmetic() {
    for (condition, op) in [
        ("n = -n", "-"),
        ("n = +n", "+"),
        ("pk = 1-2", "-"),
        ("pk = 1+2", "+"),
        ("n % 2 = 1", "%"),
        ("n / 2 = 1", "/"),
        ("s || 'x' = 'y'", "||"),
    ] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            format!("Unsupported operator in Condition Expression. Operator: {op}"),
            "{condition}"
        );
    }
    // A sign before a number is part of the number.
    assert!(parse("SELECT * FROM \"t\" WHERE n = -5").is_ok());
}

#[test]
fn an_operand_with_an_operator_as_the_condition_is_refused_for_the_operator() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE a + 1"),
        "Unsupported operator in Condition Expression. Operator: +"
    );
    // A syntax error after it wins.
    assert_eq!(err("SELECT * FROM \"t\" WHERE a + 1 AND"), wf(MISSING_RHS));
    // So it does for an operand AND or OR takes.
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE a + 1 AND pk = 'a'"),
        "Unsupported operator in Condition Expression. Operator: +"
    );
}

#[test]
fn an_operand_that_is_the_whole_condition_is_refused() {
    for condition in ["a", "1", "'x'", "?", "[1]", "m.b", "true", "a;"] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            NOT_A_CONDITION,
            "{condition}"
        );
    }
    // After what the walk meets in it.
    assert_eq!(
        parse_for_request("SELECT * FROM \"tbl\" WHERE ?", 0)
            .unwrap_err()
            .message,
        PARAMETER_COUNT_MISMATCH
    );
    // A syntax error after it wins.
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE a AND ("),
        wf("Unexpected term")
    );
}

#[test]
fn an_operand_that_and_or_or_takes_is_refused() {
    for condition in [
        "a AND pk = 'a'",
        "pk = 'a' AND a",
        "a OR pk = 'a'",
        "pk = 'a' AND a OR b = 1",
    ] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            AND_OR_ARGUMENTS,
            "{condition}"
        );
    }
    // After what the walk meets in the arguments of its operator, and
    // ahead of what it meets after them.
    let overflow = "Number overflow. Attempting to store a number with magnitude larger \
                    than supported range under root";
    for condition in [
        "a AND b = 1e126",
        "c = 1e126 AND a",
        "a OR b = 1 AND c = 1 AND d = 1e126",
        "b = 1 OR a AND c = 1e126",
    ] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            overflow,
            "{condition}"
        );
    }
    for condition in ["pk = 'a' AND a OR c = 1e126", "b = 1 OR a OR c = 1e126"] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            AND_OR_ARGUMENTS,
            "{condition}"
        );
    }
}

#[test]
fn an_operand_in_a_group_or_after_a_not_is_refused_for_its_place() {
    for (condition, want) in [
        ("(a)", NOT_A_CONDITION),
        ("((a))", NOT_A_CONDITION),
        ("pk = 'a' AND (a)", AND_OR_ARGUMENTS),
        ("(a) OR pk = 'a'", AND_OR_ARGUMENTS),
        ("(a AND b = 1)", AND_OR_ARGUMENTS),
        ("(b = 1 AND a)", AND_OR_ARGUMENTS),
        ("((a) AND b = 1)", AND_OR_ARGUMENTS),
        ("NOT a", NOT_ARGUMENT),
        ("NOT (a)", NOT_ARGUMENT),
        ("NOT ((a))", NOT_ARGUMENT),
        ("(NOT a)", NOT_ARGUMENT),
        ("NOT (a) AND pk = 'a'", NOT_ARGUMENT),
        // The NOT's refusal is met before the AND's, which comes after both
        // of its arguments.
        ("a AND NOT b", NOT_ARGUMENT),
    ] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            want,
            "{condition}"
        );
    }
    // Before anything else the operand keeps the syntax error.
    for condition in ["a b", "a LIMIT 1", "(a b)"] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            wf(NO_COMPARISON),
            "{condition}"
        );
    }
}

#[test]
fn an_operand_alone_in_parentheses_is_that_operand() {
    for condition in [
        "(pk) = 'a'",
        "((pk)) = (('a'))",
        "pk = ('a')",
        "pk = (?)",
        "(a) BETWEEN 0 AND 2",
        "(a) IN [1]",
        "(a) NOT IN [2]",
        "(m) IS NOT MISSING",
        "NOT (a) = 2",
        "(a) = 1 = true",
        "begins_with((sk), 'b')",
        "attribute_exists((a))",
        "(m).k = 1",
        "(m)['k'] = 1",
        "a = -(-1)",
    ] {
        let sql = format!("SELECT * FROM \"t\" WHERE {condition}");
        assert!(parse(&sql).is_ok(), "{condition}: {:?}", parse(&sql));
    }
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE ((pk)) = ('a')"),
        where_of("SELECT * FROM \"t\" WHERE pk = 'a'")
    );
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE (m)['k'] = -(1)"),
        where_of("SELECT * FROM \"t\" WHERE m.k = -1")
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE (a) IS INT4"),
        "Unsupported data type for IS operator: Integer4Type. Supported data types are \
         [MISSING, NULL, BOOLEAN, BLOB, STRING, NUMERIC, STRUCT, TUPLE, LIST] at 1:26:1"
    );
    for (condition, want) in [
        ("a = (1", wf("Expected RIGHT_PAREN")),
        ("a = (1))", wf("Unexpected token after expression")),
        ("a = ()", wf("Unexpected term")),
        (
            "(a + 1) = 2",
            "Unsupported operator in Condition Expression. Operator: +".to_owned(),
        ),
    ] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            want,
            "{condition}"
        );
    }
}

#[test]
fn a_group_left_open_is_missing_its_right_parenthesis() {
    for condition in ["(a = 1 b)", "(a = 1", "((a = 1)"] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            wf("Expected RIGHT_PAREN"),
            "{condition}"
        );
    }
}

#[test]
fn a_call_left_open_is_missing_its_right_parenthesis() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE attribute_exists(a"),
        wf("Expected RIGHT_PAREN")
    );
}

#[test]
fn an_is_test_on_a_parameter_is_refused() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'a' AND ? IS NULL"),
        "The left hand side argument to IS must be a Variable Reference, Literal, or Path"
    );
    assert!(parse("SELECT * FROM \"t\" WHERE pk = 'a' AND 'a' IS STRING").is_ok());
}

#[test]
fn an_is_test_as_a_set_value_is_refused() {
    assert_eq!(
        err("UPDATE \"t\" SET a = b IS NULL WHERE pk = 'a'"),
        "IS operator is not supported in update expressions"
    );
}
